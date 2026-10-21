"""
app/workers/outbox_processor.py
================================
Async outbox relay worker — polls PENDING outbox records and delivers them
to the notification service with exponential back-off + full jitter.
"""

from __future__ import annotations

import asyncio
import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import httpx
import structlog
from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import AsyncSessionLocal
from app.models import OutboxEvent, OutboxEventStatus, OutboxEventType
from app.services.cancellation import process_workshop_cancel_fanout

if TYPE_CHECKING:
    pass

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Module-level last-tick timestamp (used by /health)
# ---------------------------------------------------------------------------
last_tick_at: datetime | None = None


# ---------------------------------------------------------------------------
# Back-off with full jitter
# ---------------------------------------------------------------------------

def _base_delay(attempts: int) -> float:
    """
    Pure exponential growth capped at relay_backoff_cap_s.
    delay = min(initial × multiplier^(max(0, attempts-1)), cap)
    """
    effective_attempts = max(0, attempts - 1)
    raw = settings.relay_backoff_initial_s * (
        settings.relay_backoff_multiplier ** effective_attempts
    )
    return min(raw, settings.relay_backoff_cap_s)


def backoff_with_jitter(attempts: int) -> float:
    """
    Full-jitter back-off: uniform random in [0, base_delay].
    """
    return random.uniform(0.0, _base_delay(attempts))


# ---------------------------------------------------------------------------
# Deliver one outbox event
# ---------------------------------------------------------------------------

async def _deliver(event_id: uuid.UUID, snapshot: dict) -> None:
    """
    Attempt delivery of a single outbox event and update its status.
    Handles WORKSHOP_CANCEL_REQUESTED fan-out internally, and HTTP notification
    delivery for attendee notifications.
    """
    # ── Phase 2 Async Fan-out: Workshop Cancellation ────────────────────────
    if snapshot["event_type"] == OutboxEventType.WORKSHOP_CANCEL_REQUESTED.value:
        try:
            logger.info("outbox.processing_fanout", outbox_id=str(event_id))
            await process_workshop_cancel_fanout(event_id, snapshot["payload"])
        except Exception as exc:
            logger.error("outbox.fanout_failed", outbox_id=str(event_id), error=str(exc))
        return

    url = settings.notification_service_url.rstrip("/") + "/api/v1/notify"
    success = False
    exception_name: str | None = None
    http_status_code: int | None = None

    log = logger.bind(
        outbox_id=str(event_id),
        event_type=snapshot["event_type"],
        attempt=snapshot["attempts"],
    )

    # Ensure idempotency_key is strictly a string for HTTP headers
    idempotency_key_str = str(snapshot["idempotency_key"])

    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(settings.notification_timeout_s)
        ) as client:
            response = await client.post(
                url,
                json=snapshot["payload"],
                headers={
                    "Idempotency-Key": idempotency_key_str,
                    "X-Event-Type": str(snapshot["event_type"]),
                    "Content-Type": "application/json",
                },
            )
        http_status_code = response.status_code
        success = response.is_success
        if success:
            log.info("outbox.delivered", http_status=http_status_code)
        else:
            log.warning("outbox.delivery_failed", http_status=http_status_code)

    except httpx.TimeoutException as exc:
        exception_name = type(exc).__name__
        log.warning("outbox.delivery_timeout", exc=str(exc))
    except httpx.RequestError as exc:
        exception_name = type(exc).__name__
        log.warning("outbox.delivery_error", exc=str(exc))
    except Exception as exc:  # pragma: no cover
        exception_name = type(exc).__name__
        log.error("outbox.delivery_unexpected", exc=str(exc))

    # ── Update the outbox row based on outcome ────────────────────────────
    async with AsyncSessionLocal() as session:
        async with session.begin():
            ev = await session.get(
                OutboxEvent, event_id, with_for_update=True
            )
            if ev is None:
                log.warning("outbox.row_missing_on_update")
                return

            if success:
                ev.status = OutboxEventStatus.DELIVERED
                ev.last_attempted_at = datetime.now(timezone.utc)

            elif ev.attempts >= settings.max_notification_retries:
                ev.status = OutboxEventStatus.FAILED
                ev.last_attempted_at = datetime.now(timezone.utc)
                log.error(
                    "outbox.failed",
                    attempts=ev.attempts,
                    max_retries=settings.max_notification_retries,
                    http_status=http_status_code,
                    exception=exception_name,
                )

            else:
                jitter_s = backoff_with_jitter(ev.attempts)
                now = datetime.now(timezone.utc)
                ev.status = OutboxEventStatus.PENDING
                ev.last_attempted_at = now
                # Persist next_attempt_at with jitter to prevent thundering herd & HoL blocking
                ev.next_attempt_at = now + timedelta(seconds=jitter_s)
                log.info(
                    "outbox.scheduled_retry",
                    attempts=ev.attempts,
                    jitter_s=round(jitter_s, 2),
                    next_eligible_in_s=round(jitter_s, 2),
                )


# ---------------------------------------------------------------------------
# Claim helpers
# ---------------------------------------------------------------------------

async def _claim_batch(session: AsyncSession) -> list[OutboxEvent]:
    """
    Claim up to relay_batch_size PENDING outbox events (or stranded PROCESSING events)
    eligible for delivery, and mark them PROCESSING in the same transaction.
    Uses FOR UPDATE SKIP LOCKED.

    Eligibility:
    - PENDING records where next_attempt_at <= now
    - PROCESSING records stranded beyond (2 * timeout + 15s)
    Ordered by next_attempt_at ASC to completely prevent Head-of-Line blocking.
    """
    now = datetime.now(timezone.utc)
    stale_cutoff = now - timedelta(seconds=(settings.notification_timeout_s * 2 + 15))

    query = (
        select(OutboxEvent)
        .where(
            or_(
                and_(
                    OutboxEvent.status == OutboxEventStatus.PENDING,
                    OutboxEvent.next_attempt_at <= now,
                ),
                and_(
                    OutboxEvent.status == OutboxEventStatus.PROCESSING,
                    OutboxEvent.last_attempted_at <= stale_cutoff,
                ),
            )
        )
        .order_by(OutboxEvent.next_attempt_at.asc())
        .limit(settings.relay_batch_size)
        .with_for_update(skip_locked=True)
    )

    result = await session.execute(query)
    eligible_events = list(result.scalars().all())

    for ev in eligible_events:
        if ev.status == OutboxEventStatus.PENDING:
            ev.attempts += 1

        ev.status = OutboxEventStatus.PROCESSING
        ev.last_attempted_at = now

    return eligible_events


# ---------------------------------------------------------------------------
# Single relay tick
# ---------------------------------------------------------------------------

async def relay_tick() -> int:
    """
    One iteration of the outbox relay loop.
    """
    global last_tick_at
    last_tick_at = datetime.now(timezone.utc)

    async with AsyncSessionLocal() as session:
        async with session.begin():
            events = await _claim_batch(session)

    if not events:
        return 0

    snapshots = [
        {
            "id": ev.id,
            "event_type": ev.event_type.value if hasattr(ev.event_type, "value") else str(ev.event_type),
            "payload": ev.payload,
            "idempotency_key": str(ev.idempotency_key),
            "attempts": ev.attempts,
        }
        for ev in events
    ]

    logger.info(
        "relay_tick.dispatching",
        count=len(snapshots),
        ids=[str(s["id"]) for s in snapshots],
    )

    results = await asyncio.gather(
        *[_deliver(s["id"], s) for s in snapshots],
        return_exceptions=True,
    )

    for snap, result in zip(snapshots, results):
        if isinstance(result, Exception):
            logger.error(
                "relay_tick.deliver_raised",
                outbox_id=str(snap["id"]),
                exc=str(result),
            )

    return len(snapshots)


# ---------------------------------------------------------------------------
# Processor lifecycle
# ---------------------------------------------------------------------------

class OutboxProcessor:
    """
    Thin wrapper owning the APScheduler instance.
    """

    def __init__(self) -> None:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler

        self._scheduler = AsyncIOScheduler()
        self._scheduler.add_job(
            relay_tick,
            trigger="interval",
            seconds=settings.relay_poll_interval_s,
            id="outbox_relay",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=settings.relay_poll_interval_s,
        )

    async def start(self) -> None:
        """Start the background scheduler."""
        self._scheduler.start()
        logger.info(
            "outbox_processor.started",
            poll_interval_s=settings.relay_poll_interval_s,
            batch_size=settings.relay_batch_size,
            max_retries=settings.max_notification_retries,
            backoff_initial_s=settings.relay_backoff_initial_s,
            backoff_multiplier=settings.relay_backoff_multiplier,
            backoff_cap_s=settings.relay_backoff_cap_s,
        )

    async def shutdown(self) -> None:
        """Gracefully stop the scheduler."""
        self._scheduler.shutdown(wait=True)
        logger.info("outbox_processor.stopped")