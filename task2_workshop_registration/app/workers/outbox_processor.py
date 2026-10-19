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
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import AsyncSessionLocal
from app.models import OutboxEvent, OutboxEventStatus

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
    Attempt HTTP delivery of a single outbox event and update its status.
    """
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

            elif ev.attempts >= settings.max_notification_retries:
                ev.status = OutboxEventStatus.FAILED
                log.error(
                    "outbox.failed",
                    attempts=ev.attempts,
                    max_retries=settings.max_notification_retries,
                    http_status=http_status_code,
                    exception=exception_name,
                )

            else:
                jitter_s = backoff_with_jitter(ev.attempts)
                ev.status = OutboxEventStatus.PENDING
                ev.last_attempted_at = datetime.now(timezone.utc)
                log.info(
                    "outbox.scheduled_retry",
                    attempts=ev.attempts,
                    jitter_s=round(jitter_s, 2),
                    next_eligible_in_s=round(jitter_s, 2),
                )


# ---------------------------------------------------------------------------
# Claim helpers
# ---------------------------------------------------------------------------

def _next_attempt_eligible(ev: OutboxEvent, now: datetime) -> bool:
    """
    Check if an event's backoff period has passed.
    """
    if ev.last_attempted_at is None:
        return True

    required_delay_s = _base_delay(ev.attempts - 1)

    last_attempt = ev.last_attempted_at
    if last_attempt.tzinfo is None:
        last_attempt = last_attempt.replace(tzinfo=timezone.utc)

    return now >= (last_attempt + timedelta(seconds=required_delay_s))


async def _claim_batch(session: AsyncSession) -> list[OutboxEvent]:
    """
    Claim up to relay_batch_size PENDING outbox events (or stranded PROCESSING events)
    eligible for delivery, and mark them PROCESSING in the same transaction.
    Uses FOR UPDATE SKIP LOCKED.
    """
    now = datetime.now(timezone.utc)

    # Reclaim events stuck in PROCESSING (e.g. stranded mid-flight post-SIGKILL)
    stale_processing_threshold = now - timedelta(seconds=2)

    result = await session.execute(
        select(OutboxEvent)
        .where(
            or_(
                OutboxEvent.status == OutboxEventStatus.PENDING,
                OutboxEvent.status == OutboxEventStatus.PROCESSING,
            )
        )
        .order_by(OutboxEvent.created_at)
        .limit(settings.relay_batch_size * 2)
        .with_for_update(skip_locked=True)
    )
    candidates = list(result.scalars().all())

    eligible_events: list[OutboxEvent] = []
    for ev in candidates:
        if len(eligible_events) >= settings.relay_batch_size:
            break

        is_stale_processing = (
            ev.status == OutboxEventStatus.PROCESSING and
            (ev.last_attempted_at is None or ev.last_attempted_at <= stale_processing_threshold)
        )

        if is_stale_processing or _next_attempt_eligible(ev, now):
            # Only increment attempts if it was previously in PENDING state
            if ev.status == OutboxEventStatus.PENDING:
                ev.attempts += 1

            ev.status = OutboxEventStatus.PROCESSING
            ev.last_attempted_at = now
            eligible_events.append(ev)

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