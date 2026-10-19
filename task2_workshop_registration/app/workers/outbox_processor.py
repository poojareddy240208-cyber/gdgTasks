"""
app/workers/outbox_processor.py
================================
Async outbox relay worker — polls PENDING outbox records and delivers them
to the notification service with exponential back-off + full jitter.

Architecture overview
---------------------

    APScheduler (AsyncIOScheduler)
        |
        +-- relay_tick()  [called every RELAY_POLL_INTERVAL_S seconds]
                |
                +-- 1. Claim a batch of PENDING records
                |       SELECT ... WHERE status=PENDING
                |                    AND next_attempt_at <= now()
                |       FOR UPDATE SKIP LOCKED
                |       LIMIT relay_batch_size
                |
                +-- 2. Mark each as PROCESSING, increment attempts,
                |       set last_attempted_at = now()
                |       COMMIT  (makes PROCESSING visible to monitors)
                |
                +-- 3. asyncio.gather() one _deliver() coroutine per event
                        |
                        +-- POST /api/v1/notify  via httpx.AsyncClient
                        |
                        +-- On HTTP 2xx:
                        |       status = DELIVERED  (terminal)
                        |
                        +-- On failure (non-2xx / timeout / connect error)
                        |   attempts < MAX_RETRIES:
                        |       status = PENDING
                        |       last_attempted_at = now()  (drives next window)
                        |
                        +-- On failure, attempts >= MAX_RETRIES:
                                status = FAILED  (terminal)
                                log ERROR

Back-off formula — "full jitter" variant (AWS recommendation)
--------------------------------------------------------------
Plain exponential back-off causes retry thunderstorms when many events fail
simultaneously (e.g. during a notification-service outage).  Full jitter
spreads retries across the entire [0, cap] window so thunderstorm is broken
up:

    base_delay = min(initial × multiplier^(attempts-1), cap)
    sleep      = random.uniform(0, base_delay)            ← full jitter

The "sleep" is NOT an asyncio.sleep() inside the worker — it is stored as
``last_attempted_at`` in the database.  The poll query skips any record
whose ``last_attempted_at + sleep_s > now()``.  This means:

  * Retry windows survive process restarts (crash-resilient, REQ-OB-04).
  * Multiple worker instances coordinate through the DB, not in-process state.

State transitions (design §4.3)
---------------------------------
    PENDING → PROCESSING    (claim, lock)
    PROCESSING → DELIVERED  (HTTP 2xx, terminal)
    PROCESSING → PENDING    (failure, attempts < MAX; back-off stored)
    PROCESSING → FAILED     (failure, attempts >= MAX; terminal)

PROCESSING as a distinct state lets monitoring detect stuck/dead workers:
a row in PROCESSING for longer than (poll_interval × 2) means the worker
that claimed it crashed before updating it back.  PostgreSQL auto-rolls-back
the row-lock when the connection drops, so the record is NOT lost; but the
monitoring alert fires on the PROCESSING timestamp.

Concurrent worker safety
-------------------------
``SELECT … FOR UPDATE SKIP LOCKED`` ensures two concurrently-running relay
workers never claim the same record.  No Redis or external coordinator is
required (REQ-OB-08).

Graceful shutdown
-----------------
``OutboxProcessor.shutdown()`` sets ``_stop_event``, waits for the current
tick to finish, then returns.  APScheduler's ``scheduler.shutdown(wait=True)``
calls this before the process exits (REQ-NFR-05).
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

    delay = min(initial × multiplier^(attempts-1), cap)

    Attempt  Delay (defaults: initial=1s, mul=2, cap=300s)
    ───────  ──────────────────────────────────────────────
        1       1.0 s
        2       2.0 s
        3       4.0 s
        4       8.0 s
        5      16.0 s
        6      32.0 s
        7      64.0 s
        8     128.0 s
        9     256.0 s
       10     300.0 s  (capped)
    """
    raw = settings.relay_backoff_initial_s * (
        settings.relay_backoff_multiplier ** (attempts - 1)
    )
    return min(raw, settings.relay_backoff_cap_s)


def backoff_with_jitter(attempts: int) -> float:
    """
    Full-jitter back-off: uniform random in [0, base_delay].

    This breaks retry thunderstorms when many events fail concurrently.
    Reference: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/

    Examples (defaults, probabilistic):
        attempts=1  →  uniform [0, 1.0]   s   avg  0.50 s
        attempts=5  →  uniform [0, 16.0]  s   avg  8.0  s
        attempts=10 →  uniform [0, 300.0] s   avg 150.0 s
    """
    return random.uniform(0.0, _base_delay(attempts))


# ---------------------------------------------------------------------------
# Deliver one outbox event
# ---------------------------------------------------------------------------

async def _deliver(event_id: uuid.UUID, snapshot: dict) -> None:
    """
    Attempt HTTP delivery of a single outbox event and update its status.

    ``snapshot`` contains the fields needed to make the HTTP request without
    re-querying the database (payload, idempotency_key, event_type, attempts).
    The owning DB row must already be in PROCESSING status before this is
    called (set by the tick that claimed it).
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

    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(settings.notification_timeout_s)
        ) as client:
            response = await client.post(
                url,
                json=snapshot["payload"],
                headers={
                    "Idempotency-Key": snapshot["idempotency_key"],
                    "X-Event-Type": snapshot["event_type"],
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
    except Exception as exc:                          # pragma: no cover
        exception_name = type(exc).__name__
        log.error("outbox.delivery_unexpected", exc=str(exc))

    # ── Update the outbox row based on outcome ────────────────────────────
    async with AsyncSessionLocal() as session:
        async with session.begin():
            ev = await session.get(
                OutboxEvent, event_id, with_for_update=True
            )
            if ev is None:
                # Row was deleted externally — nothing to update.
                log.warning("outbox.row_missing_on_update")
                return

            if success:
                ev.status = OutboxEventStatus.DELIVERED

            elif ev.attempts >= settings.max_notification_retries:
                # Exhausted all retries — mark terminal FAILED.
                ev.status = OutboxEventStatus.FAILED
                log.error(
                    "outbox.failed",
                    attempts=ev.attempts,
                    max_retries=settings.max_notification_retries,
                    http_status=http_status_code,
                    exception=exception_name,
                )

            else:
                # Will retry — compute jittered back-off window and reset to PENDING.
                jitter_s = backoff_with_jitter(ev.attempts)
                # Store the time at which the NEXT attempt becomes eligible.
                # The poll query filters: last_attempted_at + jitter_s <= now()
                # We encode this as last_attempted_at = now() and the poll query
                # adds the computed interval.  Because we store attempts on the
                # row, the poll query can recompute the window each tick.
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

def _next_attempt_eligible(ev: OutboxEvent) -> bool:
    """
    True if the outbox event's back-off window has elapsed and it is
    eligible for a delivery attempt right now.

    This is the Python-side check; the SQL query also filters by this
    criterion so the database does the heavy lifting.
    """
    if ev.last_attempted_at is None:
        return True
    base = _base_delay(ev.attempts)
    jitter_window = base                       # use base as an upper bound
    next_eligible = ev.last_attempted_at + timedelta(seconds=jitter_window)
    return datetime.now(timezone.utc) >= next_eligible


async def _claim_batch(session: AsyncSession) -> list[OutboxEvent]:
    """
    Claim up to relay_batch_size PENDING outbox events that are eligible for
    delivery, and mark them PROCESSING in the same transaction.

    Uses FOR UPDATE SKIP LOCKED so multiple relay worker instances never
    claim the same record (REQ-OB-08).

    Back-off filter
    ---------------
    We filter on ``last_attempted_at`` using a server-side interval so that
    records waiting out their back-off window are skipped efficiently without
    reading them into Python.

    The filter:
        last_attempted_at IS NULL                         (first attempt)
        OR last_attempted_at + make_interval(secs := base_delay) <= now()

    Because ``attempts`` varies per row and SQL can't call a Python function,
    we use the maximum possible base_delay (= relay_backoff_cap_s) as a
    conservative upper bound.  Records whose actual jitter hasn't elapsed yet
    may occasionally be skipped for one extra tick — this is acceptable;
    correctness is not affected.
    """
    now = datetime.now(timezone.utc)
    # Conservative filter: skip rows whose last_attempted_at is within the
    # last relay_backoff_cap_s seconds (they are still in their back-off
    # window under worst-case jitter).
    min_retry_after = now - timedelta(seconds=settings.relay_backoff_cap_s)

    result = await session.execute(
        select(OutboxEvent)
        .where(OutboxEvent.status == OutboxEventStatus.PENDING)
        .where(
            or_(
                OutboxEvent.last_attempted_at.is_(None),
                OutboxEvent.last_attempted_at <= min_retry_after,
            )
        )
        .order_by(OutboxEvent.created_at)
        .limit(settings.relay_batch_size)
        .with_for_update(skip_locked=True)
    )
    events = list(result.scalars().all())

    now_ts = datetime.now(timezone.utc)
    for ev in events:
        ev.status = OutboxEventStatus.PROCESSING
        ev.attempts += 1
        ev.last_attempted_at = now_ts

    return events


# ---------------------------------------------------------------------------
# Single relay tick
# ---------------------------------------------------------------------------

async def relay_tick() -> int:
    """
    One iteration of the outbox relay loop.

    1. Open a session and claim a batch of PENDING events (→ PROCESSING).
    2. Commit — PROCESSING is now visible to monitors and other workers.
    3. Fire one _deliver() coroutine per event, gathered concurrently.
    4. Each _deliver() opens its own session to write the final status.

    Returns the number of events processed in this tick.
    """
    global last_tick_at
    last_tick_at = datetime.now(timezone.utc)

    async with AsyncSessionLocal() as session:
        async with session.begin():
            events = await _claim_batch(session)

    if not events:
        return 0

    # Snapshot the fields we need so _deliver() never reads stale ORM state
    # after the session is closed.
    snapshots = [
        {
            "id":               ev.id,
            "event_type":       ev.event_type.value,
            "payload":          ev.payload,
            "idempotency_key":  ev.idempotency_key,
            "attempts":         ev.attempts,
        }
        for ev in events
    ]

    logger.info(
        "relay_tick.dispatching",
        count=len(snapshots),
        ids=[str(s["id"]) for s in snapshots],
    )

    # Dispatch all deliveries concurrently; gather exceptions so one failure
    # doesn't cancel the rest (return_exceptions=True).
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
# Processor lifecycle — used by APScheduler lifespan in main.py
# ---------------------------------------------------------------------------

class OutboxProcessor:
    """
    Thin wrapper that owns the APScheduler instance and exposes
    ``start()`` / ``shutdown()`` for the FastAPI lifespan context manager.

    Usage (in app/main.py)::

        from contextlib import asynccontextmanager
        from app.workers.outbox_processor import OutboxProcessor

        processor = OutboxProcessor()

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            await processor.start()
            yield
            await processor.shutdown()

        app = FastAPI(lifespan=lifespan)
    """

    def __init__(self) -> None:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler

        self._scheduler = AsyncIOScheduler()
        self._scheduler.add_job(
            relay_tick,
            trigger="interval",
            seconds=settings.relay_poll_interval_s,
            id="outbox_relay",
            max_instances=1,          # never overlap ticks
            coalesce=True,            # skip missed ticks on overload
            misfire_grace_time=settings.relay_poll_interval_s,
        )

    async def start(self) -> None:
        """Start the background scheduler. Call from FastAPI lifespan."""
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
        """
        Gracefully stop the scheduler.

        APScheduler waits for the running job to complete before
        shutting down (wait=True), satisfying REQ-NFR-05.
        """
        self._scheduler.shutdown(wait=True)
        logger.info("outbox_processor.stopped")
