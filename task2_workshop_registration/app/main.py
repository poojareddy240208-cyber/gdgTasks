"""
app/main.py
-----------
FastAPI application factory — the single entry-point that unites:

  1. Database initialisation  — verifies connectivity and runs Alembic
                                 migrations to head on every startup so
                                 the schema is always up to date without
                                 a separate migration step in CI/CD.

  2. Workshop API router      — all /api/v1/* endpoints (workshops +
                                 registrations) mounted under the versioned
                                 prefix required by REQ-API-02.

  3. Outbox worker lifecycle  — OutboxProcessor (APScheduler) starts on
                                 application startup and shuts down
                                 gracefully on SIGTERM, satisfying
                                 REQ-NFR-05 (no partially-processed records
                                 are left behind).

Cross-cutting concerns
-----------------------
  * X-Request-ID middleware   — echo or generate a UUID v4 request ID on
                                 every response (REQ-API-04).
  * AppError handler          — converts every domain exception to the
                                 uniform JSON envelope
                                 {"error": {"code": ..., "message": ...}}
                                 (REQ-API-01).
  * Structured startup logging — every lifecycle event (DB ready, worker
                                 started, etc.) is emitted as a JSON log
                                 line via structlog (REQ-OBS-01).
  * GET /health               — DB ping + relay-worker heartbeat timestamp
                                 + pending outbox count (REQ-OBS-02).
  * GET /metrics              — Prometheus text-format counters / gauges /
                                 histograms (REQ-OBS-03).

Environment variables (see app/config.py for full reference)
-------------------------------------------------------------
  DATABASE_URL                postgresql+asyncpg://...
  NOTIFICATION_SERVICE_URL    http://mock_service:8001
  RELAY_POLL_INTERVAL_S       5
  MAX_NOTIFICATION_RETRIES    10
  ... (all Settings fields)
"""

from __future__ import annotations

import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import structlog
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from app.config import settings
from app.database import AsyncSessionLocal, engine
from app.exceptions import AppError
from app.models import Base  # ensures all ORM models are registered
from app.workers.outbox_processor import OutboxProcessor

# ---------------------------------------------------------------------------
# Structured logging — configure once at module level
# ---------------------------------------------------------------------------
structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(20),  # INFO and above
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
)

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Prometheus metrics (REQ-OBS-03)
# ---------------------------------------------------------------------------
REGISTRATIONS_TOTAL = Counter(
    "registrations_total",
    "Total workshop registrations attempted",
    ["status"],          # created | rejected_full | rejected_duplicate | rejected_other
)
CANCELLATIONS_TOTAL = Counter(
    "cancellations_total",
    "Total registration/workshop cancellations",
    ["type"],            # single | bulk
)
OUTBOX_PENDING = Gauge(
    "outbox_pending_count",
    "Current number of PENDING outbox records",
)
OUTBOX_DELIVERED_TOTAL = Counter(
    "outbox_delivered_total",
    "Total outbox events successfully delivered",
    ["event_type"],
)
OUTBOX_FAILED_TOTAL = Counter(
    "outbox_failed_total",
    "Total outbox events that exhausted all retries",
    ["event_type"],
)
NOTIFICATION_DURATION = Histogram(
    "notification_delivery_duration_seconds",
    "HTTP delivery round-trip latency to the notification service",
    ["event_type"],
    buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0],
)
HTTP_REQUEST_DURATION = Histogram(
    "http_request_duration_seconds",
    "API endpoint latency",
    ["method", "path", "status"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)

# ---------------------------------------------------------------------------
# Processor singleton — one instance for the whole process lifetime
# ---------------------------------------------------------------------------
_processor = OutboxProcessor()


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

async def _probe_db() -> bool:
    """
    Ping the database with a lightweight SELECT 1.
    Returns True on success, False on any error.
    """
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


async def _run_migrations() -> None:
    """
    Run Alembic migrations to 'head' synchronously on startup so the schema
    is always up-to-date without a separate migration step.

    Falls back gracefully if Alembic is not configured (e.g. during tests
    that use create_all instead).
    """
    try:
        from alembic import command
        from alembic.config import Config

        alembic_cfg = Config("alembic.ini")
        # Run in a thread so blocking file I/O doesn't stall the event loop.
        import asyncio
        await asyncio.get_event_loop().run_in_executor(
            None, command.upgrade, alembic_cfg, "head"
        )
        logger.info("db.migrations_applied", target="head")
    except FileNotFoundError:
        # alembic.ini not present — assume tables already exist (test mode).
        logger.warning("db.migrations_skipped", reason="alembic.ini not found")
    except Exception as exc:
        logger.error("db.migrations_failed", error=str(exc))
        raise


async def _get_pending_outbox_count() -> int:
    """Return the current count of PENDING outbox records (for /health + metrics)."""
    try:
        from sqlalchemy import func, select
        from app.models import OutboxEvent, OutboxEventStatus

        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(func.count()).where(
                    OutboxEvent.__table__.c.status == OutboxEventStatus.PENDING.value
                )
            )
            return result.scalar_one()
    except Exception:
        return -1


# ---------------------------------------------------------------------------
# Lifespan — startup + shutdown sequence
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Ordered startup sequence
    -------------------------
    1. Wait for database connectivity (retry up to 10 s).
    2. Run Alembic migrations to head.
    3. Start the outbox relay worker (APScheduler).
    4. Log "service ready".
    Yield — application serves requests.

    Ordered shutdown sequence
    --------------------------
    1. Stop the outbox relay worker (waits for current tick to finish).
    2. Dispose the SQLAlchemy connection pool.
    3. Log "service stopped".
    """
    # ── Startup ──────────────────────────────────────────────────────────
    logger.info(
        "service.starting",
        database_url=settings.database_url.split("@")[-1],  # hide credentials
        notification_url=settings.notification_service_url,
        relay_poll_interval_s=settings.relay_poll_interval_s,
        max_notification_retries=settings.max_notification_retries,
    )

    # 1. DB connectivity probe with simple retry loop
    db_ready = False
    for attempt in range(1, 11):
        db_ready = await _probe_db()
        if db_ready:
            logger.info("db.connected", attempt=attempt)
            break
        logger.warning("db.not_ready", attempt=attempt, retrying_in_s=1)
        import asyncio
        await asyncio.sleep(1)

    if not db_ready:
        logger.error("db.unreachable", max_attempts=10)
        raise RuntimeError(
            "Cannot connect to the database after 10 attempts. "
            "Check DATABASE_URL and that PostgreSQL is running."
        )

    # 2. Alembic migrations
    await _run_migrations()

    # 3. Outbox relay worker
    await _processor.start()

    logger.info("service.ready")

    # ── Serve requests ────────────────────────────────────────────────────
    yield

    # ── Shutdown ──────────────────────────────────────────────────────────
    logger.info("service.stopping")

    # 1. Stop relay worker — waits for current tick to finish (REQ-NFR-05)
    await _processor.shutdown()

    # 2. Dispose DB pool
    await engine.dispose()

    logger.info("service.stopped")


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Workshop Registration & Notifications Engine",
    description=(
        "GDG Workshop Registration backend: seat management, "
        "transactional outbox, and fault-resilient notification delivery."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# X-Request-ID middleware  (REQ-API-04)
# ---------------------------------------------------------------------------
@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    """
    Echo the caller's X-Request-ID header on every response, or generate a
    UUID v4 if none was provided.  Also records per-endpoint latency in the
    http_request_duration_seconds Prometheus histogram.
    """
    request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(request_id=request_id)

    start = time.perf_counter()
    response = await call_next(request)
    elapsed = time.perf_counter() - start

    response.headers["X-Request-ID"] = request_id

    # Record latency in Prometheus (skip /health and /metrics to avoid noise)
    if request.url.path not in ("/health", "/metrics"):
        HTTP_REQUEST_DURATION.labels(
            method=request.method,
            path=request.url.path,
            status=str(response.status_code),
        ).observe(elapsed)

    return response


# ---------------------------------------------------------------------------
# Domain exception → uniform JSON envelope  (REQ-API-01)
# ---------------------------------------------------------------------------
@app.exception_handler(AppError)
async def app_error_handler(request: Request, exc: AppError) -> JSONResponse:
    """
    Convert any AppError subclass (WorkshopFullError, AlreadyRegisteredError,
    etc.) to the standard error envelope so clients always get a consistent
    shape regardless of which domain rule was violated.
    """
    logger.warning(
        "domain_error",
        code=exc.error_code,
        message=exc.message,
        path=str(request.url.path),
    )
    return JSONResponse(
        status_code=exc.http_status,
        content=exc.to_dict(),
    )


# ---------------------------------------------------------------------------
# Workshop & registration routes  (REQ-API-02)
# ---------------------------------------------------------------------------
from app.api.workshops import router as workshops_router  # noqa: E402

app.include_router(workshops_router, prefix="/api/v1")


# ---------------------------------------------------------------------------
# Health probe  (REQ-OBS-02)
# ---------------------------------------------------------------------------
@app.get(
    "/health",
    tags=["ops"],
    summary="Liveness + readiness probe",
    response_description=(
        "Returns 200 when the service is healthy. "
        "Returns 503 when the database is unreachable."
    ),
)
async def health():
    """
    Checks:
      - database connectivity (SELECT 1)
      - relay worker heartbeat timestamp
      - count of PENDING outbox records

    Used as the Docker healthcheck and by load-balancer readiness probes.
    """
    from app.workers.outbox_processor import last_tick_at

    db_ok = await _probe_db()
    pending_count = await _get_pending_outbox_count()

    relay_status = "running" if last_tick_at is not None else "starting"

    status_code = 200 if db_ok else 503
    body = {
        "status": "ok" if db_ok else "degraded",
        "db": "connected" if db_ok else "error",
        "relay_worker": {
            "status": relay_status,
            "last_tick_at": last_tick_at.isoformat() if last_tick_at else None,
            "pending_outbox_count": pending_count,
        },
    }

    return JSONResponse(content=body, status_code=status_code)


# ---------------------------------------------------------------------------
# Prometheus metrics  (REQ-OBS-03)
# ---------------------------------------------------------------------------
@app.get(
    "/metrics",
    tags=["ops"],
    summary="Prometheus metrics",
    response_class=PlainTextResponse,
)
async def metrics():
    """
    Expose all registered Prometheus metrics in text exposition format.

    Metrics exported
    ----------------
    registrations_total{status}              Counter
    cancellations_total{type}                Counter
    outbox_pending_count                     Gauge  (refreshed here)
    outbox_delivered_total{event_type}       Counter
    outbox_failed_total{event_type}          Counter
    notification_delivery_duration_seconds   Histogram
    http_request_duration_seconds            Histogram
    """
    # Refresh the live gauge before scraping
    pending = await _get_pending_outbox_count()
    if pending >= 0:
        OUTBOX_PENDING.set(pending)

    return PlainTextResponse(
        content=generate_latest().decode("utf-8"),
        media_type=CONTENT_TYPE_LATEST,
    )
