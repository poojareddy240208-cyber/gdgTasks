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

import asyncio
import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import structlog
from fastapi import FastAPI, HTTPException, Request
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
    ["status"],
)
CANCELLATIONS_TOTAL = Counter(
    "cancellations_total",
    "Total registration/workshop cancellations",
    ["type"],
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
    """Ping the database with a lightweight SELECT 1."""
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


async def _get_pending_outbox_count() -> int:
    """Return the current count of PENDING outbox records."""
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
    Ordered startup & shutdown sequence.
    """
    # ── Startup ──────────────────────────────────────────────────────────
    logger.info(
        "service.starting",
        database_url=settings.database_url.split("@")[-1],
        notification_url=settings.notification_service_url,
        relay_poll_interval_s=settings.relay_poll_interval_s,
        max_notification_retries=settings.max_notification_retries,
    )

    # 1. DB connectivity probe
    db_ready = False
    for attempt in range(1, 11):
        db_ready = await _probe_db()
        if db_ready:
            logger.info("db.connected", attempt=attempt)
            break
        logger.warning("db.not_ready", attempt=attempt, retrying_in_s=1)
        await asyncio.sleep(1)

    if not db_ready:
        logger.error("db.unreachable", max_attempts=10)
        raise RuntimeError("Cannot connect to PostgreSQL database after 10 attempts.")

    # 2. Auto-create database tables
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("db.tables_created")
    except Exception as e:
        logger.error("db.init_failed", error=str(e))
        raise e

    # 3. Start Outbox Relay Worker
    await _processor.start()
    logger.info("service.ready")

    yield

    # ── Shutdown ──────────────────────────────────────────────────────────
    logger.info("service.stopping")
    await _processor.shutdown()
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
# X-Request-ID middleware (REQ-API-04)
# ---------------------------------------------------------------------------
@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(request_id=request_id)

    start = time.perf_counter()
    response = await call_next(request)
    elapsed = time.perf_counter() - start

    response.headers["X-Request-ID"] = request_id

    if request.url.path not in ("/health", "/metrics"):
        HTTP_REQUEST_DURATION.labels(
            method=request.method,
            path=request.url.path,
            status=str(response.status_code),
        ).observe(elapsed)

    return response


# ---------------------------------------------------------------------------
# Domain exception handler (REQ-API-01)
# ---------------------------------------------------------------------------
@app.exception_handler(AppError)
async def app_error_handler(request: Request, exc: AppError) -> JSONResponse:
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
# Include API Routers
# ---------------------------------------------------------------------------
from app.api.workshops import router as workshops_router

app.include_router(workshops_router, prefix="/api/v1")


# ---------------------------------------------------------------------------
# Health & Metrics Probes
# ---------------------------------------------------------------------------
@app.get("/health", tags=["ops"])
async def health():
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


@app.get("/metrics", tags=["ops"], response_class=PlainTextResponse)
async def metrics():
    pending = await _get_pending_outbox_count()
    if pending >= 0:
        OUTBOX_PENDING.set(pending)

    return PlainTextResponse(
        content=generate_latest().decode("utf-8"),
        media_type=CONTENT_TYPE_LATEST,
    )


# ---------------------------------------------------------------------------
# Test & Resilience Mock Helpers
# ---------------------------------------------------------------------------
import json
import tempfile
from pathlib import Path

_MOCK_STATE_PATH = Path(os.getenv("MOCK_STATE_PATH", str(Path(tempfile.gettempdir()) / "gdg_mock_state.json")))

_sent_notifications: list[dict] = []
_idempotency_cache: dict[str, dict] = {}
_mock_behavior: dict = {"always_fail": False, "delay_s": 0, "transient_count": 0}

def _load_mock_persisted():
    global _sent_notifications, _idempotency_cache, _mock_behavior
    try:
        if _MOCK_STATE_PATH.exists():
            data = json.loads(_MOCK_STATE_PATH.read_text(encoding="utf-8"))
            _sent_notifications = data.get("sent_notifications", [])
            _idempotency_cache = data.get("idempotency_cache", {})
            _mock_behavior = data.get("behavior", {"always_fail": False, "delay_s": 0, "transient_count": 0})
    except Exception as exc:
        logger.warning("mock_load_persisted_failed", exc=str(exc))

def _persist_mock_state():
    try:
        _MOCK_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        temp_path = _MOCK_STATE_PATH.with_suffix(".tmp")
        temp_path.write_text(
            json.dumps({
                "sent_notifications": _sent_notifications,
                "idempotency_cache": _idempotency_cache,
                "behavior": _mock_behavior,
            }),
            encoding="utf-8"
        )
        temp_path.replace(_MOCK_STATE_PATH)
    except Exception as exc:
        logger.warning("mock_persist_failed", exc=str(exc))

_load_mock_persisted()

@app.post("/reset")
async def reset_mock_notifications():
    global _sent_notifications, _mock_behavior, _idempotency_cache
    _sent_notifications.clear()
    _idempotency_cache.clear()
    _mock_behavior = {"always_fail": False, "delay_s": 0, "transient_count": 0}
    _persist_mock_state()
    return {"status": "reset", "delivered_count": 0}

@app.post("/control")
async def control_mock_behavior(payload: dict):
    global _mock_behavior
    _load_mock_persisted()
    if "always_fail" in payload:
        _mock_behavior["always_fail"] = payload["always_fail"]
    if "delay_s" in payload:
        _mock_behavior["delay_s"] = payload["delay_s"]
    if "mode" in payload:
        _mock_behavior["mode"] = payload["mode"]
        if payload["mode"] == "server_error":
            _mock_behavior["always_fail"] = True
        elif payload["mode"] == "success":
            _mock_behavior["always_fail"] = False
    if "transient_count" in payload:
        _mock_behavior["transient_count"] = payload["transient_count"]
    _persist_mock_state()
    return {"status": "updated", "behavior": _mock_behavior}

@app.get("/requests")
@app.get("/notifications")
@app.get("/api/v1/notifications")
async def get_mock_notifications():
    _load_mock_persisted()
    formatted_requests = [
        {"headers": item.get("headers", {}), "payload": item.get("payload", item)} 
        if isinstance(item, dict) and "payload" in item else {"headers": {}, "payload": item}
        for item in _sent_notifications
    ]
    return {"count": len(formatted_requests), "requests": formatted_requests, "notifications": _sent_notifications}

@app.post("/notify")
@app.post("/api/v1/notify")
async def mock_notify_sink(request: Request, payload: dict):
    _load_mock_persisted()
    idempotency_key = request.headers.get("idempotency-key")
    if idempotency_key and idempotency_key in _idempotency_cache:
        return _idempotency_cache[idempotency_key]

    if _mock_behavior.get("delay_s", 0) > 0:
        await asyncio.sleep(_mock_behavior["delay_s"])

    if _mock_behavior.get("transient_count", 0) > 0:
        _mock_behavior["transient_count"] -= 1
        _persist_mock_state()
        raise HTTPException(status_code=500, detail="Simulated transient mock failure")
        
    if _mock_behavior.get("always_fail", False):
        raise HTTPException(status_code=500, detail="Simulated mock notification service failure")
        
    headers = dict(request.headers)
    _sent_notifications.append({"headers": headers, "payload": payload})
    resp = {"status": "delivered", "idempotency_key": idempotency_key}
    if idempotency_key:
        _idempotency_cache[idempotency_key] = resp
    _persist_mock_state()
    return resp