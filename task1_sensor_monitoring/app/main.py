"""FastAPI application factory — wires routes, lifespan, and background workers."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import redis.asyncio as aioredis
from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.config import settings
from app.database import async_session_factory, engine
from app.api import telemetry as telemetry_router
from app.api import rules as rules_router
from app.api import alerts as alerts_router
from app.workers.stream_processor import (
    init_consumer_groups,
    threshold_consumer,
    window_consumer,
)

log = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
)


# ---------------------------------------------------------------------------
# Lifespan — startup / shutdown
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manage application-scoped resources and background tasks.

    Startup sequence
    ----------------
    1. Create a shared async Redis connection pool and attach to app.state.
    2. Ensure both consumer groups exist on the Redis Stream.
    3. Launch threshold and window consumer tasks in the background.

    Shutdown sequence
    -----------------
    4. Cancel both consumer tasks and wait for clean exit.
    5. Close the Redis connection pool.
    6. Dispose the SQLAlchemy async engine (returns pool connections).
    """
    # ── 1. Redis connection pool ──────────────────────────────────────────
    redis_client = aioredis.from_url(
        settings.redis_url,
        encoding="utf-8",
        decode_responses=True,
    )
    app.state.redis = redis_client
    log.info("Redis pool created: %s", settings.redis_url)

    # ── 2. Consumer group setup ───────────────────────────────────────────
    await init_consumer_groups(redis_client)

    # ── 3. Background worker tasks ────────────────────────────────────────
    threshold_task = asyncio.create_task(
        threshold_consumer(redis_client, async_session_factory),
        name="threshold-consumer",
    )
    window_task = asyncio.create_task(
        window_consumer(redis_client, async_session_factory),
        name="window-consumer",
    )
    log.info("background consumer tasks started")

    yield  # ── application is running ─────────────────────────────────────

    # ── 4. Cancel background tasks ────────────────────────────────────────
    log.info("shutting down background consumer tasks…")
    for task in (threshold_task, window_task):
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    log.info("consumer tasks stopped")

    # ── 5. Close Redis ────────────────────────────────────────────────────
    await redis_client.aclose()
    log.info("Redis pool closed")

    # ── 6. Dispose SQLAlchemy engine ──────────────────────────────────────
    await engine.dispose()
    log.info("SQLAlchemy engine disposed")


# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------

def create_app() -> FastAPI:
    app = FastAPI(
        title="Campus Sensor Monitoring and Alert Engine",
        description=(
            "High-performance telemetry ingestion with Redis Streams, "
            "threshold and rolling-window alert evaluation, "
            "and a full alert lifecycle API."
        ),
        version="1.0.0",
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
    )

    # ── Routers ───────────────────────────────────────────────────────────
    app.include_router(telemetry_router.router)
    app.include_router(rules_router.router)
    app.include_router(alerts_router.router)

    # ── Health check ──────────────────────────────────────────────────────
    @app.get(
        "/health",
        tags=["Health"],
        summary="Service health check",
        response_description="Component health status.",
    )
    async def health(request: Request) -> dict:
        """Check liveness of PostgreSQL and Redis.

        Returns a component-level status dict.  HTTP 200 even when a
        component reports 'error', so load-balancer health checks can
        distinguish between 'reachable but degraded' and 'unreachable'.
        """
        health_status: dict[str, object] = {}

        # ── PostgreSQL ────────────────────────────────────────────────────
        try:
            async with engine.connect() as conn:
                await conn.execute(__import__("sqlalchemy").text("SELECT 1"))
            health_status["postgres"] = "ok"
        except Exception as exc:
            log.warning("postgres health check failed: %s", exc)
            health_status["postgres"] = "error"

        # ── Redis ─────────────────────────────────────────────────────────
        redis: aioredis.Redis = request.app.state.redis
        try:
            await redis.ping()

            # Stream lag: difference between total entries and pending count.
            stream_len: int = await redis.xlen(settings.stream_name)
            health_status["redis"] = "ok"
            health_status["stream_length"] = stream_len
        except Exception as exc:
            log.warning("redis health check failed: %s", exc)
            health_status["redis"] = "error"
            health_status["stream_length"] = None

        return health_status

    # ── Global validation error handler ───────────────────────────────────
    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        """Return 400 (not 422) with field-level detail for schema violations.

        FastAPI defaults to 422 for Pydantic validation errors.  We remap to
        400 for client clarity — 422 implies the request is syntactically
        correct but semantically unprocessable, which is confusing for simple
        missing-field errors.
        """
        errors = []
        for error in exc.errors():
            errors.append(
                {
                    "field": " → ".join(str(loc) for loc in error["loc"]),
                    "message": error["msg"],
                    "type": error["type"],
                }
            )
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"detail": "Request validation failed.", "errors": errors},
        )

    return app


# Module-level app instance used by uvicorn.
app = create_app()
