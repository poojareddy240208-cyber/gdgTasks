"""Telemetry ingestion API — POST /api/v1/telemetry and /api/v1/telemetry/batch.

Design contract (from design §7.1 and requirements §1):
- Validation is performed by Pydantic before the handler body runs.
  Any schema violation → FastAPI returns 422 Unprocessable Entity automatically.
- On valid input the payload is pushed to Redis Streams and HTTP 202 is
  returned *immediately*, before any alert evaluation happens.
- The handler never waits for downstream consumers.
"""

from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, status
from redis.asyncio import Redis

from app.config import settings
from app.schemas.telemetry import BatchTelemetryPayload, TelemetryPayload
from app.services.ingestion import push_batch_to_stream, push_to_stream

log = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/telemetry",
    tags=["Telemetry"],
)


# ---------------------------------------------------------------------------
# Dependency — resolve the shared Redis client from app state
# ---------------------------------------------------------------------------

async def _get_redis(request: Request) -> Redis:
    """Pull the Redis client that was attached to ``app.state`` at startup.

    Using ``app.state`` rather than a module-level import keeps the router
    testable: tests can attach a ``fakeredis`` client to ``app.state.redis``
    without monkeypatching module globals.
    """
    redis: Redis | None = getattr(request.app.state, "redis", None)
    if redis is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Redis connection not initialised.",
        )
    return redis


RedisDep = Annotated[Redis, Depends(_get_redis)]


# ---------------------------------------------------------------------------
# Response models (inline — kept simple to avoid an extra schema file)
# ---------------------------------------------------------------------------

class _SingleAcceptedResponse(dict):
    """Documented response body for single-reading ingestion."""


class _BatchAcceptedResponse(dict):
    """Documented response body for batch ingestion."""


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Ingest a single sensor reading",
    response_description="Reading accepted and queued for processing.",
    responses={
        202: {
            "description": "Reading accepted.",
            "content": {
                "application/json": {
                    "example": {
                        "status": "accepted",
                        "stream_id": "1714000000000-0",
                        "sensor_id": "3f2a1b00-0000-0000-0000-000000000001",
                        "metric_name": "temperature",
                    }
                }
            },
        },
        400: {"description": "Timestamp outside skew window or invalid location."},
        422: {"description": "Missing or malformed request fields."},
        503: {"description": "Redis unavailable."},
    },
)
async def ingest_single(
    payload: TelemetryPayload,
    redis: RedisDep,
) -> dict:
    """Accept one sensor reading, push it to Redis Streams, and return 202.

    Pydantic validates the body (sensor_id UUID format, metric_name length,
    timestamp skew, location keys) before this handler runs.  The handler
    itself only pushes to Redis and returns immediately — no alert evaluation
    happens here.
    """
    try:
        stream_id = await push_to_stream(redis, payload)
    except Exception as exc:
        log.error("failed to push single reading to stream: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Failed to queue reading — stream unavailable.",
        ) from exc

    return {
        "status": "accepted",
        "stream_id": stream_id,
        "sensor_id": str(payload.sensor_id),
        "metric_name": payload.metric_name,
    }


@router.post(
    "/batch",
    status_code=status.HTTP_202_ACCEPTED,
    summary=f"Ingest a batch of sensor readings (max {settings.batch_max_size})",
    response_description="All valid readings accepted and queued for processing.",
    responses={
        202: {
            "description": "Readings accepted.",
            "content": {
                "application/json": {
                    "example": {
                        "status": "accepted",
                        "accepted": 3,
                        "submitted": 3,
                        "stream_ids": [
                            "1714000000001-0",
                            "1714000000002-0",
                            "1714000000003-0",
                        ],
                    }
                }
            },
        },
        422: {"description": "Missing fields, batch empty, or exceeds maximum size."},
        503: {"description": "Redis unavailable or partial write failure."},
    },
)
async def ingest_batch(
    payload: BatchTelemetryPayload,
    redis: RedisDep,
) -> dict:
    """Accept up to ``batch_max_size`` readings in one request.

    Each reading is independently validated by Pydantic.  All valid readings
    are pushed to the Redis Stream concurrently.  The response includes how
    many readings were accepted so callers can detect partial failures.

    Returns HTTP 503 only if *all* writes fail; partial acceptance still
    returns 202 with ``accepted < submitted``.
    """
    submitted = len(payload.readings)

    try:
        accepted, stream_ids = await push_batch_to_stream(redis, payload)
    except Exception as exc:
        log.error("batch ingestion failed entirely: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Failed to queue batch — stream unavailable.",
        ) from exc

    if accepted == 0:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"All {submitted} readings failed to queue — stream may be unavailable.",
        )

    return {
        "status": "accepted",
        "accepted": accepted,
        "submitted": submitted,
        "stream_ids": stream_ids,
    }
