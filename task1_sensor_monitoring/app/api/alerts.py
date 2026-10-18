"""Alert management API — read and lifecycle state transitions.

Routes
------
GET  /api/v1/alerts                     — list with filters
GET  /api/v1/alerts/{alert_id}          — single alert
PATCH /api/v1/alerts/{alert_id}/acknowledge
PATCH /api/v1/alerts/{alert_id}/resolve
"""

from __future__ import annotations

import datetime
import logging
import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from redis.asyncio import Redis
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.alert import Alert, AlertStatus
from app.schemas.alert import AlertAcknowledge, AlertRead

log = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/alerts",
    tags=["Alerts"],
)

DbDep = Annotated[AsyncSession, Depends(get_db)]


# ---------------------------------------------------------------------------
# Dependency — Redis from app state (needed for dedup key cleanup on resolve)
# ---------------------------------------------------------------------------

async def _get_redis(request: Request) -> Redis:
    redis: Redis | None = getattr(request.app.state, "redis", None)
    if redis is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Redis connection not initialised.",
        )
    return redis


RedisDep = Annotated[Redis, Depends(_get_redis)]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _get_alert_or_404(db: AsyncSession, alert_id: uuid.UUID) -> Alert:
    result = await db.execute(
        select(Alert).where(Alert.id == alert_id)
    )
    alert = result.scalar_one_or_none()
    if alert is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Alert {alert_id} not found.",
        )
    return alert


def _dedup_key(rule_id: uuid.UUID | None, sensor_id: str) -> str | None:
    if rule_id is None:
        return None
    return f"alert:open:{rule_id}:{sensor_id}"


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get(
    "",
    response_model=list[AlertRead],
    summary="List alerts",
    responses={200: {"description": "Filtered list of alerts."}},
)
async def list_alerts(
    db: DbDep,
    alert_status: AlertStatus | None = Query(
        default=None, alias="status", description="Filter by lifecycle status."
    ),
    sensor_id: str | None = Query(default=None, description="Filter by sensor UUID."),
    from_ts: datetime.datetime | None = Query(
        default=None, description="Include alerts triggered at or after this timestamp (ISO 8601)."
    ),
    to_ts: datetime.datetime | None = Query(
        default=None, description="Include alerts triggered at or before this timestamp (ISO 8601)."
    ),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> list[Alert]:
    """Return alerts ordered by ``triggered_at`` descending.

    All query parameters are optional and combinable.
    """
    conditions = []
    if alert_status is not None:
        conditions.append(Alert.status == alert_status)
    if sensor_id is not None:
        conditions.append(Alert.sensor_id == sensor_id)
    if from_ts is not None:
        conditions.append(Alert.triggered_at >= from_ts)
    if to_ts is not None:
        conditions.append(Alert.triggered_at <= to_ts)

    stmt = select(Alert)
    if conditions:
        stmt = stmt.where(and_(*conditions))
    stmt = stmt.order_by(Alert.triggered_at.desc()).limit(limit).offset(offset)

    result = await db.execute(stmt)
    return list(result.scalars().all())


@router.get(
    "/{alert_id}",
    response_model=AlertRead,
    summary="Get a single alert",
    responses={
        200: {"description": "Alert found."},
        404: {"description": "Alert not found."},
    },
)
async def get_alert(
    alert_id: uuid.UUID,
    db: DbDep,
) -> Alert:
    return await _get_alert_or_404(db, alert_id)


@router.patch(
    "/{alert_id}/acknowledge",
    response_model=AlertRead,
    summary="Acknowledge a TRIGGERED alert",
    responses={
        200: {"description": "Alert transitioned to ACKNOWLEDGED."},
        404: {"description": "Alert not found."},
        409: {"description": "Alert is not in TRIGGERED state."},
    },
)
async def acknowledge_alert(
    alert_id: uuid.UUID,
    body: AlertAcknowledge,
    db: DbDep,
) -> Alert:
    """Transition a TRIGGERED alert to ACKNOWLEDGED.

    Returns 409 Conflict if the alert is already ACKNOWLEDGED or RESOLVED —
    idempotent clients should treat 409 as success for their own previous ACK.
    """
    alert = await _get_alert_or_404(db, alert_id)

    if alert.status != AlertStatus.TRIGGERED:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"Cannot acknowledge alert in '{alert.status}' state. "
                "Only TRIGGERED alerts may be acknowledged."
            ),
        )

    alert.status = AlertStatus.ACKNOWLEDGED
    alert.acknowledged_at = datetime.datetime.now(datetime.timezone.utc)

    if body.note:
        prefix = f"{alert.message}\n" if alert.message else ""
        alert.message = f"{prefix}[ACK note] {body.note}"

    await db.commit()
    await db.refresh(alert)
    log.info("alert ACKNOWLEDGED id=%s", alert_id)
    return alert


@router.patch(
    "/{alert_id}/resolve",
    response_model=AlertRead,
    summary="Manually resolve an alert",
    responses={
        200: {"description": "Alert transitioned to RESOLVED."},
        404: {"description": "Alert not found."},
        409: {"description": "Alert is already RESOLVED."},
    },
)
async def resolve_alert(
    alert_id: uuid.UUID,
    db: DbDep,
    redis: RedisDep,
) -> Alert:
    """Manually transition a TRIGGERED or ACKNOWLEDGED alert to RESOLVED.

    Also removes the Redis dedup key so the evaluator can create a fresh
    TRIGGERED alert if the rule condition re-occurs.
    """
    alert = await _get_alert_or_404(db, alert_id)

    if alert.status == AlertStatus.RESOLVED:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Alert is already RESOLVED.",
        )

    alert.status = AlertStatus.RESOLVED
    alert.resolved_at = datetime.datetime.now(datetime.timezone.utc)

    await db.commit()
    await db.refresh(alert)

    # Remove the dedup key so the worker can re-open an alert if needed.
    dedup = _dedup_key(alert.rule_id, alert.sensor_id)
    if dedup:
        await redis.delete(dedup)

    log.info("alert RESOLVED (manual) id=%s", alert_id)
    return alert
