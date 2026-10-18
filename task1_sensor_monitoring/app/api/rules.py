"""Alert rule management API — CRUD for /api/v1/alert-rules."""

from __future__ import annotations

import logging
import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.alert_rule import AlertRule, RuleType
from app.schemas.alert_rule import AlertRuleCreate, AlertRuleRead, AlertRuleUpdate

log = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/alert-rules",
    tags=["Alert Rules"],
)

DbDep = Annotated[AsyncSession, Depends(get_db)]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _get_rule_or_404(db: AsyncSession, rule_id: uuid.UUID) -> AlertRule:
    result = await db.execute(
        select(AlertRule).where(AlertRule.id == rule_id)
    )
    rule = result.scalar_one_or_none()
    if rule is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Alert rule {rule_id} not found.",
        )
    return rule


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.post(
    "",
    response_model=AlertRuleRead,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new alert rule",
    responses={
        201: {"description": "Rule created."},
        422: {"description": "Validation failed (missing threshold, invalid rule_type, etc.)."},
    },
)
async def create_alert_rule(
    body: AlertRuleCreate,
    db: DbDep,
) -> AlertRule:
    """Persist a new THRESHOLD or WINDOW_AVERAGE alert rule.

    Pydantic validates that THRESHOLD rules have at least one bound and
    WINDOW_AVERAGE rules have a ``window_threshold`` before this handler runs.
    """
    rule = AlertRule(
        name=body.name,
        description=body.description,
        sensor_id=str(body.sensor_id) if body.sensor_id else None,
        metric_name=body.metric_name,
        rule_type=body.rule_type,
        upper_threshold=body.upper_threshold,
        lower_threshold=body.lower_threshold,
        window_threshold=body.window_threshold,
        window_seconds=body.window_seconds,
        is_active=body.is_active,
    )
    db.add(rule)
    await db.commit()
    await db.refresh(rule)
    log.info("created alert rule id=%s name=%r type=%s", rule.id, rule.name, rule.rule_type)
    return rule


@router.get(
    "",
    response_model=list[AlertRuleRead],
    summary="List alert rules",
    responses={200: {"description": "Filtered list of rules."}},
)
async def list_alert_rules(
    db: DbDep,
    is_active: bool | None = Query(default=None, description="Filter by active state."),
    rule_type: RuleType | None = Query(default=None, description="Filter by rule type."),
    metric_name: str | None = Query(default=None, description="Filter by metric name."),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> list[AlertRule]:
    """Return all rules, with optional filters on active state, type, and metric."""
    conditions = []
    if is_active is not None:
        conditions.append(AlertRule.is_active.is_(is_active))
    if rule_type is not None:
        conditions.append(AlertRule.rule_type == rule_type)
    if metric_name is not None:
        conditions.append(AlertRule.metric_name == metric_name)

    stmt = select(AlertRule)
    if conditions:
        stmt = stmt.where(and_(*conditions))
    stmt = stmt.order_by(AlertRule.created_at.desc()).limit(limit).offset(offset)

    result = await db.execute(stmt)
    return list(result.scalars().all())


@router.get(
    "/{rule_id}",
    response_model=AlertRuleRead,
    summary="Get a single alert rule",
    responses={
        200: {"description": "Rule found."},
        404: {"description": "Rule not found."},
    },
)
async def get_alert_rule(
    rule_id: uuid.UUID,
    db: DbDep,
) -> AlertRule:
    return await _get_rule_or_404(db, rule_id)


@router.patch(
    "/{rule_id}",
    response_model=AlertRuleRead,
    summary="Update an alert rule",
    responses={
        200: {"description": "Rule updated."},
        404: {"description": "Rule not found."},
        422: {"description": "Invalid field value."},
    },
)
async def update_alert_rule(
    rule_id: uuid.UUID,
    body: AlertRuleUpdate,
    db: DbDep,
) -> AlertRule:
    """Apply a partial update to an existing rule.

    Only fields explicitly supplied in the request body are modified.
    Changes take effect on the next stream entry processed by the consumer.
    """
    rule = await _get_rule_or_404(db, rule_id)

    update_data = body.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(rule, field, value)

    await db.commit()
    await db.refresh(rule)
    log.info("updated alert rule id=%s fields=%s", rule.id, list(update_data.keys()))
    return rule


@router.delete(
    "/{rule_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete an alert rule",
    responses={
        204: {"description": "Rule deleted."},
        404: {"description": "Rule not found."},
    },
)
async def delete_alert_rule(
    rule_id: uuid.UUID,
    db: DbDep,
) -> None:
    """Delete a rule. Existing open alerts retain their ``rule_id`` as NULL
    (ON DELETE SET NULL) so audit history is preserved."""
    rule = await _get_rule_or_404(db, rule_id)
    await db.delete(rule)
    await db.commit()
    log.info("deleted alert rule id=%s", rule_id)
