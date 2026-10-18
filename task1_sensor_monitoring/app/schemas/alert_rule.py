"""Pydantic v2 schemas for AlertRule CRUD endpoints."""

from __future__ import annotations

import datetime
import uuid

from pydantic import BaseModel, Field, model_validator

from app.models.alert_rule import RuleType


class AlertRuleBase(BaseModel):
    """Shared fields for create and read representations."""

    name: str = Field(..., min_length=1, max_length=255)
    # None = wildcard (rule applies to all sensors for this metric)
    sensor_id: uuid.UUID | None = None
    metric_name: str = Field(..., min_length=1, max_length=100)
    rule_type: RuleType
    description: str | None = None

    # ── THRESHOLD fields ──────────────────────────────────────────────────────
    upper_threshold: float | None = None
    lower_threshold: float | None = None

    # ── WINDOW_AVERAGE fields ─────────────────────────────────────────────────
    window_threshold: float | None = None
    window_seconds: int = Field(default=600, ge=60, le=3600)

    is_active: bool = True

    @model_validator(mode="after")
    def validate_thresholds(self) -> "AlertRuleBase":
        """Enforce that each rule type has its required threshold fields."""
        if self.rule_type == RuleType.THRESHOLD:
            if self.upper_threshold is None and self.lower_threshold is None:
                raise ValueError(
                    "THRESHOLD rule requires at least one of "
                    "upper_threshold or lower_threshold"
                )
        if self.rule_type == RuleType.WINDOW_AVERAGE:
            if self.window_threshold is None:
                raise ValueError(
                    "WINDOW_AVERAGE rule requires window_threshold"
                )
        return self


class AlertRuleCreate(AlertRuleBase):
    """Request body for POST /alert-rules."""


class AlertRuleUpdate(BaseModel):
    """Partial update body for PATCH /alert-rules/{rule_id}.

    All fields are optional; only supplied fields are applied.
    """

    name: str | None = Field(default=None, min_length=1, max_length=255)
    description: str | None = None
    upper_threshold: float | None = None
    lower_threshold: float | None = None
    window_threshold: float | None = None
    window_seconds: int | None = Field(default=None, ge=60, le=3600)
    is_active: bool | None = None


class AlertRuleRead(AlertRuleBase):
    """Response schema — includes server-assigned fields."""

    id: uuid.UUID
    created_at: datetime.datetime
    updated_at: datetime.datetime

    model_config = {"from_attributes": True}
