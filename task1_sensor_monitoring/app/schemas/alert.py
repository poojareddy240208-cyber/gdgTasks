"""Pydantic v2 schemas for Alert read and lifecycle endpoints."""

from __future__ import annotations

import datetime
import uuid

from pydantic import BaseModel

from app.models.alert import AlertStatus


class AlertRead(BaseModel):
    """Full alert representation returned by GET and PATCH endpoints."""

    id: uuid.UUID
    rule_id: uuid.UUID | None
    sensor_id: str
    metric_name: str
    trigger_value: float
    threshold_value: float
    status: AlertStatus
    message: str | None
    triggered_at: datetime.datetime
    acknowledged_at: datetime.datetime | None
    resolved_at: datetime.datetime | None

    # Enables construction from an ORM Alert instance via AlertRead.model_validate(alert)
    model_config = {"from_attributes": True}


class AlertAcknowledge(BaseModel):
    """Optional request body for PATCH /alerts/{id}/acknowledge."""

    # Operator may supply a note that gets appended to the alert message.
    note: str | None = None
