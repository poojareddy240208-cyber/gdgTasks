"""AlertRule ORM model — defines threshold and window-average alert rules."""

from __future__ import annotations

import datetime
import uuid
from enum import Enum as PyEnum

from sqlalchemy import Boolean, DateTime, Enum, Float, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class RuleType(str, PyEnum):
    """Discriminates between single-point and rolling-average rules."""

    THRESHOLD = "THRESHOLD"          # fires on a single reading breach
    WINDOW_AVERAGE = "WINDOW_AVERAGE"  # fires when 10-min rolling mean breaches


class AlertRule(Base):
    """Persisted configuration for one alert rule.

    A rule may target a specific sensor (``sensor_id`` set) or act as a
    wildcard across all sensors reporting ``metric_name`` (``sensor_id``
    is NULL).
    """

    __tablename__ = "alert_rules"

    # ── Primary key ──────────────────────────────────────────────────────────
    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
    )

    # ── Identity ─────────────────────────────────────────────────────────────
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    # ── Scope ─────────────────────────────────────────────────────────────────
    # NULL means the rule applies to *all* sensors for this metric.
    sensor_id: Mapped[str | None] = mapped_column(
        String(36), nullable=True, index=True
    )
    metric_name: Mapped[str] = mapped_column(
        String(100), nullable=False, index=True
    )

    # ── Rule type ─────────────────────────────────────────────────────────────
    rule_type: Mapped[RuleType] = mapped_column(
        Enum(RuleType, name="ruletype"), nullable=False
    )

    # ── THRESHOLD rule fields ─────────────────────────────────────────────────
    # At least one must be non-NULL for THRESHOLD rules (enforced in schema).
    upper_threshold: Mapped[float | None] = mapped_column(Float, nullable=True)
    lower_threshold: Mapped[float | None] = mapped_column(Float, nullable=True)

    # ── WINDOW_AVERAGE rule fields ────────────────────────────────────────────
    # window_seconds defaults to 600 (10 minutes); window_threshold is required.
    window_seconds: Mapped[int] = mapped_column(
        Integer, nullable=False, default=600
    )
    window_threshold: Mapped[float | None] = mapped_column(Float, nullable=True)

    # ── State ─────────────────────────────────────────────────────────────────
    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True
    )

    # ── Audit timestamps ──────────────────────────────────────────────────────
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
