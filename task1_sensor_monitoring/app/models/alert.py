"""Alert ORM model — a single alert instance with full lifecycle tracking."""

from __future__ import annotations

import datetime
import uuid
from enum import Enum as PyEnum

from sqlalchemy import DateTime, Enum, Float, ForeignKey, Index, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base


class AlertStatus(str, PyEnum):
    """Valid states in the alert lifecycle state machine."""

    TRIGGERED = "TRIGGERED"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    RESOLVED = "RESOLVED"


class Alert(Base):
    """One alert instance produced when a rule detects a violation.

    State transitions
    -----------------
    (none)       → TRIGGERED     : rule evaluation detects breach
    TRIGGERED    → ACKNOWLEDGED  : operator calls PATCH /alerts/{id}/acknowledge
    TRIGGERED    → RESOLVED      : breach clears (auto) or manual resolve
    ACKNOWLEDGED → RESOLVED      : breach clears (auto) or manual resolve
    RESOLVED     → TRIGGERED     : new breach detected after prior was resolved
    """

    __tablename__ = "alerts"

    # ── Composite indexes defined outside mapped_column for clarity ──────────
    __table_args__ = (
        Index("ix_alerts_status", "status"),
        Index("ix_alerts_sensor_id", "sensor_id"),
        Index("ix_alerts_triggered_at_desc", "triggered_at"),
    )

    # ── Primary key ──────────────────────────────────────────────────────────
    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
    )

    # ── Rule reference ───────────────────────────────────────────────────────
    # SET NULL on delete: alerts outlive their parent rule for audit purposes.
    rule_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("alert_rules.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    # ── Context ───────────────────────────────────────────────────────────────
    sensor_id: Mapped[str] = mapped_column(String(36), nullable=False)
    metric_name: Mapped[str] = mapped_column(String(100), nullable=False)

    # The reading (or computed mean) that caused the alert, and the threshold
    # that was breached — stored so the alert is self-contained after a rule
    # is edited or deleted.
    trigger_value: Mapped[float] = mapped_column(Float, nullable=False)
    threshold_value: Mapped[float] = mapped_column(Float, nullable=False)

    # Human-readable summary, e.g. "10-min avg 42.3 > threshold 40.0"
    message: Mapped[str | None] = mapped_column(Text, nullable=True)

    # ── Lifecycle state ───────────────────────────────────────────────────────
    status: Mapped[AlertStatus] = mapped_column(
        Enum(AlertStatus, name="alertstatus"),
        nullable=False,
        default=AlertStatus.TRIGGERED,
    )

    # ── Lifecycle timestamps ──────────────────────────────────────────────────
    triggered_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    acknowledged_at: Mapped[datetime.datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    resolved_at: Mapped[datetime.datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # ── Relationships ─────────────────────────────────────────────────────────
    # selectin loading avoids N+1 in list endpoints without requiring a JOIN.
    rule: Mapped[AlertRule | None] = relationship(  # type: ignore[name-defined]
        "AlertRule", lazy="selectin"
    )
