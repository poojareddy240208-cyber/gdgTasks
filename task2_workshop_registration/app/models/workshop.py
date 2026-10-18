"""
app/models/workshop.py
──────────────────────
Workshop model — a scheduled GDG event with a fixed seat capacity.

``registered_count`` and ``available_seats`` are intentionally **not** stored
columns.  They are derived at query time via a correlated subquery (see the
list endpoint) so they are always consistent without triggers or denormalised
counters.

State machine
─────────────
    SCHEDULED ──► CANCELLED  (terminal)

The ``CANCELLED`` state is set atomically during bulk cancellation; no other
state transitions exist.
"""

import enum
import uuid
from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, Enum as SAEnum, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin


class WorkshopStatus(str, enum.Enum):
    """
    Lifecycle state of a workshop.

    SCHEDULED — accepting registrations; seats may be available.
    CANCELLED — no longer active; all registrations have been cancelled.
    """

    SCHEDULED = "SCHEDULED"
    CANCELLED = "CANCELLED"


class Workshop(Base, TimestampMixin):
    """
    A single workshop event.

    Columns
    ───────
    id            — UUID primary key.
    title         — human-readable event name shown in notifications.
    scheduled_at  — timezone-aware start time; must be in the future at
                    creation time (enforced in the service layer, not here).
    venue         — free-text location string forwarded to notifications.
    capacity      — total seat count; CHECK ensures it is non-negative.
    status        — SCHEDULED or CANCELLED.

    Constraints
    ───────────
    ck_workshop_capacity_non_negative  — capacity >= 0 at the DB level.
    ondelete="RESTRICT" (via Registration FK)  — prevents deletion of a
        workshop that still has at least one CONFIRMED registration (REQ-WM-04).
        The RESTRICT is declared on the Registration side; see registration.py.
    """

    __tablename__ = "workshops"
    __table_args__ = (
        CheckConstraint(
            "capacity >= 0",
            name="ck_workshop_capacity_non_negative",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
    )
    title: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )
    scheduled_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        index=True,
        comment="Timezone-aware start time of the workshop.",
    )
    venue: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )
    capacity: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="Maximum number of confirmed registrations allowed.",
    )
    status: Mapped[WorkshopStatus] = mapped_column(
        SAEnum(WorkshopStatus, name="workshop_status", create_type=True),
        nullable=False,
        default=WorkshopStatus.SCHEDULED,
        index=True,
    )

    # ── Relationships ─────────────────────────────────────────────────────────
    registrations: Mapped[list["Registration"]] = relationship(  # noqa: F821
        "Registration",
        back_populates="workshop",
        cascade="all, delete-orphan",
        lazy="noload",
    )

    def __repr__(self) -> str:
        return (
            f"<Workshop id={self.id} title={self.title!r} status={self.status}>"
        )
