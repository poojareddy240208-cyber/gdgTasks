"""
app/models/registration.py
──────────────────────────
Registration model — a confirmed seat reservation linking one User to one
Workshop.

Key invariants (REQ-REG-01 / REQ-REG-03)
─────────────────────────────────────────
1. At most **one** CONFIRMED registration per (user_id, workshop_id) pair.
   Enforced by:
   a) Application layer: flush() before commit catches IntegrityError early.
   b) Database layer: ``UniqueConstraint("user_id", "workshop_id")`` — the
      final safety net for concurrent duplicate requests that race past the
      application-level check.

2. A Workshop row cannot be deleted while CONFIRMED registrations exist.
   Enforced by: ``ForeignKey(..., ondelete="RESTRICT")`` on ``workshop_id``.

State machine
─────────────
    CONFIRMED ──► CANCELLED  (terminal)

A CANCELLED registration is never reinstated.  Re-registering after
cancellation creates a brand-new row; the unique constraint allows this
because the old row has status=CANCELLED (the constraint only blocks a
second CONFIRMED row — NOTE: by design, we treat the uniqueness constraint
as covering *all* statuses so a user cannot hold multiple rows in any state.
If the business needs allow re-registration after cancellation, the
constraint must be made partial: UNIQUE (user_id, workshop_id) WHERE
status = 'CONFIRMED'.  This can be added as an Alembic migration later.)
"""

import enum
import uuid

from sqlalchemy import Enum as SAEnum, ForeignKey, Index, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin


class RegistrationStatus(str, enum.Enum):
    """
    Lifecycle state of a registration.

    CONFIRMED — active seat reservation.
    CANCELLED — seat released; no longer counts toward workshop capacity.
    """

    CONFIRMED = "CONFIRMED"
    CANCELLED = "CANCELLED"


class Registration(Base, TimestampMixin):
    """
    A seat reservation held by one attendee for one workshop.

    Columns
    ───────
    id          — UUID primary key.
    user_id     — FK → users.id  (CASCADE: deleting a User deletes their
                  registrations).
    workshop_id — FK → workshops.id  (RESTRICT: prevents workshop deletion
                  while this registration exists).
    status      — CONFIRMED or CANCELLED.

    Constraints
    ───────────
    uq_registration_user_workshop
        UniqueConstraint on (user_id, workshop_id) — DB-level guarantee that
        no two rows share the same user+workshop combination, regardless of
        status.  This is the hard guard against duplicate registrations
        (REQ-REG-01, REQ-REG-03).

    ix_registration_workshop_status
        Composite index on (workshop_id, status) — used by:
        • the seat-count query (COUNT WHERE status='CONFIRMED')
        • the bulk-cancel batch query (SELECT … WHERE workshop_id=? AND
          status='CONFIRMED' LIMIT N FOR UPDATE SKIP LOCKED)
    """

    __tablename__ = "registrations"
    __table_args__ = (
        # ── REQ-REG-01 / REQ-REG-03 ─────────────────────────────────────────
        # DB-level uniqueness: even if two concurrent requests both pass the
        # application-layer capacity check, only one INSERT will succeed.
        UniqueConstraint(
            "user_id",
            "workshop_id",
            name="uq_registration_user_workshop",
        ),
        # ── Performance index ────────────────────────────────────────────────
        # Covers: WHERE workshop_id = ? AND status = 'CONFIRMED'
        Index(
            "ix_registration_workshop_status",
            "workshop_id",
            "status",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    workshop_id: Mapped[uuid.UUID] = mapped_column(
        # RESTRICT prevents Workshop deletion while this row exists (REQ-WM-04)
        ForeignKey("workshops.id", ondelete="RESTRICT"),
        nullable=False,
    )
    status: Mapped[RegistrationStatus] = mapped_column(
        SAEnum(RegistrationStatus, name="registration_status", create_type=True),
        nullable=False,
        default=RegistrationStatus.CONFIRMED,
    )

    # ── Relationships ─────────────────────────────────────────────────────────
    user: Mapped["User"] = relationship(  # noqa: F821
        "User",
        back_populates="registrations",
        lazy="noload",
    )
    workshop: Mapped["Workshop"] = relationship(  # noqa: F821
        "Workshop",
        back_populates="registrations",
        lazy="noload",
    )

    def __repr__(self) -> str:
        return (
            f"<Registration id={self.id} "
            f"user_id={self.user_id} "
            f"workshop_id={self.workshop_id} "
            f"status={self.status}>"
        )
