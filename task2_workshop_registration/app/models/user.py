"""
app/models/user.py
──────────────────
User model — represents an attendee or an operator.

Authentication is out of scope for this service; the ``role`` column is used
only for endpoint-level authorization checks (e.g. OPERATOR-only routes).
The ``email`` column is the notification recipient address forwarded to the
outbox relay worker.
"""

import enum
import uuid

from sqlalchemy import Enum as SAEnum, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin


class UserRole(str, enum.Enum):
    """
    Role assigned to a user account.

    ATTENDEE  — can register for and cancel their own workshop seats.
    OPERATOR  — can create / delete workshops and trigger bulk cancellations.
    """

    ATTENDEE = "ATTENDEE"
    OPERATOR = "OPERATOR"


class User(Base, TimestampMixin):
    """
    Persistent user account.

    Columns
    ───────
    id        — UUID primary key; application-generated to avoid hot-page
                contention at high insert rates (sequential bigints cluster
                all new rows on the last B-tree page).
    email     — unique; used as the notification delivery address.
    full_name — displayed in notification emails.
    role      — ``ATTENDEE`` or ``OPERATOR``; stored as a native PG enum.
    """

    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
    )
    email: Mapped[str] = mapped_column(
        String(255),
        unique=True,
        nullable=False,
        index=True,
        comment="Notification delivery address; must be unique across all users.",
    )
    full_name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )
    role: Mapped[UserRole] = mapped_column(
        SAEnum(UserRole, name="user_role", create_type=True),
        nullable=False,
        default=UserRole.ATTENDEE,
    )

    # ── Relationships ────────────────────────────────────────────────────────
    # cascade="all, delete-orphan": deleting a User cascades to their
    # Registrations (and, via Registration's FK, outbox records are SET NULL).
    registrations: Mapped[list["Registration"]] = relationship(  # noqa: F821
        "Registration",
        back_populates="user",
        cascade="all, delete-orphan",
        lazy="noload",   # never eagerly load; always explicit select
    )

    def __repr__(self) -> str:
        return f"<User id={self.id} email={self.email!r} role={self.role}>"
