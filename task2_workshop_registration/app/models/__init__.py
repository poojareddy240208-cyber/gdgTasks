"""
app/models/__init__.py
──────────────────────
Re-exports every ORM model and its associated enums from a single import
location.  Import from here in Alembic's ``env.py`` and anywhere else that
needs access to ``Base.metadata`` or multiple models at once.

Example (alembic/env.py)::

    from app.models import Base          # gives access to Base.metadata
    target_metadata = Base.metadata

Example (service layer)::

    from app.models import Registration, RegistrationStatus, OutboxEvent
"""

from app.models.base import Base, TimestampMixin
from app.models.user import User, UserRole
from app.models.workshop import Workshop, WorkshopStatus
from app.models.registration import Registration, RegistrationStatus
from app.models.outbox_event import OutboxEvent, OutboxEventStatus, OutboxEventType

__all__ = [
    # Base
    "Base",
    "TimestampMixin",
    # User
    "User",
    "UserRole",
    # Workshop
    "Workshop",
    "WorkshopStatus",
    # Registration
    "Registration",
    "RegistrationStatus",
    # OutboxEvent
    "OutboxEvent",
    "OutboxEventStatus",
    "OutboxEventType",
]
