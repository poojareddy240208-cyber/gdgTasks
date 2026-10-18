"""
app/models/base.py
──────────────────
Shared SQLAlchemy declarative base and timestamp mixin.

All domain models inherit from both ``Base`` and ``TimestampMixin`` so every
table automatically gets ``created_at`` / ``updated_at`` columns with
server-side defaults.
"""

from datetime import datetime, timezone

from sqlalchemy import DateTime, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """
    Project-wide SQLAlchemy declarative base.

    Import ``Base.metadata`` in ``alembic/env.py`` so Alembic can
    auto-generate migrations from the ORM models.
    """
    pass


class TimestampMixin:
    """
    Adds ``created_at`` and ``updated_at`` columns to any model that inherits
    from it.

    - ``created_at``: set once by PostgreSQL at INSERT time via
      ``server_default=func.now()``.  Never changes.
    - ``updated_at``: set at INSERT and refreshed by PostgreSQL on every
      UPDATE via ``onupdate=func.now()``.

    Both columns are timezone-aware (``TIMESTAMPTZ`` in PostgreSQL) to avoid
    ambiguity when the application or database server is in a non-UTC zone.
    """

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        sort_order=998,   # pushed near end of column list in generated DDL
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
        sort_order=999,
    )
