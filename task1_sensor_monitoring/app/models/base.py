from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Shared declarative base for all ORM models.

    All mapped classes inherit from this base so that
    ``Base.metadata`` captures every table for Alembic
    autogenerate and ``create_all`` in tests.
    """
