"""Async SQLAlchemy engine, session factory, and FastAPI dependency."""

from __future__ import annotations

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import settings

# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------
# pool_pre_ping=True  — issues a lightweight SELECT 1 before handing out a
#                        connection, recovering cleanly from idle-timeout drops.
# pool_size / max_overflow — tune for expected concurrency; these are safe
#                             starting values for a single-process service.
engine = create_async_engine(
    settings.database_url,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
    echo=False,  # set to True locally for SQL query logging
)

# ---------------------------------------------------------------------------
# Session factory
# ---------------------------------------------------------------------------
# expire_on_commit=False prevents SQLAlchemy from expiring ORM attributes
# after a commit, which is essential in async contexts where a second
# lazy-load would require a new awaitable — attributes stay readable after
# the session is closed.
async_session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

# ---------------------------------------------------------------------------
# FastAPI dependency
# ---------------------------------------------------------------------------

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Yield a transactional async session, rolling back on any exception.

    Usage::

        @router.get("/items")
        async def list_items(db: AsyncSession = Depends(get_db)):
            ...
    """
    async with async_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
