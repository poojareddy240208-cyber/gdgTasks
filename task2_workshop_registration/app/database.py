"""
app/database.py
───────────────
Async SQLAlchemy engine and session factory.

All database I/O in this project is async via asyncpg so the FastAPI event
loop is never blocked.  Import ``AsyncSessionLocal`` (or use the ``get_db``
FastAPI dependency) wherever a database session is needed.

Usage — as a FastAPI dependency
────────────────────────────────
    from app.database import get_db
    from sqlalchemy.ext.asyncio import AsyncSession

    @router.get("/example")
    async def example(db: AsyncSession = Depends(get_db)):
        result = await db.execute(select(MyModel))
        ...

Usage — direct (services, workers)
────────────────────────────────────
    from app.database import AsyncSessionLocal

    async with AsyncSessionLocal() as session:
        async with session.begin():
            ...
"""

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
# pool_pre_ping=True: before handing a connection from the pool to a caller,
# SQLAlchemy sends a lightweight "SELECT 1" to detect stale connections
# (e.g. after a PostgreSQL restart) and transparently recycles them.
engine = create_async_engine(
    settings.database_url,
    pool_pre_ping=True,
    # Echo SQL only in debug mode to avoid noise in production logs.
    echo=settings.debug,
    # Keep a modest pool; override via DATABASE_URL query params if needed.
    pool_size=10,
    max_overflow=20,
)

# ---------------------------------------------------------------------------
# Session factory
# ---------------------------------------------------------------------------
# expire_on_commit=False: prevents SQLAlchemy from expiring ORM objects after
# a commit, which would trigger lazy-load attempts on an async connection that
# no longer exists.  All attribute access after commit uses the in-memory
# state from before the commit.
AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,    # flush explicitly; avoids surprising implicit flushes
    autocommit=False,
)


# ---------------------------------------------------------------------------
# FastAPI dependency
# ---------------------------------------------------------------------------
async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Yield a single ``AsyncSession`` for the duration of one HTTP request.

    The session is automatically closed (but NOT committed) when the request
    finishes.  Route handlers are responsible for calling
    ``await session.commit()`` on success; any exception causes an implicit
    rollback via the context-manager exit.

    Example::

        async with AsyncSessionLocal() as session:
            ...  # same behaviour as using Depends(get_db)
    """
    async with AsyncSessionLocal() as session:
        yield session
