"""
app/config.py
─────────────
Central application configuration loaded from environment variables (or a
.env file). All tuneable values live here; no magic strings scattered across
the codebase.

Usage
-----
    from app.config import settings

    print(settings.max_notification_retries)   # 10
    print(settings.database_url)               # postgresql+asyncpg://...
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    All values can be overridden via environment variables (case-insensitive)
    or a .env file placed at the project root.

    Example .env
    ────────────
    DATABASE_URL=postgresql+asyncpg://app:secret@localhost:5432/workshop_registration
    NOTIFICATION_SERVICE_URL=http://localhost:8001
    MAX_NOTIFICATION_RETRIES=10
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",          # silently drop unrecognised env vars
    )

    # ── Database ─────────────────────────────────────────────────────────────
    database_url: str = Field(
        default="postgresql+asyncpg://app:secret@localhost:5432/workshop_registration",
        description=(
            "Async SQLAlchemy connection string. Must use the asyncpg driver, "
            "e.g. postgresql+asyncpg://user:pass@host:port/dbname"
        ),
    )

    # ── Notification service ──────────────────────────────────────────────────
    notification_service_url: str = Field(
        default="http://localhost:8001",
        description="Base URL of the external (or mock) notification service.",
    )
    notification_timeout_s: float = Field(
        default=10.0,
        ge=1.0,
        description="Per-request HTTP timeout (seconds) for the relay worker.",
    )

    # ── Outbox relay worker ───────────────────────────────────────────────────
    max_notification_retries: int = Field(
        default=10,
        ge=1,
        description=(
            "Maximum delivery attempts before an outbox record is marked FAILED. "
            "Satisfies REQ-OB-06."
        ),
    )
    relay_poll_interval_s: int = Field(
        default=5,
        ge=1,
        description="How often (seconds) the relay worker polls for PENDING outbox records.",
    )
    relay_batch_size: int = Field(
        default=50,
        ge=1,
        le=500,
        description="Maximum outbox records claimed per relay tick (FOR UPDATE SKIP LOCKED).",
    )

    # ── Exponential back-off ─────────────────────────────────────────────────
    relay_backoff_initial_s: float = Field(
        default=1.0,
        ge=0.1,
        description="Initial back-off delay in seconds (n=1).",
    )
    relay_backoff_multiplier: float = Field(
        default=2.0,
        ge=1.0,
        description="Multiplicative factor applied on each successive retry.",
    )
    relay_backoff_cap_s: float = Field(
        default=300.0,
        ge=1.0,
        description="Maximum back-off ceiling in seconds (prevents unbounded waits).",
    )

    # ── Bulk cancellation ─────────────────────────────────────────────────────
    bulk_cancel_batch_size: int = Field(
        default=500,
        ge=1,
        le=1000,
        description=(
            "Maximum registrations cancelled per transaction during bulk workshop "
            "cancellation. Satisfies REQ-BK-06 (avoids long-held locks)."
        ),
    )

    # ── Redis ─────────────────────────────────────────────────────────────────
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        description=(
            "Redis connection URL used for distributed locking and optional "
            "rate-limiting. Format: redis://[:password@]host[:port][/db]"
        ),
    )
    redis_lock_timeout_s: int = Field(
        default=30,
        ge=5,
        description=(
            "TTL (seconds) for distributed locks acquired via Redis. "
            "Prevents deadlocks if a lock holder crashes."
        ),
    )

    # ── Mock notification service ─────────────────────────────────────────────
    mock_service_port: int = Field(
        default=8001,
        ge=1024,
        le=65535,
        description="Port the fault-injecting mock notification server listens on.",
    )

    # ── Application server ────────────────────────────────────────────────────
    app_host: str = Field(default="0.0.0.0", description="Uvicorn bind address.")
    app_port: int = Field(default=8000, ge=1024, le=65535, description="Uvicorn bind port.")
    debug: bool = Field(default=False, description="Enable FastAPI debug mode.")

    # ── Helpers ───────────────────────────────────────────────────────────────

    def backoff_seconds(self, attempts: int) -> float:
        """
        Compute the back-off delay before the (attempts+1)-th delivery attempt.

        Formula:  delay = min(initial × multiplier^(attempts-1), cap)

        Examples (defaults):
            attempts=1  →   1.0 s
            attempts=2  →   2.0 s
            attempts=5  →  16.0 s
            attempts=10 → 300.0 s  (capped)
        """
        delay = self.relay_backoff_initial_s * (
            self.relay_backoff_multiplier ** (attempts - 1)
        )
        return min(delay, self.relay_backoff_cap_s)


# Module-level singleton — import this everywhere instead of instantiating
# Settings() in each module.
settings = Settings()
