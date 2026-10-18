from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # ── Database ──────────────────────────────────────────────────────────────
    database_url: str = (
        "postgresql+asyncpg://sensor_user:sensor_pass@localhost:5432/sensor_db"
    )

    # ── Redis ─────────────────────────────────────────────────────────────────
    redis_url: str = "redis://localhost:6379/0"

    # ── Redis Stream ──────────────────────────────────────────────────────────
    # Name of the stream all sensor readings are published to
    stream_name: str = "sensor:telemetry"
    # Approximate maximum number of entries kept in the stream (MAXLEN ~)
    stream_max_len: int = 1_000_000

    # ── Rolling window ────────────────────────────────────────────────────────
    # Width of the sliding average window in seconds (default: 10 minutes)
    window_seconds: int = 600

    # ── Ingestion ─────────────────────────────────────────────────────────────
    # Maximum number of readings accepted in a single batch POST
    batch_max_size: int = 500
    # Maximum allowed clock skew between sensor timestamp and server time (±s)
    timestamp_skew_seconds: int = 300

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        # Allow overriding any field via environment variable of the same name
        # (case-insensitive), e.g. DATABASE_URL=...
        case_sensitive=False,
    )


# Module-level singleton — import this everywhere instead of re-instantiating.
settings = Settings()
