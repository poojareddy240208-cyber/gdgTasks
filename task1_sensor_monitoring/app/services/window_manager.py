"""Rolling-window manager using Redis ZSET.

Key strategy (from design §5.2)
--------------------------------
Key pattern : sensor:window:<sensor_id>:<metric_name>
Score       : Unix timestamp in **milliseconds** (float)
Member      : "<timestamp_ms>:<value>"

Encoding the timestamp into the member string guarantees uniqueness even when
two readings arrive within the same millisecond for the same sensor/metric.

Per-reading write sequence
--------------------------
1. ZADD   — insert the new data point
2. ZREMRANGEBYSCORE — prune anything older than the configured window
3. ZRANGE — retrieve surviving members to compute the arithmetic mean
4. EXPIRE  — set a 15-minute TTL so orphan keys are cleaned up automatically
"""

from __future__ import annotations

import datetime
import logging

from redis.asyncio import Redis

from app.config import settings

log = logging.getLogger(__name__)

# Safety-margin TTL beyond the window width (seconds).
# Ensures a key that stops receiving updates expires eventually.
_ZSET_TTL_SECONDS: int = settings.window_seconds + 300  # window + 5 min


# ---------------------------------------------------------------------------
# Key helpers
# ---------------------------------------------------------------------------

def _window_key(sensor_id: str, metric_name: str) -> str:
    """Return the Redis ZSET key for a sensor/metric pair.

    Example: ``sensor:window:3f2a1b00-...:temperature``
    """
    return f"sensor:window:{sensor_id}:{metric_name}"


def _make_member(ts_ms: int, value: float) -> str:
    """Encode a ZSET member that is unique even for duplicate timestamps.

    Format: ``<timestamp_ms>:<value>``
    """
    return f"{ts_ms}:{value}"


def _parse_value(member: str) -> float:
    """Extract the float value from a ``<ts_ms>:<value>`` member string."""
    # Split on the *first* colon only; the value itself may contain a decimal.
    _, raw_value = member.split(":", 1)
    return float(raw_value)


# ---------------------------------------------------------------------------
# Core ZSET operations
# ---------------------------------------------------------------------------

async def add_reading(
    redis: Redis,
    sensor_id: str,
    metric_name: str,
    ts_ms: int,
    value: float,
) -> None:
    """Add a single data point to the ZSET without computing an average.

    Use this when you only need to record a reading (e.g. during ingestion)
    and will compute the average separately.

    Args:
        redis:       Async Redis client.
        sensor_id:   Sensor UUID as a string.
        metric_name: Name of the metric (e.g. ``"temperature"``).
        ts_ms:       Reading timestamp in Unix milliseconds.
        value:       Numeric sensor reading.
    """
    key = _window_key(sensor_id, metric_name)
    member = _make_member(ts_ms, value)
    await redis.zadd(key, {member: float(ts_ms)})
    await redis.expire(key, _ZSET_TTL_SECONDS)


async def evict_expired(
    redis: Redis,
    sensor_id: str,
    metric_name: str,
    now_ms: int | None = None,
    window_seconds: int | None = None,
) -> int:
    """Remove ZSET members older than the rolling window.

    Args:
        redis:          Async Redis client.
        sensor_id:      Sensor UUID as a string.
        metric_name:    Name of the metric.
        now_ms:         Current time in Unix milliseconds.
                        Defaults to ``time.time() * 1000`` if not supplied.
        window_seconds: Override the window width from ``settings``.

    Returns:
        Number of entries removed.
    """
    import time

    if now_ms is None:
        now_ms = int(time.time() * 1_000)

    win = window_seconds if window_seconds is not None else settings.window_seconds
    cutoff_ms = now_ms - (win * 1_000)

    key = _window_key(sensor_id, metric_name)
    removed: int = await redis.zremrangebyscore(key, "-inf", cutoff_ms)
    return removed


async def get_window_average(
    redis: Redis,
    sensor_id: str,
    metric_name: str,
) -> float | None:
    """Return the arithmetic mean of all values currently in the ZSET.

    Callers should call ``evict_expired`` first to ensure stale entries have
    been removed.

    Returns:
        The arithmetic mean as a float, or ``None`` if the window is empty.
    """
    key = _window_key(sensor_id, metric_name)
    members: list[str] = await redis.zrange(key, 0, -1)

    if not members:
        return None

    values = [_parse_value(m) for m in members]
    return sum(values) / len(values)


# ---------------------------------------------------------------------------
# Combined high-level function used by the stream consumer
# ---------------------------------------------------------------------------

async def update_window(
    redis: Redis,
    sensor_id: str,
    metric_name: str,
    ts_ms: int,
    value: float,
    window_seconds: int | None = None,
) -> float | None:
    """Record a reading, prune the window, and return the updated mean.

    This is the single entry point called by the window consumer worker for
    each stream entry.  It atomically (from the application's perspective):

    1. Adds the new ``(ts_ms, value)`` pair to the ZSET.
    2. Removes all entries older than ``window_seconds``.
    3. Fetches the surviving members and computes the arithmetic mean.
    4. Refreshes the key TTL.

    Args:
        redis:          Async Redis client.
        sensor_id:      Sensor UUID as a string.
        metric_name:    Name of the metric (e.g. ``"temperature"``).
        ts_ms:          Reading timestamp in Unix milliseconds.
        value:          Numeric sensor reading.
        window_seconds: Override ``settings.window_seconds`` (useful in tests).

    Returns:
        The arithmetic mean of all readings in the current window, or ``None``
        if no readings survive after pruning.
    """
    win = window_seconds if window_seconds is not None else settings.window_seconds
    key = _window_key(sensor_id, metric_name)
    member = _make_member(ts_ms, value)

    # 1. Insert the new data point.
    await redis.zadd(key, {member: float(ts_ms)})

    # 2. Evict readings outside the window.
    cutoff_ms = ts_ms - (win * 1_000)
    removed = await redis.zremrangebyscore(key, "-inf", cutoff_ms)
    if removed:
        log.debug(
            "window pruned %d stale entries for sensor=%s metric=%s",
            removed,
            sensor_id,
            metric_name,
        )

    # 3. Fetch surviving members and compute mean.
    members: list[str] = await redis.zrange(key, 0, -1)
    if not members:
        # All entries were pruned (clock skew edge case); reset TTL and exit.
        await redis.expire(key, _ZSET_TTL_SECONDS)
        return None

    values = [_parse_value(m) for m in members]
    mean = sum(values) / len(values)

    # 4. Refresh TTL so the key doesn't linger forever if ingest stops.
    await redis.expire(key, _ZSET_TTL_SECONDS)

    log.debug(
        "window updated sensor=%s metric=%s n=%d mean=%.4f",
        sensor_id,
        metric_name,
        len(values),
        mean,
    )
    return mean


# ---------------------------------------------------------------------------
# Utility — current window snapshot (used by health / debug endpoints)
# ---------------------------------------------------------------------------

async def get_window_stats(
    redis: Redis,
    sensor_id: str,
    metric_name: str,
) -> dict[str, object]:
    """Return a diagnostic snapshot of the current ZSET window.

    Returns a dict with keys:
      - ``count``  : number of entries in the window
      - ``mean``   : arithmetic mean (None if empty)
      - ``min``    : minimum value (None if empty)
      - ``max``    : maximum value (None if empty)
      - ``oldest`` : oldest entry timestamp in ms (None if empty)
      - ``newest`` : newest entry timestamp in ms (None if empty)
    """
    key = _window_key(sensor_id, metric_name)

    # ZRANGE with WITHSCORES returns alternating [member, score, ...] in
    # redis-py as a list of (member, score) tuples when using zrange(..., withscores=True).
    pairs: list[tuple[str, float]] = await redis.zrange(
        key, 0, -1, withscores=True
    )

    if not pairs:
        return {
            "count": 0,
            "mean": None,
            "min": None,
            "max": None,
            "oldest": None,
            "newest": None,
        }

    values = [_parse_value(member) for member, _ in pairs]
    scores = [score for _, score in pairs]

    return {
        "count": len(values),
        "mean": sum(values) / len(values),
        "min": min(values),
        "max": max(values),
        "oldest": int(min(scores)),
        "newest": int(max(scores)),
    }


async def delete_window(
    redis: Redis,
    sensor_id: str,
    metric_name: str,
) -> None:
    """Delete the ZSET for a sensor/metric pair (e.g. when a sensor is decommissioned)."""
    key = _window_key(sensor_id, metric_name)
    await redis.delete(key)
