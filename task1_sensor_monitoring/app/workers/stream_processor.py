"""Redis Stream consumer and alert evaluation worker.

Architecture (from design §6)
------------------------------
Two independent consumer-group tasks run inside the FastAPI process via
``asyncio.create_task`` in the app lifespan:

  cg:threshold  — evaluates single-point THRESHOLD rules on every entry
  cg:window     — updates the Redis ZSET and evaluates WINDOW_AVERAGE rules

Both groups read the same ``sensor:telemetry`` stream, so every message is
processed by *both* pipelines independently.

At-least-once delivery
-----------------------
``XACK`` is only sent after a successful DB write.  If the process crashes
mid-flight the entry remains pending and is reclaimed by ``XAUTOCLAIM`` on
the next startup.

Back-pressure
-------------
Batch reads (``count=100``) amortise round-trips.  ``BLOCK 2000`` lets the
loop sleep efficiently between bursts instead of busy-polling.
"""

from __future__ import annotations

import asyncio
import datetime
import json
import logging
import uuid
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import ResponseError
from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import settings
from app.models.alert import Alert, AlertStatus
from app.models.alert_rule import AlertRule, RuleType
from app.services.window_manager import update_window

log = logging.getLogger(__name__)

# Consumer group names — must match what init_consumer_groups() creates.
_CG_THRESHOLD = "cg:threshold"
_CG_WINDOW = "cg:window"

# How many stream entries to fetch per XREADGROUP call.
_BATCH_SIZE = 100

# BLOCK timeout in milliseconds; 0 = block forever (undesirable in tests).
_BLOCK_MS = 2_000

# Seconds before a pending entry is considered stale and reclaimed.
_AUTOCLAIM_MIN_IDLE_MS = 60_000

# Back-off in seconds after an unexpected error.
_ERROR_BACKOFF_S = 1.0


# ---------------------------------------------------------------------------
# Helpers — stream entry parsing
# ---------------------------------------------------------------------------

def _parse_entry(fields: dict[str, str]) -> dict[str, Any]:
    """Decode the flat string dict from a Redis Stream entry.

    Returns a dict with properly typed values:
      sensor_id   : str  (UUID string)
      metric_name : str
      value       : float
      timestamp   : datetime (UTC-aware)
      location    : dict | None
      ts_ms       : int  (Unix milliseconds, derived from timestamp)
    """
    value = float(fields["value"])
    ts = datetime.datetime.fromisoformat(fields["timestamp"])
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=datetime.timezone.utc)

    location: dict | None = None
    raw_loc = fields.get("location", "")
    if raw_loc:
        try:
            location = json.loads(raw_loc)
        except json.JSONDecodeError:
            log.warning("unparseable location field: %r", raw_loc)

    ts_ms = int(ts.timestamp() * 1_000)

    return {
        "sensor_id": fields["sensor_id"],
        "metric_name": fields["metric_name"],
        "value": value,
        "timestamp": ts,
        "location": location,
        "ts_ms": ts_ms,
    }


# ---------------------------------------------------------------------------
# Helpers — alert deduplication
# ---------------------------------------------------------------------------

def _dedup_key(rule_id: uuid.UUID, sensor_id: str) -> str:
    """Redis key that tracks an open alert for a rule+sensor pair."""
    return f"alert:open:{rule_id}:{sensor_id}"


async def _open_alert(
    db: AsyncSession,
    redis: Redis,
    rule: AlertRule,
    sensor_id: str,
    trigger_value: float,
    threshold_value: float,
    message: str,
) -> Alert | None:
    """Create a new TRIGGERED alert, guarded by a Redis dedup key.

    Returns the created Alert, or None if an open alert already exists.
    """
    dedup = _dedup_key(rule.id, sensor_id)
    existing = await redis.get(dedup)
    if existing:
        log.debug("dedup hit — open alert already exists for rule=%s sensor=%s", rule.id, sensor_id)
        return None

    alert = Alert(
        rule_id=rule.id,
        sensor_id=sensor_id,
        metric_name=rule.metric_name,
        trigger_value=trigger_value,
        threshold_value=threshold_value,
        status=AlertStatus.TRIGGERED,
        message=message,
        triggered_at=datetime.datetime.now(datetime.timezone.utc),
    )
    db.add(alert)
    await db.flush()   # assign alert.id without committing yet
    await db.commit()

    # Set dedup key *after* a successful commit.
    await redis.set(dedup, str(alert.id))

    log.info(
        "alert TRIGGERED rule=%s sensor=%s metric=%s value=%.4f threshold=%.4f",
        rule.id, sensor_id, rule.metric_name, trigger_value, threshold_value,
    )
    return alert


async def _resolve_open_alert(
    db: AsyncSession,
    redis: Redis,
    rule: AlertRule,
    sensor_id: str,
) -> None:
    """Resolve any open TRIGGERED/ACKNOWLEDGED alert for rule+sensor."""
    dedup = _dedup_key(rule.id, sensor_id)
    alert_id_str = await redis.get(dedup)
    if not alert_id_str:
        return

    try:
        alert_id = uuid.UUID(alert_id_str)
    except ValueError:
        log.warning("malformed dedup value %r for key %s", alert_id_str, dedup)
        await redis.delete(dedup)
        return

    result = await db.execute(
        select(Alert).where(
            and_(
                Alert.id == alert_id,
                Alert.status.in_([AlertStatus.TRIGGERED, AlertStatus.ACKNOWLEDGED]),
            )
        )
    )
    alert: Alert | None = result.scalar_one_or_none()
    if alert is None:
        # Alert no longer open — clean up stale key.
        await redis.delete(dedup)
        return

    alert.status = AlertStatus.RESOLVED
    alert.resolved_at = datetime.datetime.now(datetime.timezone.utc)
    await db.commit()
    await redis.delete(dedup)

    log.info("alert RESOLVED rule=%s sensor=%s", rule.id, sensor_id)


# ---------------------------------------------------------------------------
# Helpers — rule queries
# ---------------------------------------------------------------------------

async def _fetch_threshold_rules(
    db: AsyncSession, metric_name: str, sensor_id: str
) -> list[AlertRule]:
    """Return active THRESHOLD rules matching this metric and sensor."""
    result = await db.execute(
        select(AlertRule).where(
            and_(
                AlertRule.rule_type == RuleType.THRESHOLD,
                AlertRule.is_active.is_(True),
                AlertRule.metric_name == metric_name,
                or_(
                    AlertRule.sensor_id == sensor_id,
                    AlertRule.sensor_id.is_(None),
                ),
            )
        )
    )
    return list(result.scalars().all())


async def _fetch_window_rules(
    db: AsyncSession, metric_name: str, sensor_id: str
) -> list[AlertRule]:
    """Return active WINDOW_AVERAGE rules matching this metric and sensor."""
    result = await db.execute(
        select(AlertRule).where(
            and_(
                AlertRule.rule_type == RuleType.WINDOW_AVERAGE,
                AlertRule.is_active.is_(True),
                AlertRule.metric_name == metric_name,
                or_(
                    AlertRule.sensor_id == sensor_id,
                    AlertRule.sensor_id.is_(None),
                ),
            )
        )
    )
    return list(result.scalars().all())


# ---------------------------------------------------------------------------
# Evaluators
# ---------------------------------------------------------------------------

async def _evaluate_threshold(
    db: AsyncSession,
    redis: Redis,
    entry: dict[str, Any],
) -> None:
    """Evaluate all active THRESHOLD rules against a single stream entry."""
    sensor_id: str = entry["sensor_id"]
    metric_name: str = entry["metric_name"]
    value: float = entry["value"]

    rules = await _fetch_threshold_rules(db, metric_name, sensor_id)

    for rule in rules:
        upper_breach = (
            rule.upper_threshold is not None and value > rule.upper_threshold
        )
        lower_breach = (
            rule.lower_threshold is not None and value < rule.lower_threshold
        )

        if upper_breach or lower_breach:
            threshold_value = (
                rule.upper_threshold if upper_breach else rule.lower_threshold
            )
            direction = "above" if upper_breach else "below"
            message = (
                f"Sensor {sensor_id} metric '{metric_name}' value {value:.4f} "
                f"is {direction} threshold {threshold_value:.4f} "
                f"(rule: {rule.name})"
            )
            await _open_alert(
                db, redis, rule, sensor_id, value, threshold_value, message  # type: ignore[arg-type]
            )
        else:
            # Violation cleared — resolve any open alert.
            await _resolve_open_alert(db, redis, rule, sensor_id)


async def _evaluate_window(
    db: AsyncSession,
    redis: Redis,
    entry: dict[str, Any],
) -> None:
    """Update the ZSET window and evaluate WINDOW_AVERAGE rules."""
    sensor_id: str = entry["sensor_id"]
    metric_name: str = entry["metric_name"]
    value: float = entry["value"]
    ts_ms: int = entry["ts_ms"]

    rules = await _fetch_window_rules(db, metric_name, sensor_id)
    if not rules:
        # No rules for this sensor/metric — still update window for future rules.
        await update_window(redis, sensor_id, metric_name, ts_ms, value)
        return

    # Use the widest window_seconds among matching rules so we store enough history.
    max_window = max(r.window_seconds for r in rules)

    mean = await update_window(
        redis, sensor_id, metric_name, ts_ms, value,
        window_seconds=max_window,
    )

    if mean is None:
        # Window is empty after pruning — nothing to evaluate.
        return

    for rule in rules:
        if rule.window_threshold is None:
            continue

        if mean > rule.window_threshold:
            message = (
                f"Sensor {sensor_id} metric '{metric_name}' "
                f"{rule.window_seconds}s avg {mean:.4f} "
                f"> threshold {rule.window_threshold:.4f} "
                f"(rule: {rule.name})"
            )
            await _open_alert(
                db, redis, rule, sensor_id, mean, rule.window_threshold, message
            )
        else:
            await _resolve_open_alert(db, redis, rule, sensor_id)


# ---------------------------------------------------------------------------
# Consumer group setup
# ---------------------------------------------------------------------------

async def init_consumer_groups(redis: Redis) -> None:
    """Create consumer groups for both pipelines if they don't already exist.

    Uses ``MKSTREAM`` so the stream key is created automatically.
    ``BUSYGROUP`` is silently swallowed — the group already exists, which is
    the desired end state.
    """
    for group in (_CG_THRESHOLD, _CG_WINDOW):
        try:
            await redis.xgroup_create(
                settings.stream_name, group, id="$", mkstream=True
            )
            log.info("consumer group '%s' created", group)
        except ResponseError as exc:
            if "BUSYGROUP" in str(exc):
                log.debug("consumer group '%s' already exists", group)
            else:
                raise


# ---------------------------------------------------------------------------
# Core consumer loop — shared by both pipelines
# ---------------------------------------------------------------------------

async def _consume_loop(
    redis: Redis,
    session_factory: async_sessionmaker[AsyncSession],
    group: str,
    consumer_name: str,
    evaluator,  # callable: async (db, redis, entry) -> None
) -> None:
    """Generic consumer loop for a single consumer group.

    On startup, calls XAUTOCLAIM to reclaim any pending entries that were
    not ACKed before the previous process died (at-least-once delivery).

    Then enters a BLOCK loop: reads up to _BATCH_SIZE entries, evaluates
    each one, ACKs on success.  Errors per entry are logged and the entry
    is left pending for XAUTOCLAIM on the next restart.
    """
    log.info("consumer '%s' started for group '%s'", consumer_name, group)

    # ── Reclaim stale pending entries from previous run ───────────────────
    try:
        autoclaim_result = await redis.xautoclaim(
            settings.stream_name,
            group,
            consumer_name,
            min_idle_time=_AUTOCLAIM_MIN_IDLE_MS,
            start_id="0-0",
            count=_BATCH_SIZE,
        )
        # xautoclaim returns (next_start_id, [(id, fields), ...], [deleted_ids])
        reclaimed = autoclaim_result[1] if autoclaim_result else []
        if reclaimed:
            log.info(
                "reclaimed %d pending entries for group '%s'", len(reclaimed), group
            )
            for entry_id, fields in reclaimed:
                await _process_entry(
                    redis, session_factory, group, consumer_name,
                    entry_id, fields, evaluator,
                )
    except Exception as exc:
        log.warning("XAUTOCLAIM failed for group '%s': %s", group, exc)

    # ── Main loop ─────────────────────────────────────────────────────────
    while True:
        try:
            response = await redis.xreadgroup(
                groupname=group,
                consumername=consumer_name,
                streams={settings.stream_name: ">"},
                count=_BATCH_SIZE,
                block=_BLOCK_MS,
            )

            if not response:
                # Timeout with no new entries — loop back.
                continue

            # response shape: [(stream_name, [(entry_id, fields), ...])]
            for _stream_name, entries in response:
                for entry_id, fields in entries:
                    await _process_entry(
                        redis, session_factory, group, consumer_name,
                        entry_id, fields, evaluator,
                    )

        except asyncio.CancelledError:
            log.info("consumer '%s' / group '%s' cancelled — shutting down", consumer_name, group)
            return
        except Exception as exc:
            log.error(
                "unexpected error in consumer '%s' / group '%s': %s — backing off %.1fs",
                consumer_name, group, exc, _ERROR_BACKOFF_S,
            )
            await asyncio.sleep(_ERROR_BACKOFF_S)


async def _process_entry(
    redis: Redis,
    session_factory: async_sessionmaker[AsyncSession],
    group: str,
    consumer_name: str,
    entry_id: str,
    fields: dict[str, str],
    evaluator,
) -> None:
    """Parse one stream entry, run the evaluator, then ACK."""
    try:
        parsed = _parse_entry(fields)
    except (KeyError, ValueError) as exc:
        log.error("malformed stream entry %s: %s — skipping", entry_id, exc)
        # ACK bad entries so they don't block the pending list forever.
        await redis.xack(settings.stream_name, group, entry_id)
        return

    try:
        async with session_factory() as db:
            await evaluator(db, redis, parsed)
    except Exception as exc:
        log.error(
            "evaluator error for entry %s in group '%s': %s — leaving pending",
            entry_id, group, exc,
        )
        # Do NOT ACK — entry remains pending and will be reclaimed on restart.
        return

    await redis.xack(settings.stream_name, group, entry_id)
    log.debug("ACKed entry %s in group '%s'", entry_id, group)


# ---------------------------------------------------------------------------
# Public task factories — called from the FastAPI lifespan
# ---------------------------------------------------------------------------

async def threshold_consumer(
    redis: Redis,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Consumer task for single-point THRESHOLD rule evaluation."""
    await _consume_loop(
        redis=redis,
        session_factory=session_factory,
        group=_CG_THRESHOLD,
        consumer_name="consumer-threshold",
        evaluator=_evaluate_threshold,
    )


async def window_consumer(
    redis: Redis,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Consumer task for WINDOW_AVERAGE rule evaluation."""
    await _consume_loop(
        redis=redis,
        session_factory=session_factory,
        group=_CG_WINDOW,
        consumer_name="consumer-window",
        evaluator=_evaluate_window,
    )
