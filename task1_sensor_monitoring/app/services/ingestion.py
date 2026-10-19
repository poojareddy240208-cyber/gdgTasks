"""Ingestion service — push validated telemetry into the Redis Stream.

All functions are intentionally thin: they only handle serialisation and
the XADD call.  Validation is the schema's job; alert evaluation is the
consumer worker's job.  This separation keeps each layer testable in
isolation.
"""

from __future__ import annotations

import asyncio
import logging

from redis.asyncio import Redis

from app.config import settings
from app.schemas.telemetry import BatchTelemetryPayload, TelemetryPayload

log = logging.getLogger(__name__)


async def push_to_stream(redis: Redis, payload: TelemetryPayload) -> str:
    """Serialise one reading and append it to the Redis Stream.

    Uses approximate MAXLEN trimming (``XADD ... MAXLEN ~ N``) to cap memory
    in O(1) without a blocking full-trim pass.

    Args:
        redis:   Async Redis client.
        payload: A fully-validated ``TelemetryPayload`` instance.

    Returns:
        The Redis Stream entry ID assigned by the server (e.g. ``"1714000000000-0"``).

    Raises:
        redis.exceptions.RedisError: Propagated on connection or command failure.
    """
    fields = payload.to_stream_fields()

    entry_id: str = await redis.xadd(
        settings.stream_name,
        fields,
        maxlen=settings.stream_max_len,
        approximate=True,  # MAXLEN ~ — O(1), avoids blocking trim
    )

    log.debug(
        "stream entry %s accepted sensor=%s metric=%s",
        entry_id,
        fields["sensor_id"],
        fields["metric_name"],
    )
    return entry_id


async def push_batch_to_stream(
    redis: Redis, batch: BatchTelemetryPayload
) -> tuple[int, list[str]]:
    """Push every reading in a batch to the Redis Stream concurrently.

    Fires all ``XADD`` commands as concurrent tasks so the round-trip latency
    of a 500-reading batch is bounded by the slowest single command, not the
    sum of all of them.

    Args:
        redis: Async Redis client.
        batch: A fully-validated ``BatchTelemetryPayload``.

    Returns:
        A tuple of ``(accepted_count, stream_ids)`` where ``accepted_count``
        is the number of successfully written entries and ``stream_ids`` is
        the list of Redis Stream entry IDs in submission order.  On partial
        failure the accepted count may be less than ``len(batch.readings)``.
    """
    tasks = [push_to_stream(redis, reading) for reading in batch.readings]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    stream_ids: list[str] = []
    failed = 0

    for i, result in enumerate(results):
        if isinstance(result, Exception):
            failed += 1
            log.error(
                "failed to push reading %d to stream: %s",
                i,
                result,
            )
        else:
            stream_ids.append(result)

    accepted = len(batch.readings) - failed
    if failed:
        log.warning(
            "batch ingestion partial: %d/%d readings accepted",
            accepted,
            len(batch.readings),
        )

    return accepted, stream_ids
