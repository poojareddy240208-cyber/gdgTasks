"""High-concurrency load generator for the Campus Sensor Monitoring API.

Goal
----
Sustain a burst of 10,000 requests per second against
POST /api/v1/telemetry (single) or POST /api/v1/telemetry/batch.

Strategy
---------
- A fixed-size semaphore caps the number of in-flight requests at any moment,
  preventing the local OS from running out of file descriptors.
- A token-bucket rate limiter issues tokens at the target RPS and blocks
  senders when the bucket is empty, so we never fire faster than requested.
- Requests are generated and dispatched as asyncio tasks.  aiohttp reuses a
  single TCPConnector with connection pooling to minimise per-request overhead.
- A rolling stats collector (updated without locks via asyncio single-thread
  guarantee) prints a live progress line and a final summary.

Usage examples
--------------
# 10 000 single readings, max 10k/sec, 200 concurrent:
python simulator/load_generator.py --count 10000 --rps 10000 --concurrency 200

# 500 batches of 20 readings each (10 000 readings total), capped at 1k/sec:
python simulator/load_generator.py --count 500 --batch-size 20 --rps 1000

# Dry-run: print sample payloads without sending:
python simulator/load_generator.py --count 5 --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import datetime
import json
import logging
import random
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

import aiohttp

log = logging.getLogger(__name__)

# ── Sensor catalogue ──────────────────────────────────────────────────────────
# Fixed pool of sensor UUIDs so some readings share the same sensor,
# which exercises the window-average deduplication logic.
_SENSOR_POOL: list[str] = [str(uuid.uuid4()) for _ in range(50)]

_METRICS: list[tuple[str, float, float]] = [
    # (metric_name, min_value, max_value)
    ("temperature",    15.0,  45.0),
    ("humidity",        0.0, 100.0),
    ("co2_ppm",       350.0, 5000.0),
    ("noise_db",       30.0, 110.0),
    ("occupancy_pct",   0.0, 100.0),
]

_CAMPUS_BOUNDS = {
    "lat_min": 12.9000, "lat_max": 13.0000,
    "lon_min": 77.5000, "lon_max": 77.6500,
}


# ── Payload factories ─────────────────────────────────────────────────────────

def _random_reading() -> dict[str, Any]:
    """Generate one random sensor reading that will pass schema validation."""
    metric_name, lo, hi = random.choice(_METRICS)
    return {
        "sensor_id": random.choice(_SENSOR_POOL),
        "metric_name": metric_name,
        "value": round(random.uniform(lo, hi), 4),
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "location": {
            "lat": round(random.uniform(
                _CAMPUS_BOUNDS["lat_min"], _CAMPUS_BOUNDS["lat_max"]), 6),
            "lon": round(random.uniform(
                _CAMPUS_BOUNDS["lon_min"], _CAMPUS_BOUNDS["lon_max"]), 6),
        },
    }


def _build_single_payload() -> dict[str, Any]:
    return _random_reading()


def _build_batch_payload(batch_size: int) -> dict[str, Any]:
    return {"readings": [_random_reading() for _ in range(batch_size)]}


# ── Statistics ────────────────────────────────────────────────────────────────

@dataclass
class Stats:
    sent: int = 0
    accepted: int = 0       # HTTP 202
    client_errors: int = 0  # HTTP 4xx
    server_errors: int = 0  # HTTP 5xx
    network_errors: int = 0 # aiohttp / timeout exceptions
    total_latency_s: float = 0.0
    min_latency_s: float = float("inf")
    max_latency_s: float = 0.0
    start_time: float = field(default_factory=time.monotonic)

    def record(self, status: int, latency_s: float) -> None:
        self.sent += 1
        self.total_latency_s += latency_s
        self.min_latency_s = min(self.min_latency_s, latency_s)
        self.max_latency_s = max(self.max_latency_s, latency_s)
        if status == 202:
            self.accepted += 1
        elif 400 <= status < 500:
            self.client_errors += 1
        else:
            self.server_errors += 1

    def record_network_error(self) -> None:
        self.sent += 1
        self.network_errors += 1

    @property
    def elapsed(self) -> float:
        return time.monotonic() - self.start_time

    @property
    def rps(self) -> float:
        return self.sent / self.elapsed if self.elapsed > 0 else 0.0

    @property
    def avg_latency_ms(self) -> float:
        if self.accepted == 0:
            return 0.0
        return (self.total_latency_s / self.accepted) * 1000

    def progress_line(self) -> str:
        return (
            f"\r  sent={self.sent:>7}  202={self.accepted:>7}"
            f"  4xx={self.client_errors}  5xx={self.server_errors}"
            f"  err={self.network_errors}"
            f"  rps={self.rps:>8.0f}"
            f"  avg_ms={self.avg_latency_ms:>6.1f}"
        )

    def summary(self) -> str:
        success_pct = (self.accepted / self.sent * 100) if self.sent else 0
        min_ms = self.min_latency_s * 1000 if self.min_latency_s != float("inf") else 0
        return (
            "\n── Load Test Summary ──────────────────────────────\n"
            f"  Total sent      : {self.sent}\n"
            f"  202 Accepted    : {self.accepted}  ({success_pct:.1f}%)\n"
            f"  4xx errors      : {self.client_errors}\n"
            f"  5xx errors      : {self.server_errors}\n"
            f"  Network errors  : {self.network_errors}\n"
            f"  Elapsed         : {self.elapsed:.2f}s\n"
            f"  Throughput      : {self.rps:.0f} req/s\n"
            f"  Latency avg     : {self.avg_latency_ms:.1f} ms\n"
            f"  Latency min     : {min_ms:.1f} ms\n"
            f"  Latency max     : {self.max_latency_s * 1000:.1f} ms\n"
            "───────────────────────────────────────────────────"
        )


# ── Token bucket rate limiter ─────────────────────────────────────────────────

class TokenBucket:
    """Simple async token-bucket rate limiter.

    Adds ``rate`` tokens per second.  ``acquire()`` suspends the caller
    until a token is available.
    """

    def __init__(self, rate: float) -> None:
        self._rate = rate            # tokens per second
        self._tokens = rate          # start full
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self._last_refill
                self._tokens = min(
                    self._rate,
                    self._tokens + elapsed * self._rate,
                )
                self._last_refill = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait = (1.0 - self._tokens) / self._rate

            await asyncio.sleep(wait)


# ── Request worker ────────────────────────────────────────────────────────────

async def _send_request(
    session: aiohttp.ClientSession,
    url: str,
    payload: dict[str, Any],
    stats: Stats,
    semaphore: asyncio.Semaphore,
) -> None:
    """Send one request and record the outcome in *stats*."""
    async with semaphore:
        t0 = time.monotonic()
        try:
            async with session.post(url, json=payload) as resp:
                latency = time.monotonic() - t0
                # Drain body to free the connection back to the pool promptly.
                await resp.read()
                stats.record(resp.status, latency)
                if resp.status not in (202,) and log.isEnabledFor(logging.DEBUG):
                    log.debug("non-202 response %d for %s", resp.status, url)
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            stats.record_network_error()
            log.debug("request error: %s", exc)


# ── Progress reporter ─────────────────────────────────────────────────────────

async def _progress_reporter(stats: Stats, total: int, interval: float = 1.0) -> None:
    """Print a live progress line every *interval* seconds."""
    while stats.sent < total:
        print(stats.progress_line(), end="", flush=True)
        await asyncio.sleep(interval)
    # Final overwrite with completed state.
    print(stats.progress_line(), end="", flush=True)


# ── Main orchestrator ─────────────────────────────────────────────────────────

async def run(
    base_url: str,
    count: int,
    rps: float,
    concurrency: int,
    batch_size: int,
    timeout_s: float,
    dry_run: bool,
) -> Stats:
    """Drive the load test and return the final Stats object.

    Args:
        base_url    : Target API root, e.g. ``http://localhost:8000``.
        count       : Number of HTTP requests to send.
        rps         : Maximum requests per second (token bucket).
        concurrency : Maximum in-flight requests at any moment.
        batch_size  : Readings per request. 1 = single endpoint,
                      >1 = batch endpoint.
        timeout_s   : Per-request timeout in seconds.
        dry_run     : If True, print sample payloads and exit without sending.
    """
    if batch_size > 1:
        url = f"{base_url.rstrip('/')}/api/v1/telemetry/batch"
        payload_factory = lambda: _build_batch_payload(batch_size)  # noqa: E731
    else:
        url = f"{base_url.rstrip('/')}/api/v1/telemetry"
        payload_factory = _build_single_payload  # type: ignore[assignment]

    if dry_run:
        sample = payload_factory()
        print("── Dry run — sample payload (" + url + ") ──")
        print(json.dumps(sample, indent=2))
        print(f"\nWould send {count} requests at {rps} req/s "
              f"with concurrency={concurrency}")
        return Stats()

    connector = aiohttp.TCPConnector(
        limit=concurrency,
        limit_per_host=concurrency,
        # Reuse connections aggressively to minimise TCP handshake overhead.
        enable_cleanup_closed=True,
    )
    timeout = aiohttp.ClientTimeout(total=timeout_s)

    stats = Stats(start_time=time.monotonic())
    bucket = TokenBucket(rate=rps)
    semaphore = asyncio.Semaphore(concurrency)

    print(f"\nStarting load test → {url}")
    print(f"  count={count}  rps={rps}  concurrency={concurrency}"
          f"  batch_size={batch_size}  timeout={timeout_s}s\n")

    async with aiohttp.ClientSession(
        connector=connector, timeout=timeout
    ) as session:
        reporter_task = asyncio.create_task(
            _progress_reporter(stats, count),
            name="progress-reporter",
        )

        tasks: list[asyncio.Task] = []
        for _ in range(count):
            await bucket.acquire()
            payload = payload_factory()
            task = asyncio.create_task(
                _send_request(session, url, payload, stats, semaphore)
            )
            tasks.append(task)

        # Wait for all in-flight requests to complete.
        await asyncio.gather(*tasks)
        reporter_task.cancel()
        try:
            await reporter_task
        except asyncio.CancelledError:
            pass

    return stats


# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="load_generator",
        description="High-concurrency load generator for the sensor monitoring API.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--url",
        default="http://localhost:8000",
        help="Base URL of the API server.",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=10_000,
        help="Total number of HTTP requests to send.",
    )
    parser.add_argument(
        "--rps",
        type=float,
        default=10_000.0,
        help="Target requests per second (token-bucket limited).",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=200,
        help="Maximum simultaneous in-flight requests.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1,
        help=(
            "Readings per request. "
            "1 = POST /api/v1/telemetry, "
            ">1 = POST /api/v1/telemetry/batch."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="Per-request timeout in seconds.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print a sample payload and exit without sending any requests.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable DEBUG logging (logs every non-202 response).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(levelname)s  %(name)s  %(message)s",
        stream=sys.stderr,
    )

    if args.batch_size < 1:
        sys.exit("--batch-size must be >= 1")
    if args.batch_size > 500:
        sys.exit("--batch-size must be <= 500 (API limit)")
    if args.concurrency < 1:
        sys.exit("--concurrency must be >= 1")

    stats = asyncio.run(
        run(
            base_url=args.url,
            count=args.count,
            rps=args.rps,
            concurrency=args.concurrency,
            batch_size=args.batch_size,
            timeout_s=args.timeout,
            dry_run=args.dry_run,
        )
    )

    if not args.dry_run:
        print(stats.summary())

        # Exit non-zero if error rate exceeds 1%.
        if stats.sent > 0:
            error_rate = (stats.client_errors + stats.server_errors + stats.network_errors) / stats.sent
            if error_rate > 0.01:
                sys.exit(
                    f"Error rate {error_rate:.1%} exceeds 1% threshold — "
                    "check server logs."
                )


if __name__ == "__main__":
    main()
