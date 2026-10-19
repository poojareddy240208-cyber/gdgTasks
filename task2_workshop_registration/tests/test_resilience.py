"""
tests/test_resilience.py
=========================
Crash-resilience integration test — "register 50, kill -9 mid-delivery, restart,
confirm all 50 arrive exactly once."

What this test proves
---------------------
1. **Transactional outbox durability** (REQ-OB-01, REQ-OB-04)
   Every Registration + OutboxEvent pair is written in a single DB commit.
   A SIGKILL between commit and notification delivery cannot lose either row.

2. **Crash recovery** (REQ-OB-04, design §7.5)
   On restart the relay worker finds all PENDING/PROCESSING outbox records
   and re-delivers them.  PostgreSQL rolls back PROCESSING rows whose
   connection died, returning them to the PENDING state automatically.

3. **At-least-once / exactly-once via idempotency key** (REQ-OB-05, REQ-OB-07)
   The mock server tracks every received Idempotency-Key.  After full
   recovery we assert:
     * total distinct idempotency keys  == 50  (every registration notified)
     * max delivery count per key       == 1   (no duplicate sends)

4. **All outbox records reach DELIVERED** (state machine completeness)
   Direct DB inspection confirms zero rows in PENDING or PROCESSING after
   the full recovery cycle.

Test infrastructure
--------------------
The test uses **Docker Compose** to manage all three services (db, app,
mock_service) so it is a genuine end-to-end integration test rather than
an in-process unit test.  It requires:

  * Docker Engine running on the host
  * docker compose v2 (plugin syntax)
  * The workspace root contains docker-compose.yml

Run with::

    pytest tests/test_resilience.py -v -s

Or directly::

    python tests/test_resilience.py

Environment variables
---------------------
  APP_BASE_URL       Default: http://localhost:8000
  MOCK_BASE_URL      Default: http://localhost:8001
  COMPOSE_FILE       Default: docker-compose.yml
  RECOVERY_TIMEOUT_S Default: 120   (seconds to wait for full re-delivery)
  POLL_INTERVAL_S    Default: 2

Architecture of the test
------------------------

  ┌──────────────────────────────────────────────────────────────────┐
  │ pytest session                                                    │
  │                                                                   │
  │  1. docker compose up -d  (db + mock_service + app)              │
  │  2. Wait for /health on app + mock_service                        │
  │  3. Create operator user + workshop via API                       │
  │  4. POST /registrations × 50   (atomic outbox writes)            │
  │  5. Wait briefly — let relay worker start some deliveries        │
  │  6. docker compose kill app    (SIGKILL — no graceful shutdown)   │
  │  7. Inspect DB: confirm all 50 Registration rows exist           │
  │                  confirm all 50 OutboxEvent rows exist            │
  │  8. docker compose start app   (restart)                          │
  │  9. Wait for /health on restarted app                             │
  │ 10. Poll until all 50 outbox records are DELIVERED                │
  │ 11. GET /requests on mock — assert idempotency (no duplicates)   │
  │ 12. docker compose down -v     (teardown)                         │
  └──────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import time
import uuid
from collections import Counter as PyCounter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
import pytest
import pytest_asyncio

# ---------------------------------------------------------------------------
# Configuration (all overridable via env vars)
# ---------------------------------------------------------------------------
APP_BASE_URL       = os.getenv("APP_BASE_URL",       "http://localhost:8000")
MOCK_BASE_URL      = os.getenv("MOCK_BASE_URL",      "http://localhost:8001")
COMPOSE_FILE       = os.getenv("COMPOSE_FILE",       "docker-compose.yml")
RECOVERY_TIMEOUT_S = int(os.getenv("RECOVERY_TIMEOUT_S", "120"))
POLL_INTERVAL_S    = float(os.getenv("POLL_INTERVAL_S",  "2"))
MEMBER_COUNT       = int(os.getenv("MEMBER_COUNT",       "50"))

# Project root (one level above tests/)
PROJECT_ROOT = Path(__file__).parent.parent.resolve()
COMPOSE_CMD  = ["docker", "compose", "-f", str(PROJECT_ROOT / COMPOSE_FILE)]


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def _run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    """Run a shell command, stream output, raise on non-zero exit."""
    print(f"\n[cmd] {' '.join(cmd)}", flush=True)
    result = subprocess.run(cmd, cwd=str(PROJECT_ROOT), **kwargs)
    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed (exit {result.returncode}): {' '.join(cmd)}"
        )
    return result


async def _wait_healthy(base_url: str, label: str, timeout_s: int = 60) -> None:
    """
    Poll ``GET {base_url}/health`` until it returns HTTP 200 or ``timeout_s``
    elapses.  Raises ``TimeoutError`` on expiry.
    """
    deadline = time.monotonic() + timeout_s
    last_exc: Exception | None = None
    async with httpx.AsyncClient(timeout=5.0) as client:
        while time.monotonic() < deadline:
            try:
                r = await client.get(f"{base_url}/health")
                if r.status_code == 200:
                    print(f"[health] {label} healthy after "
                          f"{timeout_s - (deadline - time.monotonic()):.0f}s",
                          flush=True)
                    return
            except Exception as exc:
                last_exc = exc
            await asyncio.sleep(POLL_INTERVAL_S)
    raise TimeoutError(
        f"{label} did not become healthy within {timeout_s}s. "
        f"Last error: {last_exc}"
    )


# ---------------------------------------------------------------------------
# Compose fixture — up once per session, down at the end
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def compose_up_down():
    """
    Session-scoped fixture that:
      1. Brings all Compose services up (build if needed).
      2. Yields.
      3. Tears everything down and removes volumes.

    Set SKIP_COMPOSE=1 to skip bring-up/tear-down (useful when services are
    already running from a previous debug session).
    """
    skip = os.getenv("SKIP_COMPOSE", "0") == "1"
    if not skip:
        print("\n[compose] Building and starting services …", flush=True)
        _run(COMPOSE_CMD + ["up", "-d", "--build", "--remove-orphans"])

    yield  # tests run here

    if not skip:
        print("\n[compose] Tearing down services …", flush=True)
        subprocess.run(
            COMPOSE_CMD + ["down", "-v", "--remove-orphans"],
            cwd=str(PROJECT_ROOT),
            check=False,  # best-effort — don't mask test failures
        )


# ---------------------------------------------------------------------------
# DB helper — direct asyncpg connection (bypasses the app process)
# ---------------------------------------------------------------------------

async def _db_query(sql: str, *args) -> list[dict]:
    """
    Run a raw SQL query against the test PostgreSQL instance.
    Uses the DATABASE_URL env var or the default docker-compose values.
    """
    import asyncpg  # type: ignore

    db_url = os.getenv(
        "DATABASE_URL",
        "postgresql://app:secret@localhost:5432/workshop_registration",
    ).replace("postgresql+asyncpg://", "postgresql://")

    conn = await asyncpg.connect(db_url)
    try:
        rows = await conn.fetch(sql, *args)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

async def _create_operator(client: httpx.AsyncClient) -> dict:
    """Insert an operator user directly via the DB (no auth endpoint exists)."""
    user_id = uuid.uuid4()
    await _db_query(
        """
        INSERT INTO users (id, email, full_name, role)
        VALUES ($1, $2, $3, 'OPERATOR')
        ON CONFLICT (email) DO NOTHING
        """,
        user_id,
        f"operator-{user_id}@test.com",
        "Test Operator",
    )
    # Re-fetch to get the actual UUID (ON CONFLICT DO NOTHING may have kept old)
    rows = await _db_query(
        "SELECT id FROM users WHERE email = $1",
        f"operator-{user_id}@test.com",
    )
    return {"id": str(rows[0]["id"])}


async def _create_workshop(
    client: httpx.AsyncClient, operator_id: str
) -> dict:
    """Create a workshop with 100-seat capacity via the API."""
    future = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
    r = await client.post(
        f"{APP_BASE_URL}/api/v1/workshops",
        json={
            "title": "Resilience Test Workshop",
            "scheduled_at": future,
            "venue": "Virtual / Crash Lab",
            "capacity": MEMBER_COUNT * 2,
        },
        headers={"X-User-Id": operator_id},
    )
    assert r.status_code == 201, f"Workshop creation failed: {r.text}"
    return r.json()


async def _create_attendees(count: int) -> list[dict]:
    """
    Bulk-insert attendee users directly into the DB.
    Returns list of {"id": str, "email": str} dicts.
    """
    import asyncpg  # type: ignore

    db_url = os.getenv(
        "DATABASE_URL",
        "postgresql://app:secret@localhost:5432/workshop_registration",
    ).replace("postgresql+asyncpg://", "postgresql://")

    conn = await asyncpg.connect(db_url)
    try:
        users: list[dict] = []
        for i in range(count):
            uid = uuid.uuid4()
            email = f"attendee-{uid}@test.com"
            await conn.execute(
                """
                INSERT INTO users (id, email, full_name, role)
                VALUES ($1, $2, $3, 'ATTENDEE')
                """,
                uid, email, f"Attendee {i+1}",
            )
            users.append({"id": str(uid), "email": email})
        return users
    finally:
        await conn.close()


async def _register_all(
    client: httpx.AsyncClient,
    workshop_id: str,
    attendees: list[dict],
) -> list[str]:
    """
    POST /registrations for every attendee concurrently.
    Returns list of registration IDs.
    """
    async def _one(attendee: dict) -> str:
        r = await client.post(
            f"{APP_BASE_URL}/api/v1/registrations",
            json={
                "user_id": attendee["id"],
                "workshop_id": workshop_id,
            },
        )
        assert r.status_code == 202, (
            f"Registration failed for {attendee['id']}: {r.status_code} {r.text}"
        )
        return r.json()["id"]

    registration_ids = await asyncio.gather(*[_one(a) for a in attendees])
    print(
        f"[register] {len(registration_ids)} registrations created",
        flush=True,
    )
    return list(registration_ids)


# ---------------------------------------------------------------------------
# Mock-service inspection helpers
# ---------------------------------------------------------------------------

async def _mock_get_requests(client: httpx.AsyncClient) -> list[dict]:
    r = await client.get(f"{MOCK_BASE_URL}/requests")
    assert r.status_code == 200
    return r.json()["requests"]


async def _mock_reset(client: httpx.AsyncClient) -> None:
    r = await client.post(f"{MOCK_BASE_URL}/reset")
    assert r.status_code == 200


async def _mock_set_success(client: httpx.AsyncClient) -> None:
    """Ensure mock is in success mode with fault_rate=0 for clean assertions."""
    r = await client.post(
        f"{MOCK_BASE_URL}/control",
        json={"mode": "success", "transient_count": 0},
    )
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# DB assertion helpers
# ---------------------------------------------------------------------------

async def _count_registrations(workshop_id: str) -> int:
    rows = await _db_query(
        "SELECT COUNT(*) AS n FROM registrations WHERE workshop_id = $1::uuid",
        workshop_id,
    )
    return rows[0]["n"]


async def _outbox_statuses(workshop_id: str) -> dict[str, int]:
    """
    Returns a dict mapping outbox status → count for all outbox events
    whose payload references the given workshop_id.
    """
    rows = await _db_query(
        """
        SELECT status, COUNT(*) AS n
        FROM   outbox_events
        WHERE  payload->>'workshop_id' = $1
        GROUP  BY status
        """,
        workshop_id,
    )
    return {r["status"]: r["n"] for r in rows}


async def _wait_all_delivered(
    workshop_id: str,
    expected: int,
    timeout_s: int = RECOVERY_TIMEOUT_S,
) -> dict[str, int]:
    """
    Poll the outbox_events table until all ``expected`` records for the
    given workshop reach DELIVERED, or raise TimeoutError.
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        statuses = await _outbox_statuses(workshop_id)
        delivered = statuses.get("DELIVERED", 0)
        pending   = statuses.get("PENDING",   0)
        processing = statuses.get("PROCESSING", 0)
        failed    = statuses.get("FAILED",    0)
        print(
            f"[outbox] DELIVERED={delivered} PENDING={pending} "
            f"PROCESSING={processing} FAILED={failed} / expected={expected}",
            flush=True,
        )
        if delivered == expected and pending == 0 and processing == 0:
            return statuses
        if failed > 0:
            raise AssertionError(
                f"{failed} outbox record(s) reached FAILED state — "
                "check MAX_NOTIFICATION_RETRIES and mock server mode."
            )
        await asyncio.sleep(POLL_INTERVAL_S)
    final = await _outbox_statuses(workshop_id)
    raise TimeoutError(
        f"Outbox not fully delivered after {timeout_s}s. Final statuses: {final}"
    )


# ===========================================================================
# THE MAIN RESILIENCE TEST
# ===========================================================================

@pytest.mark.asyncio
async def test_crash_resilience_50_members(compose_up_down):
    """
    End-to-end crash-resilience test.

    Scenario
    --------
    1.  Services start fresh.
    2.  50 attendees are registered atomically (Registration + OutboxEvent
        committed together in one DB transaction each).
    3.  The app process is SIGKILL-ed while the relay worker is mid-delivery.
    4.  DB inspection confirms all 50 Registration rows and 50 OutboxEvent
        rows survived the crash (transactional outbox durability).
    5.  App restarts, relay worker picks up undelivered/PROCESSING records
        (PostgreSQL auto-reverts PROCESSING rows on connection drop).
    6.  All 50 OutboxEvent rows reach DELIVERED.
    7.  Mock service request log shows each idempotency key appeared at most
        once — no duplicate notifications were sent.

    Assertions
    ----------
    - 50 Registration rows exist in the DB (no lost registrations)
    - All 50 OutboxEvent rows reach DELIVERED (no lost notifications)
    - No OutboxEvent row reaches FAILED (mock is in success mode)
    - Each Idempotency-Key header value appears exactly once across all
      notify calls received by the mock (at-most-once delivery)
    """
    async with httpx.AsyncClient(timeout=30.0) as client:

        # ── Step 1: Wait for all services to be healthy ───────────────────
        print("\n[step 1] Waiting for services to become healthy …", flush=True)
        await asyncio.gather(
            _wait_healthy(APP_BASE_URL,  "app",          timeout_s=90),
            _wait_healthy(MOCK_BASE_URL, "mock_service", timeout_s=90),
        )

        # ── Step 2: Reset mock, ensure success mode (fault_rate=0) ────────
        print("[step 2] Resetting mock service …", flush=True)
        await _mock_reset(client)
        await _mock_set_success(client)

        # ── Step 3: Create test data ──────────────────────────────────────
        print("[step 3] Creating operator, workshop, and attendees …", flush=True)
        operator = await _create_operator(client)
        workshop = await _create_workshop(client, operator["id"])
        workshop_id = workshop["id"]
        print(f"         workshop_id = {workshop_id}", flush=True)

        attendees = await _create_attendees(MEMBER_COUNT)
        print(f"         created {len(attendees)} attendees", flush=True)

        # ── Step 4: Register all 50 attendees concurrently ────────────────
        print(f"[step 4] Registering {MEMBER_COUNT} attendees …", flush=True)
        reg_ids = await _register_all(client, workshop_id, attendees)
        assert len(reg_ids) == MEMBER_COUNT, (
            f"Expected {MEMBER_COUNT} registrations, got {len(reg_ids)}"
        )

        # ── Step 5: Wait briefly so relay worker starts some deliveries ───
        #    We want the kill to happen while some are PROCESSING but not all
        #    DELIVERED — simulates a mid-flight crash.
        print("[step 5] Waiting 3 s for relay worker to start …", flush=True)
        await asyncio.sleep(3)

        # Snapshot: how many delivered before kill?
        pre_kill_statuses = await _outbox_statuses(workshop_id)
        pre_kill_delivered = pre_kill_statuses.get("DELIVERED", 0)
        print(
            f"         Pre-kill statuses: {pre_kill_statuses}  "
            f"({pre_kill_delivered} already delivered)",
            flush=True,
        )

        # ── Step 6: SIGKILL the app process ──────────────────────────────
        print("[step 6] Sending SIGKILL to app service …", flush=True)
        _run(COMPOSE_CMD + ["kill", "-s", "SIGKILL", "app"])
        print("         App killed.", flush=True)

        # Brief pause so the OS cleans up the dead connection and PostgreSQL
        # rolls back in-flight PROCESSING rows.
        await asyncio.sleep(2)

        # ── Step 7: DB consistency check after kill ───────────────────────
        print("[step 7] Verifying DB state after kill …", flush=True)
        reg_count = await _count_registrations(workshop_id)
        assert reg_count == MEMBER_COUNT, (
            f"Expected {MEMBER_COUNT} registrations after kill, found {reg_count}. "
            "Registrations were lost — transactional outbox is broken."
        )

        post_kill_statuses = await _outbox_statuses(workshop_id)
        total_outbox = sum(post_kill_statuses.values())
        assert total_outbox == MEMBER_COUNT, (
            f"Expected {MEMBER_COUNT} outbox records after kill, found {total_outbox}. "
            "OutboxEvent rows were lost — transactional outbox is broken."
        )
        # No row should be stuck in PROCESSING after PG connection reset
        processing_count = post_kill_statuses.get("PROCESSING", 0)
        assert processing_count == 0, (
            f"{processing_count} row(s) still in PROCESSING after kill + 2s sleep. "
            "PostgreSQL did not auto-rollback — check connection timeout settings."
        )
        print(
            f"         Post-kill statuses: {post_kill_statuses}  ✓",
            flush=True,
        )

        # ── Step 8: Restart the app ───────────────────────────────────────
        print("[step 8] Restarting app service …", flush=True)
        _run(COMPOSE_CMD + ["start", "app"])

        # ── Step 9: Wait for healthy restart ─────────────────────────────
        print("[step 9] Waiting for app to become healthy again …", flush=True)
        await _wait_healthy(APP_BASE_URL, "app (restarted)", timeout_s=60)

        # ── Step 10: Wait for all outbox records to reach DELIVERED ───────
        print(
            f"[step 10] Waiting up to {RECOVERY_TIMEOUT_S}s for all "
            f"{MEMBER_COUNT} outbox records to be DELIVERED …",
            flush=True,
        )
        final_statuses = await _wait_all_delivered(
            workshop_id, MEMBER_COUNT, timeout_s=RECOVERY_TIMEOUT_S
        )
        print(f"          Final outbox statuses: {final_statuses}  ✓", flush=True)

        # ── Step 11: Idempotency assertion — no duplicate sends ───────────
        print("[step 11] Checking mock request log for duplicates …", flush=True)
        mock_requests = await _mock_get_requests(client)

        # Collect all Idempotency-Key header values seen by the mock
        idem_keys: list[str] = []
        for req in mock_requests:
            # Headers dict may have mixed case
            headers = {k.lower(): v for k, v in req.get("headers", {}).items()}
            key = headers.get("idempotency-key", "")
            if key:
                idem_keys.append(key)

        key_counts = PyCounter(idem_keys)
        duplicates = {k: c for k, c in key_counts.items() if c > 1}
        total_distinct = len(key_counts)

        print(
            f"          Mock received {len(mock_requests)} notify calls, "
            f"{total_distinct} distinct idempotency keys.",
            flush=True,
        )

        assert total_distinct == MEMBER_COUNT, (
            f"Expected {MEMBER_COUNT} distinct idempotency keys, "
            f"got {total_distinct}. "
            f"Missing notifications: {MEMBER_COUNT - total_distinct}"
        )

        assert not duplicates, (
            f"Duplicate notification deliveries detected for "
            f"{len(duplicates)} key(s): {duplicates}. "
            "Idempotency via Idempotency-Key is not working correctly."
        )

        print(
            f"\n[PASS] All {MEMBER_COUNT} registrations delivered exactly once "
            f"after a SIGKILL crash. ✓",
            flush=True,
        )


# ===========================================================================
# Supplementary: outbox state after normal operation (no crash)
# ===========================================================================

@pytest.mark.asyncio
async def test_outbox_delivered_without_crash(compose_up_down):
    """
    Baseline test: register 5 members without any crash and confirm all
    outbox records reach DELIVERED within the normal polling window.

    This test runs in the same Compose session as the crash test.  The
    compose_up_down fixture is session-scoped so services are already up.
    """
    member_count = 5

    async with httpx.AsyncClient(timeout=30.0) as client:
        await _wait_healthy(APP_BASE_URL,  "app",          timeout_s=30)
        await _wait_healthy(MOCK_BASE_URL, "mock_service", timeout_s=30)

        await _mock_reset(client)
        await _mock_set_success(client)

        operator = await _create_operator(client)
        workshop = await _create_workshop(client, operator["id"])
        workshop_id = workshop["id"]

        attendees = await _create_attendees(member_count)
        await _register_all(client, workshop_id, attendees)

        final = await _wait_all_delivered(
            workshop_id, member_count, timeout_s=60
        )
        assert final.get("DELIVERED", 0) == member_count
        assert final.get("FAILED", 0) == 0

        reqs = await _mock_get_requests(client)
        keys = [
            dict(r["headers"]).get("idempotency-key", "")
            for r in reqs
            if dict(r["headers"]).get("idempotency-key")
        ]
        assert len(set(keys)) == member_count, (
            f"Expected {member_count} distinct keys, got {len(set(keys))}"
        )
        print(
            f"\n[PASS] Baseline: {member_count} registrations all delivered "
            f"without crash. ✓",
            flush=True,
        )


# ===========================================================================
# Supplementary: PROCESSING rows recover after simulated stale lock
# ===========================================================================

@pytest.mark.asyncio
async def test_stale_processing_rows_recover(compose_up_down):
    """
    Verify that rows stuck in PROCESSING (e.g. left by a previous crashed
    worker) are correctly reclaimed and delivered on the next relay tick.

    This test directly injects a PROCESSING row into the outbox table
    without holding a PG lock (simulating what PostgreSQL does after it
    detects a dead connection — it reverts to PENDING, but here we test
    the case where the row is already PENDING after such a rollback).

    Actual mechanism tested:
      - Insert an OutboxEvent with status=PENDING and a past last_attempted_at
      - The relay worker must pick it up within the next poll cycle
      - It must reach DELIVERED
    """
    async with httpx.AsyncClient(timeout=30.0) as client:
        await _wait_healthy(APP_BASE_URL,  "app",          timeout_s=30)
        await _wait_healthy(MOCK_BASE_URL, "mock_service", timeout_s=30)

        await _mock_reset(client)
        await _mock_set_success(client)

        operator = await _create_operator(client)
        workshop = await _create_workshop(client, operator["id"])
        workshop_id = workshop["id"]

        # Create one attendee and register them (triggers outbox write)
        attendees = await _create_attendees(1)
        await _register_all(client, workshop_id, attendees)

        # Manually set the outbox row to look like it was stranded mid-delivery
        await _db_query(
            """
            UPDATE outbox_events
            SET    status = 'PENDING',
                   last_attempted_at = NOW() - INTERVAL '10 minutes'
            WHERE  payload->>'workshop_id' = $1
            """,
            workshop_id,
        )
        print("[stale] Injected stale PENDING row (last_attempted_at = -10 min)",
              flush=True)

        final = await _wait_all_delivered(workshop_id, 1, timeout_s=60)
        assert final.get("DELIVERED", 0) == 1

        print("\n[PASS] Stale PENDING row recovered and delivered. ✓", flush=True)


# ===========================================================================
# Entry point — run directly with python tests/test_resilience.py
# ===========================================================================

if __name__ == "__main__":
    import pytest as _pytest
    sys.exit(_pytest.main([__file__, "-v", "-s", "--tb=short"]))
