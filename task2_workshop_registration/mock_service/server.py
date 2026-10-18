"""
mock_service/server.py
──────────────────────
Fault-injecting mock notification server.

Purpose
───────
Acts as a drop-in replacement for the real external notification service
during local development and automated testing.  It lets the relay worker
exercise all retry / back-off code paths without any real email or push
infrastructure.

Random fault injection on /api/v1/notify
─────────────────────────────────────────
When the current mode is ``success`` AND ``state.fault_rate`` > 0, each
incoming notify request is randomly faulted with probability ``fault_rate``.
The random fault is either a 500 error or a timeout, chosen with equal
probability.  This simulates a partially unstable downstream service.

Default ``fault_rate`` is **0.30** (30 %).  Override with env var:
    MOCK_FAULT_RATE=0.0   # always succeed
    MOCK_FAULT_RATE=1.0   # always fault

Explicit fault modes (via POST /control) take complete precedence over the
random fault rate.  Random faults only fire in SUCCESS mode.

Endpoints
─────────
POST  /api/v1/notify   ← main notification receiver; random/explicit faults
POST  /control         ← switch fault mode at runtime
POST  /reset           ← clear log + return to SUCCESS
GET   /requests        ← inspect logged delivery attempts
GET   /health          ← always 200; liveness probe

Run standalone
──────────────
    uvicorn mock_service.server:app --port 8001 --reload

Or via docker-compose (see docker-compose.yml service: mock_service).
"""

import asyncio
import random
from datetime import datetime, timezone

import structlog
from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel, Field

from mock_service.state import FaultMode, MockState, state

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(),
    ]
)
logger = structlog.get_logger("mock_service")

# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Mock Notification Service",
    description=(
        "Fault-injecting test double for the external notification service. "
        "Supports configurable error modes and an inspectable request log."
    ),
    version="1.0.0",
)

# ---------------------------------------------------------------------------
# Helper: decide effective fault action for this request
# ---------------------------------------------------------------------------
_RANDOM_FAULTS = (FaultMode.SERVER_ERROR, FaultMode.TIMEOUT)


def _effective_fault(s: MockState) -> FaultMode | None:
    """
    Return the fault action to apply for this specific request, or None for
    a clean 200 response.

    Logic
    ─────
    1. If mode is explicitly non-SUCCESS (set via /control), always honour it.
    2. If mode is SUCCESS and fault_rate > 0, apply a random fault with the
       configured probability.
    3. Otherwise return None (success).
    """
    mode = s.mode

    if mode == FaultMode.SERVER_ERROR:
        return FaultMode.SERVER_ERROR

    if mode == FaultMode.TIMEOUT:
        return FaultMode.TIMEOUT

    if mode == FaultMode.CONNECTION_DROP:
        return FaultMode.CONNECTION_DROP

    if mode == FaultMode.TRANSIENT_ERROR:
        # Handled inline in the route; return the mode so the route knows.
        return FaultMode.TRANSIENT_ERROR

    # SUCCESS mode — apply random fault if configured
    if mode == FaultMode.SUCCESS and s.fault_rate > 0.0:
        if random.random() < s.fault_rate:
            # Equal chance of a 500 or a timeout
            return random.choice(_RANDOM_FAULTS)

    return None  # clean success


# ---------------------------------------------------------------------------
# POST /api/v1/notify
# ---------------------------------------------------------------------------
@app.post(
    "/api/v1/notify",
    summary="Receive a notification",
    description=(
        "Main notification endpoint.  30 % of requests randomly return "
        "HTTP 500 or simulate a timeout when the server is in SUCCESS mode.  "
        "Use POST /control to override fault behaviour explicitly."
    ),
    status_code=200,
)
async def receive_notification(request: Request) -> dict:
    """
    Accept a notification payload from the relay worker.

    Request body
    ────────────
    Any JSON object.  The mock stores the full body in the request log so
    tests can assert on the exact payload delivered.

    Response (success)
    ──────────────────
    {
        "status":          "accepted",
        "idempotency_key": "<value from Idempotency-Key header or null>",
        "event_type":      "<value from X-Event-Type header or null>"
    }

    Fault behaviour
    ───────────────
    See module docstring.  Random faults fire here only.
    """
    # --- Read and log the request before applying any fault -----------------
    try:
        body = await request.json()
    except Exception:
        body = None

    idempotency_key = request.headers.get("idempotency-key")
    event_type = request.headers.get("x-event-type")

    log_entry: dict = {
        "timestamp":       datetime.now(timezone.utc).isoformat(),
        "method":          request.method,
        "path":            str(request.url.path),
        "headers":         dict(request.headers),
        "body":            body,
        "idempotency_key": idempotency_key,
        "event_type":      event_type,
    }

    with state._lock:
        state.request_log.append(log_entry)
        # Capture values under lock to avoid TOCTOU
        current_mode = state.mode
        transient_remaining = state.transient_count

    logger.info(
        "notify.received",
        mode=current_mode,
        idempotency_key=idempotency_key,
        event_type=event_type,
    )

    # --- Determine effective fault -------------------------------------------
    # TRANSIENT_ERROR is stateful (counter), so handle it directly here.
    if current_mode == FaultMode.TRANSIENT_ERROR:
        with state._lock:
            if state.transient_count > 0:
                state.transient_count -= 1
                remaining_after = state.transient_count
                if remaining_after == 0:
                    state.mode = FaultMode.SUCCESS
                    logger.info("transient_error.exhausted", switched_to="success")
            else:
                # Counter already at 0 — treat as success
                current_mode = FaultMode.SUCCESS

        if current_mode == FaultMode.TRANSIENT_ERROR:
            logger.warning("notify.transient_error", remaining=remaining_after)
            raise HTTPException(
                status_code=500,
                detail={
                    "error": {
                        "code":    "TRANSIENT_ERROR",
                        "message": "Simulated transient server error.",
                    }
                },
            )

    fault = _effective_fault(state)

    if fault is None or current_mode == FaultMode.TRANSIENT_ERROR:
        # Already handled above or no fault — fall through to success
        pass
    elif fault == FaultMode.SERVER_ERROR:
        logger.warning(
            "notify.server_error",
            random=(current_mode == FaultMode.SUCCESS),
        )
        raise HTTPException(
            status_code=500,
            detail={
                "error": {
                    "code":    "SERVER_ERROR",
                    "message": "Simulated server error.",
                }
            },
        )
    elif fault == FaultMode.TIMEOUT:
        logger.warning(
            "notify.timeout",
            random=(current_mode == FaultMode.SUCCESS),
        )
        # Sleep longer than any reasonable client timeout.  The relay worker's
        # httpx client will raise ReadTimeout and treat it as a failed attempt.
        await asyncio.sleep(3600)
        # If somehow we wake up (e.g. test teardown shortens the sleep), still
        # return a 500 so the caller retries.
        raise HTTPException(status_code=500, detail={"error": {"code": "TIMEOUT_WOKE"}})
    elif fault == FaultMode.CONNECTION_DROP:
        logger.warning("notify.connection_drop")
        # Raise an unhandled exception; Uvicorn closes the connection without
        # sending any HTTP response bytes.
        raise RuntimeError("Simulated connection drop — no response sent.")

    # --- Success path --------------------------------------------------------
    logger.info(
        "notify.accepted",
        idempotency_key=idempotency_key,
        event_type=event_type,
    )
    return {
        "status":          "accepted",
        "idempotency_key": idempotency_key,
        "event_type":      event_type,
    }


# ---------------------------------------------------------------------------
# POST /control  — switch fault mode at runtime
# ---------------------------------------------------------------------------
class ControlRequest(BaseModel):
    mode: FaultMode = Field(
        ...,
        description=(
            "Fault mode to activate. "
            "'transient_error' requires transient_count > 0."
        ),
    )
    transient_count: int = Field(
        default=0,
        ge=0,
        description=(
            "Number of consecutive 500 responses before auto-switching back to "
            "success.  Only meaningful when mode='transient_error'."
        ),
    )

    model_config = {"json_schema_extra": {"example": {"mode": "transient_error", "transient_count": 3}}}


@app.post(
    "/control",
    summary="Set fault mode",
    description="Switch the server's fault behaviour without restarting it.",
    status_code=200,
)
async def set_fault_mode(body: ControlRequest) -> dict:
    """
    Change the active fault mode.

    Examples
    ────────
    Always fail:
        {"mode": "server_error"}

    Fail 3 times then succeed:
        {"mode": "transient_error", "transient_count": 3}

    Never respond (relay client times out):
        {"mode": "timeout"}

    Close connection immediately:
        {"mode": "connection_drop"}

    Return to normal:
        {"mode": "success"}
    """
    with state._lock:
        state.mode = body.mode
        state.transient_count = body.transient_count

    logger.info("control.mode_set", mode=body.mode, transient_count=body.transient_count)
    return {
        "mode":             state.mode,
        "transient_count":  state.transient_count,
        "fault_rate":       state.fault_rate,
    }


# ---------------------------------------------------------------------------
# POST /reset  — test isolation helper
# ---------------------------------------------------------------------------
@app.post(
    "/reset",
    summary="Reset to defaults",
    description=(
        "Clear the request log and return the fault mode to 'success'. "
        "Call this in a test fixture's teardown to ensure test isolation."
    ),
    status_code=200,
)
async def reset() -> dict:
    state.reset()
    logger.info("reset.done")
    return {"status": "reset", "mode": state.mode}


# ---------------------------------------------------------------------------
# GET /requests  — test assertion helper
# ---------------------------------------------------------------------------
@app.get(
    "/requests",
    summary="List logged requests",
    description=(
        "Return all notification requests received since the last /reset. "
        "Use in tests to assert on delivery counts, headers, and payloads."
    ),
)
async def get_requests() -> dict:
    with state._lock:
        log_copy = list(state.request_log)
    return {"count": len(log_copy), "requests": log_copy}


# ---------------------------------------------------------------------------
# GET /health  — liveness probe
# ---------------------------------------------------------------------------
@app.get(
    "/health",
    summary="Liveness probe",
    description="Always returns 200. Used by Docker healthcheck and load balancers.",
)
async def health() -> dict:
    return {
        "status":          "ok",
        "mode":            state.mode,
        "fault_rate":      state.fault_rate,
        "requests_logged": len(state.request_log),
    }
