"""
mock_service/state.py
─────────────────────
In-process shared state for the fault-injecting mock notification server.

All request handlers share the same ``state`` singleton.  Access is
serialised by a ``threading.Lock`` so concurrent FastAPI worker threads
don't race on counters or the request log.

Fault modes
───────────
success          – HTTP 200 on every call (default after /reset).
server_error     – HTTP 500 on every call.
transient_error  – HTTP 500 for the first N calls, then HTTP 200.
                   N is set via POST /control {"transient_count": N}.
timeout          – Connection accepted; response never sent (relay client
                   will eventually time out and retry).
connection_drop  – TCP connection closed immediately, no response bytes.

Random fault injection (FAULT_RATE)
─────────────────────────────────────
When ``fault_rate`` > 0 **and** the current mode is ``success``, each
incoming /notify request has a ``fault_rate`` chance of randomly switching
to either a 500 error or a timeout for that single call only.  The mode is
NOT permanently changed.  This simulates real-world partial instability
without requiring any /control call.

Default ``fault_rate`` is 0.30 (30 %) as requested.  Override at startup
via the MOCK_FAULT_RATE environment variable.
"""

import dataclasses
import enum
import os
import threading


class FaultMode(str, enum.Enum):
    SUCCESS         = "success"
    SERVER_ERROR    = "server_error"
    TRANSIENT_ERROR = "transient_error"
    TIMEOUT         = "timeout"
    CONNECTION_DROP = "connection_drop"


@dataclasses.dataclass
class MockState:
    """
    Mutable runtime state shared across all request handlers.

    Fields
    ──────
    mode             – current fault mode; changed via POST /control.
    transient_count  – remaining 500 responses before auto-reset to SUCCESS.
    request_log      – every inbound /notify request, newest last.
    fault_rate       – probability [0.0, 1.0] of a random fault when mode
                       is SUCCESS.  Set once at startup from env var.
    _lock            – protects all mutable fields against concurrent writes.
    """

    mode: FaultMode = FaultMode.SUCCESS
    transient_count: int = 0
    request_log: list[dict] = dataclasses.field(default_factory=list)
    # Loaded from env at module init; not changed via /control.
    fault_rate: float = dataclasses.field(
        default_factory=lambda: float(os.getenv("MOCK_FAULT_RATE", "0.30"))
    )
    # In-memory idempotency cache {idempotency_key: response_dict}
    idempotency_cache: dict[str, dict] = dataclasses.field(default_factory=dict)
    _lock: threading.Lock = dataclasses.field(
        default_factory=threading.Lock, repr=False
    )

    def reset(self) -> None:
        """Clear the request log, idempotency cache, and return to SUCCESS mode."""
        with self._lock:
            self.mode = FaultMode.SUCCESS
            self.transient_count = 0
            self.request_log.clear()
            self.idempotency_cache.clear()


# Module-level singleton – import this in main.py
state = MockState()
