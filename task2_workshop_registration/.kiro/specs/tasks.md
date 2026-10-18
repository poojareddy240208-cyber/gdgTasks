# Implementation Tasks — Task 2: Workshop Registration and Notifications Engine

> Each task touches **at most 1–2 files**. Complete tasks in order; later tasks depend on earlier ones.  
> Cross-references: `REQ-*` → requirements.md · `§*` → design.md

---

## Phase 0 — Project Scaffold

### Task 0.1 — `requirements.txt`
**File:** `requirements.txt`

Add all Python dependencies with pinned versions:

```
fastapi==0.111.0
uvicorn[standard]==0.29.0
sqlalchemy==2.0.30
asyncpg==0.29.0
alembic==1.13.1
pydantic==2.7.1
pydantic-settings==2.2.1
httpx==0.27.0
apscheduler==3.10.4
structlog==24.1.0
prometheus-client==0.20.0
python-multipart==0.0.9
```

**Acceptance:** `pip install -r requirements.txt` exits 0 in a clean virtualenv.

---

### Task 0.2 — `app/config.py`
**File:** `app/config.py`

Implement `Settings` using `pydantic-settings`. Cover every variable from the design §10:

- `database_url`, `notification_service_url`, `notification_timeout_s`
- `max_notification_retries`, `relay_poll_interval_s`, `relay_batch_size`
- `relay_backoff_initial_s`, `relay_backoff_multiplier`, `relay_backoff_cap_s`
- `bulk_cancel_batch_size`, `mock_service_port`

Expose a module-level `settings = Settings()` singleton.

**Acceptance:** `from app.config import settings; print(settings.max_notification_retries)` prints `10`.

---

### Task 0.3 — `app/database.py`
**File:** `app/database.py`

Create the async SQLAlchemy engine and session factory:

```python
engine = create_async_engine(settings.database_url, pool_pre_ping=True)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session
```

**Acceptance:** Module imports without error. No DB connection is made at import time.

---

## Phase 1 — SQLAlchemy Models

### Task 1.1 — `app/models/base.py`
**File:** `app/models/base.py`

Define `Base = DeclarativeBase()` and a `TimestampMixin` that adds `created_at` and `updated_at` columns with server-side defaults and an `onupdate` hook.

**Acceptance:** `from app.models.base import Base, TimestampMixin` works cleanly.

---

### Task 1.2 — `app/models/user.py`
**File:** `app/models/user.py`

Implement the `User` model exactly as specified in design §3.1:
- UUID primary key
- `email` with `unique=True`
- `full_name`
- `UserRole` enum (`ATTENDEE`, `OPERATOR`) as a native PostgreSQL enum type
- `registrations` relationship (back-populates)

**Acceptance:** `from app.models.user import User, UserRole` works.

---

### Task 1.3 — `app/models/workshop.py`
**File:** `app/models/workshop.py`

Implement the `Workshop` model (design §3.2):
- UUID primary key
- `title`, `scheduled_at` (timezone-aware), `venue`, `capacity`
- `WorkshopStatus` enum (`SCHEDULED`, `CANCELLED`)
- `CheckConstraint("capacity >= 0")`
- `registrations` relationship (back-populates)

**Acceptance:** `from app.models.workshop import Workshop, WorkshopStatus` works.

---

### Task 1.4 — `app/models/registration.py`
**File:** `app/models/registration.py`

Implement the `Registration` model (design §3.3):
- UUID primary key
- `user_id` FK → `users.id` (`ondelete="CASCADE"`)
- `workshop_id` FK → `workshops.id` (`ondelete="RESTRICT"`)
- `RegistrationStatus` enum (`CONFIRMED`, `CANCELLED`)
- `UniqueConstraint("user_id", "workshop_id", name="uq_registration_user_workshop")`
- `Index("ix_registration_workshop_status", "workshop_id", "status")`
- `user` and `workshop` relationships

**Acceptance:** `from app.models.registration import Registration, RegistrationStatus` works.

---

### Task 1.5 — `app/models/outbox_event.py`
**File:** `app/models/outbox_event.py`

Implement the `OutboxEvent` model (design §3.4):
- UUID primary key
- `OutboxEventType` enum (`REGISTRATION_CONFIRMED`, `REGISTRATION_CANCELLED`, `WORKSHOP_CANCELLED`)
- `OutboxEventStatus` enum (`PENDING`, `PROCESSING`, `DELIVERED`, `FAILED`)
- `registration_id` FK → `registrations.id` (`ondelete="SET NULL"`, nullable)
- `payload` as `JSON`
- `attempts` (Integer, default 0)
- `last_attempted_at` (DateTime timezone-aware, nullable)
- `idempotency_key` (String(36), default `str(uuid.uuid4())`)
- `Index("ix_outbox_status_created", "status", "created_at")`

**Acceptance:** `from app.models.outbox_event import OutboxEvent, OutboxEventStatus, OutboxEventType` works.

---

### Task 1.6 — `app/models/__init__.py`
**File:** `app/models/__init__.py`

Re-export all models so Alembic's `target_metadata` can discover them in one import:

```python
from app.models.base import Base, TimestampMixin
from app.models.user import User, UserRole
from app.models.workshop import Workshop, WorkshopStatus
from app.models.registration import Registration, RegistrationStatus
from app.models.outbox_event import OutboxEvent, OutboxEventStatus, OutboxEventType

__all__ = [
    "Base", "TimestampMixin",
    "User", "UserRole",
    "Workshop", "WorkshopStatus",
    "Registration", "RegistrationStatus",
    "OutboxEvent", "OutboxEventStatus", "OutboxEventType",
]
```

**Acceptance:** `from app.models import OutboxEvent` works.

---

## Phase 2 — Alembic Migrations

### Task 2.1 — Alembic initialisation
**Files:** `alembic.ini`, `alembic/env.py`

Run `alembic init alembic` and edit `alembic/env.py` to:
- Import `settings` from `app.config` and set `config.set_main_option("sqlalchemy.url", settings.database_url)`
- Import `Base` from `app.models` and set `target_metadata = Base.metadata`
- Use the async engine pattern: `connectable = async_engine_from_config(...)`

**Acceptance:** `alembic current` exits 0 when the DB is reachable.

---

### Task 2.2 — Initial migration
**Files:** `alembic/versions/<hash>_initial_schema.py`

Generate and review the initial autogenerated migration:

```
alembic revision --autogenerate -m "initial_schema"
```

Verify the generated file creates:
- `users`, `workshops`, `registrations`, `outbox_events` tables
- All enums, constraints, and indexes from Phase 1

**Acceptance:** `alembic upgrade head` against a fresh PostgreSQL instance creates all four tables with correct columns and constraints.

---

## Phase 3 — Pydantic Schemas

### Task 3.1 — `app/schemas/workshop.py`
**File:** `app/schemas/workshop.py`

Define:
- `WorkshopCreate` — title, scheduled_at, venue, capacity (all required; validate `scheduled_at` is in the future)
- `WorkshopResponse` — all fields plus `registered_count: int` and `available_seats: int`
- `WorkshopListResponse` — `items: list[WorkshopResponse]`, `total: int`, `page: int`, `page_size: int`

**Acceptance:** `WorkshopCreate(title="x", scheduled_at=..., venue="y", capacity=50)` validates correctly.

---

### Task 3.2 — `app/schemas/registration.py`
**File:** `app/schemas/registration.py`

Define:
- `RegistrationCreate` — `user_id: UUID`, `workshop_id: UUID`
- `RegistrationResponse` — `id`, `user_id`, `workshop_id`, `status`, `created_at`
- `BulkCancelResponse` — `workshop_id: UUID`, `cancelled_registration_count: int`

**Acceptance:** All three schemas instantiate without error.

---

### Task 3.3 — `app/schemas/outbox_event.py`
**File:** `app/schemas/outbox_event.py`

Define:
- `OutboxEventResponse` — all OutboxEvent fields (for the health endpoint's pending count)
- `NotificationPayload` — typed schema matching the JSON payload stored in `OutboxEvent.payload`

**Acceptance:** `NotificationPayload(user_id=..., user_email="a@b.com", ...)` validates correctly.

---

## Phase 4 — Custom Exceptions & Error Middleware

### Task 4.1 — `app/exceptions.py`
**File:** `app/exceptions.py`

Define domain exception classes:
- `WorkshopNotFoundError(workshop_id)`
- `WorkshopCancelledError(workshop_id)`
- `WorkshopFullError(workshop_id)`
- `AlreadyRegisteredError(user_id, workshop_id)`
- `RegistrationNotFoundError(registration_id)`
- `OperatorRequiredError()`

Each exception carries its own `error_code: str` and `http_status: int` class attributes.

**Acceptance:** `raise WorkshopFullError(id)` carries `error_code == "WORKSHOP_FULL"`.

---

### Task 4.2 — error handler registration in `app/main.py`
**File:** `app/main.py`

Create the FastAPI `app` with:
- `X-Request-ID` middleware (echo or generate UUID v4)
- Exception handlers for every domain exception that return the uniform `{"error": {"code": ..., "message": ...}}` envelope (REQ-API-01)
- A stub `lifespan` context manager (relay worker wiring comes in Task 7.2)

**Acceptance:** Starting `uvicorn app.main:app` runs without error. `GET /docs` returns 200.

---

## Phase 5 — Workshop API

### Task 5.1 — `app/api/workshops.py` — CRUD endpoints
**File:** `app/api/workshops.py`

Implement under `/api/v1/workshops`:
- `POST /` — create workshop; validate `scheduled_at` in the future; return 201 + `WorkshopResponse`
- `GET /` — paginated list with computed `registered_count` / `available_seats` via a single SQL query using a correlated subquery (design §3.2); return `WorkshopListResponse`
- `GET /{workshop_id}` — fetch single workshop; 404 if missing
- `DELETE /{workshop_id}` — guard: if any `CONFIRMED` registration exists return 409; otherwise delete; return 204

**Acceptance:** All four routes reachable; 409 returned when deleting a workshop with registrations.

---

### Task 5.2 — `app/api/workshops.py` — bulk cancel endpoint
**File:** `app/api/workshops.py`

Add to the existing router:
- `POST /{workshop_id}/cancel` — operator-only (check `UserRole`; return 403 if not OPERATOR)
- Idempotent: if workshop already `CANCELLED`, return 200 `{"message": "already cancelled", "cancelled_registration_count": 0}`
- Delegate all business logic to `cancellation_service.bulk_cancel` (implemented in Task 6.2)
- Return HTTP 202 `BulkCancelResponse`

**Acceptance:** Calling cancel twice on the same workshop returns 200 on the second call without touching registrations.

---

## Phase 6 — Registration & Cancellation Services

### Task 6.1 — `app/services/registration.py`
**File:** `app/services/registration.py`

Implement `async def register(session, user_id, workshop_id) -> Registration` following design §5:

1. `SELECT workshop FOR UPDATE` — raises `WorkshopNotFoundError`, `WorkshopCancelledError`
2. Count `CONFIRMED` registrations; raise `WorkshopFullError` if at capacity
3. `session.add(Registration(...))` then `await session.flush()` — catch `IntegrityError` → `AlreadyRegisteredError`
4. Build denormalised payload (join user + workshop for email/name/title/venue)
5. `session.add(OutboxEvent(event_type=REGISTRATION_CONFIRMED, payload=..., idempotency_key=str(registration.id)))`
6. Return `registration` — caller commits

**Acceptance:** Two concurrent calls for the same `(user_id, workshop_id)` result in exactly one `CONFIRMED` registration and one `PENDING` outbox record.

---

### Task 6.2 — `app/services/cancellation.py`
**File:** `app/services/cancellation.py`

Implement two functions:

**`async def cancel_single(session, registration_id, requesting_user_id) -> Registration`**
1. Fetch registration; raise `RegistrationNotFoundError` if absent
2. Verify `registration.user_id == requesting_user_id`; raise 403 otherwise
3. Set `registration.status = CANCELLED`
4. Insert `OutboxEvent(event_type=REGISTRATION_CANCELLED, ...)`
5. Return registration — caller commits

**`async def bulk_cancel(session, workshop_id) -> int`**
1. Fetch & lock workshop; raise `WorkshopNotFoundError` if absent
2. Return 0 immediately if `status == CANCELLED` (idempotent)
3. Loop in batches of `settings.bulk_cancel_batch_size`:
   - `SELECT registrations WHERE workshop_id=? AND status=CONFIRMED LIMIT N FOR UPDATE SKIP LOCKED`
   - Bulk `UPDATE` to `CANCELLED`
   - Insert one `OutboxEvent(event_type=WORKSHOP_CANCELLED)` per affected registration
   - Commit batch
4. After loop: set `workshop.status = CANCELLED`, commit
5. Return total `cancelled_registration_count`

**Acceptance:** Bulk cancel on a 1 500-seat workshop with 1 500 registrations produces exactly 1 500 `WORKSHOP_CANCELLED` outbox records across 3 committed batch transactions.

---

### Task 6.3 — `app/api/registrations.py`
**File:** `app/api/registrations.py`

Implement under `/api/v1/registrations`:
- `POST /` — call `registration_service.register`; commit; return HTTP 202 `RegistrationResponse`
- `GET /` — list caller's registrations (filter by `user_id` query param)
- `DELETE /{registration_id}` — call `cancellation_service.cancel_single`; commit; return HTTP 202

Wire router into `app/main.py` router include list.

**Acceptance:** `POST /api/v1/registrations` returns 202; a duplicate call returns 409.

---

## Phase 7 — Outbox Relay Worker

### Task 7.1 — `app/workers/relay.py` — back-off and delivery logic
**File:** `app/workers/relay.py`

Implement:

**`backoff_seconds(attempts: int) -> float`**
```python
min(initial * (multiplier ** (attempts - 1)), cap)
```
using `settings.relay_backoff_*` values.

**`async def _deliver(session_factory, event_snapshot: dict) -> None`**
- POST to `settings.notification_service_url + "/notify"` using `httpx.AsyncClient`
- Set `Idempotency-Key` header to `event_snapshot["idempotency_key"]` (REQ-OB-07)
- Set `X-Event-Type` header
- On HTTP 2xx: re-open session, set `status = DELIVERED`
- On failure with `attempts < MAX_RETRIES`: set `status = PENDING` (back-off respected via `last_attempted_at` already written)
- On failure with `attempts >= MAX_RETRIES`: set `status = FAILED`, log at ERROR with structlog

**Acceptance:** `backoff_seconds(1) == 1.0`, `backoff_seconds(10) == 300.0`.

---

### Task 7.2 — `app/workers/relay.py` — poll loop & APScheduler wiring
**Files:** `app/workers/relay.py`, `app/main.py`

Add to `relay.py`:

**`async def relay_tick(session_factory) -> None`**
- Open session with `BEGIN`
- Query `PENDING` outbox events whose back-off window has elapsed, `LIMIT relay_batch_size`, `FOR UPDATE SKIP LOCKED`
- Set each to `PROCESSING`, increment `attempts`, set `last_attempted_at = now()`
- Commit (makes PROCESSING visible)
- `asyncio.gather(*[_deliver(session_factory, ev) for ev in events])`

In `app/main.py` lifespan:
```python
scheduler = AsyncIOScheduler()
scheduler.add_job(
    relay_tick,
    "interval",
    seconds=settings.relay_poll_interval_s,
    args=[AsyncSessionLocal],
)
scheduler.start()
yield
scheduler.shutdown(wait=True)
```

**Acceptance:** After a registration, the outbox record transitions `PENDING → PROCESSING → DELIVERED` within `relay_poll_interval_s` seconds when the notification service returns 200.

---

## Phase 8 — Mock Notification Service

### Task 8.1 — `mock_service/state.py`
**File:** `mock_service/state.py`

Implement `FaultMode` enum and `MockState` dataclass exactly as in design §8.1. Export a module-level `state = MockState()` singleton.

**Acceptance:** `from mock_service.state import state, FaultMode` works; `state.mode == FaultMode.SUCCESS`.

---

### Task 8.2 — `mock_service/main.py`
**File:** `mock_service/main.py`

Implement all five endpoints (design §8.3):

- **`POST /notify`** — logs request; applies fault mode (design §8.2 transition table); returns 200 or 500 or hangs or drops
- **`POST /control`** — accepts `ControlRequest(mode, transient_count)`; updates `state` under lock
- **`POST /reset`** — clears `state.request_log`, resets `mode = SUCCESS`, `transient_count = 0`
- **`GET /requests`** — returns `{"count": N, "requests": [...]}`
- **`GET /health`** — always returns `{"status": "ok"}`

For `timeout` mode: `await asyncio.sleep(3600)`.  
For `connection_drop` mode: raise an unhandled exception that forces Uvicorn to close the connection without sending a response.

**Acceptance:**  
1. `POST /control {"mode":"server_error"}` → all subsequent `POST /notify` return 500.  
2. `POST /reset` → `GET /requests` returns `{"count": 0, "requests": []}`.

---

## Phase 9 — Observability

### Task 9.1 — `app/api/workshops.py` + `app/main.py` — health endpoint
**Files:** `app/main.py`

Add `GET /health` outside the `/api/v1` prefix:
- Probe DB with `SELECT 1`
- Read relay worker's last tick timestamp from a module-level variable updated by `relay_tick`
- Return `{"status": "ok"|"degraded", "db": "connected"|"error", "relay_worker": {"status": ..., "last_tick_at": ..., "pending_outbox_count": N}}`

**Acceptance:** `GET /health` returns 200 with `db: "connected"` when PostgreSQL is reachable.

---

### Task 9.2 — `app/main.py` — Prometheus metrics endpoint
**File:** `app/main.py`

Using `prometheus-client`:
- Define counters/gauges/histograms from design §11
- Increment `registrations_total` in `registration_service.register`
- Increment `cancellations_total` in both cancellation functions
- Expose `GET /metrics` returning Prometheus text format

**Acceptance:** `GET /metrics` returns a text body containing `registrations_total`.

---

## Phase 10 — Containerisation

### Task 10.1 — `Dockerfile`
**File:** `Dockerfile`

Multi-stage build:
```dockerfile
FROM python:3.12-slim AS base
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

FROM base AS runtime
COPY app/ ./app/
COPY alembic/ ./alembic/
COPY alembic.ini .
CMD ["sh", "-c", "alembic upgrade head && uvicorn app.main:app --host 0.0.0.0 --port 8000"]
```

**Acceptance:** `docker build -t workshop-app .` exits 0.

---

### Task 10.2 — `Dockerfile.mock`
**File:** `Dockerfile.mock`

```dockerfile
FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir fastapi==0.111.0 uvicorn[standard]==0.29.0 pydantic==2.7.1
COPY mock_service/ ./mock_service/
CMD ["uvicorn", "mock_service.main:app", "--host", "0.0.0.0", "--port", "8001"]
```

**Acceptance:** `docker build -f Dockerfile.mock -t workshop-mock .` exits 0.

---

### Task 10.3 — `docker-compose.yml`
**File:** `docker-compose.yml`

Wire three services as in design §12:
- `db`: `postgres:16-alpine` with healthcheck `pg_isready`
- `mock_service`: built from `Dockerfile.mock`, port 8001, healthcheck `curl /health`
- `app`: built from `Dockerfile`, port 8000, `depends_on` with `condition: service_healthy` for both `db` and `mock_service`; env vars for all `Settings` fields

**Acceptance:** `docker compose up --build` starts all three services; `curl localhost:8000/health` returns `{"status":"ok"}`.

---

## Phase 11 — Integration Tests

### Task 11.1 — `tests/conftest.py`
**File:** `tests/conftest.py`

Define pytest fixtures:
- `db_session` — async session pointing at a test PostgreSQL instance (use `pytest-asyncio`)
- `client` — `httpx.AsyncClient` wrapping the FastAPI app with `lifespan=True`
- `mock_server_url` — base URL of the mock service (read from env or default to `http://localhost:8001`)
- `reset_mock` — autouse fixture that calls `POST /reset` before each test

**Acceptance:** `pytest tests/conftest.py` collects fixtures without error.

---

### Task 11.2 — `tests/test_registration.py`
**File:** `tests/test_registration.py`

Write tests covering:
1. **Happy path** — register returns 202; DB has `CONFIRMED` registration + `PENDING` outbox record
2. **Duplicate registration** — second call returns 409 `ALREADY_REGISTERED`
3. **Workshop full** — register when capacity exhausted returns 409 `WORKSHOP_FULL`
4. **Concurrent registrations** — send 10 concurrent requests for a 1-seat workshop; assert exactly 1 succeeds
5. **Outbox delivery** — after successful registration, wait for relay worker; assert outbox record becomes `DELIVERED` and `GET /requests` on mock shows exactly 1 entry
6. **Transient failure recovery** — set mock to `transient_error` count=2; register; wait; assert outbox eventually `DELIVERED` and mock received 3 total requests (2 failures + 1 success)

**Acceptance:** All six test cases pass with `pytest -x tests/test_registration.py`.

---

### Task 11.3 — `tests/test_bulk_cancel.py`
**File:** `tests/test_bulk_cancel.py`

Write tests covering:
1. **Bulk cancel** — cancel workshop with N registrations; assert HTTP 202 with correct count; all registrations `CANCELLED`; N outbox records created
2. **Idempotent cancel** — cancel twice; second call returns 200 with count 0
3. **Non-OPERATOR blocked** — attendee calling cancel returns 403
4. **Large workshop batching** — create workshop with 600 registrations; cancel; assert all 600 registrations cancelled and 600 `WORKSHOP_CANCELLED` outbox records created
5. **Non-blocking response** — set mock to `timeout`; issue bulk cancel; assert HTTP 202 arrives before mock receives any request (i.e., response is not blocked by notification delivery)

**Acceptance:** All five test cases pass with `pytest -x tests/test_bulk_cancel.py`.

---

### Task 11.4 — `tests/test_relay_worker.py`
**File:** `tests/test_relay_worker.py`

Write tests covering:
1. **Back-off formula** — unit test `backoff_seconds(n)` for n = 1…10; assert values match the table in design §7.3
2. **Max retries → FAILED** — set mock to `server_error`; register; wait for `MAX_NOTIFICATION_RETRIES + 1` ticks; assert outbox record is `FAILED`; assert `GET /requests` count == `MAX_NOTIFICATION_RETRIES`
3. **Crash recovery simulation** — manually insert a `PROCESSING` outbox record (simulating a crash mid-delivery); restart relay worker tick; assert record is re-claimed and eventually `DELIVERED`
4. **Concurrent worker safety** — run two relay ticks concurrently against the same `PENDING` record; assert it is delivered exactly once (mock `GET /requests` count == 1)
5. **Idempotency key stability** — assert all retry attempts for the same outbox record send the same `Idempotency-Key` header value

**Acceptance:** All five test cases pass with `pytest -x tests/test_relay_worker.py`.

---

## Completion Checklist

| Phase | Files | Status |
|-------|-------|--------|
| 0 — Scaffold | `requirements.txt`, `app/config.py`, `app/database.py` | ☐ |
| 1 — Models | `app/models/*.py` (6 files) | ☐ |
| 2 — Migrations | `alembic.ini`, `alembic/env.py`, `alembic/versions/*.py` | ☐ |
| 3 — Schemas | `app/schemas/*.py` (3 files) | ☐ |
| 4 — Exceptions | `app/exceptions.py`, `app/main.py` (stub) | ☐ |
| 5 — Workshop API | `app/api/workshops.py` | ☐ |
| 6 — Services + Reg API | `app/services/registration.py`, `app/services/cancellation.py`, `app/api/registrations.py` | ☐ |
| 7 — Relay Worker | `app/workers/relay.py`, `app/main.py` (lifespan) | ☐ |
| 8 — Mock Service | `mock_service/state.py`, `mock_service/main.py` | ☐ |
| 9 — Observability | `app/main.py` (health + metrics) | ☐ |
| 10 — Docker | `Dockerfile`, `Dockerfile.mock`, `docker-compose.yml` | ☐ |
| 11 — Tests | `tests/conftest.py`, `test_registration.py`, `test_bulk_cancel.py`, `test_relay_worker.py` | ☐ |
