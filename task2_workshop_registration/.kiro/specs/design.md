# Design Document — Task 2: Workshop Registration and Notifications Engine

> Derived from approved requirements in `requirements.md`.  
> Stack: FastAPI · SQLAlchemy 2 (async) · PostgreSQL 16 · asyncpg · APScheduler · FastAPI (mock server)

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Project Layout](#2-project-layout)
3. [Database Schema & SQLAlchemy Models](#3-database-schema--sqlalchemy-models)
   - 3.1 User
   - 3.2 Workshop
   - 3.3 Registration
   - 3.4 OutboxEvent
   - 3.5 Entity-Relationship Summary
4. [State Machines](#4-state-machines)
   - 4.1 WorkshopStatus
   - 4.2 RegistrationStatus
   - 4.3 OutboxEventStatus
5. [Registration Flow (Happy Path)](#5-registration-flow-happy-path)
6. [Bulk Cancellation Flow](#6-bulk-cancellation-flow)
7. [Transactional Outbox & Relay Worker](#7-transactional-outbox--relay-worker)
   - 7.1 Dual-write atomicity
   - 7.2 Relay worker loop
   - 7.3 Exponential back-off logic
   - 7.4 Concurrent worker safety
   - 7.5 Crash-recovery path
8. [Mock Notification Service Architecture](#8-mock-notification-service-architecture)
   - 8.1 Fault modes
   - 8.2 Internal state machine
   - 8.3 Endpoints
   - 8.4 Request log
9. [API Layer](#9-api-layer)
10. [Configuration Reference](#10-configuration-reference)
11. [Observability](#11-observability)
12. [Docker Compose Topology](#12-docker-compose-topology)

---

## 1. System Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        FastAPI Application                       │
│                                                                   │
│  HTTP Router ──► Registration Service ──► PostgreSQL DB          │
│                        │                   (users, workshops,     │
│                        │                    registrations,        │
│                        │                    outbox_events)        │
│                        │                                          │
│                        └──► Outbox record (same transaction)      │
│                                                                   │
│  APScheduler (async) ──► Relay Worker ──► Notification Service   │
│   (background)               │               (external HTTP)      │
│                              └──► Mock Notification Server       │
│                                   (fault-injecting test double)  │
└─────────────────────────────────────────────────────────────────┘
```

Key design decisions:

- **All DB work is async** via `asyncpg` + SQLAlchemy 2 async session to keep the FastAPI event loop unblocked.
- **Outbox pattern** decouples notification delivery from the HTTP path entirely; the relay worker runs independently.
- **Single-writer-per-record** concurrency is enforced by `SELECT … FOR UPDATE SKIP LOCKED` inside the relay worker.
- **Mock server** is a separate FastAPI process so it can be swapped for the real service by changing one env var.

---

## 2. Project Layout

```
task2_workshop_registration/
├── app/
│   ├── main.py                  # FastAPI app factory, lifespan hooks
│   ├── config.py                # Settings (Pydantic BaseSettings)
│   ├── database.py              # Async engine, session factory
│   ├── models/
│   │   ├── __init__.py
│   │   ├── base.py              # DeclarativeBase, TimestampMixin
│   │   ├── user.py              # User model
│   │   ├── workshop.py          # Workshop model + WorkshopStatus enum
│   │   ├── registration.py      # Registration model + RegistrationStatus enum
│   │   └── outbox_event.py      # OutboxEvent model + enums
│   ├── schemas/
│   │   ├── workshop.py          # Pydantic request/response schemas
│   │   ├── registration.py
│   │   └── outbox_event.py
│   ├── api/
│   │   ├── __init__.py
│   │   ├── workshops.py         # Workshop CRUD + bulk cancel
│   │   └── registrations.py    # Register + cancel endpoints
│   ├── services/
│   │   ├── registration.py      # Business logic, seat claim, outbox write
│   │   └── cancellation.py      # Single + bulk cancellation logic
│   └── workers/
│       └── relay.py             # Outbox relay worker (APScheduler job)
├── mock_service/
│   ├── main.py                  # Fault-injecting mock notification server
│   └── state.py                 # In-process shared state (mode, log)
├── tests/
│   ├── conftest.py
│   ├── test_registration.py
│   ├── test_bulk_cancel.py
│   └── test_relay_worker.py
├── Dockerfile
├── Dockerfile.mock
└── docker-compose.yml
```

---

## 3. Database Schema & SQLAlchemy Models

### 3.1 User

Represents an attendee or operator. Authentication is out of scope; the model stores just enough for notification routing.

```python
# app/models/user.py
import uuid
from sqlalchemy import String, Enum as SAEnum
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.models.base import Base, TimestampMixin
import enum

class UserRole(str, enum.Enum):
    ATTENDEE = "ATTENDEE"
    OPERATOR = "OPERATOR"

class User(Base, TimestampMixin):
    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, default=uuid.uuid4
    )
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    full_name: Mapped[str] = mapped_column(String(255), nullable=False)
    role: Mapped[UserRole] = mapped_column(
        SAEnum(UserRole, name="user_role"), nullable=False, default=UserRole.ATTENDEE
    )

    registrations: Mapped[list["Registration"]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )
```

**DDL notes**
- `id` — UUID primary key, generated by the application layer (avoids hot-page contention from sequential integers at high insert rates).
- `email` — unique constraint; used as the notification recipient address.

---

### 3.2 Workshop

```python
# app/models/workshop.py
import uuid
import enum
from datetime import datetime
from sqlalchemy import String, Integer, DateTime, Enum as SAEnum, CheckConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.models.base import Base, TimestampMixin

class WorkshopStatus(str, enum.Enum):
    SCHEDULED  = "SCHEDULED"
    CANCELLED  = "CANCELLED"

class Workshop(Base, TimestampMixin):
    __tablename__ = "workshops"
    __table_args__ = (
        CheckConstraint("capacity >= 0", name="ck_workshop_capacity_non_negative"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    title: Mapped[str]    = mapped_column(String(255), nullable=False)
    scheduled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    venue: Mapped[str]    = mapped_column(String(255), nullable=False)
    capacity: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[WorkshopStatus] = mapped_column(
        SAEnum(WorkshopStatus, name="workshop_status"),
        nullable=False,
        default=WorkshopStatus.SCHEDULED,
    )

    registrations: Mapped[list["Registration"]] = relationship(
        back_populates="workshop", cascade="all, delete-orphan"
    )
```

**Computed fields** (`registered_count`, `available_seats`) are derived at query time via a correlated subquery or window function — never stored — so they are always consistent without triggers.

```sql
-- Example query used by the list endpoint
SELECT
    w.*,
    COUNT(r.id) FILTER (WHERE r.status = 'CONFIRMED') AS registered_count,
    w.capacity - COUNT(r.id) FILTER (WHERE r.status = 'CONFIRMED') AS available_seats
FROM workshops w
LEFT JOIN registrations r ON r.workshop_id = w.id
GROUP BY w.id;
```

---

### 3.3 Registration

```python
# app/models/registration.py
import uuid
import enum
from sqlalchemy import ForeignKey, Enum as SAEnum, UniqueConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.models.base import Base, TimestampMixin

class RegistrationStatus(str, enum.Enum):
    CONFIRMED  = "CONFIRMED"
    CANCELLED  = "CANCELLED"

class Registration(Base, TimestampMixin):
    __tablename__ = "registrations"
    __table_args__ = (
        # REQ-REG-01 / REQ-REG-03 — DB-level uniqueness
        UniqueConstraint("user_id", "workshop_id", name="uq_registration_user_workshop"),
        # Covering index for the relay worker's bulk-cancel batch query
        Index("ix_registration_workshop_status", "workshop_id", "status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    workshop_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("workshops.id", ondelete="RESTRICT"), nullable=False
    )
    status: Mapped[RegistrationStatus] = mapped_column(
        SAEnum(RegistrationStatus, name="registration_status"),
        nullable=False,
        default=RegistrationStatus.CONFIRMED,
    )

    user: Mapped["User"]         = relationship(back_populates="registrations")
    workshop: Mapped["Workshop"] = relationship(back_populates="registrations")
```

**Constraint details**

| Constraint | Purpose |
|------------|---------|
| `uq_registration_user_workshop` | Guarantees at-most-one row per `(user_id, workshop_id)` even under concurrent inserts (REQ-REG-03). |
| `ondelete="RESTRICT"` on `workshop_id` | Prevents deleting a workshop that still has registrations (REQ-WM-04). |
| `ix_registration_workshop_status` | Accelerates the bulk-cancel batch query that filters by `workshop_id` and `status = 'CONFIRMED'`. |

---

### 3.4 OutboxEvent

```python
# app/models/outbox_event.py
import uuid
import enum
from datetime import datetime
from sqlalchemy import (
    ForeignKey, Enum as SAEnum, JSON, Integer,
    DateTime, Index, String
)
from sqlalchemy.orm import Mapped, mapped_column
from app.models.base import Base, TimestampMixin

class OutboxEventType(str, enum.Enum):
    REGISTRATION_CONFIRMED  = "REGISTRATION_CONFIRMED"
    REGISTRATION_CANCELLED  = "REGISTRATION_CANCELLED"
    WORKSHOP_CANCELLED      = "WORKSHOP_CANCELLED"

class OutboxEventStatus(str, enum.Enum):
    PENDING    = "PENDING"
    PROCESSING = "PROCESSING"
    DELIVERED  = "DELIVERED"
    FAILED     = "FAILED"

class OutboxEvent(Base, TimestampMixin):
    __tablename__ = "outbox_events"
    __table_args__ = (
        # Relay worker polls this index: status='PENDING' ordered by created_at
        Index("ix_outbox_status_created", "status", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    event_type: Mapped[OutboxEventType] = mapped_column(
        SAEnum(OutboxEventType, name="outbox_event_type"), nullable=False
    )
    # Denormalised reference — kept even if the registration is later deleted
    registration_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("registrations.id", ondelete="SET NULL"), nullable=True
    )
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    status: Mapped[OutboxEventStatus] = mapped_column(
        SAEnum(OutboxEventStatus, name="outbox_event_status"),
        nullable=False,
        default=OutboxEventStatus.PENDING,
        index=True,
    )
    attempts: Mapped[int]                    = mapped_column(Integer, nullable=False, default=0)
    last_attempted_at: Mapped[datetime|None] = mapped_column(DateTime(timezone=True), nullable=True)
    # Idempotency key forwarded to the notification service (REQ-OB-07)
    idempotency_key: Mapped[str] = mapped_column(
        String(36), nullable=False,
        default=lambda: str(uuid.uuid4())
    )
```

**Payload schema** (stored as JSON):

```json
{
  "user_id":       "uuid",
  "user_email":    "alice@example.com",
  "user_name":     "Alice",
  "workshop_id":   "uuid",
  "workshop_title":"Intro to Kubernetes",
  "scheduled_at":  "2026-11-01T10:00:00Z",
  "venue":         "Room 101"
}
```

All notification-relevant data is denormalised into `payload` at write time so the relay worker never needs to re-join tables (avoids phantom-read edge cases after bulk cancellation).

---

### 3.5 Entity-Relationship Summary

```
users ──< registrations >── workshops
               │
               │ (ondelete SET NULL)
               ▼
         outbox_events
```

- One user → many registrations (across different workshops).
- One workshop → many registrations.
- Each registration → zero or more outbox_events (typically one per lifecycle event).
- `ondelete="SET NULL"` on `outbox_events.registration_id` preserves the outbox record for audit even if the registration row is deleted.

---

## 4. State Machines

### 4.1 WorkshopStatus

```
           [CREATE]
               │
               ▼
          SCHEDULED ──── operator POST /cancel ──►  CANCELLED
                                                        │
                                                   (terminal)
```

- Only `SCHEDULED → CANCELLED` is permitted.
- Attempting to cancel an already-`CANCELLED` workshop is idempotent (HTTP 200, no further work).
- No `RESCHEDULED` state in scope; a reschedule is modelled as cancel + new workshop creation.

---

### 4.2 RegistrationStatus

```
           [POST /registrations]
                    │
                    ▼
               CONFIRMED ──── DELETE /registrations/{id} ──►  CANCELLED
                    │                                               │
                    └── bulk workshop cancel ──────────────────────┘
                                                             (terminal)
```

- `CONFIRMED` is the only non-terminal state.
- `CANCELLED` is terminal — a cancelled registration cannot be reinstated (re-register creates a new row, but the unique constraint prevents this while a `CONFIRMED` row exists; after cancellation the constraint allows a fresh registration).

---

### 4.3 OutboxEventStatus

This is the core state machine for crash-resilient notification delivery.

```
              [inserted with registration / cancellation]
                              │
                              ▼
                           PENDING
                              │
              relay worker claims the row (FOR UPDATE SKIP LOCKED)
                              │
                              ▼
                         PROCESSING  ◄──── re-entered on each retry
                         /        \
            HTTP 2xx received     HTTP non-2xx / connection error
                   │                           │
                   ▼                           ▼
               DELIVERED              attempts < MAX_RETRIES?
               (terminal)             /                    \
                                    yes                     no
                                     │                       │
                          back-off, reset to PENDING      FAILED
                                                         (terminal)
```

**Transition rules**

| From | To | Condition |
|------|----|-----------|
| `PENDING` | `PROCESSING` | Relay worker acquires row lock |
| `PROCESSING` | `DELIVERED` | Notification service returns HTTP 2xx |
| `PROCESSING` | `PENDING` | Delivery failed, `attempts < MAX_RETRIES`; back-off delay applied |
| `PROCESSING` | `FAILED` | Delivery failed, `attempts >= MAX_RETRIES` |
| `FAILED` | — | Terminal; no automatic recovery |

> **Why PROCESSING?**  
> Keeping a distinct in-flight state means a monitoring query can detect worker crashes: a row stuck in `PROCESSING` for longer than `(poll_interval × 2)` indicates a dead worker, distinguishable from normal `PENDING` records waiting for their next retry window.

---

## 5. Registration Flow (Happy Path)

```
Client                FastAPI Handler          DB Transaction           Relay Worker
  │                        │                        │                        │
  │  POST /api/v1/         │                        │                        │
  │  registrations         │                        │                        │
  │──────────────────────► │                        │                        │
  │                        │  BEGIN                 │                        │
  │                        │───────────────────────►│                        │
  │                        │  SELECT workshop       │                        │
  │                        │  FOR UPDATE            │  (locks capacity row)  │
  │                        │───────────────────────►│                        │
  │                        │  check capacity & uniqueness                    │
  │                        │  INSERT registration   │                        │
  │                        │───────────────────────►│                        │
  │                        │  INSERT outbox_event   │                        │
  │                        │  (PENDING)             │                        │
  │                        │───────────────────────►│                        │
  │                        │  COMMIT                │                        │
  │                        │───────────────────────►│                        │
  │  HTTP 202 Accepted     │                        │                        │
  │◄──────────────────────  │                        │                        │
  │                        │                        │   polls PENDING rows   │
  │                        │                        │◄───────────────────────│
  │                        │                        │   FOR UPDATE           │
  │                        │                        │   SKIP LOCKED          │
  │                        │                        │   → status=PROCESSING  │
  │                        │                        │◄───────────────────────│
  │                        │                        │   POST /notify         │
  │                        │    Notification Svc ◄──┼────────────────────────│
  │                        │    HTTP 200 ──────────►│   → status=DELIVERED   │
  │                        │                        │◄───────────────────────│
```

**Seat-claim logic** inside the service layer:

```python
async def register(session: AsyncSession, user_id: UUID, workshop_id: UUID) -> Registration:
    # Lock the workshop row to prevent oversell
    workshop = await session.execute(
        select(Workshop)
        .where(Workshop.id == workshop_id)
        .with_for_update()           # row-level lock
    )
    workshop = workshop.scalar_one_or_none()
    if workshop is None:
        raise WorkshopNotFoundError(workshop_id)
    if workshop.status == WorkshopStatus.CANCELLED:
        raise WorkshopCancelledError(workshop_id)

    confirmed_count = await _count_confirmed(session, workshop_id)
    if confirmed_count >= workshop.capacity:
        raise WorkshopFullError(workshop_id)

    registration = Registration(user_id=user_id, workshop_id=workshop_id)
    session.add(registration)

    try:
        await session.flush()        # catches UniqueConstraint violation early
    except IntegrityError:
        raise AlreadyRegisteredError(user_id, workshop_id)

    outbox = OutboxEvent(
        event_type=OutboxEventType.REGISTRATION_CONFIRMED,
        registration_id=registration.id,
        payload=await _build_payload(session, registration),
        idempotency_key=str(registration.id),   # REQ-OB-07
    )
    session.add(outbox)
    # Caller commits the session — both rows land atomically
    return registration
```

---

## 6. Bulk Cancellation Flow

For workshops with ≤ 1 000 registrations the entire operation is a single transaction. For larger workshops it is batched (REQ-BK-06).

```
POST /api/v1/workshops/{workshop_id}/cancel
        │
        ▼
    Guard: workshop exists?  ──No──►  404
        │ Yes
        ▼
    Guard: workshop.status == CANCELLED?  ──Yes──►  200 (no-op)
        │ No
        ▼
    LOOP until no more CONFIRMED rows:
    ┌────────────────────────────────────────────────────────────────────┐
    │  BEGIN                                                             │
    │    SELECT id, user_id FROM registrations                          │
    │    WHERE workshop_id = ? AND status = 'CONFIRMED'                 │
    │    LIMIT 500                                                       │
    │    FOR UPDATE SKIP LOCKED                                          │
    │                                                                    │
    │    UPDATE registrations SET status='CANCELLED'                    │
    │    WHERE id IN (batch_ids)                                        │
    │                                                                    │
    │    INSERT INTO outbox_events (one per affected registration)       │
    │    event_type = 'WORKSHOP_CANCELLED'                              │
    │                                                                    │
    │  COMMIT                                                            │
    └────────────────────────────────────────────────────────────────────┘
        │
        ▼  (all batches done)
    UPDATE workshops SET status = 'CANCELLED'  (single statement, own tx)
        │
        ▼
    HTTP 202 { "cancelled_registration_count": N }
```

**Why batch inside a loop rather than a single UPDATE?**  
A single `UPDATE … WHERE workshop_id = ?` on 10 000 rows holds a page-level lock for the entire duration. Batching at 500 rows releases locks between iterations, keeping other transactions unblocked (REQ-BK-06).

---

## 7. Transactional Outbox & Relay Worker

### 7.1 Dual-write atomicity

The registration/cancellation service and the outbox insert always share the same `AsyncSession`. The caller (the FastAPI route handler) calls `await session.commit()` exactly once. Both the domain row and the outbox row are committed together or not at all.

```python
async with db_session() as session:        # unit-of-work boundary
    registration = await registration_service.register(session, ...)
    await session.commit()                  # outbox row committed here too
    return RegistrationResponse.from_orm(registration)
```

### 7.2 Relay worker loop

The relay worker is an `AsyncIOScheduler` job (APScheduler 3.x) started inside FastAPI's `lifespan` context manager so it shares the process lifetime with the web server.

```python
# app/workers/relay.py  (pseudocode)
POLL_INTERVAL_SECONDS = settings.relay_poll_interval  # default: 5

async def relay_tick(session_factory):
    async with session_factory() as session:
        async with session.begin():
            # Claim a batch of PENDING records, lock them
            rows = await session.execute(
                select(OutboxEvent)
                .where(OutboxEvent.status == OutboxEventStatus.PENDING)
                .where(
                    # Skip records whose next_attempt_at is in the future
                    or_(
                        OutboxEvent.last_attempted_at.is_(None),
                        OutboxEvent.last_attempted_at + backoff_interval(OutboxEvent.attempts)
                        <= func.now()
                    )
                )
                .order_by(OutboxEvent.created_at)
                .limit(settings.relay_batch_size)           # default: 50
                .with_for_update(skip_locked=True)
            )
            events = rows.scalars().all()

            for event in events:
                event.status = OutboxEventStatus.PROCESSING
                event.attempts += 1
                event.last_attempted_at = datetime.utcnow()

        # Session committed; status=PROCESSING is visible to other workers
        for event in events:
            await _deliver(session_factory, event)
```

### 7.3 Exponential back-off logic

```
delay(n) = min(initial × multiplier^(n−1), cap)

initial    = 1 second      (RELAY_BACKOFF_INITIAL_S, default 1)
multiplier = 2             (RELAY_BACKOFF_MULTIPLIER, default 2)
cap        = 300 seconds   (RELAY_BACKOFF_CAP_S, default 300)
```

| Attempt (n) | delay before next retry |
|:-----------:|:----------------------:|
| 1 | 1 s |
| 2 | 2 s |
| 3 | 4 s |
| 4 | 8 s |
| 5 | 16 s |
| 6 | 32 s |
| 7 | 64 s |
| 8 | 128 s |
| 9 | 256 s |
| 10 | 300 s (capped) |
| > 10 | → `FAILED` |

The relay worker does **not** `asyncio.sleep` between retries. Instead it stores `last_attempted_at` in the database and the polling query skips any record whose `last_attempted_at + backoff_interval(attempts) > now()`. This means:

- The retry schedule survives process restarts.
- Multiple worker instances all see the same schedule without coordination.

```python
def backoff_seconds(attempts: int) -> float:
    """Return seconds to wait before the next delivery attempt."""
    initial    = settings.relay_backoff_initial_s   # 1.0
    multiplier = settings.relay_backoff_multiplier  # 2.0
    cap        = settings.relay_backoff_cap_s       # 300.0
    return min(initial * (multiplier ** (attempts - 1)), cap)
```

```python
async def _deliver(session_factory, event: OutboxEvent) -> None:
    try:
        async with httpx.AsyncClient(timeout=settings.notification_timeout_s) as client:
            response = await client.post(
                settings.notification_service_url + "/notify",
                json=event.payload,
                headers={
                    "Idempotency-Key": event.idempotency_key,  # REQ-OB-07
                    "X-Event-Type":    event.event_type.value,
                },
            )
        success = response.is_success
        exception_name = None
    except Exception as exc:
        success = False
        exception_name = type(exc).__name__

    async with session_factory() as session:
        async with session.begin():
            ev = await session.get(OutboxEvent, event.id, with_for_update=True)
            if success:
                ev.status = OutboxEventStatus.DELIVERED
                logger.info("outbox.delivered", outbox_id=ev.id, event_type=ev.event_type)
            elif ev.attempts >= settings.max_notification_retries:
                ev.status = OutboxEventStatus.FAILED
                logger.error(
                    "outbox.failed",
                    outbox_id=ev.id,
                    event_type=ev.event_type,
                    attempts=ev.attempts,
                    exception=exception_name,
                )
            else:
                # Reset to PENDING; next poll respects backoff window
                ev.status = OutboxEventStatus.PENDING
```

### 7.4 Concurrent worker safety

`SELECT … FOR UPDATE SKIP LOCKED` ensures two relay workers never process the same outbox record simultaneously:

- Worker A locks rows 1-50; Worker B's query skips those and locks rows 51-100.
- If Worker A crashes mid-delivery, its transaction is rolled back and the rows return to `PENDING` (still locked by A's connection until PG detects the client disconnect).
- This is the only coordination mechanism needed — no Redis, no message broker.

### 7.5 Crash-recovery path

```
Timeline:
  t=0   Registration committed + OutboxEvent(status=PENDING) committed
  t=1   Relay worker sets status=PROCESSING
  t=2   Process crashes (SIGKILL / OOM / power loss)
  t=3   PostgreSQL rolls back the in-flight transaction for the dead connection
        → row reverts to status=PENDING  ← automatic, no manual intervention
  t=4   Application restarts
  t=5   Relay worker polls PENDING rows, finds the record, re-delivers
```

No stale `PROCESSING` rows can survive a process crash because the row-level lock and the `PROCESSING` status update are both inside the same database transaction — a crash rolls both back atomically.

---

## 8. Mock Notification Service Architecture

The mock is a standalone FastAPI application (`mock_service/main.py`). It owns its state entirely in process memory (`mock_service/state.py`) — no database — making it lightweight and fast to reset.

### 8.1 Fault modes

```python
# mock_service/state.py
import enum, threading, dataclasses
from datetime import datetime

class FaultMode(str, enum.Enum):
    SUCCESS          = "success"
    SERVER_ERROR     = "server_error"
    TRANSIENT_ERROR  = "transient_error"
    TIMEOUT          = "timeout"
    CONNECTION_DROP  = "connection_drop"

@dataclasses.dataclass
class MockState:
    mode: FaultMode            = FaultMode.SUCCESS
    transient_count: int       = 0   # remaining 500 responses before switching to 200
    request_log: list[dict]    = dataclasses.field(default_factory=list)
    _lock: threading.Lock      = dataclasses.field(default_factory=threading.Lock)

# Module-level singleton shared across all request handlers
state = MockState()
```

### 8.2 Internal state machine

```
                   POST /control { mode: "transient_error", transient_count: 3 }
                                    │
                                    ▼
              ┌──────────────── MockState ──────────────────────┐
              │  mode = TRANSIENT_ERROR                          │
              │  transient_count = 3                            │
              └──────────────────────────────────────────────────┘
                    │            │            │            │
                 request 1    request 2    request 3    request 4
                 500 (-1)     500 (-1)     500 (0)      200
                 count=2      count=1      count=0    (mode auto-resets
                                                       to SUCCESS)
```

Mode transitions on notification request (`POST /notify`):

| Current mode | transient_count | Action | Next state |
|---|---|---|---|
| `SUCCESS` | — | Return 200 | `SUCCESS` |
| `SERVER_ERROR` | — | Return 500 | `SERVER_ERROR` |
| `TRANSIENT_ERROR` | > 0 | Return 500, decrement | `TRANSIENT_ERROR` (count-1) |
| `TRANSIENT_ERROR` | 0 | Return 200 | auto-reset to `SUCCESS` |
| `TIMEOUT` | — | `asyncio.sleep(forever)` | `TIMEOUT` |
| `CONNECTION_DROP` | — | Close transport | `CONNECTION_DROP` |

### 8.3 Endpoints

```
POST   /notify           ← main notification receiver (all fault modes apply here)
POST   /control          ← set fault mode at runtime
POST   /reset            ← clear log, reset to SUCCESS
GET    /requests         ← inspect delivery attempts
GET    /health           ← always 200; verifies mock is reachable
```

**`POST /notify`** — main handler

```python
@app.post("/notify", status_code=200)
async def receive_notification(request: Request):
    body = await request.json()
    entry = {
        "timestamp":  datetime.utcnow().isoformat(),
        "method":     request.method,
        "path":       str(request.url.path),
        "headers":    dict(request.headers),
        "body":       body,
    }
    with state._lock:
        state.request_log.append(entry)
        current_mode = state.mode
        if current_mode == FaultMode.TRANSIENT_ERROR and state.transient_count > 0:
            state.transient_count -= 1
            if state.transient_count == 0:
                state.mode = FaultMode.SUCCESS
            raise HTTPException(status_code=500, detail={"error": "transient"})

    if current_mode == FaultMode.SERVER_ERROR:
        raise HTTPException(status_code=500, detail={"error": "server_error"})
    if current_mode == FaultMode.TIMEOUT:
        await asyncio.sleep(3600)          # relay client will time out first
    if current_mode == FaultMode.CONNECTION_DROP:
        # Access the underlying ASGI transport and close it
        await request.close()
        raise RuntimeError("connection_drop")

    return {"status": "accepted", "idempotency_key": request.headers.get("Idempotency-Key")}
```

**`POST /control`**

```python
class ControlRequest(BaseModel):
    mode: FaultMode
    transient_count: int = 0

@app.post("/control", status_code=200)
async def set_mode(body: ControlRequest):
    with state._lock:
        state.mode = body.mode
        state.transient_count = body.transient_count
    return {"mode": state.mode, "transient_count": state.transient_count}
```

**`POST /reset`**

```python
@app.post("/reset", status_code=200)
async def reset():
    with state._lock:
        state.mode = FaultMode.SUCCESS
        state.transient_count = 0
        state.request_log.clear()
    return {"status": "reset"}
```

**`GET /requests`**

```python
@app.get("/requests")
async def get_requests():
    with state._lock:
        return {"count": len(state.request_log), "requests": list(state.request_log)}
```

### 8.4 Request log

Each log entry is a dict:

```json
{
  "timestamp":  "2026-10-19T12:34:56.789Z",
  "method":     "POST",
  "path":       "/notify",
  "headers":    {
    "idempotency-key": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
    "x-event-type":    "REGISTRATION_CONFIRMED",
    "content-type":    "application/json"
  },
  "body": {
    "user_id": "...",
    "user_email": "alice@example.com",
    ...
  }
}
```

Tests use `GET /requests` to assert:
- The relay worker retried exactly N times (check `count`).
- The `Idempotency-Key` header is stable across retries (same `outbox_id`).
- The payload matches the registered attendee's details.

---

## 9. API Layer

### Route map

| Method | Path | Handler | Auth |
|--------|------|---------|------|
| `POST` | `/api/v1/workshops` | Create workshop | OPERATOR |
| `GET` | `/api/v1/workshops` | List workshops (paginated) | Any |
| `GET` | `/api/v1/workshops/{id}` | Get workshop detail | Any |
| `DELETE` | `/api/v1/workshops/{id}` | Delete workshop | OPERATOR |
| `POST` | `/api/v1/workshops/{id}/cancel` | Bulk cancel | OPERATOR |
| `POST` | `/api/v1/registrations` | Register attendee | ATTENDEE |
| `GET` | `/api/v1/registrations` | List user's registrations | ATTENDEE |
| `DELETE` | `/api/v1/registrations/{id}` | Cancel single registration | ATTENDEE |
| `GET` | `/api/v1/health` | Health check | None |
| `GET` | `/api/v1/metrics` | Prometheus metrics | None |

### Error envelope

All non-2xx responses use:

```json
{
  "error": {
    "code": "WORKSHOP_FULL",
    "message": "Workshop has no available seats."
  }
}
```

### `X-Request-ID` middleware

```python
@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    return response
```

---

## 10. Configuration Reference

All values read from environment variables via `pydantic-settings`.

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql+asyncpg://...` | Async PostgreSQL DSN |
| `NOTIFICATION_SERVICE_URL` | `http://mock_service:8001` | Base URL of notification endpoint |
| `NOTIFICATION_TIMEOUT_S` | `10` | Per-request timeout for relay worker (seconds) |
| `MAX_NOTIFICATION_RETRIES` | `10` | Attempts before marking outbox record FAILED |
| `RELAY_POLL_INTERVAL_S` | `5` | How often the relay worker polls the outbox table |
| `RELAY_BATCH_SIZE` | `50` | Max outbox records per relay tick |
| `RELAY_BACKOFF_INITIAL_S` | `1.0` | Back-off base delay (seconds) |
| `RELAY_BACKOFF_MULTIPLIER` | `2.0` | Back-off growth factor |
| `RELAY_BACKOFF_CAP_S` | `300.0` | Back-off ceiling (seconds) |
| `BULK_CANCEL_BATCH_SIZE` | `500` | Max registrations per bulk-cancel transaction |
| `MOCK_SERVICE_PORT` | `8001` | Port the mock notification server listens on |

---

## 11. Observability

### Structured logging

All log lines are JSON via `structlog`:

```json
{"event": "registration.created", "user_id": "...", "workshop_id": "...", "registration_id": "...", "timestamp": "..."}
{"event": "outbox.delivering",    "outbox_id": "...", "attempt": 3, "timestamp": "..."}
{"event": "outbox.delivered",     "outbox_id": "...", "attempt": 3, "duration_ms": 42, "timestamp": "..."}
{"event": "outbox.failed",        "outbox_id": "...", "event_type": "REGISTRATION_CONFIRMED", "attempts": 10, "exception": "ConnectError", "timestamp": "..."}
```

### Health endpoint response

```json
{
  "status": "ok",
  "db": "connected",
  "relay_worker": {
    "status": "running",
    "last_tick_at": "2026-10-19T12:34:50Z",
    "pending_outbox_count": 3
  }
}
```

### Prometheus metrics

| Metric | Type | Labels |
|--------|------|--------|
| `registrations_total` | Counter | `status` (created, rejected_full, rejected_duplicate) |
| `cancellations_total` | Counter | `type` (single, bulk) |
| `outbox_pending_count` | Gauge | — |
| `outbox_delivered_total` | Counter | `event_type` |
| `outbox_failed_total` | Counter | `event_type` |
| `notification_delivery_duration_seconds` | Histogram | `event_type` |

---

## 12. Docker Compose Topology

```
services:
  db:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: workshop_registration
      POSTGRES_USER: app
      POSTGRES_PASSWORD: secret
    ports: ["5432:5432"]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U app"]

  mock_service:
    build:
      context: .
      dockerfile: Dockerfile.mock
    ports: ["8001:8001"]
    environment:
      MOCK_SERVICE_PORT: "8001"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8001/health"]

  app:
    build: .
    ports: ["8000:8000"]
    depends_on:
      db:           { condition: service_healthy }
      mock_service: { condition: service_healthy }
    environment:
      DATABASE_URL:             "postgresql+asyncpg://app:secret@db:5432/workshop_registration"
      NOTIFICATION_SERVICE_URL: "http://mock_service:8001"
      MAX_NOTIFICATION_RETRIES: "10"
      RELAY_POLL_INTERVAL_S:    "5"
```

**Service resolution**: the main application resolves `mock_service` via Docker's internal DNS. Switching to the real notification service in production requires only changing `NOTIFICATION_SERVICE_URL` — no code changes.
