# Design Document — Task 1: Campus Sensor Monitoring and Alert Engine

## 1. System Overview

The system is a stateless FastAPI service that accepts sensor telemetry over HTTP, buffers it through Redis Streams, evaluates threshold and windowed-average alert rules in background workers, and persists alerts and rule state in PostgreSQL. All I/O paths are fully async.

```
HTTP Client
    │  POST /telemetry  (single or batch)
    ▼
FastAPI Ingestion Route
    │  asyncpg + aioredis
    ├─► Redis Stream  (sensor:telemetry)          ← write & return 202 immediately
    │
    ▼
Stream Consumer Workers (asyncio tasks, one per consumer group member)
    ├─► Threshold Evaluator   ──► PostgreSQL  (alerts table)
    └─► Window Evaluator      ──► Redis ZSET  (rolling window)
                                 └─► PostgreSQL  (alerts table)

FastAPI Alert / Rule Routes
    │
    ▼
PostgreSQL via SQLAlchemy 2.0 async session
```

---

## 2. Project Layout

```
task1_sensor_monitoring/
├── app/
│   ├── main.py                  # FastAPI app factory, lifespan hooks
│   ├── config.py                # Settings (pydantic-settings)
│   ├── database.py              # Async SQLAlchemy engine + session factory
│   ├── redis_client.py          # aioredis connection pool
│   ├── models/
│   │   ├── __init__.py
│   │   ├── base.py              # DeclarativeBase
│   │   ├── sensor.py            # SensorReading (ORM, optional persistence)
│   │   ├── alert_rule.py        # AlertRule ORM model
│   │   └── alert.py             # Alert ORM model
│   ├── schemas/
│   │   ├── __init__.py
│   │   ├── telemetry.py         # TelemetryPayload, BatchTelemetryPayload
│   │   ├── alert_rule.py        # AlertRuleCreate, AlertRuleRead, AlertRuleUpdate
│   │   └── alert.py             # AlertRead, AlertAcknowledge
│   ├── routers/
│   │   ├── __init__.py
│   │   ├── telemetry.py         # POST /telemetry
│   │   ├── alert_rules.py       # CRUD /alert-rules
│   │   └── alerts.py            # GET/PATCH /alerts
│   ├── services/
│   │   ├── ingestion.py         # Validates + pushes to Redis Stream
│   │   ├── threshold.py         # Single-point rule evaluator
│   │   └── window.py            # ZSET rolling-average evaluator
│   └── workers/
│       ├── __init__.py
│       └── consumer.py          # Stream consumer loop (asyncio)
├── tests/
├── simulator/
├── Dockerfile
└── docker-compose.yml
```

---

## 3. SQLAlchemy ORM Models

### 3.1 Base

```python
# app/models/base.py
from sqlalchemy.orm import DeclarativeBase, MappedColumn
from sqlalchemy import DateTime, func
import datetime

class Base(DeclarativeBase):
    pass
```

### 3.2 AlertRule

```python
# app/models/alert_rule.py
import uuid
import datetime
from enum import Enum as PyEnum
from sqlalchemy import String, Float, Boolean, Enum, DateTime, func, Text
from sqlalchemy.orm import Mapped, mapped_column
from app.models.base import Base


class RuleType(str, PyEnum):
    THRESHOLD = "THRESHOLD"          # single-point comparison
    WINDOW_AVERAGE = "WINDOW_AVERAGE"  # 10-min rolling mean


class AlertRule(Base):
    __tablename__ = "alert_rules"

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    sensor_id: Mapped[str | None] = mapped_column(
        String(36), nullable=True, index=True
    )  # NULL  ⟹  rule applies to all sensors
    metric_name: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    rule_type: Mapped[RuleType] = mapped_column(
        Enum(RuleType), nullable=False
    )

    # Threshold bounds — at least one must be set for THRESHOLD rules
    upper_threshold: Mapped[float | None] = mapped_column(Float, nullable=True)
    lower_threshold: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Window average — used only for WINDOW_AVERAGE rules
    window_seconds: Mapped[int] = mapped_column(default=600)   # 600 s = 10 min
    window_threshold: Mapped[float | None] = mapped_column(Float, nullable=True)

    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
```

### 3.3 Alert

```python
# app/models/alert.py
import uuid
import datetime
from enum import Enum as PyEnum
from sqlalchemy import String, Float, ForeignKey, Enum, DateTime, func, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.models.base import Base


class AlertStatus(str, PyEnum):
    TRIGGERED    = "TRIGGERED"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    RESOLVED     = "RESOLVED"


class Alert(Base):
    __tablename__ = "alerts"

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, default=uuid.uuid4
    )
    rule_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("alert_rules.id", ondelete="SET NULL"), nullable=True, index=True
    )
    sensor_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    metric_name: Mapped[str] = mapped_column(String(100), nullable=False)

    # Value that caused the alert
    # For THRESHOLD rules: the raw reading; for WINDOW_AVERAGE: the computed mean
    trigger_value: Mapped[float] = mapped_column(Float, nullable=False)
    threshold_value: Mapped[float] = mapped_column(Float, nullable=False)

    status: Mapped[AlertStatus] = mapped_column(
        Enum(AlertStatus), nullable=False, default=AlertStatus.TRIGGERED, index=True
    )

    triggered_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    acknowledged_at: Mapped[datetime.datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    resolved_at: Mapped[datetime.datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Human-readable context (e.g. "10-min avg 42.3 > threshold 40.0")
    message: Mapped[str | None] = mapped_column(Text, nullable=True)

    rule: Mapped["AlertRule"] = relationship("AlertRule", lazy="selectin")
```

**State-transition table**

| From → To | Trigger | Guard |
|---|---|---|
| _(none)_ → TRIGGERED | Rule evaluation detects violation | No existing open alert for same rule+sensor |
| TRIGGERED → ACKNOWLEDGED | `PATCH /alerts/{id}/acknowledge` | Alert must be in TRIGGERED |
| TRIGGERED → RESOLVED | Rule evaluation: violation gone | Alert must be in TRIGGERED or ACKNOWLEDGED |
| ACKNOWLEDGED → RESOLVED | Rule evaluation: violation gone | — |
| RESOLVED → TRIGGERED | New violation detected | Previous alert already RESOLVED |

---

## 4. Pydantic v2 Schemas

### 4.1 Telemetry

```python
# app/schemas/telemetry.py
from __future__ import annotations
import uuid
import datetime
from pydantic import BaseModel, Field, field_validator, model_validator

class TelemetryPayload(BaseModel):
    sensor_id: uuid.UUID
    metric_name: str = Field(..., min_length=1, max_length=100)
    value: float
    timestamp: datetime.datetime  # must be UTC-aware
    location: dict[str, float] | None = None  # {"lat": ..., "lon": ...}

    @field_validator("timestamp")
    @classmethod
    def timestamp_within_skew(cls, v: datetime.datetime) -> datetime.datetime:
        """Reject readings more than ±5 min from server time."""
        now = datetime.datetime.now(datetime.timezone.utc)
        delta = abs((v.replace(tzinfo=datetime.timezone.utc) - now).total_seconds())
        if delta > 300:
            raise ValueError("timestamp outside ±5-minute skew window")
        return v


class BatchTelemetryPayload(BaseModel):
    readings: list[TelemetryPayload] = Field(..., min_length=1, max_length=500)
```

### 4.2 AlertRule

```python
# app/schemas/alert_rule.py
from __future__ import annotations
import uuid, datetime
from pydantic import BaseModel, Field, model_validator
from app.models.alert_rule import RuleType

class AlertRuleBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    sensor_id: uuid.UUID | None = None      # None = wildcard
    metric_name: str = Field(..., min_length=1, max_length=100)
    rule_type: RuleType
    upper_threshold: float | None = None
    lower_threshold: float | None = None
    window_threshold: float | None = None
    window_seconds: int = Field(default=600, ge=60, le=3600)
    is_active: bool = True
    description: str | None = None

    @model_validator(mode="after")
    def validate_thresholds(self) -> "AlertRuleBase":
        if self.rule_type == RuleType.THRESHOLD:
            if self.upper_threshold is None and self.lower_threshold is None:
                raise ValueError(
                    "THRESHOLD rule requires at least one of upper_threshold / lower_threshold"
                )
        if self.rule_type == RuleType.WINDOW_AVERAGE:
            if self.window_threshold is None:
                raise ValueError("WINDOW_AVERAGE rule requires window_threshold")
        return self

class AlertRuleCreate(AlertRuleBase):
    pass

class AlertRuleUpdate(BaseModel):
    name: str | None = None
    upper_threshold: float | None = None
    lower_threshold: float | None = None
    window_threshold: float | None = None
    window_seconds: int | None = Field(default=None, ge=60, le=3600)
    is_active: bool | None = None
    description: str | None = None

class AlertRuleRead(AlertRuleBase):
    id: uuid.UUID
    created_at: datetime.datetime
    updated_at: datetime.datetime
    model_config = {"from_attributes": True}
```

### 4.3 Alert

```python
# app/schemas/alert.py
from __future__ import annotations
import uuid, datetime
from pydantic import BaseModel
from app.models.alert import AlertStatus

class AlertRead(BaseModel):
    id: uuid.UUID
    rule_id: uuid.UUID | None
    sensor_id: str
    metric_name: str
    trigger_value: float
    threshold_value: float
    status: AlertStatus
    message: str | None
    triggered_at: datetime.datetime
    acknowledged_at: datetime.datetime | None
    resolved_at: datetime.datetime | None
    model_config = {"from_attributes": True}

class AlertAcknowledge(BaseModel):
    note: str | None = None   # optional operator note, stored in message
```

---

## 5. Redis Key Strategy

### 5.1 Ingestion Stream

| Key | Type | Purpose |
|---|---|---|
| `sensor:telemetry` | Stream | Single stream; all sensor readings land here |

**Stream entry fields:**

```
XADD sensor:telemetry * \
  sensor_id   <uuid>       \
  metric_name <str>        \
  value       <float:str>  \
  timestamp   <iso8601>    \
  location    <json|"">
```

Consumer groups:
- `cg:threshold` — threshold rule evaluators
- `cg:window`    — rolling-average evaluators

Both groups read the **same stream** independently; each gets every message.

### 5.2 Rolling Window ZSET

One sorted-set per `(sensor_id, metric_name)` pair.

```
Key pattern:  sensor:window:<sensor_id>:<metric_name>
Score:        Unix timestamp in milliseconds  (float)
Member:       "<timestamp_ms>:<value>"        (string, ensures uniqueness)
```

**Example key:** `sensor:window:3f2a1b...:temperature`

**Read/write operations executed by the window worker per incoming reading:**

```
# 1. Add new data point
ZADD sensor:window:<sid>:<metric>  <ts_ms>  "<ts_ms>:<value>"

# 2. Prune entries older than 10 min
cutoff = now_ms - 600_000
ZREMRANGEBYSCORE sensor:window:<sid>:<metric>  -inf  <cutoff>

# 3. Fetch remaining members for average calculation
ZRANGE sensor:window:<sid>:<metric>  0  -1  WITHSCORES
# Parse values from member strings, compute arithmetic mean

# 4. Set TTL to avoid orphan keys (15-min safety margin)
EXPIRE sensor:window:<sid>:<metric>  900
```

Why member string encodes both timestamp and value: ZSET members must be unique; using `<ts_ms>:<value>` guarantees distinctness even when two readings arrive at the same millisecond with different values.

### 5.3 Alert Deduplication Key

```
Key pattern:  alert:open:<rule_id>:<sensor_id>
Type:         String  (stores alert UUID)
TTL:          None — removed explicitly on RESOLVED transition
```

Checked before creating a new TRIGGERED alert to prevent duplicates for the same rule+sensor combination.

---

## 6. Redis Stream Consumer Architecture

```
asyncio event loop
│
├── FastAPI ASGI server (uvicorn workers)
│
└── consumer.py  (started in app lifespan via asyncio.create_task)
    │
    ├── Task: threshold_consumer()
    │     loop:
    │       entries = await redis.xreadgroup("cg:threshold", consumer_id,
    │                                        {"sensor:telemetry": ">"}, count=100)
    │       for entry in entries:
    │           await threshold_evaluator.process(entry, db_session)
    │           await redis.xack("sensor:telemetry", "cg:threshold", entry.id)
    │
    └── Task: window_consumer()
          loop:
            entries = await redis.xreadgroup("cg:window", consumer_id,
                                             {"sensor:telemetry": ">"}, count=100)
            for entry in entries:
                await window_evaluator.process(entry, db_session, redis)
                await redis.xack("sensor:telemetry", "cg:window", entry.id)
```

**Key design decisions:**

- **Single process, two consumer group tasks** — runs inside the same uvicorn process using `asyncio.create_task` in the FastAPI `lifespan` context manager. No separate worker process needed for development; can be split into a separate container for production scale.
- **At-least-once delivery** — `XACK` only fires after successful DB write. If the process crashes mid-flight, the entry stays pending and is reclaimed on restart via `XAUTOCLAIM`.
- **Batch reads (`count=100`)** — amortises round-trips; each batch is processed sequentially to preserve ordering within a sensor's stream.
- **Back-pressure** — if the stream length exceeds 1,000,000 entries, `XADD` uses `MAXLEN ~ 1000000` trimming (approximate, O(1)) to cap memory.
- **Consumer group creation** — created with `XGROUP CREATE ... MKSTREAM $` on app startup; idempotent (`BUSYGROUP` error swallowed).

---

## 7. FastAPI Route Signatures

### 7.1 Telemetry Ingestion

```python
# app/routers/telemetry.py

router = APIRouter(prefix="/telemetry", tags=["Telemetry"])

@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Ingest a single sensor reading",
)
async def ingest_single(
    payload: TelemetryPayload,
    redis: Redis = Depends(get_redis),
) -> dict[str, str]:
    """Validates payload via Pydantic, pushes to Redis Stream, returns 202."""
    ...

@router.post(
    "/batch",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Ingest a batch of sensor readings (max 500)",
)
async def ingest_batch(
    payload: BatchTelemetryPayload,
    redis: Redis = Depends(get_redis),
) -> dict[str, int]:
    """Pushes each validated reading to Redis Stream, returns accepted count."""
    ...
```

### 7.2 Alert Rules (CRUD)

```python
# app/routers/alert_rules.py

router = APIRouter(prefix="/alert-rules", tags=["Alert Rules"])

@router.post(
    "",
    response_model=AlertRuleRead,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new alert rule",
)
async def create_alert_rule(
    body: AlertRuleCreate,
    db: AsyncSession = Depends(get_db),
) -> AlertRuleRead: ...

@router.get(
    "",
    response_model=list[AlertRuleRead],
    summary="List all alert rules",
)
async def list_alert_rules(
    is_active: bool | None = Query(default=None),
    rule_type: RuleType | None = Query(default=None),
    db: AsyncSession = Depends(get_db),
) -> list[AlertRuleRead]: ...

@router.get(
    "/{rule_id}",
    response_model=AlertRuleRead,
    summary="Get a single alert rule by ID",
)
async def get_alert_rule(
    rule_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
) -> AlertRuleRead: ...

@router.patch(
    "/{rule_id}",
    response_model=AlertRuleRead,
    summary="Update an alert rule",
)
async def update_alert_rule(
    rule_id: uuid.UUID,
    body: AlertRuleUpdate,
    db: AsyncSession = Depends(get_db),
) -> AlertRuleRead: ...

@router.delete(
    "/{rule_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete an alert rule",
)
async def delete_alert_rule(
    rule_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
) -> None: ...
```

### 7.3 Alerts (Read + Lifecycle)

```python
# app/routers/alerts.py

router = APIRouter(prefix="/alerts", tags=["Alerts"])

@router.get(
    "",
    response_model=list[AlertRead],
    summary="List alerts with optional status filter",
)
async def list_alerts(
    status: AlertStatus | None = Query(default=None),
    sensor_id: str | None = Query(default=None),
    from_ts: datetime | None = Query(default=None),
    to_ts: datetime | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
) -> list[AlertRead]: ...

@router.get(
    "/{alert_id}",
    response_model=AlertRead,
    summary="Get a single alert by ID",
)
async def get_alert(
    alert_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
) -> AlertRead: ...

@router.patch(
    "/{alert_id}/acknowledge",
    response_model=AlertRead,
    summary="Acknowledge a TRIGGERED alert",
)
async def acknowledge_alert(
    alert_id: uuid.UUID,
    body: AlertAcknowledge,
    db: AsyncSession = Depends(get_db),
) -> AlertRead:
    """Transitions TRIGGERED → ACKNOWLEDGED. Returns 409 if already in another state."""
    ...

@router.patch(
    "/{alert_id}/resolve",
    response_model=AlertRead,
    summary="Manually resolve an alert",
)
async def resolve_alert(
    alert_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
) -> AlertRead:
    """Transitions TRIGGERED or ACKNOWLEDGED → RESOLVED."""
    ...
```

### 7.4 Health Check

```python
# app/routers/health.py  (registered at app root, no prefix)

@router.get("/health", tags=["Health"])
async def health_check(
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
) -> dict[str, str]:
    """Returns component-level health: postgres, redis, stream_lag."""
    ...
```

---

## 8. Database Migrations

Alembic with `async` driver (`asyncpg`). Migration scripts live in `alembic/versions/`. Initial migration creates:
- `alert_rules` table
- `alerts` table
- Index on `alerts(status)`, `alerts(sensor_id)`, `alerts(triggered_at DESC)`

---

## 9. Dependency Injection Providers

```python
# app/database.py
async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with async_session_factory() as session:
        yield session

# app/redis_client.py
async def get_redis() -> Redis:
    return redis_pool  # module-level aioredis.Redis instance
```

---

## 10. Configuration (pydantic-settings)

```python
# app/config.py
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    database_url: str           # postgresql+asyncpg://...
    redis_url: str              # redis://...
    stream_name: str = "sensor:telemetry"
    stream_max_len: int = 1_000_000
    window_seconds: int = 600
    batch_max_size: int = 500
    timestamp_skew_seconds: int = 300

    model_config = {"env_file": ".env"}

settings = Settings()
```
