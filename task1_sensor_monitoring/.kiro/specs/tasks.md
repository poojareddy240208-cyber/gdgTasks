# Implementation Tasks — Task 1: Campus Sensor Monitoring and Alert Engine

Tasks are ordered by dependency. Each task touches at most 1–2 files.
Mark a task `[x]` once its acceptance criteria are met and the file(s) compile without errors.

---

## Phase 1 — Project Scaffold & Configuration

- [x] **Task 1.1 — Create `requirements.txt`**
  - File: `task1_sensor_monitoring/requirements.txt`
  - Pin all runtime dependencies:
    ```
    fastapi==0.111.0
    uvicorn[standard]==0.29.0
    sqlalchemy==2.0.30
    asyncpg==0.29.0
    alembic==1.13.1
    redis==5.0.4
    pydantic==2.7.1
    pydantic-settings==2.2.1
    ```
  - No other files change.

- [x] **Task 1.2 — Create `app/config.py`**
  - File: `task1_sensor_monitoring/app/config.py`
  - Implement the `Settings` class using `pydantic-settings` exactly as specified in design §10.
  - Fields: `database_url`, `redis_url`, `stream_name`, `stream_max_len`, `window_seconds`, `batch_max_size`, `timestamp_skew_seconds`.
  - Export a module-level `settings = Settings()` singleton.

- [x] **Task 1.3 — Create `app/models/base.py`**
  - File: `task1_sensor_monitoring/app/models/base.py`
  - Define `Base(DeclarativeBase)` with no extra columns.
  - Add `app/models/__init__.py` (empty) in the same commit.

- [x] **Task 1.4 — Create `app/database.py`**
  - File: `task1_sensor_monitoring/app/database.py`
  - Create async SQLAlchemy engine from `settings.database_url`.
  - Create `async_session_factory` with `expire_on_commit=False`.
  - Implement `get_db() -> AsyncGenerator[AsyncSession, None]` dependency.

- [ ] **Task 1.5 — Create `app/redis_client.py`**
  - File: `task1_sensor_monitoring/app/redis_client.py`
  - Create a module-level `redis_pool` using `redis.asyncio.from_url(settings.redis_url, decode_responses=True)`.
  - Implement `get_redis() -> Redis` dependency that returns `redis_pool`.
  - Implement `init_consumer_groups()` coroutine: calls `XGROUP CREATE sensor:telemetry cg:threshold $ MKSTREAM` and `XGROUP CREATE sensor:telemetry cg:window $ MKSTREAM`, swallowing `ResponseError: BUSYGROUP` on both.

---

## Phase 2 — ORM Models

- [x] **Task 2.1 — Create `app/models/alert_rule.py`**
  - File: `task1_sensor_monitoring/app/models/alert_rule.py`
  - Implement `RuleType` enum (`THRESHOLD`, `WINDOW_AVERAGE`).
  - Implement `AlertRule` ORM model exactly as specified in design §3.2.
  - All columns: `id`, `name`, `sensor_id`, `metric_name`, `rule_type`, `upper_threshold`, `lower_threshold`, `window_seconds`, `window_threshold`, `is_active`, `description`, `created_at`, `updated_at`.

- [x] **Task 2.2 — Create `app/models/alert.py`**
  - File: `task1_sensor_monitoring/app/models/alert.py`
  - Implement `AlertStatus` enum (`TRIGGERED`, `ACKNOWLEDGED`, `RESOLVED`).
  - Implement `Alert` ORM model exactly as specified in design §3.3.
  - All columns: `id`, `rule_id` (FK → `alert_rules.id` ON DELETE SET NULL), `sensor_id`, `metric_name`, `trigger_value`, `threshold_value`, `status`, `triggered_at`, `acknowledged_at`, `resolved_at`, `message`.
  - Add `rule` relationship with `lazy="selectin"`.

- [x] **Task 2.3 — Update `app/models/__init__.py`**
  - File: `task1_sensor_monitoring/app/models/__init__.py`
  - Re-export `Base`, `AlertRule`, `RuleType`, `Alert`, `AlertStatus` so Alembic autogenerate can discover all mappers from one import.

---

## Phase 3 — Alembic Migration

- [ ] **Task 3.1 — Initialise Alembic**
  - Files: `task1_sensor_monitoring/alembic.ini`, `task1_sensor_monitoring/alembic/env.py`
  - Run `alembic init alembic` inside `task1_sensor_monitoring/`.
  - In `alembic/env.py`: import `settings` and set `config.set_main_option("sqlalchemy.url", settings.database_url)`.
  - Set `target_metadata = Base.metadata` (import `Base` from `app.models`).
  - Configure `run_migrations_online()` to use `AsyncEngine` via `asyncio.run()`.

- [ ] **Task 3.2 — Generate and edit initial migration**
  - Files: `task1_sensor_monitoring/alembic/versions/<rev>_initial_schema.py`
  - Run `alembic revision --autogenerate -m "initial_schema"`.
  - Verify the generated file creates tables `alert_rules` and `alerts` with all columns and enums.
  - Add explicit indexes: `ix_alerts_status`, `ix_alerts_sensor_id`, `ix_alerts_triggered_at_desc`.

---

## Phase 4 — Pydantic Schemas

- [x] **Task 4.1 — Create `app/schemas/telemetry.py`**
  - File: `task1_sensor_monitoring/app/schemas/telemetry.py`
  - Implement `TelemetryPayload` with `sensor_id: uuid.UUID`, `metric_name`, `value: float`, `timestamp: datetime`, `location: dict[str, float] | None`.
  - Add `@field_validator("timestamp")` that rejects readings outside ±`settings.timestamp_skew_seconds` of `utcnow()`.
  - Implement `BatchTelemetryPayload` with `readings: list[TelemetryPayload]` capped at `settings.batch_max_size`.
  - Add `app/schemas/__init__.py` (empty).

- [x] **Task 4.2 — Create `app/schemas/alert_rule.py`**
  - File: `task1_sensor_monitoring/app/schemas/alert_rule.py`
  - Implement `AlertRuleBase`, `AlertRuleCreate`, `AlertRuleUpdate`, `AlertRuleRead` exactly as specified in design §4.2.
  - `@model_validator` must enforce that `THRESHOLD` rules have at least one of `upper_threshold`/`lower_threshold`, and `WINDOW_AVERAGE` rules have `window_threshold`.

- [x] **Task 4.3 — Create `app/schemas/alert.py`**
  - File: `task1_sensor_monitoring/app/schemas/alert.py`
  - Implement `AlertRead` and `AlertAcknowledge` exactly as specified in design §4.3.
  - `AlertRead` must use `model_config = {"from_attributes": True}`.

---

## Phase 5 — Ingestion Service

- [x] **Task 5.1 — Create `app/services/ingestion.py`**
  - File: `task1_sensor_monitoring/app/services/ingestion.py`
  - Implement `async def push_to_stream(redis: Redis, payload: TelemetryPayload) -> str`.
  - Serialise payload to a flat dict (all values as strings): `sensor_id`, `metric_name`, `value`, `timestamp` (ISO 8601), `location` (JSON string or empty string).
  - Call `XADD settings.stream_name MAXLEN ~ settings.stream_max_len * <fields>` and return the entry ID.
  - Implement `async def push_batch_to_stream(redis: Redis, batch: BatchTelemetryPayload) -> int` that calls `push_to_stream` for each reading and returns the accepted count.

---

## Phase 6 — Alert Rule & Alert Services

- [ ] **Task 6.1 — Create `app/services/alert_rule_service.py`**
  - File: `task1_sensor_monitoring/app/services/alert_rule_service.py`
  - CRUD helpers used by the router:
    - `create_rule(db, body: AlertRuleCreate) -> AlertRule`
    - `list_rules(db, is_active, rule_type) -> list[AlertRule]`
    - `get_rule(db, rule_id) -> AlertRule | None`
    - `update_rule(db, rule_id, body: AlertRuleUpdate) -> AlertRule`
    - `delete_rule(db, rule_id) -> None`
  - All functions are `async`; use `select()` with `where()` clauses; commit inside each mutating function.

- [ ] **Task 6.2 — Create `app/services/alert_service.py`**
  - File: `task1_sensor_monitoring/app/services/alert_service.py`
  - Implement:
    - `async def open_alert(db, redis, rule: AlertRule, sensor_id, trigger_value, threshold_value, message) -> Alert | None`
      - Checks dedup key `alert:open:<rule_id>:<sensor_id>` in Redis; returns `None` if already open.
      - Creates `Alert(status=TRIGGERED)`, persists, then sets dedup key to `str(alert.id)` with no TTL.
    - `async def acknowledge_alert(db, alert_id, note) -> Alert`
      - Guards: status must be `TRIGGERED`; raises `HTTPException(409)` otherwise.
      - Sets `status=ACKNOWLEDGED`, `acknowledged_at=utcnow()`, appends note to `message`.
    - `async def resolve_alert(db, redis, alert_id) -> Alert`
      - Guards: status must be `TRIGGERED` or `ACKNOWLEDGED`.
      - Sets `status=RESOLVED`, `resolved_at=utcnow()`.
      - Deletes dedup key from Redis.
    - `async def list_alerts(db, status, sensor_id, from_ts, to_ts, limit, offset) -> list[Alert]`
    - `async def get_alert(db, alert_id) -> Alert | None`

---

## Phase 7 — Threshold Evaluator

- [x] **Task 7.1 — Create `app/services/threshold.py`** _(implemented inside `app/workers/stream_processor.py`)_
  - File: `task1_sensor_monitoring/app/services/threshold.py`
  - Implement `async def evaluate_threshold(db: AsyncSession, redis: Redis, entry: dict) -> None`.
  - Parse `sensor_id`, `metric_name`, `value` from the stream entry dict.
  - Query all active `THRESHOLD` rules matching `metric_name` AND (`sensor_id = entry.sensor_id` OR `sensor_id IS NULL`).
  - For each matching rule:
    - If `value > upper_threshold` (when set): call `open_alert(...)`.
    - If `value < lower_threshold` (when set): call `open_alert(...)`.
    - If no violation and an open alert exists for rule+sensor: call `resolve_alert(...)`.

---

## Phase 8 — Rolling Window Evaluator

- [x] **Task 8.1 — Create `app/services/window.py`** _(implemented as `window_manager.py`)_
  - File: `task1_sensor_monitoring/app/services/window.py`
  - Implement `async def update_window(redis: Redis, sensor_id: str, metric_name: str, ts_ms: int, value: float) -> float`:
    - Key: `sensor:window:<sensor_id>:<metric_name>`
    - `ZADD` with score=`ts_ms`, member=`f"{ts_ms}:{value}"`.
    - `ZREMRANGEBYSCORE` from `-inf` to `now_ms - window_seconds*1000`.
    - `ZRANGE 0 -1` → parse values from member strings → compute arithmetic mean.
    - `EXPIRE key 900`.
    - Return the computed mean (or `None` if no members remain after pruning).
  - Implement `async def evaluate_window(db: AsyncSession, redis: Redis, entry: dict) -> None`:
    - Parse entry fields, compute `ts_ms = int(datetime.fromisoformat(entry["timestamp"]).timestamp() * 1000)`.
    - Call `update_window(...)` to get the current mean.
    - Query all active `WINDOW_AVERAGE` rules for matching `metric_name` / `sensor_id`.
    - For each rule: if `mean > window_threshold` call `open_alert`; else if open alert exists call `resolve_alert`.

---

## Phase 9 — Stream Consumer

- [x] **Task 9.1 — Create `app/workers/consumer.py`** _(implemented as `app/workers/stream_processor.py`)_
  - Files: `task1_sensor_monitoring/app/workers/consumer.py`, `task1_sensor_monitoring/app/workers/__init__.py`
  - Implement `async def threshold_consumer(redis: Redis, session_factory) -> None`:
    - Infinite loop with `try/except` and 1-second back-off on error.
    - `XREADGROUP GROUP cg:threshold consumer-threshold BLOCK 2000 COUNT 100 STREAMS sensor:telemetry >`.
    - For each entry: open a new `AsyncSession`, call `evaluate_threshold(db, redis, fields)`, `XACK`.
  - Implement `async def window_consumer(redis: Redis, session_factory) -> None` — same shape, calls `evaluate_window`.
  - Both consumers call `XAUTOCLAIM sensor:telemetry cg:<group> consumer-<group> 60000 0-0 COUNT 50` at startup to reclaim stale pending entries.

---

## Phase 10 — FastAPI Routers

- [x] **Task 10.1 — Create `app/routers/telemetry.py`** _(implemented as `app/api/telemetry.py`)_
  - File: `task1_sensor_monitoring/app/routers/telemetry.py`
  - Implement `POST /telemetry` → calls `push_to_stream`, returns `{"status": "accepted", "stream_id": <id>}` with HTTP 202.
  - Implement `POST /telemetry/batch` → calls `push_batch_to_stream`, returns `{"accepted": <count>}` with HTTP 202.

- [x] **Task 10.2 — Create `app/routers/alert_rules.py`** _(implemented as `app/api/rules.py`)_
  - File: `task1_sensor_monitoring/app/routers/alert_rules.py`
  - Wire up all five route handlers to `alert_rule_service` functions:
    `POST /alert-rules`, `GET /alert-rules`, `GET /alert-rules/{rule_id}`, `PATCH /alert-rules/{rule_id}`, `DELETE /alert-rules/{rule_id}`.
  - Return `404` from `get_rule` and `update_rule` when the rule doesn't exist.

- [x] **Task 10.3 — Create `app/routers/alerts.py`** _(implemented as `app/api/alerts.py`)_
  - File: `task1_sensor_monitoring/app/routers/alerts.py`
  - Wire up all four route handlers to `alert_service` functions:
    `GET /alerts`, `GET /alerts/{alert_id}`, `PATCH /alerts/{alert_id}/acknowledge`, `PATCH /alerts/{alert_id}/resolve`.
  - Return `404` when alert not found; propagate `409` from service layer.

- [ ] **Task 10.4 — Create `app/routers/health.py`**
  - File: `task1_sensor_monitoring/app/routers/health.py`
  - `GET /health`: run `SELECT 1` against DB and `PING` against Redis.
  - Compute `stream_lag` = `XLEN sensor:telemetry` − pending count from `XPENDING`.
  - Return `{"postgres": "ok"|"error", "redis": "ok"|"error", "stream_lag": <int>}`.

---

## Phase 11 — App Entry Point

- [x] **Task 11.1 — Create `app/main.py`**
  - File: `task1_sensor_monitoring/app/main.py`
  - Define `@asynccontextmanager async def lifespan(app)`:
    1. Call `init_consumer_groups()` from `redis_client`.
    2. Launch `asyncio.create_task(threshold_consumer(...))`.
    3. Launch `asyncio.create_task(window_consumer(...))`.
    4. `yield`.
    5. Cancel both tasks on shutdown.
  - Create `app = FastAPI(title="Campus Sensor Monitor", lifespan=lifespan)`.
  - Register all four routers with `app.include_router(...)`.
  - Add global exception handler for `RequestValidationError` → HTTP 400 with field-level detail.

---

## Phase 12 — Infrastructure

- [ ] **Task 12.1 — Create `Dockerfile`**
  - File: `task1_sensor_monitoring/Dockerfile`
  - Multi-stage: `python:3.11-slim` builder installs deps into `/install`; final stage copies only `/install` and `app/`.
  - `CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]`.

- [ ] **Task 12.2 — Create `docker-compose.yml`**
  - File: `task1_sensor_monitoring/docker-compose.yml`
  - Services: `api` (build from `.`), `postgres` (`postgres:16-alpine`), `redis` (`redis:7-alpine`).
  - `api` depends on `postgres` and `redis` with `healthcheck` conditions.
  - Environment variables for `DATABASE_URL` and `REDIS_URL` wired from compose service names.
  - Mount `alembic/` into `api` container; add `command` to run `alembic upgrade head && uvicorn ...`.

---

## Phase 13 — Simulator

- [x] **Task 13.1 — Create `simulator/simulate.py`** _(implemented as `simulator/load_generator.py`)_
  - File: `task1_sensor_monitoring/simulator/simulate.py`
  - Async script using `httpx.AsyncClient` that fires `N` concurrent `POST /telemetry` requests (configurable via `--count` and `--concurrency` CLI args).
  - Generates random `sensor_id` (UUID), `metric_name` from a fixed list, `value` in a configurable range, `timestamp=utcnow()`.
  - Prints throughput (req/sec) and success/error counts on completion.

---

## Phase 14 — Tests

- [ ] **Task 14.1 — Create `tests/test_telemetry_ingestion.py`**
  - File: `task1_sensor_monitoring/tests/test_telemetry_ingestion.py`
  - Use `pytest` + `httpx.AsyncClient` with `ASGITransport`.
  - Tests: single valid payload → 202; batch → 202; invalid timestamp skew → 400; missing field → 422; batch over 500 → 422.

- [ ] **Task 14.2 — Create `tests/test_alert_rules.py`**
  - File: `task1_sensor_monitoring/tests/test_alert_rules.py`
  - Tests: create THRESHOLD rule → 201; create WINDOW_AVERAGE rule → 201; create rule missing threshold → 422; list rules; get by ID; patch; delete → 204; get deleted → 404.

- [ ] **Task 14.3 — Create `tests/test_alert_lifecycle.py`**
  - File: `task1_sensor_monitoring/tests/test_alert_lifecycle.py`
  - Tests: threshold evaluator creates TRIGGERED alert on violation; second evaluation with same violation does not duplicate; violation clears → RESOLVED; acknowledge TRIGGERED → ACKNOWLEDGED; acknowledge RESOLVED → 409; window evaluator creates alert when mean exceeds threshold.

- [ ] **Task 14.4 — Create `tests/conftest.py`**
  - File: `task1_sensor_monitoring/tests/conftest.py`
  - Fixtures: in-memory SQLite async engine (or `testcontainers` PostgreSQL), mock Redis using `fakeredis.aioredis`, `AsyncClient` pointed at the FastAPI app.
  - Scope: `function` — fresh DB and Redis state per test.
