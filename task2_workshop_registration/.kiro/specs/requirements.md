# Requirements Specification — Task 2: Workshop Registration and Notifications Engine

> Written in **EARS (Easy Approach to Requirements Syntax)** notation.  
> Patterns used: Ubiquitous (`The system shall`), Event-driven (`When … the system shall`),
> State-driven (`While … the system shall`), Conditional (`If … the system shall`),
> Optional feature (`Where … the system shall`).

---

## 1. Glossary

| Term | Definition |
|------|-----------|
| **Workshop** | A scheduled GDG event with a fixed seat capacity. |
| **Registration** | A confirmed, unique seat reservation held by one attendee for one workshop. |
| **Attendee** | An authenticated user identified by a stable `user_id`. |
| **Outbox record** | A notification payload persisted in the same database transaction as the registration, pending delivery to the external notification service. |
| **Notification service** | The external HTTP endpoint that sends confirmation/cancellation emails and push notifications. |
| **Fault-injecting mock** | A test-double notification server that can be configured to return 5xx errors, time out, or drop connections on demand. |
| **Bulk cancellation** | An operator-initiated action that cancels an entire workshop and all of its registrations. |

---

## 2. Workshop Management

**REQ-WM-01**  
The system shall store each workshop with a unique `workshop_id`, a human-readable title, a scheduled datetime, a venue, and a non-negative integer seat capacity.

**REQ-WM-02**  
When an operator creates a workshop, the system shall reject the request and return HTTP 422 if the scheduled datetime is in the past relative to the server clock.

**REQ-WM-03**  
The system shall expose a paginated read endpoint that returns all workshops with their current `registered_count` and `available_seats` computed at query time.

**REQ-WM-04**  
While a workshop has at least one confirmed registration, the system shall prevent deletion of that workshop and return HTTP 409 with an explanatory error body.

---

## 3. Unique Single-Registration Invariant

**REQ-REG-01** *(Uniqueness)*  
The system shall enforce that each `(user_id, workshop_id)` pair has at most one confirmed registration at any point in time.

**REQ-REG-02** *(Idempotency guard)*  
When a registration request arrives for a `(user_id, workshop_id)` pair that already has a confirmed registration, the system shall return HTTP 409 Conflict with error code `ALREADY_REGISTERED` and shall not create a duplicate record.

**REQ-REG-03** *(Database-level enforcement)*  
The system shall back the uniqueness invariant with a database-level unique constraint on `(user_id, workshop_id)` so that concurrent duplicate requests that pass application-layer checks are still rejected by the database.

**REQ-REG-04** *(Capacity)*  
When a registration request arrives for a workshop whose `registered_count` equals its `capacity`, the system shall return HTTP 409 Conflict with error code `WORKSHOP_FULL` and shall not create a registration record.

**REQ-REG-05** *(Atomic seat claim)*  
The system shall reserve a seat and insert the registration record within a single database transaction so that no two concurrent registrations can each observe available capacity and both succeed when only one seat remains.

---

## 4. Immediate Non-Blocking HTTP Responses

**REQ-NB-01** *(Accepted response)*  
When a valid registration request is received and the seat claim succeeds, the system shall return HTTP 202 Accepted immediately — before any attempt to contact the external notification service.

**REQ-NB-02** *(Response latency budget)*  
The system shall return its HTTP response to the caller within 500 ms at the 99th percentile under the nominal load profile, regardless of the availability or latency of the external notification service.

**REQ-NB-03** *(Non-blocking cancellation)*  
When a single-registration cancellation request is received and the registration is successfully marked cancelled in the database, the system shall return HTTP 202 Accepted before notification delivery is attempted.

**REQ-NB-04** *(Decoupled delivery)*  
The system shall dispatch notification delivery to an internal background worker or async task queue so that the HTTP request handler thread is never blocked waiting for the notification service to respond.

---

## 5. Transactional Outbox for Crash Resilience

**REQ-OB-01** *(Atomic write)*  
When a registration or cancellation is committed to the database, the system shall insert a corresponding outbox record in the same database transaction, ensuring both writes succeed or both roll back together.

**REQ-OB-02** *(Outbox schema)*  
The system shall persist each outbox record with at minimum: `outbox_id`, `event_type` (`REGISTRATION_CONFIRMED` | `REGISTRATION_CANCELLED` | `WORKSHOP_CANCELLED`), `payload` (JSON), `status` (`PENDING` | `DELIVERED` | `FAILED`), `created_at`, `attempts`, and `last_attempted_at`.

**REQ-OB-03** *(Relay worker)*  
The system shall run a background relay worker that polls for `PENDING` outbox records and attempts delivery to the external notification service.

**REQ-OB-04** *(Crash resilience)*  
If the application process crashes after committing a registration but before the notification is delivered, the system shall, upon restart, detect the undelivered outbox records and re-attempt delivery without requiring any manual intervention.

**REQ-OB-05** *(At-least-once delivery)*  
The relay worker shall retry delivery of a `PENDING` outbox record using exponential back-off (initial interval 1 s, multiplier 2, cap 5 min) until the notification service acknowledges success with HTTP 2xx.

**REQ-OB-06** *(Failure marking)*  
If a relay worker delivery attempt has been retried more than the configured `MAX_NOTIFICATION_RETRIES` times (default: 10) without a 2xx response, the system shall mark the outbox record as `FAILED` and emit a structured log entry at ERROR level containing the `outbox_id`, `event_type`, and final HTTP status code or exception type.

**REQ-OB-07** *(Delivery idempotency key)*  
Where the notification service supports idempotency keys, the system shall include the `outbox_id` as the idempotency key on every delivery attempt so that re-delivery of the same record is safe.

**REQ-OB-08** *(Concurrent relay safety)*  
While multiple relay worker instances are running, the system shall use a pessimistic row-level lock (e.g., `SELECT … FOR UPDATE SKIP LOCKED`) when claiming outbox records so that each record is delivered by at most one worker at a time.

---

## 6. Bulk Workshop Cancellation

**REQ-BK-01** *(Trigger endpoint)*  
The system shall expose a `POST /workshops/{workshop_id}/cancel` endpoint restricted to users holding the `OPERATOR` role.

**REQ-BK-02** *(Idempotent cancellation)*  
When the bulk cancellation endpoint is called on a workshop that is already in `CANCELLED` state, the system shall return HTTP 200 with a body indicating no action was taken, without re-processing registrations.

**REQ-BK-03** *(Atomic state transition)*  
When bulk cancellation is triggered, the system shall update the workshop status to `CANCELLED` and update all `CONFIRMED` registrations for that workshop to `CANCELLED` within a single database transaction.

**REQ-BK-04** *(Outbox fan-out)*  
Within the same transaction as REQ-BK-03, the system shall insert one `WORKSHOP_CANCELLED` outbox record per affected attendee so that each attendee receives an individual cancellation notification.

**REQ-BK-05** *(Non-blocking bulk response)*  
The bulk cancellation endpoint shall return HTTP 202 Accepted with a `cancelled_registration_count` field as soon as the database transaction commits, without waiting for any notification to be delivered.

**REQ-BK-06** *(Large workshop resilience)*  
If a workshop has more than 1 000 confirmed registrations, the system shall process the cancellation and outbox fan-out in batches of at most 500 rows per transaction to avoid long-held locks and oversized transactions.

---

## 7. Fault-Injecting Mock Notification Server

**REQ-MOCK-01** *(Purpose)*  
The system shall include a configurable mock notification server that acts as a drop-in replacement for the external notification service during local development and automated testing.

**REQ-MOCK-02** *(Fault modes)*  
The mock server shall support the following independently configurable fault modes, activatable at runtime via a control API:

| Mode | Behaviour |
|------|----------|
| `success` | Returns HTTP 200 with a JSON acknowledgement body (default). |
| `server_error` | Returns HTTP 500 with a JSON error body on every request. |
| `transient_error` | Returns HTTP 500 for the first N requests, then HTTP 200 (N is configurable). |
| `timeout` | Accepts the connection but does not respond until the client times out. |
| `connection_drop` | Closes the TCP connection immediately without sending a response. |

**REQ-MOCK-03** *(Control API)*  
The mock server shall expose a `POST /control` endpoint that accepts a JSON body `{ "mode": "<fault_mode>", "transient_count": <int> }` and switches behaviour without requiring a server restart.

**REQ-MOCK-04** *(Request log)*  
The mock server shall maintain an in-memory log of every inbound notification request (method, path, headers, body, timestamp) and expose it via `GET /requests` so that tests can assert on delivery attempts without side effects.

**REQ-MOCK-05** *(Reset)*  
The mock server shall expose a `POST /reset` endpoint that clears the request log and returns the fault mode to `success`, enabling test isolation between test cases.

**REQ-MOCK-06** *(Containerised deployment)*  
The mock server shall be deployable as a separate service in the `docker-compose.yml` used for local development and integration testing, reachable by the main application via its Docker service hostname.

---

## 8. API Contract

**REQ-API-01**  
The system shall return all error responses in a uniform JSON envelope: `{ "error": { "code": "<SNAKE_UPPER>", "message": "<human-readable>" } }`.

**REQ-API-02**  
The system shall version all routes under the `/api/v1` prefix.

**REQ-API-03**  
The system shall validate all incoming request bodies against JSON schema (via Pydantic or equivalent) and return HTTP 422 with a field-level error list for any validation failure.

**REQ-API-04**  
The system shall include an `X-Request-ID` header in every response, echoing the value from the request if present or generating a UUID v4 if absent.

---

## 9. Observability

**REQ-OBS-01**  
The system shall emit structured JSON logs for every registration event, cancellation event, outbox delivery attempt, and outbox delivery failure.

**REQ-OBS-02**  
The system shall expose a `GET /health` endpoint that returns HTTP 200 with database connectivity status and relay worker heartbeat timestamp so that the Docker healthcheck can detect degraded states.

**REQ-OBS-03**  
The system shall expose a `GET /metrics` endpoint (Prometheus text format) that includes at minimum: `registrations_total`, `cancellations_total`, `outbox_pending_count`, `outbox_delivered_total`, `outbox_failed_total`, and `notification_delivery_duration_seconds` histogram.

---

## 10. Non-Functional Requirements

**REQ-NFR-01** *(Concurrency)*  
The system shall handle at least 200 concurrent registration requests without data corruption, duplicate registrations, or overselling of seats.

**REQ-NFR-02** *(Persistence)*  
All registration state and outbox records shall be stored in a PostgreSQL database; no in-memory-only state that affects correctness shall be used.

**REQ-NFR-03** *(Configuration)*  
All environment-specific values (database URL, notification service URL, retry limits, batch sizes, poll intervals) shall be supplied via environment variables and shall have documented defaults.

**REQ-NFR-04** *(Containerisation)*  
The application shall be fully runnable via `docker compose up` with no host-level dependencies beyond Docker and Docker Compose.

**REQ-NFR-05** *(Graceful shutdown)*  
When the application receives `SIGTERM`, the system shall complete in-flight HTTP requests and the current relay worker iteration before exiting, ensuring no outbox records are left in a partially-processed state.
