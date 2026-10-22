# Task 2: Workshop Registration and Notifications Engine

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB.svg?style=flat&logo=python)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-009688.svg?style=flat&logo=fastapi)](https://fastapi.tiangolo.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15.0+-336791.svg?style=flat&logo=postgresql)](https://www.postgresql.org/)
[![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.0+-D71100.svg?style=flat&logo=python)](https://www.sqlalchemy.org/)
[![Docker](https://img.shields.io/badge/Docker-Enabled-2496ED.svg?style=flat&logo=docker)](https://www.docker.com/)

An asynchronous, fault-tolerant workshop registration microservice that uses the **Transactional Outbox Pattern** to coordinate participant registration and notification delivery without requiring distributed transactions.

The application uses FastAPI, PostgreSQL, SQLAlchemy, Alembic, and an asynchronous background worker to process notifications reliably, recover pending work after restarts, and retry transient downstream failures.

---

## 📌 Table of Contents

- [Architecture Overview](#-architecture-overview)
- [Key Features](#-key-features)
- [Technology Stack](#️-technology-stack)
- [Directory Structure](#-directory-structure)
- [Requirements and Prerequisites](#-requirements-and-prerequisites)
- [Installation and Setup](#-installation-and-setup)
- [Data Models and Status Lifecycle](#-data-models-and-status-lifecycle)
- [API Specifications](#-api-specifications)
- [Running the Application](#-running-the-application)
- [Testing and Crash Recovery](#-testing-and-crash-recovery)
- [Monitoring and Troubleshooting](#-monitoring-and-troubleshooting)
- [Stopping the Application](#-stopping-the-application)

---

## 🏗️ Architecture Overview

### The Transactional Outbox Pattern

Calling a notification service directly inside a registration endpoint introduces reliability problems. A slow downstream service can delay the HTTP response, and a process crash between saving a registration and sending its notification can result in lost notifications.

The Transactional Outbox Pattern addresses this by storing the registration and its corresponding notification event in the same PostgreSQL transaction.

```text
                CLIENT REQUEST
                      |
                      v
             +--------------------+
             |  FastAPI Endpoint  |
             | Workshop Register |
             +--------------------+
                      |
                      v
          +---------------------------+
          |   PostgreSQL Transaction  |
          |                           |
          |  1. Save Registration     |
          |  2. Save Outbox Event     |
          +---------------------------+
                      |
                      v
             HTTP 201 Created
                      |
                      v
                 CLIENT RESPONSE


        ASYNCHRONOUS BACKGROUND PROCESSING

             +--------------------+
             | Outbox Worker      |
             | Poll Pending Events|
             +--------------------+
                      |
                      v
             +--------------------+
             | Notification Mock  |
             | Service            |
             +--------------------+
                      |
              +-------+-------+
              |               |
              v               v
           Success          Failure
              |               |
              v               v
        Mark DELIVERED    Schedule Retry
                              |
                              v
                    Exponential Backoff
                              |
                              v
                       Retry Delivery
```

### Processing Workflow

1. A client submits a workshop registration request.
2. FastAPI validates the request payload.
3. PostgreSQL creates the registration and its corresponding outbox event in one transaction.
4. The transaction commits, and the API returns a successful response.
5. A background worker identifies pending notification events.
6. The worker sends each notification to the downstream notification service.
7. Successful deliveries are marked `DELIVERED`.
8. Transient failures are retried with exponential backoff and jitter.
9. Pending events remain persisted in PostgreSQL so they can be processed after a worker restart.

---

## 🚀 Key Features

### 1. Atomic Registration and Outbox Persistence

Registration data and notification scheduling are committed in the same database transaction.

This prevents the application from committing a registration while failing to persist the corresponding notification event.

### 2. Duplicate Registration Prevention

A PostgreSQL `UniqueConstraint` on `(user_id, workshop_id)` prevents the same user from registering for the same workshop more than once.

Concurrent duplicate requests should be handled gracefully, with HTTP `409 Conflict` returned when a duplicate registration is detected.

### 3. Asynchronous Notification Processing

Notification delivery happens outside the registration request's critical path.

This reduces registration latency and prevents temporary notification-service failures from directly blocking the registration endpoint.

### 4. Crash Recovery

Pending notification events are stored persistently in PostgreSQL.

If the worker or application process crashes, the system can resume processing after restart, provided the worker correctly recovers pending or stale processing events.

### 5. Exponential Backoff with Jitter

Failed notification attempts can be retried using an increasing delay, with random jitter to reduce synchronized retry bursts.

A conceptual retry schedule is:

\[
d_n = \min(d_{\max}, d_0 \times 2^n) + J_n
\]

Where:

- \(d_n\) is the delay before a retry.
- \(d_0\) is the initial delay.
- \(d_{\max}\) is the maximum backoff delay.
- \(n\) is the retry number.
- \(J_n\) is a random jitter component.

The actual formula, maximum delay, and retry count should match the implementation.

### 6. Fault-Injection Testing

The mock notification service can simulate intermittent HTTP errors and timeouts to validate retry behaviour and worker recovery.

A configured 30% failure rate can be used to test the system under transient downstream failures.

---

## 🛠️ Technology Stack

| Technology | Purpose |
|---|---|
| Python 3.11+ | Backend programming language |
| FastAPI | REST API framework |
| PostgreSQL 15+ | Persistent storage and transaction management |
| SQLAlchemy 2.0+ | ORM and asynchronous database access |
| Alembic | Database schema migrations |
| Pydantic | Request and response validation |
| HTTPX | Asynchronous HTTP requests to notification services |
| AsyncIO | Asynchronous background processing |
| Pytest | Automated testing |
| Docker | Application containerization |
| Docker Compose | Multi-container orchestration |

---

## 📁 Directory Structure

```text
task2_workshop_registration/
├── app/
│   ├── api/
│   │   └── workshops.py          # Registration, cancellation, and query routes
│   ├── models/
│   │   ├── user.py               # User database entity
│   │   ├── workshop.py           # Workshop database entity
│   │   ├── registration.py       # Registration entity and uniqueness constraint
│   │   └── outbox.py             # Outbox event entity
│   ├── schemas/
│   │   └──                      # Pydantic request and response schemas
│   ├── workers/
│   │   └── outbox_processor.py   # Background notification worker
│   ├── config.py                 # Environment configuration
│   ├── database.py               # SQLAlchemy engine and session factory
│   └── main.py                   # FastAPI entry point and worker lifecycle
├── alembic/
│   ├── versions/                 # Database migration revisions
│   └── env.py                    # Alembic migration environment
├── mock_service/
│   └── server.py                 # Fault-injecting notification mock server
├── tests/
│   └── test_resilience.py        # Crash recovery and reliability tests
├── alembic.ini                   # Alembic configuration
├── Dockerfile                    # Application container configuration
├── docker-compose.yml             # Task-specific container orchestration
├── requirements.txt              # Python dependencies
└── README.md                     # Task documentation
```

*Note: The tree shows the intended project layout. Keep the filenames consistent with your actual repository. The `alembic/versions/` directory should contain migration revision files when migrations have been generated.*

---

## 📋 Requirements and Prerequisites

Before running the application, install or configure the following:

- **Python 3.11 or later** — for local development and testing.
- **Docker Desktop** with Docker Compose v2 — to run the application and infrastructure containers.
- **Git** — to clone the repository.
- **PostgreSQL 15 or later** — either locally or through Docker.
- An internet connection to download Python packages and Docker images.

Verify the installed tools:

```bash
python --version
python -m pip --version
docker --version
docker compose version
git --version
```

### Install Python Dependencies

The project should include a `requirements.txt` file in this directory.

Run the following command to install the required Python packages:

```bash
python -m pip install -r requirements.txt
```

For isolated development, create a virtual environment first.

**Windows PowerShell:**

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

**Linux or macOS:**

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

### Example `requirements.txt`

If the dependency file does not yet exist, the following is a starting point. Adjust it to match the imports in your code and pin tested versions before deployment.

```text
fastapi
uvicorn[standard]
sqlalchemy
asyncpg
alembic
pydantic
pydantic-settings
httpx
pytest
pytest-asyncio
```

Add any other dependencies required by the mock service, tests, or worker.

**Important:** Installing Python packages does not install PostgreSQL itself. PostgreSQL must be available separately or through Docker Compose.

---

## ⚙️ Installation and Setup

### Step 1: Clone the Repository

If you have not cloned the monorepo yet:

```bash
git clone https://github.com/poojareddy240208-cyber/gdg-backend-wildcard.git

cd gdg_backend/task2_workshop_registration
```



If you already have the repository locally, navigate directly to the task directory:

```bash
cd gdg_backend/task2_workshop_registration
```

### Step 2: Configure Environment Variables

If the project provides an `.env.example` file, copy it to `.env`.

**Windows PowerShell:**

```powershell
Copy-Item .env.example .env
```

**Linux or macOS:**

```bash
cp .env.example .env
```

If no example file exists, create `.env` manually using the environment variables expected by `app/config.py`.

Example local-development configuration:

```dotenv
DATABASE_URL=postgresql+asyncpg://postgres:password@localhost:5432/workshop_db
NOTIFICATION_SERVICE_URL=http://localhost:9000
```

These are illustrative values. Use the variable names and configuration expected by the actual application.

When running the application inside Docker Compose, replace `localhost` in service URLs with the appropriate Compose service hostname. For example, the database hostname might be `postgres`, and the mock service hostname might be `mock_service`, depending on the Compose configuration.

Do not commit real credentials to GitHub.

### Step 3: Start the Docker Services

From the `task2_workshop_registration/` directory, run:

```bash
docker compose up --build -d
```

This builds the application image, downloads required base images, and starts the services defined in the task's Compose file.

To run the entire monorepo, navigate to the repository root and run the root Compose configuration instead:

```bash
docker compose up --build -d
```

Choose the appropriate Compose file for your setup. Starting multiple configurations with overlapping host ports can cause conflicts.

### Step 4: Verify Service Status

```bash
docker compose ps
```

Inspect startup logs:

```bash
docker compose logs -f
```

If the application is available on port `8001`, open:

- Swagger UI: http://localhost:8001/docs
- OpenAPI schema: http://localhost:8001/openapi.json

These endpoints require the application to be running and its documentation routes to be enabled.

### Step 5: Run Database Migrations

This project includes an `alembic/` directory for managing database schema changes.

If Alembic is configured for the application, run:

```bash
alembic upgrade head
```

Execute the command from the directory containing `alembic.ini`, with the correct database URL configured.

If your Docker setup already runs migrations automatically during startup, you may not need to run them separately. Verify your entrypoint and deployment configuration before applying migrations twice.

To generate a new migration after changing SQLAlchemy models:

```bash
alembic revision --autogenerate -m "describe schema changes"
```

Review the generated migration before applying it. Autogeneration does not guarantee that every schema change is detected correctly.

---

## 🗄️ Data Models and Status Lifecycle

### 1. Registration Entity

The `Registration` entity records a user's participation in a workshop.

A representative SQLAlchemy model is:

```python
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

class Registration(Base):
    __tablename__ = "registrations"

    __table_args__ = (
        UniqueConstraint(
            "user_id",
            "workshop_id",
            name="uq_user_workshop",
        ),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True)
    user_id: Mapped[str] = mapped_column(
        ForeignKey("users.id"),
        nullable=False,
    )
    workshop_id: Mapped[str] = mapped_column(
        ForeignKey("workshops.id"),
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
    )
```

This is an illustrative example, not a replacement for the project's existing model. Ensure `Base` is imported from your application's declarative base and that its column types and timestamp handling match the rest of your code.

The uniqueness constraint prevents duplicate registrations for the same user and workshop at the database level.

### 2. OutboxEvent Entity

The outbox entity records notification work that must be processed asynchronously.

Typical fields include:

- Event identifier.
- Related registration identifier or notification payload.
- Event type.
- Processing status.
- Retry count.
- Next retry time.
- Creation timestamp.
- Last error details, if recorded.

The registration and outbox event should be inserted in the same database transaction.

### 3. Outbox Status Lifecycle

| Status | Description |
|---|---|
| `PENDING` | Event is persisted and waiting for processing. |
| `PROCESSING` | Worker has claimed the event for delivery. |
| `DELIVERED` | Notification delivery has been recorded as successful. |
| `FAILED` | Retry policy has been exhausted and the event requires further handling. |

The worker should recover stale `PROCESSING` events after a crash. Otherwise, an event claimed before a crash could remain stuck indefinitely.

---

## 🌐 API Specifications

The following endpoint represents the intended registration API. Confirm the final routes and request schemas in the running application.

| Method | Endpoint | Description | Expected status |
|---|---|---|---|
| `POST` | `/api/v1/workshops/register` | Register a user for a workshop | `201 Created` |
| `GET` | `/api/v1/workshops` | List workshops, if implemented | `200 OK` |
| `GET` | `/api/v1/workshops/{workshop_id}` | Retrieve a workshop, if implemented | `200 OK` |
| `POST` | `/api/v1/workshops/{workshop_id}/cancel` | Cancel a registration, if implemented | Depends on implementation |

Duplicate registration attempts should return `409 Conflict` when the application catches the database uniqueness violation and translates it into the appropriate HTTP response.

---

## 🧪 Running and Validating Task 2

### 1. Register a User for a Workshop

Send a registration request:

```bash
curl -X POST "http://localhost:8001/api/v1/workshops/register" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "user-2026",
    "workshop_id": "ws-backend-advanced"
  }'
```

**Expected behaviour:** If the user and workshop exist, the request is valid, and the registration is new, the server returns HTTP `201 Created`.

The exact response body depends on your API response model.

### 2. Test Duplicate Registration Prevention

Run the same request again with the same user and workshop identifiers.

Expected behaviour:

```text
HTTP 409 Conflict
```

The response should indicate that the user is already registered for that workshop.

The database uniqueness constraint protects against duplicates even when concurrent requests arrive simultaneously. The application must handle the corresponding database exception correctly.

### 3. Run Test

```bash
python task2_workshop_registration/tests/test_resilience.py
```

The script may require Docker access and running API, database, and mock notification services. Review its configuration before executing destructive container operations.

### 4. Crash and Recovery Test

The intended resilience test validates the system's ability to recover pending notification events after a process or container restart.

A typical test flow is:

1. Submit multiple workshop registration requests.
2. Confirm that registration and outbox records are committed.
3. Simulate notification-service errors and timeouts.
4. Terminate the application or worker while notifications are pending.
5. Restart the application.
6. Verify that pending events are picked up again.
7. Confirm that successful notifications are recorded as `DELIVERED`.
8. Check that registration uniqueness is preserved.

If your test script uses a command such as:

```bash
docker kill task2_workshop_api
```

ensure the target container name matches your actual Docker configuration. When using the root Compose file, container names may differ.

Similarly, use the actual Compose service name when restarting a service:

```bash
docker compose start task2-api
```

The name `task2-api` is an example; check the service names with:

```bash
docker compose config --services
```

**Reliability note:** A transactional outbox provides durable notification scheduling, but it does not by itself guarantee exactly-once external delivery. A crash after the downstream service accepts a notification but before the worker records success can lead to duplicate delivery on retry. Use idempotency keys or deduplication where supported.

---

## 🔍 Monitoring and Troubleshooting

### View Application Logs

```bash
docker compose logs -f
```

View logs for a particular service:

```bash
docker compose logs -f task2-api
```

Replace `task2-api` with the actual service name defined in your Compose file.

### Common Issues

| Problem | Suggested solution |
|---|---|
| `requirements.txt` not found | Confirm that you are in `task2_workshop_registration/` or use the correct relative path. |
| Database connection failure | Check PostgreSQL status, credentials, and `DATABASE_URL`. |
| Alembic migration fails | Verify the migration configuration, database URL, and SQLAlchemy metadata imports. |
| Duplicate registration returns HTTP 500 | Handle the uniqueness violation and return HTTP 409 after rolling back the failed transaction. |
| Notifications remain pending | Inspect worker logs, event status transitions, and notification-service connectivity. |
| Events remain stuck in `PROCESSING` | Implement stale-claim recovery or a lease/timeout mechanism. |
| Container cannot be restarted | Check the container and Compose service names. |
| Resilience test fails | Verify Docker access, environment variables, health checks, and mock-service availability. |
| Notifications are duplicated | Add idempotency or downstream deduplication and review crash-recovery behaviour. |

---

## 🧹 Stopping the Application

### Stop Containers

```bash
docker compose down
```

This stops and removes Compose-managed containers and networks while normally preserving named volumes.

### Remove Persistent Volumes

```bash
docker compose down -v
```

**Warning:** This can delete persistent PostgreSQL data. Use it only when you intentionally want to reset the environment.

### Rebuild After Code Changes

```bash
docker compose up --build -d
```

---

## 📄 License

This project is part of the GDG Tech Wildcard submission. Refer to the repository's root [`LICENSE`](../LICENSE) file for licensing terms.
