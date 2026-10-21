# Task 1: Campus Sensor Monitoring & Alert Engine

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB.svg?style=flat&logo=python)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-009688.svg?style=flat&logo=fastapi)](https://fastapi.tiangolo.com/)
[![Redis](https://img.shields.io/badge/Redis-7.0+-DC382D.svg?style=flat&logo=redis)](https://redis.io/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15.0+-336791.svg?style=flat&logo=postgresql)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Enabled-2496ED.svg?style=flat&logo=docker)](https://www.docker.com/)

A high-concurrency backend microservice designed to ingest campus sensor telemetry, calculate rolling 10-minute metric averages, and evaluate dynamic alert rules asynchronously.

The system uses FastAPI for REST APIs, Redis Streams for event buffering, Redis Sorted Sets (ZSETs) for sliding-window calculations, and PostgreSQL for persistent storage.

**Performance target:** Support high-volume telemetry bursts targeting 10,000 requests per second, subject to validation through load testing and the available infrastructure.

---

## 📌 Table of Contents

- [Architecture Overview](#-architecture-overview)
- [Key Features](#-key-features)
- [Technology Stack](#️-technology-stack)
- [Directory Structure](#-directory-structure)
- [Requirements and Prerequisites](#-requirements-and-prerequisites)
- [Installation and Setup](#-installation-and-setup)
- [Data Models](#-data-models)
- [API Specifications](#-api-specifications)
- [Running the Application](#-running-the-application)
- [Testing and Load Validation](#-testing-and-load-validation)
- [Monitoring and Troubleshooting](#-monitoring-and-troubleshooting)
- [Stopping the Application](#-stopping-the-application)

---

## 🏗️ Architecture Overview

Synchronous database writes for every incoming telemetry reading can create database bottlenecks under heavy traffic. This application separates ingestion from processing by buffering events in Redis Streams and processing them asynchronously.

```text
              SENSOR TELEMETRY CLIENTS
                         |
                         v
              +----------------------+
              |   FastAPI Ingestion  |
              |   Payload Validation |
              +----------------------+
                         |
                         v
              +----------------------+
              |    Redis Streams     |
              |   Event Buffering    |
              +----------------------+
                         |
                         v
              +----------------------+
              |  Stream Consumer     |
              |      Worker          |
              +----------------------+
                         |
              +----------+-----------+
              |                      |
              v                      v
      +----------------+     +----------------+
      | Redis Sorted   |     | Alert Rule     |
      | Set (ZSET)     |     | Evaluation     |
      | Sliding Window |     +----------------+
      +----------------+              |
              |                       v
              |                +----------------+
              +--------------->|   PostgreSQL   |
                               | Alerts & Rules |
                               +----------------+
```

### Processing Workflow

1. A sensor submits a telemetry reading to the FastAPI ingestion endpoint.
2. FastAPI validates the payload using Pydantic models.
3. The event is appended to a Redis Stream.
4. The API returns `202 Accepted` after successful queueing.
5. A background worker consumes the queued event.
6. The worker updates the sensor's Redis Sorted Set and removes entries older than the rolling 10-minute window.
7. The worker evaluates applicable alert rules against the calculated metric.
8. Triggered alerts are persisted in PostgreSQL.

This design reduces synchronous database work on the ingestion path and allows telemetry processing to proceed independently of incoming HTTP requests.

---

## 🚀 Key Features

### 1. Asynchronous Telemetry Ingestion

- REST endpoints for individual and batch sensor readings.
- Pydantic-based payload validation.
- Redis Streams for buffering incoming events.
- HTTP `202 Accepted` responses when events have been successfully queued.
- Separation of request handling from downstream processing.

### 2. Rolling 10-Minute Sliding Window

Redis Sorted Sets store telemetry events with timestamps as scores.

For each sensor, the application can:

- Insert readings into a time-ordered collection.
- Remove readings older than 10 minutes.
- Calculate averages over the retained readings.
- Maintain rolling metric state without repeatedly querying PostgreSQL.

Sorted Set insertions and removals typically have \(O(\log N)\) complexity per operation, where \(N\) is the number of elements in the set. The overall cost also depends on the number of expired entries removed and the average calculation implementation.

### 3. Dynamic Alert Rules

The application supports configurable monitoring rules based on metric types, thresholds, and comparison operators.

For example, a rule could trigger an alert when the average campus temperature exceeds a configured threshold.

### 4. Persistent Alert Management

PostgreSQL stores alert rules and generated alerts. Alert APIs can expose filtering and acknowledgement functionality, depending on the implemented endpoints.

### 5. Load Testing

An asynchronous load generator simulates telemetry traffic and helps measure:

- Request throughput.
- Average response latency.
- P95 and P99 latency.
- Successful and failed requests.
- System behaviour under high concurrency.

---

## 🛠️ Technology Stack

| Technology | Purpose |
|---|---|
| Python 3.11+ | Application programming language |
| FastAPI | REST API framework |
| Pydantic | Request validation and data schemas |
| SQLAlchemy | Database ORM and asynchronous database access |
| PostgreSQL 15+ | Persistent storage for rules and alerts |
| Redis 7+ | Event streams and sliding-window data |
| AsyncIO | Asynchronous processing |
| aiohttp | Asynchronous HTTP load generator |
| Docker | Application containerization |
| Docker Compose | Local infrastructure orchestration |
| Pytest | Automated testing |

---

## 📁 Directory Structure

```text
task1_sensor_monitoring/
├── app/
│   ├── api/
│   │   ├── telemetry.py         # Single and batch telemetry endpoints
│   │   ├── rules.py             # Alert rule management endpoints
│   │   └── alerts.py            # Alert querying and acknowledgement
│   ├── models/
│   │   └──                    # SQLAlchemy database models
│   ├── schemas/
│   │   └──                    # Pydantic request and response schemas
│   ├── services/
│   │   └── window_manager.py    # Redis sliding-window operations
│   ├── workers/
│   │   └── stream_processor.py  # Redis Stream consumer
│   ├── config.py                # Application configuration
│   ├── database.py              # Database connection and sessions
│   └── main.py                  # FastAPI application entry point
├── simulator/
│   └── load_generator.py        # Asynchronous load-testing client
├── tests/                       # Unit and integration tests
├── Dockerfile                   # Application image configuration
├── docker-compose.yml           # Task-specific service orchestration
├── requirements.txt             # Python dependencies
├── LOAD_TEST_REPORT.md          # Load-test metrics and analysis
└── README.md                    # Task documentation
```


---

## 📋 Requirements and Prerequisites

Before running the application, ensure you have the following installed:

- **Python 3.11 or later** — for local development and testing.
- **Docker Desktop** with Docker Compose v2 — recommended for running the complete stack.
- **Git** — for cloning and managing the repository.
- An internet connection to download Python dependencies and Docker images.

Verify your installations:

```bash
python --version
python -m pip --version
docker --version
docker compose version
git --version
```

### Python Dependencies

The project should include a `requirements.txt` file in this directory.

Install all required Python packages by running:

```bash
python -m pip install -r requirements.txt
```

For an isolated local development environment, create a virtual environment first.

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

If the project does not yet have a dependency file, use the following as a starting point and adjust the package versions to match your code:

```text
fastapi
uvicorn[standard]
pydantic
pydantic-settings
sqlalchemy
asyncpg
redis
aiohttp
pytest
pytest-asyncio
httpx
```

Add any other dependencies imported by your implementation. If you use database migrations, include the migration tool, such as Alembic, if required.

**Important:** Installing packages locally does not install PostgreSQL or Redis. Those services must be available separately or started through Docker Compose.

---

## ⚙️ Installation and Setup

### Step 1: Navigate to the Task Directory

If you have already cloned the monorepo:

```bash
cd gdg_backend/task1_sensor_monitoring
```

Otherwise, clone the repository first:

```bash
git clone https://github.com/poojareddy240208-cyber/gdg-backend-wildcard.git

cd gdg_backend/task1_sensor_monitoring
```


### Step 2: Configure Environment Variables

Configure the database and Redis connection settings expected by `app/config.py` and your Docker Compose configuration.

If your project contains an `.env.example` file, copy it to `.env` and update its values.

```bash
cp .env.example .env
```

For Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

Example configuration:

```dotenv
DATABASE_URL=postgresql+asyncpg://postgres:password@localhost:5432/sensor_db
REDIS_URL=redis://localhost:6379/0
```

These values are illustrative. Use the variable names and connection strings required by your actual application.

**Docker networking:** When the application runs inside Docker Compose, `localhost` refers to the application container itself. Use the database and Redis service names as hostnames, for example `postgres` and `redis`, if those are the service names in your Compose file.

Never commit real passwords or other secrets to version control.

### Step 3: Start the Services

From the task directory, run:

```bash
docker compose up --build -d
```

This builds the application image, pulls any required base images, and starts the services defined in the task's `docker-compose.yml`.

If you intend to run the entire monorepo using the root Compose file, navigate to the repository root and run:

```bash
docker compose up --build -d
```

Use the Compose file appropriate to your intended deployment. Do not start both configurations unnecessarily if they map the same host ports or create conflicting resources.

### Step 4: Verify the Containers

```bash
docker compose ps
```

Confirm that the application, PostgreSQL, and Redis services are running.

Inspect startup logs:

```bash
docker compose logs -f
```

Once the API is ready, open:

- Swagger UI: http://localhost:8000/docs
- OpenAPI schema: http://localhost:8000/openapi.json

---

## 🗄️ Data Models

### AlertRule

Represents a configurable monitoring rule.

Typical fields include:

- Rule identifier.
- Sensor or metric type.
- Comparison operator.
- Threshold value.
- Active or inactive status.
- Creation timestamp.

### Alert

Represents a triggered monitoring event.

Typical fields include:

- Alert identifier.
- Related sensor or metric.
- Triggered metric value.
- Associated rule.
- Alert status.
- Creation timestamp.
- Acknowledgement timestamp, if supported.

Typical alert states are:

- `TRIGGERED`
- `ACKNOWLEDGED`
- `RESOLVED`

The exact fields, status values, and transitions should match the SQLAlchemy models and application logic.

---

## 🌐 API Specifications

The following table describes the intended core endpoints. Verify the actual route prefixes in your FastAPI routers.

| Method | Endpoint | Description | Expected status |
|---|---|---|---|
| `POST` | `/api/v1/telemetry` | Ingest a single sensor reading | `202 Accepted` |
| `POST` | `/api/v1/telemetry/batch` | Ingest a batch of sensor readings | `202 Accepted` |
| `POST` | `/api/v1/rules` | Create an alert rule | `201 Created` |
| `GET` | `/api/v1/alerts` | Retrieve alerts, optionally filtered | `200 OK` |

Additional endpoints may be available for listing, updating, or deleting rules and acknowledging alerts.

Use the Swagger UI at `http://localhost:8000/docs` to inspect the routes, request schemas, response models, and supported query parameters exposed by your running application.

---

## 🧪 Running the Application

### 1. Submit a Single Telemetry Reading

Once the API is running, submit a request using `curl`:

```bash
curl -X POST "http://localhost:8000/api/v1/telemetry" \
  -H "Content-Type: application/json" \
  -d '{
    "sensorId": "sensor-campus-01",
    "type": "temperature",
    "value": 38.5,
    "timestamp": "2026-10-21T12:00:00Z"
  }'
```

The field names and timestamp format must match your Pydantic schema. For example, if your model uses `sensor_id` rather than `sensorId`, update the JSON accordingly.

**Expected behaviour:** If the payload is valid and the event is successfully queued, the API should return HTTP `202 Accepted`.

An example response is:

```json
{
  "status": "queued"
}
```

The exact response body depends on your implementation.

### 2. Submit a Batch of Telemetry Readings

If the batch endpoint accepts an array of readings, a request may look like this:

```bash
curl -X POST "http://localhost:8000/api/v1/telemetry/batch" \
  -H "Content-Type: application/json" \
  -d '[
    {
      "sensorId": "sensor-campus-01",
      "type": "temperature",
      "value": 38.5,
      "timestamp": "2026-10-21T12:00:00Z"
    },
    {
      "sensorId": "sensor-campus-02",
      "type": "temperature",
      "value": 36.8,
      "timestamp": "2026-10-21T12:00:01Z"
    }
  ]'
```

Adjust the payload to match the batch schema defined by your application.

### 3. Create an Alert Rule

The following is an illustrative request. Replace the JSON field names with the fields accepted by your `AlertRuleCreate` schema.

```bash
curl -X POST "http://localhost:8000/api/v1/rules" \
  -H "Content-Type: application/json" \
  -d '{
    "metric_type": "temperature",
    "threshold": 38.0,
    "operator": "gt",
    "is_active": true
  }'
```

### 4. Query Alerts

```bash
curl "http://localhost:8000/api/v1/alerts"
```

If filtering by alert status is implemented, use the query parameters documented in Swagger UI.

---

## 📈 Testing and Load Validation

### Run Unit and Integration Tests

From the task directory, execute:

```bash
python -m pytest tests/ -v
```

For a concise test summary:

```bash
python -m pytest tests/ -q
```

Make sure the required dependencies are installed and the relevant external services are running for integration tests.

### Run the Load Generator

The project includes an asynchronous load generator intended to simulate high-volume telemetry traffic.

```bash
python simulator/load_generator.py --rate 10000 --duration 10
```

This command requests a target rate of 10,000 requests per second for 10 seconds, provided that the script supports these arguments.

If the generator runs outside Docker, install its dependencies locally, including `aiohttp` if used by the script. If it runs inside a container, ensure those dependencies are included in that container's image.

### Review Load-Test Results

Inspect:

```text
LOAD_TEST_REPORT.md
```

Evaluate the following metrics:

| Metric | Meaning |
|---|---|
| Throughput | Number of requests processed per second |
| Average latency | Mean time taken to respond to requests |
| P95 latency | Latency below which 95% of requests complete |
| P99 latency | Latency below which 99% of requests complete |
| Error rate | Percentage of requests that fail |
| Queue backlog | Events waiting for consumer processing |

The target of 10,000 requests per second and sub-15-millisecond ingestion latency should be treated as performance objectives until confirmed by reproducible test results.

---

## 🔍 Monitoring and Troubleshooting

### View Application Logs

If using the task-specific Compose file:

```bash
docker compose logs -f
```

To inspect only the application service:

```bash
docker compose logs -f task1_sensor_api
```

Replace `task1_sensor_api` with the actual service name in `docker-compose.yml`.

If the container is explicitly named `task1_sensor_api`, you can also run:

```bash
docker logs task1_sensor_api --tail 50 -f
```

### Common Issues

| Issue | Solution |
|---|---|
| `requirements.txt` not found | Confirm that the file exists in the current directory or use its correct relative path. |
| Docker daemon unavailable | Start Docker Desktop and verify `docker info`. |
| Port `8000` already in use | Stop the conflicting service or change the host port mapping. |
| PostgreSQL connection failure | Check database health, credentials, and the connection URL. |
| Redis connection failure | Check the Redis container and configured hostname. |
| HTTP `422 Unprocessable Entity` | Verify request fields, data types, and timestamp format against the Pydantic schema. |
| HTTP `500 Internal Server Error` | Inspect application logs for the underlying exception. |
| Events remain unprocessed | Check the Redis Stream, worker logs, consumer configuration, and PostgreSQL connectivity. |
| Load test reports connection errors | Check client concurrency, server capacity, resource usage, and the configured target rate. |

---

## 🧹 Stopping the Application

### Stop Services

```bash
docker compose down
```

This stops and removes the Compose-managed containers and network while normally preserving named volumes.

### Remove Persistent Volumes

```bash
docker compose down -v
```

**Warning:** This may delete persisted PostgreSQL data. Use it only when you intentionally want to reset the local environment.

### Rebuild After Code Changes

```bash
docker compose up --build -d
```

---

## 📄 License

This project is part of the GDG Tech Wildcard submission. Refer to the repository's root [`LICENSE`](../LICENSE) file for licensing terms.
