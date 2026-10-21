# GDG Tech Wildcard Submission: Advanced Backend Architecture Monorepo

![Python](https://img.shields.io/badge/Python-3.11+-3776AB.svg?style=flat&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-009688.svg?style=flat&logo=fastapi&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15.0+-336791.svg?style=flat&logo=postgresql&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-7.0+-DC382D.svg?style=flat&logo=redis&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Enabled-2496ED.svg?style=flat&logo=docker&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green.svg)

A unified backend monorepo implementing two Google Developer Groups (GDG) Backend Domain tasks:

1. **Campus Sensor Monitoring Engine** — High-throughput telemetry ingestion and real-time sliding-window evaluation.
2. **Workshop Registration & Notifications Engine** — Reliable workshop registration with asynchronous notifications and crash recovery.

The project uses FastAPI, PostgreSQL, Redis, asynchronous processing, and Docker Compose to provide modular, scalable backend services.

---

## 🏛️ Architecture Overview

Both applications run as independent services while sharing PostgreSQL and Redis infrastructure through Docker Compose.

```text
gdg_backend/
│
├── task1_sensor_monitoring/          # Sensor monitoring service (:8000)
│   ├── app/
│   ├── simulator/
│   ├── tests/
│   ├── Dockerfile
│   ├── docker-compose.yml
│   ├── requirements.txt
│   ├── LOAD_TEST_REPORT.md
│   └── README.md
│
├── task2_workshop_registration/      # Workshop service (:8001)
│   ├── app/
│   ├── mock_service/
│   ├── tests/
│   ├── Dockerfile
│   ├── docker-compose.yml
|   |--alembic/
|   |   
│   ├── requirements.txt
│   └── README.md
│
├── docker-compose.yml                # Root orchestration
├── .env.example                      # Example environment variables
├── .gitignore
├── LICENSE
└── README.md
```

### System Architecture

```text
                  GDG BACKEND MONOREPO
                           |
              +------------+------------+
              |                         |
              v                         v
      TASK 1: SENSOR             TASK 2: WORKSHOP
      MONITORING ENGINE          REGISTRATION ENGINE
         Port 8000                  Port 8001
              |                         |
              v                         v
      Redis Streams              PostgreSQL Outbox
      Redis Sorted Sets          Async Notification
              |                         |
              +------------+------------+
                           |
                           v
                 SHARED INFRASTRUCTURE
                           |
              +------------+------------+
              |                         |
              v                         v
          PostgreSQL                  Redis
           Port 5432                 Port 6379
```

---

## 📊 Comparative Architectural Summary

| Feature | Task 1: Sensor Monitoring | Task 2: Workshop Registration |
|---|---|---|
| Primary objective | Process sensor telemetry efficiently | Register participants and deliver notifications reliably |
| Main design pattern | Event-driven ingestion and sliding window | Transactional Outbox Pattern |
| Framework | FastAPI | FastAPI |
| Data storage | PostgreSQL and Redis | PostgreSQL and Redis, as required by the implementation |
| Asynchronous processing | Redis Streams consumer | Background notification worker |
| Performance strategy | Buffered ingestion and in-memory window evaluation | Non-blocking registration and retry processing |
| Failure handling | Stream recovery and consumer processing | Persistent outbox records and notification retries |
| API port | `8000` | `8001` |

## 🚀 Key Features

### Task 1: Campus Sensor Monitoring Engine

- High-throughput sensor telemetry ingestion.
- Asynchronous request processing using FastAPI.
- Redis Streams for buffering incoming sensor events.
- Redis Sorted Sets (ZSETs) for time-based sliding-window evaluation.
- PostgreSQL for persistent data storage.
- Load-testing utilities for measuring throughput and latency.
- Unit and integration tests for validating functionality.

**Performance objective:** Evaluate the system under high-concurrency workloads, targeting bursts of up to 10,000 requests per second. Actual throughput and latency must be verified using load tests and the available hardware.

### Task 2: Workshop Registration & Notifications Engine

- Workshop registration through REST APIs.
- PostgreSQL-backed persistent registration data.
- Transactional Outbox Pattern for reliable notification processing.
- Asynchronous notification workers.
- Retry handling with exponential backoff and jitter.
- A mock notification service for simulating failures.
- Crash-recovery tests for validating pending notification processing.

**Reliability objective:** Prevent notifications from being silently lost when a worker crashes, provided the registration and outbox transaction commits successfully and recovery processing is configured correctly.

---

## 🛠️ Technology Stack

| Technology | Purpose |
|---|---|
| Python 3.11+ | Backend programming language |
| FastAPI | REST API development |
| PostgreSQL 15+ | Relational database and persistent storage |
| Redis 7+ | Event buffering, caching, and sliding-window operations |
| Docker | Application containerization |
| Docker Compose | Multi-container orchestration |
| Pytest | Automated testing |
| HTTPX / AsyncIO | Asynchronous HTTP requests and load generation |

---

## ⚙️ Prerequisites

Before running the project, install the following:

- **Git** — to clone the repository.
- **Python 3.11 or later** — for local development and testing.
- **Docker Desktop** — with Docker Compose v2 enabled.
- **Internet connection** — to download Python packages and Docker images.

Verify your installations:

```bash
git --version
python --version
docker --version
docker compose version
```

On Windows, use `py --version` if the `python` command is unavailable.

---

## 📥 Installation and Setup

### Step 1: Clone the Repository


```bash
git clone https://github.com/poojareddy240208-cyber/gdg-backend-wildcard.git

cd gdg_backend
```

### Step 2: Download and Install Python Dependencies

**Run this command to install all Python packages listed in the root `requirements.txt` file:**

```bash
python -m pip install -r requirements.txt
```

If each task has its own `requirements.txt` instead of a root requirements file, install them separately:

```bash
python -m pip install -r task1_sensor_monitoring/requirements.txt

python -m pip install -r task2_workshop_registration/requirements.txt
```

Run only the command that matches the actual location of your requirements files. If the project has a root requirements file that already includes all dependencies, the first command is sufficient.

If you need to create a virtual environment for local development, run:

```bash
python -m venv .venv
```

Activate it on Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

Activate it on Linux or macOS:

```bash
source .venv/bin/activate
```

Then install the requirements using the appropriate command above.

### Step 3: Configure Environment Variables

Create a `.env` file in the repository root if the root Docker Compose configuration expects one.

If an `.env.example` file is provided, copy it first.

Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

Linux or macOS:

```bash
cp .env.example .env
```

Update the values in `.env` according to your configuration.

Typical variables may include:

```dotenv
DATABASE_URL=postgresql://postgres:password@postgres:5432/gdg_db
REDIS_URL=redis://redis:6379/0
```

These are illustrative examples. Use the variable names, credentials, database names, and connection formats expected by the actual application code and Docker Compose configuration. Do not commit real credentials to GitHub.

### Step 4: Build and Start All Services

From the repository root, execute:

```bash
docker compose up --build -d
```

This command builds the application images, downloads required base images, and starts the configured containers in detached mode.

**Note:** If your Dockerfiles install Python packages using `pip install -r requirements.txt`, you do not need to install those same packages on your host machine just to run the containerized APIs. Local installation is useful when running Python scripts or tests directly on your computer.

### Step 5: Verify Running Containers

```bash
docker compose ps
```

The expected services are:

| Service | Expected port | Purpose |
|---|---:|---|
| PostgreSQL | `5432` | Persistent database |
| Redis | `6379` | Event buffering and in-memory operations |
| Sensor API | `8000` | Sensor monitoring endpoints |
| Workshop API | `8001` | Registration and notification endpoints |

Container names depend on the service names defined in your root `docker-compose.yml`. Check the actual service status and logs if a container is not running.

To inspect logs:

```bash
docker compose logs -f
```

To inspect one service:

```bash
docker compose logs -f task1_sensor_api
```

Replace `task1_sensor_api` with the actual Compose service name if it differs.

---

## 🧪 API Documentation and Testing

### Interactive Swagger Documentation

Once the services are running, open the following URLs in your browser:

| Service | URL |
|---|---|
| Sensor Monitoring API | http://localhost:8000/docs |
| Sensor Monitoring OpenAPI schema | http://localhost:8000/openapi.json |
| Workshop Registration API | http://localhost:8001/docs |
| Workshop OpenAPI schema | http://localhost:8001/openapi.json |

These URLs are available only if the corresponding application services expose their documentation and are reachable on the listed ports.

### Run Task 1 Load Tests

The following example runs the sensor load generator at the configured target rate for 10 seconds:

```bash
python task1_sensor_monitoring/simulator/load_generator.py --rate 10000 --duration 10
```

Before running, confirm that the script accepts the `--rate` and `--duration` arguments. The specified rate is a target, not a guarantee of actual throughput.

Review the results in:

```text
task1_sensor_monitoring/LOAD_TEST_REPORT.md
```

Evaluate throughput, average latency, P95/P99 latency, error rate, and resource utilization.

### Run Task 1 Tests

```bash
python -m pytest task1_sensor_monitoring/tests -v
```

### Run Task 2 Tests

```bash
python -m pytest task2_workshop_registration/tests -v
```

If `test_resilience.py` is designed as a standalone script, you can also run:

```bash
python task2_workshop_registration/tests/test_resilience.py
```

Ensure the relevant services are running and any required environment variables are configured before executing integration or crash-recovery tests.

---

## 🔄 Notification Reliability Workflow

Task 2 uses the Transactional Outbox Pattern to coordinate registration and notification processing.

```text
Client submits registration
          |
          v
Database transaction begins
          |
          v
Save registration + outbox event
          |
          v
Database transaction commits
          |
          v
Background worker reads outbox
          |
          v
Send notification request
          |
          +----------------------+
          |                      |
          v                      v
       Success                 Failure
          |                      |
          v                      v
  Mark event processed     Schedule retry
                                 |
                                 v
                      Exponential backoff
                                 |
                                 v
                        Retry notification
```

The registration and outbox event should be persisted in the same database transaction. This avoids committing a registration without also recording the notification work that needs to be performed.

Notification delivery may still be repeated if a worker crashes after sending a notification but before recording success. Use idempotency keys or deduplication where the notification provider supports them.

---

## 🧹 Stopping and Cleaning Up

### Stop Containers

```bash
docker compose down
```

Stops and removes the containers and network created by the Compose project while normally preserving named volumes.

### Reset the Environment

```bash
docker compose down -v
```

Removes the Compose containers, network, and associated named volumes. **Warning:** This can permanently delete locally stored PostgreSQL data.

### Rebuild After Code Changes

```bash
docker compose up --build -d
```

### Check Container Status

```bash
docker compose ps
```

---

## 🐛 Troubleshooting

| Problem | Suggested solution |
|---|---|
| `requirements.txt` not found | Run the command from the directory containing the file, or use its correct relative path. |
| Docker command not found | Install Docker Desktop and ensure it is running. |
| Port `8000` or `8001` already in use | Stop the conflicting application or change the host port mapping. |
| Database connection refused | Check PostgreSQL container status, credentials, and database URL. |
| Redis connection refused | Check Redis container status and the configured Redis URL. |
| API returns an internal server error | Inspect the application logs with `docker compose logs -f`. |
| Python import errors | Activate the correct virtual environment and install the required dependencies. |
| Resilience test fails | Verify that PostgreSQL, Redis, the API, and the mock notification service are running as required. |

---

## 📁 Project Documentation

Refer to the individual task directories for implementation-specific details.

- `task1-sensor-monitoring/README.md` — Sensor API setup, endpoints, and usage.
- `task1-sensor-monitoring/LOAD_TEST_REPORT.md` — Load-testing results and performance analysis.
- `task2-workshop-registration/README.md` — Registration workflow and notification architecture.

---

## 📄 License

This project is licensed under the MIT License. See the [`LICENSE`](LICENSE) file for details.
