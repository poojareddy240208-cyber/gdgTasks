# Steering Rules for Task 1: Sensor Engine
- Language & Framework: Python 3.11+, FastAPI (Async)
- Database: PostgreSQL with asyncpg & SQLAlchemy 2.0
- Ingestion & Windowing: Redis Streams for non-blocking ingestion buffer; Redis ZSET for 10-minute rolling window averages
- Validation: Pydantic v2
- Performance Goal: Ingestion endpoint must accept single and batch payloads, returning HTTP 202 Accepted immediately.