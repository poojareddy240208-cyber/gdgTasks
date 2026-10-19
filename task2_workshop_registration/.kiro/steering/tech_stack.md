# Steering Rules for Task 2: Workshop Engine
- Language & Framework: Python 3.11+, FastAPI (Async)
- Database: PostgreSQL with asyncpg & SQLAlchemy 2.0
- Architecture: Transactional Outbox Pattern for crash recovery
- Resilience: Exponential Backoff with Jitter for HTTP notification retries
- Rules: Single unique registration per user per workshop. Registrations write Outbox items inside the exact same DB transaction.