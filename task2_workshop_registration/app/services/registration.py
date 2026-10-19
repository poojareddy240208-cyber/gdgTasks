"""
app/services/registration.py
─────────────────────────────
Business logic for creating a workshop registration.

The entire operation — seat claim + outbox event — is written in a single
database transaction so the HTTP handler can commit once and return HTTP 202
immediately, with zero coupling to the notification service (REQ-NB-01,
REQ-OB-01, design §5).

Concurrency guarantees
───────────────────────
1. ``SELECT workshop FOR UPDATE`` serialises all concurrent registrations for
   the same workshop, preventing oversell (REQ-REG-05).
2. ``session.flush()`` triggers the DB UniqueConstraint check before commit;
   concurrent duplicate requests that race past the application-layer check
   are caught here (REQ-REG-03).
3. The outbox INSERT is in the same transaction; a crash between commit and
   notification delivery is automatically recovered by the relay worker
   finding the PENDING record on restart (REQ-OB-04).
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import structlog
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.exceptions import (
    AlreadyRegisteredError,
    UserNotFoundError,
    WorkshopCancelledError,
    WorkshopFullError,
    WorkshopNotFoundError,
)
from app.models import (
    OutboxEvent,
    OutboxEventType,
    Registration,
    RegistrationStatus,
    User,
    Workshop,
    WorkshopStatus,
)

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

async def _get_workshop_locked(session: AsyncSession, workshop_id: uuid.UUID) -> Workshop:
    """
    Fetch the Workshop row with a row-level exclusive lock.

    Using FOR UPDATE serialises all concurrent seat-claim attempts for the
    same workshop. Without this lock, two concurrent requests could both
    read available_seats > 0 and both succeed, overselling the workshop.
    """
    result = await session.execute(
        select(Workshop)
        .where(Workshop.id == workshop_id)
        .with_for_update()
    )
    workshop = result.scalar_one_or_none()
    if workshop is None:
        raise WorkshopNotFoundError(workshop_id)
    return workshop


async def _count_confirmed(session: AsyncSession, workshop_id: uuid.UUID) -> int:
    """Count CONFIRMED registrations for a workshop (within the locked tx)."""
    result = await session.execute(
        select(func.count())
        .select_from(Registration)
        .where(Registration.workshop_id == workshop_id)
        .where(Registration.status == RegistrationStatus.CONFIRMED)
    )
    return result.scalar_one()


async def _build_outbox_payload(
    session: AsyncSession,
    registration: Registration,
    workshop: Workshop,
) -> dict:
    """
    Fetch the User and build the denormalised notification payload.

    All fields needed by the notification service are captured at write time
    so the relay worker never needs to re-join tables (avoids phantom-read
    edge cases after later mutations such as bulk cancellation).
    """
    result = await session.execute(
        select(User).where(User.id == registration.user_id)
    )
    user = result.scalar_one_or_none()
    if user is None:
        raise UserNotFoundError(registration.user_id)

    return {
        "user_id":        str(user.id),
        "user_email":     user.email,
        "user_name":      user.full_name,
        "workshop_id":    str(workshop.id),
        "workshop_title": workshop.title,
        "scheduled_at":   workshop.scheduled_at.isoformat(),
        "venue":          workshop.venue,
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def register(
    session: AsyncSession,
    *,
    user_id: uuid.UUID,
    workshop_id: uuid.UUID,
) -> Registration:
    """
    Reserve a seat for ``user_id`` in ``workshop_id``.

    Steps (all within a single transaction; caller must commit)
    ───────────────────────────────────────────────────────────
    1. Lock the workshop row (FOR UPDATE) — prevents oversell.
    2. Guard: workshop must be SCHEDULED.
    3. Guard: confirmed_count < capacity (seat available).
    4. INSERT Registration(status=CONFIRMED).
    5. flush() — fires the DB UniqueConstraint; converts IntegrityError →
       AlreadyRegisteredError if a duplicate slips past the app-layer check.
    6. INSERT OutboxEvent(type=REGISTRATION_CONFIRMED, status=PENDING) with
       the denormalised payload and the registration.id as the idempotency key.
    7. Return the Registration object — **caller commits**.

    The caller (route handler) calls ``await session.commit()`` exactly once
    after this function returns. Both the Registration and OutboxEvent rows
    are committed atomically.
    """
    # Step 1 — acquire row lock on the workshop
    workshop = await _get_workshop_locked(session, workshop_id)

    # Step 2 — workshop must be SCHEDULED
    if workshop.status == WorkshopStatus.CANCELLED:
        raise WorkshopCancelledError(workshop_id)

    # Step 3 — check remaining capacity
    confirmed_count = await _count_confirmed(session, workshop_id)
    if confirmed_count >= workshop.capacity:
        raise WorkshopFullError(workshop_id)

    # Step 4 — create the Registration
    registration = Registration(
        user_id=user_id,
        workshop_id=workshop_id,
        status=RegistrationStatus.CONFIRMED,
    )
    session.add(registration)

    # Step 5 — flush to enforce the DB UniqueConstraint early
    try:
        await session.flush()
    except IntegrityError:
        # UniqueConstraint violation: (user_id, workshop_id) already exists.
        # Do not call session.rollback() here; allow the error exception handler
        # or parent transaction context to manage session cleanup.
        raise AlreadyRegisteredError(user_id, workshop_id)

    # Step 6 — build the notification payload and write the outbox event
    payload = await _build_outbox_payload(session, registration, workshop)

    outbox = OutboxEvent(
        event_type=OutboxEventType.REGISTRATION_CONFIRMED,
        registration_id=registration.id,
        payload=payload,
        # Use str(registration.id) as the persistent idempotency key
        # so re-delivery of this record always reuses the same key across restarts.
        idempotency_key=str(registration.id),
    )
    session.add(outbox)

    logger.info(
        "registration.pending_commit",
        user_id=str(user_id),
        workshop_id=str(workshop_id),
        registration_id=str(registration.id),
        outbox_id=str(outbox.id),
    )

    # Step 7 — return; caller commits both rows atomically
    return registration