"""
app/services/cancellation.py
─────────────────────────────
Business logic for single-registration cancellation and bulk workshop
cancellation.

Single cancellation
────────────────────
Cancel one attendee's own registration.  Writes Registration(CANCELLED) +
OutboxEvent(REGISTRATION_CANCELLED) atomically.  Returns 202 immediately;
the relay worker handles notification delivery (REQ-NB-03, REQ-OB-01).

Bulk cancellation
──────────────────
Cancel every CONFIRMED registration for a workshop in batches of
``settings.bulk_cancel_batch_size`` (default 500) to avoid long-held locks
on large workshops (REQ-BK-06).  Each batch is its own committed transaction,
so locks are released between iterations.  One OutboxEvent per affected
attendee is inserted in the same batch transaction (REQ-BK-04) so every
attendee receives an individual WORKSHOP_CANCELLED notification.

The workshop's own status is set to CANCELLED in a final separate transaction
after all registration batches complete.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import structlog
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import AsyncSessionLocal
from app.exceptions import (
    OperatorRequiredError,
    RegistrationNotFoundError,
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
# Helpers
# ---------------------------------------------------------------------------

async def _load_user(session: AsyncSession, user_id: uuid.UUID) -> User:
    result = await session.execute(select(User).where(User.id == user_id))
    return result.scalar_one_or_none()


async def _build_cancellation_payload(
    session: AsyncSession,
    registration: Registration,
    workshop: Workshop,
) -> dict:
    """Denormalised payload for the notification service."""
    user_result = await session.execute(
        select(User).where(User.id == registration.user_id)
    )
    user = user_result.scalar_one_or_none()
    return {
        "user_id":        str(registration.user_id),
        "user_email":     user.email if user else "",
        "user_name":      user.full_name if user else "",
        "workshop_id":    str(workshop.id),
        "workshop_title": workshop.title,
        "scheduled_at":   workshop.scheduled_at.isoformat(),
        "venue":          workshop.venue,
    }


# ---------------------------------------------------------------------------
# Single-registration cancellation
# ---------------------------------------------------------------------------

async def cancel_registration(
    session: AsyncSession,
    *,
    registration_id: uuid.UUID,
    requesting_user_id: uuid.UUID,
) -> Registration:
    """
    Cancel a single registration.

    Guards
    ──────
    - Registration must exist.
    - Requesting user must own the registration (or be an OPERATOR — checked
      at the route level before calling this function).
    - Registration must currently be CONFIRMED (idempotent: already-cancelled
      returns 409 at the route level, not here).

    Transaction contract
    ─────────────────────
    Caller provides an open session and calls ``await session.commit()`` after
    this function returns.  Both the status update and the OutboxEvent INSERT
    land in the same commit.
    """
    # Load registration
    result = await session.execute(
        select(Registration)
        .where(Registration.id == registration_id)
        .with_for_update()
    )
    registration = result.scalar_one_or_none()
    if registration is None:
        raise RegistrationNotFoundError(registration_id)

    # Load workshop (needed for payload)
    ws_result = await session.execute(
        select(Workshop).where(Workshop.id == registration.workshop_id)
    )
    workshop = ws_result.scalar_one_or_none()
    if workshop is None:
        raise WorkshopNotFoundError(registration.workshop_id)

    # Mark cancelled
    registration.status = RegistrationStatus.CANCELLED

    # Build denormalised payload and write outbox event in the same tx
    payload = await _build_cancellation_payload(session, registration, workshop)
    outbox = OutboxEvent(
        event_type=OutboxEventType.REGISTRATION_CANCELLED,
        registration_id=registration.id,
        payload=payload,
        idempotency_key=f"cancel-{registration.id}",
    )
    session.add(outbox)

    logger.info(
        "cancellation.single.pending_commit",
        registration_id=str(registration_id),
        user_id=str(requesting_user_id),
        outbox_id=str(outbox.id),
    )
    return registration


# ---------------------------------------------------------------------------
# Bulk workshop cancellation
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Bulk workshop cancellation (Two-Phase Commit Pattern)
# ---------------------------------------------------------------------------

async def bulk_cancel_workshop(
    session: AsyncSession,
    *,
    workshop_id: uuid.UUID,
    operator_user_id: uuid.UUID,
) -> int:
    """
    Phase 1: Mark Workshop CANCELLED and stage ONE single outbox event.
    Runs in < 15ms. Holds no locks on registrations or long batch loops.
    Returns the count of confirmed registrations that will be cancelled.
    """
    # ── 1. Load + lock the workshop ───────────────────────────────────────
    result = await session.execute(
        select(Workshop)
        .where(Workshop.id == workshop_id)
        .with_for_update()
    )
    workshop = result.scalar_one_or_none()
    if workshop is None:
        raise WorkshopNotFoundError(workshop_id)

    # ── 2. Idempotency guard ──────────────────────────────────────────────
    if workshop.status == WorkshopStatus.CANCELLED:
        logger.info("bulk_cancel.already_cancelled", workshop_id=str(workshop_id))
        return 0

    # Count confirmed registrations for response metadata
    count_result = await session.execute(
        select(func.count())
        .select_from(Registration)
        .where(Registration.workshop_id == workshop_id)
        .where(Registration.status == RegistrationStatus.CONFIRMED)
    )
    confirmed_count = count_result.scalar_one()

    # ── 3. Mark the workshop itself as CANCELLED ──────────────────────────
    workshop.status = WorkshopStatus.CANCELLED

    # ── 4. Stage exactly ONE WORKSHOP_CANCEL_REQUESTED outbox event ───────
    outbox = OutboxEvent(
        event_type=OutboxEventType.WORKSHOP_CANCEL_REQUESTED,
        payload={
            "workshop_id":    str(workshop.id),
            "workshop_title": workshop.title,
            "scheduled_at":   workshop.scheduled_at.isoformat(),
            "venue":          workshop.venue,
            "operator_id":    str(operator_user_id),
            "expected_count": confirmed_count,
        },
        idempotency_key=f"bulk-cancel-{workshop.id}",
    )
    session.add(outbox)

    logger.info(
        "bulk_cancel.phase1_queued",
        workshop_id=str(workshop_id),
        operator_user_id=str(operator_user_id),
        confirmed_count=confirmed_count,
    )
    return confirmed_count


async def process_workshop_cancel_fanout(event_id: uuid.UUID, payload: dict) -> int:
    """
    Phase 2: Background worker handler that catches WORKSHOP_CANCEL_REQUESTED.
    Queries attendees in batches of settings.bulk_cancel_batch_size, marks registrations
    CANCELLED, and inserts individual NOTIFICATION_SEND events (without locking the parent workshop).
    """
    workshop_id = uuid.UUID(payload["workshop_id"])
    ws_title = payload.get("workshop_title", "")
    ws_scheduled = payload.get("scheduled_at", "")
    ws_venue = payload.get("venue", "")
    batch_size = settings.bulk_cancel_batch_size

    total_cancelled = 0

    # ── Process in batches ────────────────────────────────────────────────
    while True:
        async with AsyncSessionLocal() as batch_session:
            async with batch_session.begin():
                rows_result = await batch_session.execute(
                    select(Registration)
                    .where(Registration.workshop_id == workshop_id)
                    .where(Registration.status == RegistrationStatus.CONFIRMED)
                    .limit(batch_size)
                    .with_for_update(skip_locked=True)
                )
                batch = rows_result.scalars().all()
                if not batch:
                    break

                user_ids = [r.user_id for r in batch]
                users_result = await batch_session.execute(
                    select(User).where(User.id.in_(user_ids))
                )
                users_by_id = {u.id: u for u in users_result.scalars().all()}

                for reg in batch:
                    reg.status = RegistrationStatus.CANCELLED
                    user = users_by_id.get(reg.user_id)
                    item_payload = {
                        "user_id":        str(reg.user_id),
                        "user_email":     user.email if user else "",
                        "user_name":      user.full_name if user else "",
                        "workshop_id":    str(workshop_id),
                        "workshop_title": ws_title,
                        "scheduled_at":   ws_scheduled,
                        "venue":          ws_venue,
                    }
                    notification_event = OutboxEvent(
                        event_type=OutboxEventType.NOTIFICATION_SEND,
                        registration_id=reg.id,
                        payload=item_payload,
                        idempotency_key=f"bulk-cancel-notify-{reg.id}",
                    )
                    batch_session.add(notification_event)

                total_cancelled += len(batch)
                logger.info(
                    "bulk_cancel.fanout_batch_committed",
                    workshop_id=str(workshop_id),
                    batch_size=len(batch),
                    total_so_far=total_cancelled,
                )

    # ── Mark the parent WORKSHOP_CANCEL_REQUESTED event as DELIVERED ──────
    async with AsyncSessionLocal() as session:
        async with session.begin():
            parent_ev = await session.get(OutboxEvent, event_id, with_for_update=True)
            if parent_ev:
                parent_ev.status = OutboxEventStatus.DELIVERED
                parent_ev.last_attempted_at = datetime.now(timezone.utc)

    logger.info(
        "bulk_cancel.phase2_complete",
        workshop_id=str(workshop_id),
        total_cancelled=total_cancelled,
    )
    return total_cancelled
