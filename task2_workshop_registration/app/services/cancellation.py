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

import structlog
from sqlalchemy import select, update
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

async def bulk_cancel_workshop(
    session: AsyncSession,
    *,
    workshop_id: uuid.UUID,
    operator_user_id: uuid.UUID,
) -> int:
    """
    Cancel a workshop and all its CONFIRMED registrations.

    Returns
    ────────
    Total number of registrations that were cancelled.

    Transaction strategy
    ─────────────────────
    The provided ``session`` is used only to load + lock the workshop and
    check the idempotency guard.  Each batch of registrations is processed
    in its own fresh ``AsyncSessionLocal`` transaction so that:

    - Locks are released between batches (REQ-BK-06).
    - A failure mid-way leaves completed batches committed (outbox records
      are already durably written; partially-cancelled workshops are safe
      because the relay worker will re-attempt any already-written outbox).

    The final step — setting workshop.status = CANCELLED — is committed in
    its own transaction after all registration batches succeed.

    Idempotency
    ───────────
    If the workshop is already CANCELLED the function returns 0 immediately
    without touching any registration rows (REQ-BK-02).
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

    # ── 3. Snapshot workshop fields for outbox payloads ──────────────────
    # Capture now before any mutations so every batch payload is identical.
    ws_title       = workshop.title
    ws_scheduled   = workshop.scheduled_at.isoformat()
    ws_venue       = workshop.venue
    batch_size     = settings.bulk_cancel_batch_size

    total_cancelled = 0

    # ── 4. Process in batches ─────────────────────────────────────────────
    while True:
        async with AsyncSessionLocal() as batch_session:
            async with batch_session.begin():
                # Claim a batch of CONFIRMED registrations with a row lock.
                # SKIP LOCKED so multiple concurrent batch workers don't block
                # each other (relevant if the endpoint is called concurrently).
                rows_result = await batch_session.execute(
                    select(Registration)
                    .where(Registration.workshop_id == workshop_id)
                    .where(Registration.status == RegistrationStatus.CONFIRMED)
                    .limit(batch_size)
                    .with_for_update(skip_locked=True)
                )
                batch = rows_result.scalars().all()

                if not batch:
                    break  # all registrations processed

                # Load users for this batch in one query
                user_ids = [r.user_id for r in batch]
                users_result = await batch_session.execute(
                    select(User).where(User.id.in_(user_ids))
                )
                users_by_id = {u.id: u for u in users_result.scalars().all()}

                for reg in batch:
                    reg.status = RegistrationStatus.CANCELLED

                    user = users_by_id.get(reg.user_id)
                    payload = {
                        "user_id":        str(reg.user_id),
                        "user_email":     user.email if user else "",
                        "user_name":      user.full_name if user else "",
                        "workshop_id":    str(workshop_id),
                        "workshop_title": ws_title,
                        "scheduled_at":   ws_scheduled,
                        "venue":          ws_venue,
                    }
                    outbox = OutboxEvent(
                        event_type=OutboxEventType.WORKSHOP_CANCELLED,
                        registration_id=reg.id,
                        payload=payload,
                        # Stable idempotency key per registration for this
                        # specific bulk-cancel operation (REQ-OB-07).
                        idempotency_key=f"bulk-cancel-{reg.id}",
                    )
                    batch_session.add(outbox)

                total_cancelled += len(batch)
                logger.info(
                    "bulk_cancel.batch_committed",
                    workshop_id=str(workshop_id),
                    batch_size=len(batch),
                    total_so_far=total_cancelled,
                )
                # batch_session.begin() auto-commits here on context exit

    # ── 5. Mark the workshop itself as CANCELLED ──────────────────────────
    async with AsyncSessionLocal() as final_session:
        async with final_session.begin():
            ws_final = await final_session.get(Workshop, workshop_id, with_for_update=True)
            if ws_final and ws_final.status != WorkshopStatus.CANCELLED:
                ws_final.status = WorkshopStatus.CANCELLED

    logger.info(
        "bulk_cancel.complete",
        workshop_id=str(workshop_id),
        total_cancelled=total_cancelled,
        operator_user_id=str(operator_user_id),
    )
    return total_cancelled
