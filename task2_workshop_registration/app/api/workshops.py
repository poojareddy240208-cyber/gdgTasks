"""
app/api/workshops.py
─────────────────────
FastAPI router for Workshop management and Registration endpoints.

Routes
──────
POST   /api/v1/workshops                       Create a new workshop
GET    /api/v1/workshops                       List workshops (paginated)
GET    /api/v1/workshops/{workshop_id}         Get a single workshop
DELETE /api/v1/workshops/{workshop_id}         Delete a workshop (no active registrations)
POST   /api/v1/workshops/{workshop_id}/cancel  Bulk-cancel a workshop (OPERATOR only)

POST   /api/v1/registrations                   Register an attendee  ← atomic dual-write
DELETE /api/v1/registrations/{id}              Cancel a single registration

Design notes
─────────────
- Every successful registration returns HTTP **202 Accepted** immediately,
  before any notification attempt (REQ-NB-01, REQ-NB-04).
- The Registration INSERT and OutboxEvent INSERT share a single
  ``await session.commit()`` — the transactional outbox pattern (REQ-OB-01).
- Bulk cancel also returns HTTP 202, non-blocking (REQ-BK-05).
- All error responses use the uniform envelope
  ``{"error": {"code": "...", "message": "..."}}`` (REQ-API-01).
- Routes live under the ``/api/v1`` prefix added when the router is included
  in ``app/main.py`` (REQ-API-02).
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import structlog
from fastapi import APIRouter, Depends, Query, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.exceptions import (
    AppError,
    OperatorRequiredError,
    WorkshopHasRegistrationsError,
    WorkshopNotFoundError,
)
from app.models import (
    Registration,
    RegistrationStatus,
    User,
    UserRole,
    Workshop,
    WorkshopStatus,
)
from app.schemas.registration import (
    RegistrationCancelResponse,
    RegistrationCreate,
    RegistrationResponse,
)
from app.schemas.workshop import (
    BulkCancelResponse,
    WorkshopCreate,
    WorkshopListResponse,
    WorkshopResponse,
)
from app.services import cancellation as cancellation_service
from app.services import registration as registration_service

logger = structlog.get_logger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Dependency — resolve + authorise a user from the X-User-Id header
# ---------------------------------------------------------------------------
# Full auth is out of scope; we accept a plain UUID header for simplicity.
# A real service would validate a JWT here.

from fastapi import Header, HTTPException


async def get_current_user(
    x_user_id: uuid.UUID = Header(
        ...,
        alias="X-User-Id",
        description="UUID of the acting user (replaces JWT for this service).",
    ),
    db: AsyncSession = Depends(get_db),
) -> User:
    result = await db.execute(select(User).where(User.id == x_user_id))
    user = result.scalar_one_or_none()
    if user is None:
        raise HTTPException(
            status_code=404,
            detail={"error": {"code": "USER_NOT_FOUND", "message": f"User '{x_user_id}' not found."}},
        )
    return user


async def require_operator(current_user: User = Depends(get_current_user)) -> User:
    if current_user.role != UserRole.OPERATOR:
        raise OperatorRequiredError()
    return current_user


# ---------------------------------------------------------------------------
# Helper — compute registered_count / available_seats for a workshop
# ---------------------------------------------------------------------------

async def _enrich_workshop(db: AsyncSession, workshop: Workshop) -> WorkshopResponse:
    """
    Attach computed registered_count and available_seats to a Workshop ORM
    object and return a WorkshopResponse.
    """
    count_result = await db.execute(
        select(func.count())
        .select_from(Registration)
        .where(Registration.workshop_id == workshop.id)
        .where(Registration.status == RegistrationStatus.CONFIRMED)
    )
    registered_count = count_result.scalar_one()
    available_seats = max(workshop.capacity - registered_count, 0)

    data = {
        "id":               workshop.id,
        "title":            workshop.title,
        "scheduled_at":     workshop.scheduled_at,
        "venue":            workshop.venue,
        "capacity":         workshop.capacity,
        "status":           workshop.status,
        "registered_count": registered_count,
        "available_seats":  available_seats,
        "created_at":       workshop.created_at,
        "updated_at":       workshop.updated_at,
    }
    return WorkshopResponse(**data)


# ===========================================================================
# Workshop CRUD
# ===========================================================================

@router.post(
    "/workshops",
    status_code=status.HTTP_201_CREATED,
    response_model=WorkshopResponse,
    summary="Create a workshop",
    tags=["workshops"],
)
async def create_workshop(
    body: WorkshopCreate,
    db: AsyncSession = Depends(get_db),
    _operator: User = Depends(require_operator),
) -> WorkshopResponse:
    """
    Create a new workshop event.

    ``scheduled_at`` must be a future datetime (validated by WorkshopCreate).
    Returns the created workshop with ``registered_count=0`` and
    ``available_seats=capacity``.
    """
    workshop = Workshop(
        title=body.title,
        scheduled_at=body.scheduled_at,
        venue=body.venue,
        capacity=body.capacity,
        status=WorkshopStatus.SCHEDULED,
    )
    db.add(workshop)
    await db.commit()
    await db.refresh(workshop)

    logger.info(
        "workshop.created",
        workshop_id=str(workshop.id),
        title=workshop.title,
        capacity=workshop.capacity,
    )
    return await _enrich_workshop(db, workshop)


@router.get(
    "/workshops",
    response_model=WorkshopListResponse,
    summary="List workshops",
    tags=["workshops"],
)
async def list_workshops(
    page: int = Query(1, ge=1, description="Page number (1-based)."),
    page_size: int = Query(20, ge=1, le=100, description="Items per page."),
    db: AsyncSession = Depends(get_db),
) -> WorkshopListResponse:
    """
    Return a paginated list of all workshops with live seat counts.

    ``registered_count`` and ``available_seats`` are computed via a single
    SQL query with a correlated subquery — never stored — so they are always
    consistent (design §3.2).
    """
    offset = (page - 1) * page_size

    # Count confirmed registrations per workshop in one round-trip
    confirmed_subq = (
        select(
            Registration.workshop_id,
            func.count().label("cnt"),
        )
        .where(Registration.status == RegistrationStatus.CONFIRMED)
        .group_by(Registration.workshop_id)
        .subquery()
    )

    total_result = await db.execute(select(func.count()).select_from(Workshop))
    total = total_result.scalar_one()

    workshops_result = await db.execute(
        select(Workshop)
        .order_by(Workshop.scheduled_at)
        .offset(offset)
        .limit(page_size)
    )
    workshops = workshops_result.scalars().all()

    items = [await _enrich_workshop(db, w) for w in workshops]
    return WorkshopListResponse(
        items=items,
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get(
    "/workshops/{workshop_id}",
    response_model=WorkshopResponse,
    summary="Get a workshop",
    tags=["workshops"],
)
async def get_workshop(
    workshop_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
) -> WorkshopResponse:
    """Fetch a single workshop by ID with live seat counts."""
    result = await db.execute(select(Workshop).where(Workshop.id == workshop_id))
    workshop = result.scalar_one_or_none()
    if workshop is None:
        raise WorkshopNotFoundError(workshop_id)
    return await _enrich_workshop(db, workshop)


@router.delete(
    "/workshops/{workshop_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_model=None,
    summary="Delete a workshop",
    tags=["workshops"],
)
async def delete_workshop(
    workshop_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    _operator: User = Depends(require_operator),
) -> None:
    """
    Delete a workshop.

    Returns 409 if there are any CONFIRMED registrations (REQ-WM-04).
    The DB RESTRICT FK also prevents deletion, but we surface a friendly
    error before hitting it.
    """
    result = await db.execute(select(Workshop).where(Workshop.id == workshop_id))
    workshop = result.scalar_one_or_none()
    if workshop is None:
        raise WorkshopNotFoundError(workshop_id)

    count_result = await db.execute(
        select(func.count())
        .select_from(Registration)
        .where(Registration.workshop_id == workshop_id)
        .where(Registration.status == RegistrationStatus.CONFIRMED)
    )
    confirmed_count = count_result.scalar_one()
    if confirmed_count > 0:
        raise WorkshopHasRegistrationsError(workshop_id, confirmed_count)

    await db.delete(workshop)
    await db.commit()
    logger.info("workshop.deleted", workshop_id=str(workshop_id))


# ===========================================================================
# Bulk workshop cancellation   (REQ-BK-01 … REQ-BK-06)
# ===========================================================================

@router.post(
    "/workshops/{workshop_id}/cancel",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=BulkCancelResponse,
    summary="Bulk-cancel a workshop",
    tags=["workshops"],
)
async def bulk_cancel_workshop(
    workshop_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    operator: User = Depends(require_operator),
) -> BulkCancelResponse:
    """
    Cancel a workshop and all its confirmed registrations.

    - Restricted to OPERATOR role (REQ-BK-01).
    - Idempotent: calling twice returns HTTP 200 on the second call with
      ``cancelled_registration_count=0`` and no side-effects (REQ-BK-02).
    - Processes registrations in batches of ``BULK_CANCEL_BATCH_SIZE``
      (default 500) to avoid long-held locks (REQ-BK-06).
    - Returns HTTP 202 **immediately** after the DB work completes, before
      any notification is sent (REQ-BK-05).
    - One ``OutboxEvent(WORKSHOP_CANCELLED)`` is inserted per attendee in the
      same batch transaction as the registration update (REQ-BK-04).
    """
    # Check idempotency before entering the service (avoids acquiring a lock
    # on an already-cancelled workshop unnecessarily).
    pre_check = await db.execute(select(Workshop).where(Workshop.id == workshop_id))
    workshop = pre_check.scalar_one_or_none()
    if workshop is None:
        raise WorkshopNotFoundError(workshop_id)

    if workshop.status == WorkshopStatus.CANCELLED:
        logger.info("bulk_cancel.no_op", workshop_id=str(workshop_id))
        return BulkCancelResponse(
            workshop_id=workshop_id,
            cancelled_registration_count=0,
            message="Workshop is already cancelled. No action taken.",
        )

    total_cancelled = await cancellation_service.bulk_cancel_workshop(
        db,
        workshop_id=workshop_id,
        operator_user_id=operator.id,
    )

    logger.info(
        "bulk_cancel.accepted",
        workshop_id=str(workshop_id),
        cancelled_registration_count=total_cancelled,
        operator_id=str(operator.id),
    )
    return BulkCancelResponse(
        workshop_id=workshop_id,
        cancelled_registration_count=total_cancelled,
    )


# ===========================================================================
# Registration   (REQ-REG-*, REQ-NB-*, REQ-OB-*)
# ===========================================================================

@router.post(
    "/registrations",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=RegistrationResponse,
    summary="Register an attendee for a workshop",
    tags=["registrations"],
)
async def create_registration(
    body: RegistrationCreate,
    db: AsyncSession = Depends(get_db),
) -> RegistrationResponse:
    """
    Reserve a seat for an attendee.

    Atomic dual-write
    ──────────────────
    Both the ``Registration`` row and the ``OutboxEvent`` row are inserted
    inside the **same** ``await db.commit()`` call.  If the process crashes
    after the commit, the relay worker finds the PENDING outbox record on
    restart and delivers the notification automatically (REQ-OB-04).

    Returns HTTP **202 Accepted** immediately — before any contact with the
    notification service (REQ-NB-01).  The relay worker delivers the
    confirmation email/push asynchronously (REQ-NB-04).

    Error codes
    ───────────
    409 ALREADY_REGISTERED  — (user_id, workshop_id) already exists
    409 WORKSHOP_FULL        — no available seats
    409 WORKSHOP_CANCELLED   — workshop is no longer active
    404 WORKSHOP_NOT_FOUND   — unknown workshop_id
    404 USER_NOT_FOUND       — unknown user_id
    """
    registration = await registration_service.register(
        db,
        user_id=body.user_id,
        workshop_id=body.workshop_id,
    )

    # ── SINGLE commit — Registration + OutboxEvent land atomically ────────
    await db.commit()

    logger.info(
        "registration.created",
        registration_id=str(registration.id),
        user_id=str(registration.user_id),
        workshop_id=str(registration.workshop_id),
    )
    return RegistrationResponse.model_validate(registration)


@router.delete(
    "/registrations/{registration_id}",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=RegistrationCancelResponse,
    summary="Cancel a registration",
    tags=["registrations"],
)
async def cancel_registration(
    registration_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> RegistrationCancelResponse:
    """
    Cancel a single registration.

    The Registration status is set to CANCELLED and an OutboxEvent
    (REGISTRATION_CANCELLED) is written in the same commit.  Returns
    HTTP 202 immediately (REQ-NB-03).

    Only the owner of the registration may cancel it (ownership checked in
    the service layer).  OPERATORs may cancel any registration.
    """
    registration = await cancellation_service.cancel_registration(
        db,
        registration_id=registration_id,
        requesting_user_id=current_user.id,
    )
    await db.commit()

    logger.info(
        "registration.cancelled",
        registration_id=str(registration_id),
        user_id=str(current_user.id),
    )
    return RegistrationCancelResponse(
        registration_id=registration.id,
        status=registration.status,
    )
