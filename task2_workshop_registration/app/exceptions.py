"""
app/exceptions.py
─────────────────
Domain exception hierarchy for the Workshop Registration service.

Every exception carries:
  error_code  — SCREAMING_SNAKE string used in the JSON error envelope.
  http_status — HTTP status code the exception handler maps to.
  message     — Human-readable default message included in responses.

The FastAPI error handler (registered in app/main.py) catches any
``AppError`` subclass and converts it to::

    HTTP <http_status>
    {"error": {"code": "<error_code>", "message": "<message>"}}

This satisfies REQ-API-01 (uniform error envelope).
"""

from __future__ import annotations

import uuid


class AppError(Exception):
    """Base class for all application domain errors."""

    error_code: str = "INTERNAL_ERROR"
    http_status: int = 500

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message

    def to_dict(self) -> dict:
        return {"error": {"code": self.error_code, "message": self.message}}


# ---------------------------------------------------------------------------
# 404 — Not Found
# ---------------------------------------------------------------------------

class WorkshopNotFoundError(AppError):
    """Raised when a workshop_id does not correspond to any row in the DB."""

    error_code = "WORKSHOP_NOT_FOUND"
    http_status = 404

    def __init__(self, workshop_id: uuid.UUID) -> None:
        super().__init__(f"Workshop '{workshop_id}' not found.")
        self.workshop_id = workshop_id


class RegistrationNotFoundError(AppError):
    """Raised when a registration_id does not correspond to any row in the DB."""

    error_code = "REGISTRATION_NOT_FOUND"
    http_status = 404

    def __init__(self, registration_id: uuid.UUID) -> None:
        super().__init__(f"Registration '{registration_id}' not found.")
        self.registration_id = registration_id


class UserNotFoundError(AppError):
    """Raised when a user_id does not correspond to any row in the DB."""

    error_code = "USER_NOT_FOUND"
    http_status = 404

    def __init__(self, user_id: uuid.UUID) -> None:
        super().__init__(f"User '{user_id}' not found.")
        self.user_id = user_id


# ---------------------------------------------------------------------------
# 403 — Forbidden
# ---------------------------------------------------------------------------

class OperatorRequiredError(AppError):
    """Raised when an OPERATOR-only route is accessed by a non-operator."""

    error_code = "OPERATOR_REQUIRED"
    http_status = 403

    def __init__(self) -> None:
        super().__init__("This operation requires the OPERATOR role.")


# ---------------------------------------------------------------------------
# 409 — Conflict
# ---------------------------------------------------------------------------

class WorkshopCancelledError(AppError):
    """Raised when attempting to register for a workshop that is CANCELLED."""

    error_code = "WORKSHOP_CANCELLED"
    http_status = 409

    def __init__(self, workshop_id: uuid.UUID) -> None:
        super().__init__(
            f"Workshop '{workshop_id}' has been cancelled and is no longer accepting registrations."
        )
        self.workshop_id = workshop_id


class WorkshopFullError(AppError):
    """Raised when a workshop has no remaining seats (capacity reached)."""

    error_code = "WORKSHOP_FULL"
    http_status = 409

    def __init__(self, workshop_id: uuid.UUID) -> None:
        super().__init__(f"Workshop '{workshop_id}' is fully booked.")
        self.workshop_id = workshop_id


class AlreadyRegisteredError(AppError):
    """
    Raised when a user already holds a registration (any status) for a
    workshop and attempts to register again.

    The DB-level UniqueConstraint on (user_id, workshop_id) is the final
    safety net, but this exception is raised when the application layer
    detects the duplicate first (via flush() IntegrityError).
    """

    error_code = "ALREADY_REGISTERED"
    http_status = 409

    def __init__(self, user_id: uuid.UUID, workshop_id: uuid.UUID) -> None:
        super().__init__(
            f"User '{user_id}' is already registered for workshop '{workshop_id}'."
        )
        self.user_id = user_id
        self.workshop_id = workshop_id


class WorkshopHasRegistrationsError(AppError):
    """
    Raised when attempting to DELETE a workshop that still has at least one
    CONFIRMED registration (REQ-WM-04).  The DB enforces this via
    ondelete=RESTRICT, but we raise this before attempting the delete.
    """

    error_code = "WORKSHOP_HAS_REGISTRATIONS"
    http_status = 409

    def __init__(self, workshop_id: uuid.UUID, count: int) -> None:
        super().__init__(
            f"Workshop '{workshop_id}' has {count} confirmed registration(s) and cannot be deleted."
        )
        self.workshop_id = workshop_id
        self.count = count


# ---------------------------------------------------------------------------
# 422 — Validation / Business Rule
# ---------------------------------------------------------------------------

class WorkshopInPastError(AppError):
    """Raised when creating a workshop with a scheduled_at in the past."""

    error_code = "WORKSHOP_IN_PAST"
    http_status = 422

    def __init__(self) -> None:
        super().__init__("Workshop scheduled_at must be in the future.")
