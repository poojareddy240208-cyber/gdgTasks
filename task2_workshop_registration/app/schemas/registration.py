"""
app/schemas/registration.py
────────────────────────────
Pydantic request/response schemas for the Registration resource.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class RegistrationCreate(BaseModel):
    """Body for POST /api/v1/registrations."""

    model_config = ConfigDict(populate_by_name=True)

    user_id: uuid.UUID = Field(..., description="UUID of the attendee being registered.")
    workshop_id: uuid.UUID = Field(..., description="UUID of the workshop to register for.")


class RegistrationResponse(BaseModel):
    """Single registration as returned by the API."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    user_id: uuid.UUID
    workshop_id: uuid.UUID
    status: str
    created_at: datetime
    updated_at: datetime


class RegistrationCancelResponse(BaseModel):
    """Response for DELETE /api/v1/registrations/{registration_id}."""

    registration_id: uuid.UUID
    status: str
    message: str = "Registration cancelled successfully."
