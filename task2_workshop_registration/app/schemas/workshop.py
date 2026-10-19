"""
app/schemas/workshop.py
───────────────────────
Pydantic request/response schemas for the Workshop resource.

Separation of concerns:
  WorkshopCreate / WorkshopUpdate — inbound request validation.
  WorkshopResponse                — a single workshop as returned by the API.
  WorkshopListResponse            — paginated list wrapper.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, Field, field_validator


class WorkshopCreate(BaseModel):
    """Body for POST /api/v1/workshops."""

    model_config = ConfigDict(populate_by_name=True)

    title: str = Field(..., min_length=1, max_length=255, description="Human-readable event title.")
    scheduled_at: datetime = Field(
        ...,
        description="Timezone-aware start time.  Must be in the future.",
    )
    venue: str = Field(..., min_length=1, max_length=255, description="Location string.")
    capacity: int = Field(..., ge=1, description="Maximum number of seats.")

    @field_validator("scheduled_at")
    @classmethod
    def must_be_future(cls, v: datetime) -> datetime:
        """Reject workshops scheduled in the past (REQ-WM-02)."""
        now = datetime.now(tz=timezone.utc)
        # Normalise naive datetimes to UTC before comparison.
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        if v <= now:
            raise ValueError("scheduled_at must be a future datetime.")
        return v


class WorkshopResponse(BaseModel):
    """Single workshop as returned by GET /workshops or POST /workshops."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    title: str
    scheduled_at: datetime
    venue: str
    capacity: int
    status: str
    registered_count: int = Field(
        0,
        description="Number of CONFIRMED registrations (computed at query time).",
    )
    available_seats: int = Field(
        0,
        description="Remaining seats (capacity − registered_count).",
    )
    created_at: datetime
    updated_at: datetime


class WorkshopListResponse(BaseModel):
    """Paginated list of workshops."""

    items: list[WorkshopResponse]
    total: int
    page: int
    page_size: int


class BulkCancelResponse(BaseModel):
    """Response for POST /api/v1/workshops/{workshop_id}/cancel."""

    workshop_id: uuid.UUID
    cancelled_registration_count: int
    message: str = "Workshop and all confirmed registrations cancelled."
