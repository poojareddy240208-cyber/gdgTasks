"""Pydantic v2 schemas for sensor telemetry ingestion."""

from __future__ import annotations

import datetime
import json
import uuid

from pydantic import BaseModel, Field, field_validator, model_validator

from app.config import settings


class TelemetryPayload(BaseModel):
    """A single sensor reading submitted to the ingestion endpoint."""

    sensor_id: uuid.UUID
    metric_name: str = Field(..., min_length=1, max_length=100)
    value: float
    # Clients must send a timezone-aware datetime (UTC preferred).
    timestamp: datetime.datetime
    # Optional geographic position {"lat": <float>, "lon": <float>}
    location: dict[str, float] | None = None

    @field_validator("timestamp", mode="before")
    @classmethod
    def normalise_timezone(cls, v: object) -> datetime.datetime:
        """Coerce a naive datetime to UTC-aware."""
        if isinstance(v, str):
            v = datetime.datetime.fromisoformat(v)
        if isinstance(v, datetime.datetime) and v.tzinfo is None:
            v = v.replace(tzinfo=datetime.timezone.utc)
        return v  # type: ignore[return-value]

    @field_validator("timestamp", mode="after")
    @classmethod
    def timestamp_within_skew(cls, v: datetime.datetime) -> datetime.datetime:
        """Reject readings whose timestamp is outside the configured skew window.

        Protects the rolling-average ZSET from being poisoned by far-future
        or very-old timestamps that would corrupt the 10-minute window.
        """
        now = datetime.datetime.now(datetime.timezone.utc)
        skew = abs(
            (v.astimezone(datetime.timezone.utc) - now).total_seconds()
        )
        if skew > settings.timestamp_skew_seconds:
            raise ValueError(
                f"timestamp deviates {skew:.0f}s from server time "
                f"(limit ±{settings.timestamp_skew_seconds}s)"
            )
        return v

    @field_validator("location", mode="after")
    @classmethod
    def validate_location_keys(
        cls, v: dict[str, float] | None
    ) -> dict[str, float] | None:
        """If location is provided it must contain 'lat' and 'lon' keys."""
        if v is not None:
            missing = {"lat", "lon"} - v.keys()
            if missing:
                raise ValueError(
                    f"location dict is missing required keys: {missing}"
                )
        return v

    def to_stream_fields(self) -> dict[str, str]:
        """Serialise to a flat string dict suitable for XADD field encoding."""
        return {
            "sensor_id": str(self.sensor_id),
            "metric_name": self.metric_name,
            "value": str(self.value),
            "timestamp": self.timestamp.isoformat(),
            "location": json.dumps(self.location) if self.location else "",
        }


class BatchTelemetryPayload(BaseModel):
    """Up to ``settings.batch_max_size`` readings in a single HTTP request."""

    readings: list[TelemetryPayload] = Field(
        ...,
        min_length=1,
        max_length=settings.batch_max_size,
        description=(
            f"Between 1 and {settings.batch_max_size} telemetry readings."
        ),
    )

    @model_validator(mode="after")
    def check_not_empty(self) -> "BatchTelemetryPayload":
        # Redundant with min_length=1 but provides a clearer error message.
        if not self.readings:
            raise ValueError("batch must contain at least one reading")
        return self
