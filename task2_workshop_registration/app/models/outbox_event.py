"""
app/models/outbox_event.py
──────────────────────────
OutboxEvent model — the persistent queue that decouples notification delivery
from the HTTP request path (transactional outbox pattern).

Every domain state change (registration created, registration cancelled,
workshop bulk-cancelled) writes one OutboxEvent **in the same database
transaction** as the domain row.  A background relay worker polls PENDING
records and delivers them to the external notification service, retrying with
exponential back-off until success or until MAX_NOTIFICATION_RETRIES is
reached.

State machine  (REQ-OB-02, design §4.3)
────────────────────────────────────────
    PENDING
       │
       │  relay worker claims row (FOR UPDATE SKIP LOCKED)
       ▼
    PROCESSING  ◄──── re-entered on each retry
       /     \
  HTTP 2xx   failure
      │           │
      ▼           ▼
  DELIVERED   attempts < MAX_RETRIES?
  (terminal)    yes → back-off → PENDING
                no  → FAILED (terminal)

Why PROCESSING?
───────────────
A row stuck in PROCESSING beyond (poll_interval × 2) seconds indicates a
dead worker.  Monitoring can alert on this without confusing it with normal
PENDING records that are intentionally waiting out their back-off window.

Payload schema (stored as JSON)
────────────────────────────────
{
    "user_id":        "<uuid>",
    "user_email":     "alice@example.com",
    "user_name":      "Alice",
    "workshop_id":    "<uuid>",
    "workshop_title": "Intro to Kubernetes",
    "scheduled_at":   "2026-11-01T10:00:00Z",
    "venue":          "Room 101"
}

All notification-relevant data is denormalised into ``payload`` at write time
so the relay worker never needs to re-join tables (avoids phantom-read edge
cases after bulk cancellation deletes or modifies related rows).
"""

import enum
import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, Enum as SAEnum, ForeignKey, Index, Integer, JSON, String
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin


class OutboxEventType(str, enum.Enum):
    """
    Domain event that triggered this outbox record.

    REGISTRATION_CONFIRMED    — a new seat was claimed.
    REGISTRATION_CANCELLED    — an attendee cancelled their own seat.
    WORKSHOP_CANCELLED        — an operator cancelled the whole workshop.
    WORKSHOP_CANCEL_REQUESTED — Phase 1 async fan-out trigger for workshop cancellation.
    NOTIFICATION_SEND         — individual attendee notification dispatch task.
    """

    REGISTRATION_CONFIRMED = "REGISTRATION_CONFIRMED"
    REGISTRATION_CANCELLED = "REGISTRATION_CANCELLED"
    WORKSHOP_CANCELLED = "WORKSHOP_CANCELLED"
    WORKSHOP_CANCEL_REQUESTED = "WORKSHOP_CANCEL_REQUESTED"
    NOTIFICATION_SEND = "NOTIFICATION_SEND"


class OutboxEventStatus(str, enum.Enum):
    """
    Delivery lifecycle state of an outbox record.

    PENDING     — waiting to be picked up by the relay worker.
    PROCESSING  — relay worker has claimed this record and is attempting
                  (or has just attempted) delivery.
    DELIVERED   — notification service returned HTTP 2xx; terminal state.
    FAILED      — all retries exhausted; requires manual inspection; terminal.
    """

    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    DELIVERED = "DELIVERED"
    FAILED = "FAILED"


class OutboxEvent(Base, TimestampMixin):
    """
    A single notification delivery record in the transactional outbox.

    Columns
    ───────
    id                — UUID primary key; also used as the idempotency key
                        sent to the notification service (REQ-OB-07).
    event_type        — which domain event this represents.
    registration_id   — soft FK → registrations.id (SET NULL on delete) so
                        the outbox record is retained for audit even after a
                        registration row is removed.
    payload           — denormalised JSON blob; all data needed by the
                        notification service, captured at write time.
    status            — PENDING | PROCESSING | DELIVERED | FAILED.
    attempts          — incremented each time the relay worker tries delivery.
    last_attempted_at — timestamp of the most recent delivery attempt; used
                        together with ``attempts`` to compute the back-off
                        window without any sleeping in the worker.
    idempotency_key   — stable UUID forwarded as the ``Idempotency-Key`` HTTP
                        header so duplicate delivery attempts are safe (REQ-OB-07).

    Indexes
    ───────
    ix_outbox_status_created
        Composite on (status, created_at) — the relay worker's primary access
        pattern: WHERE status = 'PENDING' ORDER BY created_at LIMIT N.
    """

    __tablename__ = "outbox_events"
    __table_args__ = (
        # Relay worker poll query: status='PENDING' ORDER BY created_at
        Index("ix_outbox_status_created", "status", "created_at"),
        # Composite index for HoL-free eligible polling
        Index("ix_outbox_eligible", "status", "next_attempt_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
    )
    event_type: Mapped[OutboxEventType] = mapped_column(
        SAEnum(OutboxEventType, name="outbox_event_type", create_type=True),
        nullable=False,
    )
    # Soft reference: SET NULL preserves the outbox row for audit if the
    # registration is ever deleted.
    registration_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("registrations.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    payload: Mapped[dict] = mapped_column(
        JSON,
        nullable=False,
        comment="Denormalised notification payload captured at write time.",
    )
    status: Mapped[OutboxEventStatus] = mapped_column(
        SAEnum(OutboxEventStatus, name="outbox_event_status", create_type=True),
        nullable=False,
        default=OutboxEventStatus.PENDING,
        index=True,
    )
    attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="Total delivery attempts made so far.",
    )
    last_attempted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Timestamp of the most recent delivery attempt; drives back-off logic.",
    )
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        index=True,
        comment="When this event becomes eligible for delivery attempt.",
    )
    # Stable across all retries — this is the value forwarded as the
    # Idempotency-Key header so the notification service can deduplicate.
    idempotency_key: Mapped[str] = mapped_column(
        String(36),
        nullable=False,
        default=lambda: str(uuid.uuid4()),
        comment="Forwarded as Idempotency-Key HTTP header on every delivery attempt.",
    )

    def __repr__(self) -> str:
        return (
            f"<OutboxEvent id={self.id} "
            f"type={self.event_type} "
            f"status={self.status} "
            f"attempts={self.attempts}>"
        )
