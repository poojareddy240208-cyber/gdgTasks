"""add_next_attempt_at_to_outbox_events

Revision ID: 0001_add_next_attempt_at
Revises: 
Create Date: 2026-10-20 23:15:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0001_add_next_attempt_at'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Add next_attempt_at column with default NOW()
    op.add_column(
        'outbox_events',
        sa.Column(
            'next_attempt_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('NOW()'),
            nullable=False,
            comment='When this event becomes eligible for delivery attempt.',
        ),
    )

    # 2. Add composite index on (status, next_attempt_at) for efficient eligible polling
    op.create_index(
        'ix_outbox_eligible',
        'outbox_events',
        ['status', 'next_attempt_at'],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index('ix_outbox_eligible', table_name='outbox_events')
    op.drop_column('outbox_events', 'next_attempt_at')
