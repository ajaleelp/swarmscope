"""Add delivery state to outbox events.

Revision ID: 20260818_0002
Revises: 20260817_0001
Create Date: 2026-08-18
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260818_0002"
down_revision: str | Sequence[str] | None = "20260817_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add retry and lease tracking to committed outbox events."""
    op.add_column(
        "outbox_events",
        sa.Column(
            "publish_attempts",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        schema="orders",
    )
    op.add_column(
        "outbox_events",
        sa.Column(
            "next_attempt_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        schema="orders",
    )
    op.add_column(
        "outbox_events",
        sa.Column(
            "lease_owner",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
        schema="orders",
    )
    op.add_column(
        "outbox_events",
        sa.Column(
            "lease_expires_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        schema="orders",
    )
    op.add_column(
        "outbox_events",
        sa.Column(
            "last_attempt_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        schema="orders",
    )
    op.add_column(
        "outbox_events",
        sa.Column(
            "last_publish_error",
            sa.String(length=1000),
            nullable=True,
        ),
        schema="orders",
    )

    op.create_check_constraint(
        op.f("ck_outbox_events_publish_attempts_nonnegative"),
        "outbox_events",
        "publish_attempts >= 0",
        schema="orders",
    )
    op.create_check_constraint(
        op.f("ck_outbox_events_lease_fields_match"),
        "outbox_events",
        "(lease_owner IS NULL) = (lease_expires_at IS NULL)",
        schema="orders",
    )
    op.create_check_constraint(
        op.f("ck_outbox_events_published_has_no_lease"),
        "outbox_events",
        "published_at IS NULL OR (lease_owner IS NULL AND lease_expires_at IS NULL)",
        schema="orders",
    )

    op.drop_index(
        op.f("ix_outbox_events_pending"),
        table_name="outbox_events",
        schema="orders",
    )
    op.create_index(
        op.f("ix_outbox_events_pending"),
        "outbox_events",
        ["next_attempt_at", "occurred_at"],
        unique=False,
        schema="orders",
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    """Remove retry and lease tracking from outbox events."""
    op.drop_index(
        op.f("ix_outbox_events_pending"),
        table_name="outbox_events",
        schema="orders",
    )
    op.create_index(
        op.f("ix_outbox_events_pending"),
        "outbox_events",
        ["occurred_at"],
        unique=False,
        schema="orders",
        postgresql_where=sa.text("published_at IS NULL"),
    )

    op.drop_constraint(
        op.f("ck_outbox_events_published_has_no_lease"),
        "outbox_events",
        schema="orders",
        type_="check",
    )
    op.drop_constraint(
        op.f("ck_outbox_events_lease_fields_match"),
        "outbox_events",
        schema="orders",
        type_="check",
    )
    op.drop_constraint(
        op.f("ck_outbox_events_publish_attempts_nonnegative"),
        "outbox_events",
        schema="orders",
        type_="check",
    )

    op.drop_column("outbox_events", "last_publish_error", schema="orders")
    op.drop_column("outbox_events", "last_attempt_at", schema="orders")
    op.drop_column("outbox_events", "lease_expires_at", schema="orders")
    op.drop_column("outbox_events", "lease_owner", schema="orders")
    op.drop_column("outbox_events", "next_attempt_at", schema="orders")
    op.drop_column("outbox_events", "publish_attempts", schema="orders")
