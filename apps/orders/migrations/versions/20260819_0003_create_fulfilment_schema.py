"""Create the fulfilment schema.

Revision ID: 20260819_0003
Revises: 20260818_0002
Create Date: 2026-08-19
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260819_0003"
down_revision: str | Sequence[str] | None = "20260818_0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add the tables the fulfilment consumer owns."""
    op.execute("CREATE SCHEMA IF NOT EXISTS fulfilment")

    op.create_table(
        "processed_events",
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_type", sa.String(length=100), nullable=False),
        sa.Column("correlation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "processed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("event_id", name=op.f("pk_processed_events")),
        schema="fulfilment",
    )
    op.create_index(
        op.f("ix_processed_events_correlation_id"),
        "processed_events",
        ["correlation_id"],
        schema="fulfilment",
    )

    op.create_table(
        "fulfilments",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("order_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("customer_id", sa.String(length=100), nullable=False),
        sa.Column("sku", sa.String(length=100), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("correlation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint("quantity > 0", name=op.f("ck_fulfilments_quantity_positive")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_fulfilments")),
        sa.UniqueConstraint("order_id", name=op.f("uq_fulfilments_order_id")),
        schema="fulfilment",
    )
    op.create_index(
        op.f("ix_fulfilments_correlation_id"),
        "fulfilments",
        ["correlation_id"],
        schema="fulfilment",
    )


def downgrade() -> None:
    """Remove the fulfilment tables and schema."""
    op.drop_index(
        op.f("ix_fulfilments_correlation_id"), table_name="fulfilments", schema="fulfilment"
    )
    op.drop_table("fulfilments", schema="fulfilment")
    op.drop_index(
        op.f("ix_processed_events_correlation_id"),
        table_name="processed_events",
        schema="fulfilment",
    )
    op.drop_table("processed_events", schema="fulfilment")
    op.execute("DROP SCHEMA IF EXISTS fulfilment")
