"""Create the Orders service tables.

Revision ID: 20260817_0001
Revises:
Create Date: 2026-08-17
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260817_0001"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create orders and outbox_events."""
    op.execute("CREATE SCHEMA IF NOT EXISTS orders")

    op.create_table(
        "orders",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("customer_id", sa.String(length=100), nullable=False),
        sa.Column("sku", sa.String(length=100), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("correlation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint("quantity > 0", name=op.f("ck_orders_quantity_positive")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_orders")),
        schema="orders",
    )
    op.create_index(
        op.f("ix_orders_correlation_id"),
        "orders",
        ["correlation_id"],
        schema="orders",
    )

    op.create_table(
        "outbox_events",
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("order_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_type", sa.String(length=100), nullable=False),
        sa.Column("event_version", sa.Integer(), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("correlation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "event_version > 0",
            name=op.f("ck_outbox_events_event_version_positive"),
        ),
        sa.ForeignKeyConstraint(
            ["order_id"],
            ["orders.orders.id"],
            name=op.f("fk_outbox_events_order_id_orders"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("event_id", name=op.f("pk_outbox_events")),
        schema="orders",
    )
    op.create_index(
        op.f("ix_outbox_events_correlation_id"),
        "outbox_events",
        ["correlation_id"],
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


def downgrade() -> None:
    """Drop orders and outbox_events."""
    op.drop_index(
        op.f("ix_outbox_events_pending"),
        table_name="outbox_events",
        schema="orders",
    )
    op.drop_index(
        op.f("ix_outbox_events_correlation_id"),
        table_name="outbox_events",
        schema="orders",
    )
    op.drop_table("outbox_events", schema="orders")
    op.drop_index(
        op.f("ix_orders_correlation_id"),
        table_name="orders",
        schema="orders",
    )
    op.drop_table("orders", schema="orders")
