from datetime import datetime
from uuid import UUID

from sqlalchemy import CheckConstraint, DateTime, Integer, String, func
from sqlalchemy.dialects.postgresql import UUID as PostgreSQLUUID
from sqlalchemy.orm import Mapped, mapped_column

from packages.persistence import Base


class ProcessedEvent(Base):
    """One consumed event, recorded so a redelivery does no work twice.

    The primary key is the event id the publisher set as the broker message id.
    Inserting it in the same transaction as the business effect is what makes an
    at-least-once stream safe to consume.
    """

    __tablename__ = "processed_events"
    __table_args__ = ({"schema": "fulfilment"},)

    event_id: Mapped[UUID] = mapped_column(PostgreSQLUUID(as_uuid=True), primary_key=True)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    correlation_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True), nullable=False, index=True
    )
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class Fulfilment(Base):
    """Work carried out in response to an order being placed.

    There is deliberately no foreign key to the orders schema. This service
    works from the event payload alone, so that it can move to its own database
    without a schema change.
    """

    __tablename__ = "fulfilments"
    __table_args__ = (
        CheckConstraint("quantity > 0", name="quantity_positive"),
        {"schema": "fulfilment"},
    )

    id: Mapped[UUID] = mapped_column(PostgreSQLUUID(as_uuid=True), primary_key=True)
    # Unique: one fulfilment per order, independently of the processed-events
    # check, so the database refuses a repeat even if that check is bypassed.
    order_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True), nullable=False, unique=True
    )
    customer_id: Mapped[str] = mapped_column(String(100), nullable=False)
    sku: Mapped[str] = mapped_column(String(100), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    correlation_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True), nullable=False, index=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
