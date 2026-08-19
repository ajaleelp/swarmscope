from uuid import uuid4

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from apps.fulfilment.models import Fulfilment, ProcessedEvent


def a_fulfilment(*, order_id=None) -> Fulfilment:
    return Fulfilment(
        id=uuid4(),
        order_id=order_id or uuid4(),
        customer_id="customer-1",
        sku="widget-blue",
        quantity=2,
        correlation_id=uuid4(),
    )


@pytest.mark.asyncio
async def test_processed_event_records_server_side_defaults(
    db_session: AsyncSession,
) -> None:
    event = ProcessedEvent(
        event_id=uuid4(), event_type="OrderPlaced", correlation_id=uuid4()
    )
    db_session.add(event)
    await db_session.flush()
    await db_session.refresh(event)

    assert event.processed_at is not None


@pytest.mark.asyncio
async def test_the_same_event_cannot_be_recorded_twice(
    db_session: AsyncSession,
) -> None:
    """This is what makes an at-least-once stream safe to consume."""
    event_id = uuid4()
    db_session.add(
        ProcessedEvent(event_id=event_id, event_type="OrderPlaced", correlation_id=uuid4())
    )
    await db_session.flush()

    db_session.add(
        ProcessedEvent(event_id=event_id, event_type="OrderPlaced", correlation_id=uuid4())
    )
    with pytest.raises(IntegrityError):
        await db_session.flush()


@pytest.mark.asyncio
async def test_an_order_cannot_be_fulfilled_twice(db_session: AsyncSession) -> None:
    """A second guard, independent of the processed-events check."""
    order_id = uuid4()
    db_session.add(a_fulfilment(order_id=order_id))
    await db_session.flush()

    db_session.add(a_fulfilment(order_id=order_id))
    with pytest.raises(IntegrityError):
        await db_session.flush()


@pytest.mark.asyncio
async def test_fulfilment_records_server_side_defaults(db_session: AsyncSession) -> None:
    fulfilment = a_fulfilment()
    db_session.add(fulfilment)
    await db_session.flush()
    await db_session.refresh(fulfilment)

    assert fulfilment.created_at is not None


@pytest.mark.asyncio
async def test_quantity_must_be_positive(db_session: AsyncSession) -> None:
    fulfilment = a_fulfilment()
    fulfilment.quantity = 0
    db_session.add(fulfilment)

    with pytest.raises(IntegrityError):
        await db_session.flush()


def test_fulfilment_does_not_reference_the_orders_schema() -> None:
    """Deliberate: this service must work from the event payload alone.

    A foreign key here would prevent fulfilment from ever moving to its own
    database, which is the point of consuming events rather than sharing tables.
    """
    assert Fulfilment.__table__.foreign_keys == set()
    assert ProcessedEvent.__table__.foreign_keys == set()
