from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from apps.fulfilment.models import Fulfilment, ProcessedEvent
from apps.fulfilment.service import Handled, fulfil_order, is_duplicate
from apps.orders.messaging import OutboundEvent
from apps.orders.servicebus import to_service_bus_message
from packages.contracts.order_placed import OrderPlacedPayloadV1, OrderPlacedV1


def an_event(*, order_id: UUID | None = None) -> OrderPlacedV1:
    return OrderPlacedV1(
        event_id=uuid4(),
        occurred_at=datetime.now(UTC),
        correlation_id=uuid4(),
        payload=OrderPlacedPayloadV1(
            order_id=order_id or uuid4(),
            customer_id="customer-1",
            sku="widget-blue",
            quantity=3,
        ),
    )


def an_event_with_invalid_quantity(quantity: int) -> OrderPlacedV1:
    """Build an event the contract would normally reject.

    model_construct skips validation, which is how to reach the database
    constraint rather than being stopped by Pydantic first.
    """
    return OrderPlacedV1.model_construct(
        event_id=uuid4(),
        event_type="OrderPlaced",
        event_version=1,
        occurred_at=datetime.now(UTC),
        correlation_id=uuid4(),
        payload=OrderPlacedPayloadV1.model_construct(
            order_id=uuid4(),
            customer_id="customer-1",
            sku="widget-blue",
            quantity=quantity,
        ),
    )


# --- the happy path ---------------------------------------------------------


@pytest.mark.asyncio
async def test_fulfilling_writes_the_work_and_records_the_event(
    committed_sessions: async_sessionmaker[AsyncSession],
    clean_fulfilment_schema: None,
) -> None:
    event = an_event()

    async with committed_sessions() as session:
        assert await fulfil_order(event=event, session=session) is Handled.FULFILLED

    async with committed_sessions() as session:
        processed = await session.get(ProcessedEvent, event.event_id)
        fulfilment = await session.scalar(
            select(Fulfilment).where(Fulfilment.order_id == event.payload.order_id)
        )

    assert processed is not None
    assert processed.event_type == "OrderPlaced"
    assert processed.correlation_id == event.correlation_id

    assert fulfilment is not None
    assert fulfilment.sku == "widget-blue"
    assert fulfilment.quantity == 3
    assert fulfilment.correlation_id == event.correlation_id


# --- idempotency ------------------------------------------------------------


@pytest.mark.asyncio
async def test_redelivering_the_same_event_changes_nothing(
    committed_sessions: async_sessionmaker[AsyncSession],
    clean_fulfilment_schema: None,
) -> None:
    """The publisher is at-least-once, so this is the normal case, not an edge."""
    event = an_event()

    async with committed_sessions() as session:
        first = await fulfil_order(event=event, session=session)
    async with committed_sessions() as session:
        second = await fulfil_order(event=event, session=session)

    async with committed_sessions() as session:
        fulfilments = await session.scalar(select(func.count()).select_from(Fulfilment))
        processed = await session.scalar(select(func.count()).select_from(ProcessedEvent))

    assert first is Handled.FULFILLED
    assert second is Handled.DUPLICATE
    assert fulfilments == 1
    assert processed == 1


@pytest.mark.asyncio
async def test_a_different_event_for_the_same_order_is_still_a_duplicate(
    committed_sessions: async_sessionmaker[AsyncSession],
    clean_fulfilment_schema: None,
) -> None:
    """The second guard: same order, new event id, must not fulfil twice."""
    order_id = uuid4()

    async with committed_sessions() as session:
        first = await fulfil_order(event=an_event(order_id=order_id), session=session)
    async with committed_sessions() as session:
        second = await fulfil_order(event=an_event(order_id=order_id), session=session)

    async with committed_sessions() as session:
        fulfilments = await session.scalar(select(func.count()).select_from(Fulfilment))

    assert first is Handled.FULFILLED
    assert second is Handled.DUPLICATE
    assert fulfilments == 1


@pytest.mark.asyncio
async def test_nothing_is_written_when_the_transaction_fails(
    committed_sessions: async_sessionmaker[AsyncSession],
    clean_fulfilment_schema: None,
) -> None:
    """Both rows land together or neither does."""
    async with committed_sessions() as session:
        with pytest.raises(IntegrityError):
            await fulfil_order(
                event=an_event_with_invalid_quantity(0), session=session
            )

    async with committed_sessions() as session:
        processed = await session.scalar(select(func.count()).select_from(ProcessedEvent))

    assert processed == 0, "the event was recorded despite the work failing"


# --- telling a duplicate from a defect --------------------------------------


@pytest.mark.asyncio
async def test_an_unrelated_constraint_failure_is_not_treated_as_a_duplicate(
    committed_sessions: async_sessionmaker[AsyncSession],
    clean_fulfilment_schema: None,
) -> None:
    """Swallowing this would complete the message and lose the event forever."""
    async with committed_sessions() as session:
        with pytest.raises(IntegrityError) as raised:
            await fulfil_order(
                event=an_event_with_invalid_quantity(-5), session=session
            )

    assert not is_duplicate(raised.value)
    assert "quantity_positive" in str(raised.value.orig)


# --- the contract the publisher and consumer share --------------------------


def test_a_published_message_parses_back_into_the_contract() -> None:
    """Publisher and consumer share one model, so they cannot drift apart."""
    order_id = uuid4()
    outbound = OutboundEvent(
        event_id=uuid4(),
        event_type="OrderPlaced",
        event_version=1,
        occurred_at=datetime.now(UTC),
        correlation_id=uuid4(),
        payload={
            "order_id": str(order_id),
            "customer_id": "customer-1",
            "sku": "widget-blue",
            "quantity": 3,
        },
    )
    body = b"".join(to_service_bus_message(outbound).body).decode()

    parsed = OrderPlacedV1.model_validate_json(body)

    assert parsed.event_id == outbound.event_id
    assert parsed.correlation_id == outbound.correlation_id
    assert parsed.payload.order_id == order_id
    assert parsed.payload.quantity == 3
