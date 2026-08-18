from uuid import uuid4

import pytest
from pydantic import ValidationError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from apps.orders.models import Order, OutboxEvent
from apps.orders.schemas import CreateOrder
from apps.orders.service import place_order


def sample_request() -> CreateOrder:
    return CreateOrder(
        customer_id="customer-123",
        sku="widget-blue",
        quantity=2,
    )


@pytest.mark.asyncio
async def test_place_order_writes_order_and_outbox(
    db_session: AsyncSession,
) -> None:
    correlation_id = uuid4()

    response = await place_order(
        request=sample_request(),
        correlation_id=correlation_id,
        session=db_session,
    )

    order = await db_session.get(Order, response.order_id)
    event = await db_session.scalar(
        select(OutboxEvent).where(OutboxEvent.order_id == response.order_id)
    )
    order_count = await db_session.scalar(
        select(func.count())
        .select_from(Order)
        .where(Order.correlation_id == correlation_id)
    )
    event_count = await db_session.scalar(
        select(func.count())
        .select_from(OutboxEvent)
        .where(OutboxEvent.correlation_id == correlation_id)
    )

    assert order is not None
    assert order_count == 1
    assert order.customer_id == "customer-123"
    assert order.sku == "widget-blue"
    assert order.quantity == 2
    assert order.correlation_id == correlation_id

    assert event is not None
    assert event_count == 1
    assert event.event_type == "OrderPlaced"
    assert event.event_version == 1
    assert event.correlation_id == correlation_id
    assert event.published_at is None
    assert event.publish_attempts == 0
    assert event.next_attempt_at is not None
    assert event.lease_owner is None
    assert event.lease_expires_at is None
    assert event.last_attempt_at is None
    assert event.last_publish_error is None
    assert event.payload == {
        "order_id": str(response.order_id),
        "customer_id": "customer-123",
        "sku": "widget-blue",
        "quantity": 2,
    }

    assert response.correlation_id == correlation_id
    assert response.status == "accepted"


@pytest.mark.asyncio
async def test_failure_after_order_flush_rolls_back_both_rows(
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    correlation_id = uuid4()
    original_add = db_session.add

    def fail_when_adding_outbox(
        instance: object,
        *,
        _warn: bool = True,
    ) -> None:
        if isinstance(instance, OutboxEvent):
            raise RuntimeError("forced failure after order flush")
        original_add(instance, _warn=_warn)

    monkeypatch.setattr(db_session, "add", fail_when_adding_outbox)

    with pytest.raises(RuntimeError, match="forced failure"):
        await place_order(
            request=sample_request(),
            correlation_id=correlation_id,
            session=db_session,
        )

    order_count = await db_session.scalar(
        select(func.count())
        .select_from(Order)
        .where(Order.correlation_id == correlation_id)
    )
    event_count = await db_session.scalar(
        select(func.count())
        .select_from(OutboxEvent)
        .where(OutboxEvent.correlation_id == correlation_id)
    )

    assert order_count == 0
    assert event_count == 0


def test_invalid_order_is_rejected_before_database_access() -> None:
    with pytest.raises(ValidationError):
        CreateOrder(
            customer_id="customer-123",
            sku="widget-blue",
            quantity=0,
        )
