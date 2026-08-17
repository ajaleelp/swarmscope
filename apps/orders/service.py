from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from apps.orders.models import Order, OutboxEvent
from apps.orders.schemas import CreateOrder, OrderAccepted
from packages.contracts.order_placed import OrderPlacedPayloadV1, OrderPlacedV1


async def place_order(
    *,
    request: CreateOrder,
    correlation_id: UUID,
    session: AsyncSession,
) -> OrderAccepted:
    """Atomically create an order and its OrderPlaced outbox event."""
    order_id = uuid4()

    async with session.begin():
        order = Order(
            id=order_id,
            customer_id=request.customer_id,
            sku=request.sku,
            quantity=request.quantity,
            correlation_id=correlation_id,
        )
        session.add(order)
        await session.flush()

        event = OrderPlacedV1(
            event_id=uuid4(),
            occurred_at=datetime.now(UTC),
            correlation_id=correlation_id,
            payload=OrderPlacedPayloadV1(
                order_id=order_id,
                customer_id=request.customer_id,
                sku=request.sku,
                quantity=request.quantity,
            ),
        )
        session.add(
            OutboxEvent(
                event_id=event.event_id,
                order_id=order_id,
                event_type=event.event_type,
                event_version=event.event_version,
                occurred_at=event.occurred_at,
                correlation_id=event.correlation_id,
                payload=event.payload.model_dump(mode="json"),
                published_at=None,
            )
        )

    return OrderAccepted(
        order_id=order_id,
        correlation_id=correlation_id,
    )
