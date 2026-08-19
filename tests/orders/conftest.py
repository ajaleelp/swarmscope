from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from apps.orders.models import Order, OutboxEvent

SeedEvent = Callable[..., Awaitable[UUID]]


@pytest_asyncio.fixture
async def clean_orders_schema(
    committed_sessions: async_sessionmaker[AsyncSession],
) -> AsyncIterator[None]:
    """Empty the orders tables before and after a test that commits."""

    async def truncate() -> None:
        async with committed_sessions() as session, session.begin():
            await session.execute(text("TRUNCATE orders.outbox_events, orders.orders"))

    await truncate()

    try:
        yield
    finally:
        await truncate()


@pytest_asyncio.fixture
async def seed_event(
    committed_sessions: async_sessionmaker[AsyncSession],
    clean_orders_schema: None,
) -> SeedEvent:
    """Insert a committed order and its outbox event, returning the event id."""

    async def _seed(
        *,
        next_attempt_at: datetime | None = None,
        occurred_at: datetime | None = None,
        lease_owner: UUID | None = None,
        lease_expires_at: datetime | None = None,
        publish_attempts: int = 0,
    ) -> UUID:
        order_id = uuid4()
        event_id = uuid4()
        correlation_id = uuid4()
        now = datetime.now(UTC)

        async with committed_sessions() as session, session.begin():
            session.add(
                Order(
                    id=order_id,
                    customer_id="customer-outbox",
                    sku="widget-outbox",
                    quantity=1,
                    correlation_id=correlation_id,
                )
            )
            await session.flush()
            session.add(
                OutboxEvent(
                    event_id=event_id,
                    order_id=order_id,
                    event_type="OrderPlaced",
                    event_version=1,
                    occurred_at=occurred_at or now,
                    correlation_id=correlation_id,
                    payload={
                        "order_id": str(order_id),
                        "customer_id": "customer-outbox",
                        "sku": "widget-outbox",
                        "quantity": 1,
                    },
                    published_at=None,
                    publish_attempts=publish_attempts,
                    next_attempt_at=next_attempt_at or now - timedelta(seconds=1),
                    lease_owner=lease_owner,
                    lease_expires_at=lease_expires_at,
                )
            )

        return event_id

    return _seed
