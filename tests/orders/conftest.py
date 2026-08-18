from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest_asyncio
from sqlalchemy import URL, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from apps.orders.models import Order, OutboxEvent

SeedEvent = Callable[..., Awaitable[UUID]]


@pytest_asyncio.fixture
async def database_engine(
    prepared_test_database: URL,
) -> AsyncIterator[AsyncEngine]:
    """Provide an engine bound to the isolated test database."""
    engine = create_async_engine(prepared_test_database, pool_pre_ping=True)

    try:
        yield engine
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def db_session(database_engine: AsyncEngine) -> AsyncIterator[AsyncSession]:
    """Provide a test session whose changes never persist."""
    async with database_engine.connect() as connection:
        outer_transaction = await connection.begin()
        session = AsyncSession(
            bind=connection,
            expire_on_commit=False,
            join_transaction_mode="create_savepoint",
        )

        try:
            yield session
        finally:
            await session.close()
            await outer_transaction.rollback()


@pytest_asyncio.fixture
async def committed_sessions(
    database_engine: AsyncEngine,
) -> async_sessionmaker[AsyncSession]:
    """Provide sessions that really commit.

    The outbox worker is only correct because it commits its claim before
    contacting the broker, so it cannot be proven inside a transaction that is
    always rolled back.
    """
    return async_sessionmaker(database_engine, expire_on_commit=False)


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
