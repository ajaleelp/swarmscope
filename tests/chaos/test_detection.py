from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from functools import partial
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from apps.orders.models import Order, OutboxEvent
from chaos.catalogue import load_catalogue
from chaos.environment import FaultEnvironment, read_sql_scalar


@pytest_asyncio.fixture
async def empty_orders(
    committed_sessions: async_sessionmaker[AsyncSession],
) -> AsyncIterator[None]:
    """Detection counts rows across the whole table, so it needs a known table."""

    async def truncate() -> None:
        async with committed_sessions() as session, session.begin():
            await session.execute(text("TRUNCATE orders.outbox_events, orders.orders"))

    await truncate()
    try:
        yield
    finally:
        await truncate()


async def write_outbox_row(
    sessions: async_sessionmaker[AsyncSession],
    *,
    seconds_old: int,
    published: bool,
    publish_attempts: int = 1,
) -> None:
    order_id, event_id, correlation_id = uuid4(), uuid4(), uuid4()
    occurred_at = datetime.now(UTC) - timedelta(seconds=seconds_old)

    async with sessions() as session, session.begin():
        session.add(
            Order(
                id=order_id,
                customer_id="customer-detection",
                sku="widget-detection",
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
                occurred_at=occurred_at,
                correlation_id=correlation_id,
                payload={"order_id": str(order_id)},
                published_at=occurred_at if published else None,
                publish_attempts=publish_attempts,
                next_attempt_at=occurred_at,
            )
        )


def sql_only_environment(engine: AsyncEngine) -> FaultEnvironment:
    """Detection for this fault is pure SQL; the other dependencies are unused."""
    return FaultEnvironment(
        sql_scalar=partial(read_sql_scalar, engine),
        http_client=None,
        administration=None,
    )


@pytest.mark.asyncio
async def test_a_send_backlog_is_detected_without_any_row_being_retried(
    database_engine: AsyncEngine,
    committed_sessions: async_sessionmaker[AsyncSession],
    empty_orders: None,
) -> None:
    """The symptom of a rejected send is rows piling up, not attempts climbing.

    publish_attempts increments when a row is claimed, so a check written against
    ``attempts > 1`` needs a row to be claimed twice. A publisher stalled inside
    one rejected send never gets that far, which is how a live run of this fault
    produced no detectable symptom at all. Every row here is left at a single
    attempt so the check cannot pass by accident.
    """
    fault = load_catalogue()["topic-send-disabled"]
    environment = sql_only_environment(database_engine)

    assert not (await environment.detect(fault.detects)).matched

    for _ in range(6):
        await write_outbox_row(
            committed_sessions, seconds_old=60, published=False, publish_attempts=1
        )

    detected = await environment.detect(fault.detects)

    assert detected.matched
    assert detected.observed == 6


@pytest.mark.asyncio
async def test_a_healthy_outbox_does_not_look_like_a_backlog(
    database_engine: AsyncEngine,
    committed_sessions: async_sessionmaker[AsyncSession],
    empty_orders: None,
) -> None:
    """Rows that were published, and rows still in flight, are not evidence.

    Without this the check would fire during ordinary traffic and the runner
    would refuse to inject, or worse would call an untouched system broken.
    """
    fault = load_catalogue()["topic-send-disabled"]
    environment = sql_only_environment(database_engine)

    for _ in range(20):
        await write_outbox_row(committed_sessions, seconds_old=300, published=True)
    for _ in range(20):
        await write_outbox_row(committed_sessions, seconds_old=2, published=False)

    observation = await environment.detect(fault.detects)

    assert not observation.matched
    assert observation.observed == 0
