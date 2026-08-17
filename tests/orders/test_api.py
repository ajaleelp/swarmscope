from collections.abc import AsyncIterator
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from apps.orders.database import get_session
from apps.orders.main import app
from apps.orders.models import Order, OutboxEvent


@pytest_asyncio.fixture
async def api_client(db_session: AsyncSession) -> AsyncIterator[AsyncClient]:
    async def override_session() -> AsyncIterator[AsyncSession]:
        yield db_session

    app.dependency_overrides[get_session] = override_session
    transport = ASGITransport(app=app)

    try:
        async with AsyncClient(
            transport=transport,
            base_url="http://testserver",
        ) as client:
            yield client
    finally:
        app.dependency_overrides.pop(get_session, None)


@pytest.mark.asyncio
async def test_create_order_returns_201_and_persists_order_and_outbox(
    api_client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    correlation_id = uuid4()

    response = await api_client.post(
        "/orders",
        headers={"X-Correlation-ID": str(correlation_id)},
        json={
            "customer_id": "customer-api",
            "sku": "widget-api",
            "quantity": 3,
        },
    )

    assert response.status_code == 201
    body = response.json()
    order_id = UUID(body["order_id"])

    assert body == {
        "order_id": str(order_id),
        "correlation_id": str(correlation_id),
        "status": "accepted",
    }
    assert response.headers["x-correlation-id"] == str(correlation_id)

    order = await db_session.get(Order, order_id)
    event = await db_session.scalar(
        select(OutboxEvent).where(OutboxEvent.order_id == order_id)
    )

    assert order is not None
    assert order.correlation_id == correlation_id
    assert event is not None
    assert event.correlation_id == correlation_id
    assert event.payload["order_id"] == str(order_id)


@pytest.mark.asyncio
async def test_create_order_generates_correlation_id_when_header_is_missing(
    api_client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    response = await api_client.post(
        "/orders",
        json={
            "customer_id": "customer-generated",
            "sku": "widget-generated",
            "quantity": 1,
        },
    )

    assert response.status_code == 201
    body = response.json()
    correlation_id = UUID(body["correlation_id"])
    order = await db_session.get(Order, UUID(body["order_id"]))

    assert response.headers["x-correlation-id"] == str(correlation_id)
    assert order is not None
    assert order.correlation_id == correlation_id


@pytest.mark.asyncio
async def test_invalid_order_returns_422_without_persisting_rows(
    api_client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    correlation_id = uuid4()

    response = await api_client.post(
        "/orders",
        headers={"X-Correlation-ID": str(correlation_id)},
        json={
            "customer_id": "customer-invalid",
            "sku": "widget-invalid",
            "quantity": 0,
        },
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

    assert response.status_code == 422
    assert order_count == 0
    assert event_count == 0
