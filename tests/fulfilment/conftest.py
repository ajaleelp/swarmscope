from collections.abc import AsyncIterator

import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


@pytest_asyncio.fixture
async def clean_fulfilment_schema(
    committed_sessions: async_sessionmaker[AsyncSession],
) -> AsyncIterator[None]:
    """Empty the fulfilment tables before and after a test that commits."""

    async def truncate() -> None:
        async with committed_sessions() as session, session.begin():
            await session.execute(
                text("TRUNCATE fulfilment.processed_events, fulfilment.fulfilments")
            )

    await truncate()

    try:
        yield
    finally:
        await truncate()
