from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from apps.orders.config import get_settings


def create_database_engine() -> AsyncEngine:
    """Create the Orders service's shared PostgreSQL engine."""
    return create_async_engine(
        get_settings().database_url,
        pool_pre_ping=True,
    )


engine = create_database_engine()
session_factory = async_sessionmaker(engine, expire_on_commit=False)


async def get_session() -> AsyncIterator[AsyncSession]:
    """Provide one database session to a request."""
    async with session_factory() as session:
        yield session


async def close_database() -> None:
    """Release pooled database connections during shutdown."""
    await engine.dispose()
