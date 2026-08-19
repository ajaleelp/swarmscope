from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import psycopg
import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from psycopg import sql
from sqlalchemy import URL
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from apps.orders.config import Settings, get_settings

REPO_ROOT = Path(__file__).resolve().parents[1]
TEST_DATABASE_SUFFIX = "_test"


def isolated_settings() -> Settings:
    """Return the application settings pointed at the isolated test database."""
    settings = get_settings()
    return settings.model_copy(
        update={"postgres_db": f"{settings.postgres_db}{TEST_DATABASE_SUFFIX}"}
    )


def create_database_if_missing(maintenance: Settings, database_name: str) -> None:
    """Create the test database, connecting through the development database.

    CREATE DATABASE cannot run inside a transaction, so this uses a plain
    autocommit connection rather than the application's async engine.
    """
    with psycopg.connect(
        host=maintenance.postgres_host,
        port=maintenance.postgres_port,
        user=maintenance.postgres_user,
        password=maintenance.postgres_password.get_secret_value(),
        dbname=maintenance.postgres_db,
        autocommit=True,
    ) as connection, connection.cursor() as cursor:
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database_name,))
        if cursor.fetchone() is None:
            cursor.execute(
                sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name))
            )


@pytest.fixture(scope="session")
def prepared_test_database() -> Iterator[URL]:
    """Create and migrate the isolated test database once per test run."""
    isolated = isolated_settings()
    create_database_if_missing(get_settings(), isolated.postgres_db)

    config = Config(str(REPO_ROOT / "alembic.ini"))
    config.set_main_option(
        "sqlalchemy.url",
        isolated.database_url.render_as_string(hide_password=False),
    )
    command.upgrade(config, "head")

    yield isolated.database_url


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
