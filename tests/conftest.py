from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest
from alembic import command
from alembic.config import Config
from psycopg import sql
from sqlalchemy import URL

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
