import asyncio

from alembic import context
from sqlalchemy import Connection, pool
from sqlalchemy.ext.asyncio import create_async_engine

from apps.orders.config import get_settings
from apps.orders.models import Base

config = context.config
settings = get_settings()
target_metadata = Base.metadata


def database_url() -> str:
    """Return the database this migration run targets.

    An explicitly configured URL wins so that tooling can migrate a database
    other than the one in the environment, such as the isolated test database.
    """
    configured = config.get_main_option("sqlalchemy.url")
    if configured:
        return configured
    return settings.database_url.render_as_string(hide_password=False)


def configure_context(*, connection: Connection | None = None) -> None:
    context.configure(
        connection=connection,
        url=None if connection is not None else database_url(),
        target_metadata=target_metadata,
        include_schemas=True,
        compare_type=True,
        version_table="orders_alembic_version",
    )


def run_migrations_offline() -> None:
    configure_context()

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    configure_context(connection=connection)

    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    connectable = create_async_engine(database_url(), poolclass=pool.NullPool)

    try:
        async with connectable.connect() as connection:
            await connection.run_sync(do_run_migrations)
    finally:
        await connectable.dispose()


def run_migrations_online() -> None:
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
