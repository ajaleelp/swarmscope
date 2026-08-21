import asyncio
import logging
from dataclasses import dataclass
from datetime import timedelta
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from apps.orders.config import get_settings
from apps.orders.database import close_database, session_factory
from apps.orders.messaging import EventPublisher
from apps.orders.outbox import OutboxPublisher, PublishResult
from apps.orders.servicebus import ServiceBusEventPublisher
from packages.observability import configure_logging
from packages.runtime import install_shutdown_handlers, sleep_unless_shutdown

logger = logging.getLogger(__name__)

DEFAULT_IDLE_SLEEP = timedelta(seconds=1)
DEFAULT_ERROR_BACKOFF = timedelta(seconds=5)


@dataclass
class LoopStats:
    """What one run of the loop accomplished."""

    published: int = 0
    failed: int = 0
    idle: int = 0
    errors: int = 0


async def run_publisher_loop(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    publisher: EventPublisher,
    shutdown: asyncio.Event,
    owner: UUID | None = None,
    idle_sleep: timedelta = DEFAULT_IDLE_SLEEP,
    error_backoff: timedelta = DEFAULT_ERROR_BACKOFF,
) -> LoopStats:
    """Publish due outbox events until shutdown is requested.

    Shutdown is only checked between events, so an event already being sent is
    always seen through to its recorded outcome. Interrupting between a
    successful send and recording it would manufacture the very duplicate the
    outbox works to avoid.
    """
    worker = OutboxPublisher(
        session_factory=session_factory,
        publisher=publisher,
        owner=owner,
    )
    stats = LoopStats()

    while not shutdown.is_set():
        try:
            outcome = await worker.run_once()
        except Exception:
            # A publish failure is already handled inside run_once. Reaching
            # here means something unexpected broke, and killing the process
            # would drop the broker connection with it.
            stats.errors += 1
            logger.exception("outbox worker raised; continuing")
            await sleep_unless_shutdown(shutdown, error_backoff)
            continue

        if outcome.result is PublishResult.PUBLISHED:
            stats.published += 1
            logger.info(
                "published event",
                extra={"event_id": str(outcome.event_id), "attempts": outcome.attempts},
            )
            continue

        if outcome.result is PublishResult.FAILED:
            stats.failed += 1
            logger.warning(
                "publish failed; event rescheduled",
                extra={
                    "event_id": str(outcome.event_id),
                    "attempts": outcome.attempts,
                    "error": outcome.error,
                },
            )
            # Pause rather than racing to the next event. If the broker is
            # down, every due event would otherwise fail in quick succession
            # and burn through its retry budget.
            await sleep_unless_shutdown(shutdown, error_backoff)
            continue

        stats.idle += 1
        await sleep_unless_shutdown(shutdown, idle_sleep)

    return stats


async def main() -> None:
    """Run the publisher against Service Bus until told to stop.

    The broker connection is opened once and reused for every event. Note there
    is no reconnection logic: if the connection fails permanently, publishing
    keeps failing and the process must be restarted. A liveness probe is the
    intended answer once this runs under Kubernetes.
    """
    configure_logging()
    settings = get_settings()
    shutdown = asyncio.Event()
    install_shutdown_handlers(shutdown)

    logger.info(
        "publisher starting",
        extra={
            "namespace": settings.service_bus_fully_qualified_namespace,
            "topic": settings.service_bus_topic,
        },
    )

    try:
        async with ServiceBusEventPublisher(
            fully_qualified_namespace=settings.service_bus_fully_qualified_namespace,
            topic_name=settings.service_bus_topic,
        ) as publisher:
            stats = await run_publisher_loop(
                session_factory=session_factory,
                publisher=publisher,
                shutdown=shutdown,
            )
    finally:
        await close_database()

    logger.info(
        "publisher stopped",
        extra={
            "published": stats.published,
            "failed": stats.failed,
            "errors": stats.errors,
        },
    )


if __name__ == "__main__":
    asyncio.run(main())
