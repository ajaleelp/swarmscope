import asyncio
import logging
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from typing import Any, Protocol

from azure.identity.aio import DefaultAzureCredential
from azure.servicebus.aio import ServiceBusClient
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from apps.fulfilment.service import fulfil_order
from apps.orders.config import get_settings
from apps.orders.database import close_database, session_factory
from packages.contracts.order_placed import OrderPlacedV1
from packages.runtime import install_shutdown_handlers, sleep_unless_shutdown

logger = logging.getLogger(__name__)

DEFAULT_RECEIVE_WAIT = timedelta(seconds=5)
DEFAULT_ERROR_BACKOFF = timedelta(seconds=5)
DEFAULT_BATCH_SIZE = 1
MAX_DESCRIPTION = 4000


class MessageReceiver(Protocol):
    """The part of the Service Bus receiver this loop uses.

    Declared so the loop can be tested without a broker. The SDK receiver
    satisfies it structurally; this is a testing seam rather than an attempt at
    transport neutrality, since settlement semantics are Service Bus specific.
    """

    async def receive_messages(
        self,
        max_message_count: int | None = 1,
        max_wait_time: float | None = None,
    ) -> list[Any]: ...

    async def complete_message(self, message: Any) -> None: ...

    async def abandon_message(self, message: Any) -> None: ...

    async def dead_letter_message(
        self,
        message: Any,
        reason: str | None = None,
        error_description: str | None = None,
    ) -> None: ...


class Settlement(StrEnum):
    """How one delivery was settled with the broker."""

    COMPLETED = "completed"
    DEAD_LETTERED = "dead_lettered"
    ABANDONED = "abandoned"


@dataclass
class ConsumerStats:
    """What one run of the loop accomplished."""

    fulfilled: int = 0
    dead_lettered: int = 0
    abandoned: int = 0
    idle: int = 0
    errors: int = 0


def read_body(message: Any) -> str:
    """Join the message body, which the SDK exposes as byte chunks."""
    body = message.body
    if isinstance(body, bytes):
        return body.decode()
    if isinstance(body, str):
        return body
    return b"".join(body).decode()


async def settle_message(
    *,
    message: Any,
    receiver: MessageReceiver,
    session_factory: async_sessionmaker[AsyncSession],
) -> Settlement:
    """Handle one delivery and tell the broker what became of it."""
    try:
        event = OrderPlacedV1.model_validate_json(read_body(message))
    except ValidationError as error:
        # Permanent: no number of redeliveries makes an invalid envelope valid,
        # so dead-letter now rather than spending the delivery budget to reach
        # the same conclusion five lock timeouts later.
        logger.warning(
            "dead-lettering unparseable message",
            extra={"message_id": message.message_id},
        )
        await receiver.dead_letter_message(
            message,
            reason="InvalidEnvelope",
            error_description=str(error)[:MAX_DESCRIPTION],
        )
        return Settlement.DEAD_LETTERED

    try:
        async with session_factory() as session:
            outcome = await fulfil_order(event=event, session=session)
    except Exception:
        # Possibly transient. Abandoning returns it to the subscription; Azure
        # dead-letters it once the configured delivery count is exhausted.
        logger.exception(
            "fulfilment failed; abandoning for redelivery",
            extra={
                "event_id": str(event.event_id),
                "correlation_id": str(event.correlation_id),
                "delivery_count": getattr(message, "delivery_count", None),
            },
        )
        await receiver.abandon_message(message)
        return Settlement.ABANDONED

    await receiver.complete_message(message)
    logger.info(
        "settled event",
        extra={
            "event_id": str(event.event_id),
            "correlation_id": str(event.correlation_id),
            "outcome": outcome.value,
        },
    )
    return Settlement.COMPLETED


async def run_consumer_loop(
    *,
    receiver: MessageReceiver,
    session_factory: async_sessionmaker[AsyncSession],
    shutdown: asyncio.Event,
    batch_size: int = DEFAULT_BATCH_SIZE,
    receive_wait: timedelta = DEFAULT_RECEIVE_WAIT,
    error_backoff: timedelta = DEFAULT_ERROR_BACKOFF,
) -> ConsumerStats:
    """Consume order events until shutdown is requested.

    There is no idle sleep: receive_messages holds a long poll open for
    receive_wait, so an empty subscription costs one blocked call rather than a
    spin. A batch already received is settled in full even once shutdown
    arrives, because every message in it is holding a broker lock.
    """
    stats = ConsumerStats()

    while not shutdown.is_set():
        try:
            messages = await receiver.receive_messages(
                max_message_count=batch_size,
                max_wait_time=receive_wait.total_seconds(),
            )
        except Exception:
            stats.errors += 1
            logger.exception("receive failed; continuing")
            await sleep_unless_shutdown(shutdown, error_backoff)
            continue

        if not messages:
            stats.idle += 1
            continue

        for message in messages:
            settlement = await settle_message(
                message=message,
                receiver=receiver,
                session_factory=session_factory,
            )
            if settlement is Settlement.DEAD_LETTERED:
                stats.dead_lettered += 1
            elif settlement is Settlement.ABANDONED:
                stats.abandoned += 1
            else:
                stats.fulfilled += 1

    return stats


async def main() -> None:
    """Consume the fulfilment subscription until told to stop."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    settings = get_settings()
    shutdown = asyncio.Event()
    install_shutdown_handlers(shutdown)

    logger.info(
        "consumer starting",
        extra={
            "topic": settings.service_bus_topic,
            "subscription": settings.service_bus_subscription,
        },
    )

    try:
        async with DefaultAzureCredential() as credential:
            async with ServiceBusClient(
                fully_qualified_namespace=settings.service_bus_fully_qualified_namespace,
                credential=credential,
            ) as client:
                receiver = client.get_subscription_receiver(
                    topic_name=settings.service_bus_topic,
                    subscription_name=settings.service_bus_subscription,
                )
                async with receiver:
                    stats = await run_consumer_loop(
                        receiver=receiver,
                        session_factory=session_factory,
                        shutdown=shutdown,
                    )
    finally:
        await close_database()

    logger.info("consumer stopped", extra=vars(stats))


if __name__ == "__main__":
    asyncio.run(main())
