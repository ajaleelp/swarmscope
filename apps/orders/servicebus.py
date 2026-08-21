import json
from types import TracebackType

from azure.core.credentials_async import AsyncTokenCredential
from azure.identity.aio import DefaultAzureCredential
from azure.servicebus import ServiceBusMessage
from azure.servicebus.aio import ServiceBusClient, ServiceBusSender

from apps.orders.messaging import OutboundEvent

CONTENT_TYPE = "application/json"

# The outbox owns retry. It counts attempts, schedules the next one with
# exponential backoff, and survives a crash. The SDK's own retry loop has none of
# those properties, and because the publisher handles one event at a time an
# in-SDK retry stalls the whole worker instead of failing a single event. The
# defaults back off up to two minutes, which turns a rejected send into a
# throughput collapse that hides the rejection. Fail fast and let the outbox
# decide when to try again.
SDK_RETRY_TOTAL = 1
SDK_RETRY_BACKOFF_MAX_SECONDS = 2.0


def to_service_bus_message(event: OutboundEvent) -> ServiceBusMessage:
    """Render one outbound event as a Service Bus message.

    ``message_id`` carries the event id so the topic's duplicate detection
    discards a redelivery of the same event. The outbox deliberately allows a
    duplicate after a crash between sending and recording success; within the
    topic's detection window the broker removes it, and beyond that window the
    consumer must still be idempotent.

    The body is the complete event envelope rather than the payload alone, so a
    consumer can validate it against the published contract without depending on
    broker metadata.
    """
    body = json.dumps(
        {
            "event_id": str(event.event_id),
            "event_type": event.event_type,
            "event_version": event.event_version,
            "occurred_at": event.occurred_at.isoformat(),
            "correlation_id": str(event.correlation_id),
            "payload": dict(event.payload),
        },
        sort_keys=True,
        separators=(",", ":"),
    )

    return ServiceBusMessage(
        body,
        message_id=str(event.event_id),
        correlation_id=str(event.correlation_id),
        subject=event.event_type,
        content_type=CONTENT_TYPE,
        application_properties={
            "event_type": event.event_type,
            "event_version": event.event_version,
            "occurred_at": event.occurred_at.isoformat(),
        },
    )


class ServiceBusEventPublisher:
    """Publish outbound events to a Service Bus topic as a managed identity.

    Satisfies the EventPublisher protocol: ``publish`` returns on success and
    raises on failure, leaving the outbox worker to decide what to record.

    The client and sender are opened once and reused. Establishing an AMQP
    connection and acquiring a token for every message would dominate the cost
    of sending, so this is an async context manager rather than a plain callable.
    """

    def __init__(
        self,
        *,
        fully_qualified_namespace: str,
        topic_name: str,
        credential: AsyncTokenCredential | None = None,
    ) -> None:
        self._fully_qualified_namespace = fully_qualified_namespace
        self._topic_name = topic_name
        self._credential = credential
        self._owns_credential = credential is None
        self._client: ServiceBusClient | None = None
        self._sender: ServiceBusSender | None = None

    async def __aenter__(self) -> "ServiceBusEventPublisher":
        credential = self._credential or DefaultAzureCredential()
        self._credential = credential
        self._client = ServiceBusClient(
            fully_qualified_namespace=self._fully_qualified_namespace,
            credential=credential,
            retry_total=SDK_RETRY_TOTAL,
            retry_backoff_max=SDK_RETRY_BACKOFF_MAX_SECONDS,
        )
        self._sender = self._client.get_topic_sender(topic_name=self._topic_name)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._sender is not None:
            await self._sender.close()
            self._sender = None
        if self._client is not None:
            await self._client.close()
            self._client = None
        if self._owns_credential and self._credential is not None:
            await self._credential.close()
            self._credential = None

    async def publish(self, event: OutboundEvent) -> None:
        """Send one event to the topic, or raise."""
        if self._sender is None:
            raise RuntimeError(
                "publisher is not open; use 'async with ServiceBusEventPublisher(...)'"
            )
        await self._sender.send_messages(to_service_bus_message(event))
