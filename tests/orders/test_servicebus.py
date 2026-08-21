import inspect
import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest

import apps.orders.servicebus as servicebus_module
from apps.orders.messaging import EventPublisher, OutboundEvent
from apps.orders.servicebus import (
    CONTENT_TYPE,
    SDK_RETRY_BACKOFF_MAX_SECONDS,
    SDK_RETRY_TOTAL,
    ServiceBusEventPublisher,
    to_service_bus_message,
)


def sample_event() -> OutboundEvent:
    return OutboundEvent(
        event_id=uuid4(),
        event_type="OrderPlaced",
        event_version=1,
        occurred_at=datetime(2026, 8, 19, 10, 30, tzinfo=UTC),
        correlation_id=uuid4(),
        payload={"order_id": str(uuid4()), "sku": "widget-blue", "quantity": 2},
    )


def read_body(message) -> str:
    """The SDK exposes body as a one-shot generator of byte chunks."""
    return b"".join(message.body).decode()


# --- the mapping: pure, no Azure contact ------------------------------------


def test_message_id_is_the_event_id_so_the_broker_can_deduplicate() -> None:
    event = sample_event()
    assert to_service_bus_message(event).message_id == str(event.event_id)


def test_envelope_and_payload_survive_the_round_trip() -> None:
    event = sample_event()

    assert json.loads(read_body(to_service_bus_message(event))) == {
        "event_id": str(event.event_id),
        "event_type": "OrderPlaced",
        "event_version": 1,
        "occurred_at": "2026-08-19T10:30:00+00:00",
        "correlation_id": str(event.correlation_id),
        "payload": dict(event.payload),
    }


def test_routing_metadata_is_set() -> None:
    event = sample_event()
    message = to_service_bus_message(event)

    assert message.correlation_id == str(event.correlation_id)
    assert message.subject == "OrderPlaced"
    assert message.content_type == CONTENT_TYPE
    assert message.application_properties == {
        "event_type": "OrderPlaced",
        "event_version": 1,
        "occurred_at": "2026-08-19T10:30:00+00:00",
    }


def test_application_properties_contain_only_primitives() -> None:
    """Service Bus rejects arbitrary objects in application properties."""
    properties = to_service_bus_message(sample_event()).application_properties
    for value in properties.values():
        assert isinstance(value, (str, bytes, int, float, bool))


def test_two_events_get_different_message_ids() -> None:
    assert (
        to_service_bus_message(sample_event()).message_id
        != to_service_bus_message(sample_event()).message_id
    )


# --- the protocol and lifecycle ---------------------------------------------


def test_publisher_satisfies_the_event_publisher_protocol() -> None:
    """Compare the implementation's signature against the protocol's.

    isinstance cannot be used here: EventPublisher is a structural protocol and
    is deliberately not @runtime_checkable. A runtime protocol check would only
    compare method names anyway, whereas comparing signatures catches a drifting
    parameter name or annotation.
    """
    assert inspect.iscoroutinefunction(ServiceBusEventPublisher.publish)
    assert inspect.signature(ServiceBusEventPublisher.publish) == inspect.signature(
        EventPublisher.publish
    )


@pytest.mark.asyncio
async def test_publishing_before_opening_raises_rather_than_silently_failing() -> None:
    publisher = ServiceBusEventPublisher(
        fully_qualified_namespace="example.servicebus.windows.net",
        topic_name="orders",
    )
    with pytest.raises(RuntimeError, match="not open"):
        await publisher.publish(sample_event())


@pytest.mark.asyncio
async def test_the_sdk_does_not_retry_underneath_the_outbox(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A rejected send must surface in seconds rather than after the SDK's backoff.

    The outbox already retries, durably and with its attempts recorded. The SDK
    retrying as well is a second, invisible loop, and because the publisher
    handles one event at a time a long in-SDK retry stalls the whole worker
    instead of failing the single event it belongs to. The SDK default backs off
    up to two minutes, which turns a rejected send into a throughput collapse
    that hides the rejection entirely.
    """
    recorded: dict[str, object] = {}

    class RecordingSender:
        async def close(self) -> None: ...

    class RecordingClient:
        def __init__(self, **kwargs: object) -> None:
            recorded.update(kwargs)

        def get_topic_sender(self, topic_name: str) -> RecordingSender:
            return RecordingSender()

        async def close(self) -> None: ...

    class StubCredential:
        async def close(self) -> None: ...

    monkeypatch.setattr(servicebus_module, "ServiceBusClient", RecordingClient)

    async with ServiceBusEventPublisher(
        fully_qualified_namespace="example.servicebus.windows.net",
        topic_name="orders",
        credential=StubCredential(),
    ):
        pass

    assert recorded["retry_total"] == SDK_RETRY_TOTAL
    assert recorded["retry_backoff_max"] == SDK_RETRY_BACKOFF_MAX_SECONDS
    assert SDK_RETRY_BACKOFF_MAX_SECONDS <= 5, (
        "a rejected send must hand back to the outbox quickly"
    )


# --- against real Azure: excluded by default --------------------------------


@pytest.mark.cloud
@pytest.mark.asyncio
async def test_event_reaches_the_fulfilment_subscription() -> None:
    """Send through the real topic and read it back off the real subscription."""
    from azure.identity.aio import DefaultAzureCredential
    from azure.servicebus.aio import ServiceBusClient

    from apps.orders.config import get_settings

    settings = get_settings()
    event = sample_event()

    async with DefaultAzureCredential() as credential:
        async with ServiceBusEventPublisher(
            fully_qualified_namespace=settings.service_bus_fully_qualified_namespace,
            topic_name=settings.service_bus_topic,
            credential=credential,
        ) as publisher:
            await publisher.publish(event)

        async with ServiceBusClient(
            fully_qualified_namespace=settings.service_bus_fully_qualified_namespace,
            credential=credential,
        ) as client:
            receiver = client.get_subscription_receiver(
                topic_name=settings.service_bus_topic,
                subscription_name=settings.service_bus_subscription,
                max_wait_time=30,
            )
            async with receiver:
                bodies = {}
                async for message in receiver:
                    bodies[message.message_id] = read_body(message)
                    await receiver.complete_message(message)

    assert str(event.event_id) in bodies, (
        f"event did not arrive; saw message ids {sorted(bodies)}"
    )
    assert json.loads(bodies[str(event.event_id)])["event_id"] == str(event.event_id)
