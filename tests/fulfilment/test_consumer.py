import asyncio
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from apps.fulfilment.consumer import (
    ConsumerStats,
    Settlement,
    run_consumer_loop,
    settle_message,
)
from apps.fulfilment.models import Fulfilment, ProcessedEvent


@dataclass
class FakeMessage:
    """Stands in for a ServiceBusReceivedMessage."""

    body_text: str
    message_id: str = field(default_factory=lambda: str(uuid4()))
    delivery_count: int = 1

    @property
    def body(self) -> Any:
        return iter([self.body_text.encode()])


class FakeReceiver:
    """Serves prepared batches and records how each message was settled."""

    def __init__(
        self,
        batches: list[list[FakeMessage]] | None = None,
        *,
        shutdown: asyncio.Event | None = None,
        fail_receives: int = 0,
    ) -> None:
        self._batches = list(batches or [])
        self._shutdown = shutdown
        self._remaining_receive_failures = fail_receives
        self.completed: list[FakeMessage] = []
        self.abandoned: list[FakeMessage] = []
        self.dead_lettered: list[tuple[FakeMessage, str | None, str | None]] = []

    async def receive_messages(
        self,
        max_message_count: int | None = 1,
        max_wait_time: float | None = None,
    ) -> list[FakeMessage]:
        if self._remaining_receive_failures > 0:
            self._remaining_receive_failures -= 1
            raise RuntimeError("receive failed")
        if self._batches:
            return self._batches.pop(0)
        if self._shutdown is not None:
            self._shutdown.set()
        return []

    async def complete_message(self, message: FakeMessage) -> None:
        self.completed.append(message)

    async def abandon_message(self, message: FakeMessage) -> None:
        self.abandoned.append(message)

    async def dead_letter_message(
        self,
        message: FakeMessage,
        reason: str | None = None,
        error_description: str | None = None,
    ) -> None:
        self.dead_lettered.append((message, reason, error_description))


def an_envelope(
    *,
    order_id: UUID | None = None,
    event_id: UUID | None = None,
    quantity: int = 2,
) -> str:
    return json.dumps(
        {
            "event_id": str(event_id or uuid4()),
            "event_type": "OrderPlaced",
            "event_version": 1,
            "occurred_at": datetime.now(UTC).isoformat(),
            "correlation_id": str(uuid4()),
            "payload": {
                "order_id": str(order_id or uuid4()),
                "customer_id": "customer-1",
                "sku": "widget-blue",
                "quantity": quantity,
            },
        }
    )


def a_message(**kwargs: Any) -> FakeMessage:
    return FakeMessage(body_text=an_envelope(**kwargs))


# --- settling one message ---------------------------------------------------


@pytest.mark.asyncio
async def test_a_valid_message_is_fulfilled_and_completed(
    committed_sessions: async_sessionmaker[AsyncSession],
    clean_fulfilment_schema: None,
) -> None:
    receiver = FakeReceiver()
    message = a_message()

    settlement = await settle_message(
        message=message, receiver=receiver, session_factory=committed_sessions
    )

    async with committed_sessions() as session:
        fulfilments = await session.scalar(select(func.count()).select_from(Fulfilment))

    assert settlement is Settlement.COMPLETED
    assert receiver.completed == [message]
    assert receiver.dead_lettered == []
    assert receiver.abandoned == []
    assert fulfilments == 1


@pytest.mark.asyncio
async def test_a_redelivered_message_is_completed_without_new_work(
    committed_sessions: async_sessionmaker[AsyncSession],
    clean_fulfilment_schema: None,
) -> None:
    """A duplicate is the normal consequence of at-least-once, not an error."""
    receiver = FakeReceiver()
    event_id, order_id = uuid4(), uuid4()

    for _ in range(2):
        settlement = await settle_message(
            message=a_message(event_id=event_id, order_id=order_id),
            receiver=receiver,
            session_factory=committed_sessions,
        )
        assert settlement is Settlement.COMPLETED

    async with committed_sessions() as session:
        fulfilments = await session.scalar(select(func.count()).select_from(Fulfilment))

    assert len(receiver.completed) == 2
    assert fulfilments == 1


@pytest.mark.asyncio
async def test_an_unparseable_message_is_dead_lettered_immediately(
    committed_sessions: async_sessionmaker[AsyncSession],
    clean_fulfilment_schema: None,
) -> None:
    """Retrying malformed JSON five times reaches the same place, slower."""
    receiver = FakeReceiver()
    message = FakeMessage(body_text="{not json at all")

    settlement = await settle_message(
        message=message, receiver=receiver, session_factory=committed_sessions
    )

    async with committed_sessions() as session:
        processed = await session.scalar(select(func.count()).select_from(ProcessedEvent))

    assert settlement is Settlement.DEAD_LETTERED
    assert receiver.abandoned == [], "a permanent failure must not be retried"
    assert receiver.completed == []
    assert len(receiver.dead_lettered) == 1
    settled, reason, description = receiver.dead_lettered[0]
    assert settled is message
    assert reason == "InvalidEnvelope"
    assert description
    assert processed == 0


@pytest.mark.asyncio
async def test_a_message_failing_the_contract_is_dead_lettered(
    committed_sessions: async_sessionmaker[AsyncSession],
    clean_fulfilment_schema: None,
) -> None:
    """Valid JSON that violates the published contract is still permanent."""
    receiver = FakeReceiver()

    settlement = await settle_message(
        message=FakeMessage(body_text=an_envelope(quantity=0)),
        receiver=receiver,
        session_factory=committed_sessions,
    )

    assert settlement is Settlement.DEAD_LETTERED
    assert receiver.dead_lettered[0][1] == "InvalidEnvelope"


@pytest.mark.asyncio
async def test_an_unexpected_failure_abandons_for_redelivery(
    committed_sessions: async_sessionmaker[AsyncSession],
    clean_fulfilment_schema: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A possibly transient failure must be retried, not discarded."""

    async def explode(**_: Any) -> None:
        raise RuntimeError("database unreachable")

    monkeypatch.setattr("apps.fulfilment.consumer.fulfil_order", explode)

    receiver = FakeReceiver()
    message = a_message()

    settlement = await settle_message(
        message=message, receiver=receiver, session_factory=committed_sessions
    )

    assert settlement is Settlement.ABANDONED
    assert receiver.abandoned == [message]
    assert receiver.completed == []
    assert receiver.dead_lettered == []


# --- the loop ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_loop_settles_every_message_it_receives(
    committed_sessions: async_sessionmaker[AsyncSession],
    clean_fulfilment_schema: None,
) -> None:
    shutdown = asyncio.Event()
    batch = [a_message(), a_message(), a_message()]
    receiver = FakeReceiver([batch], shutdown=shutdown)

    stats = await asyncio.wait_for(
        run_consumer_loop(
            receiver=receiver,
            session_factory=committed_sessions,
            shutdown=shutdown,
            batch_size=3,
        ),
        timeout=10,
    )

    async with committed_sessions() as session:
        fulfilments = await session.scalar(select(func.count()).select_from(Fulfilment))

    assert stats.fulfilled == 3
    assert len(receiver.completed) == 3
    assert fulfilments == 3


@pytest.mark.asyncio
async def test_loop_stops_when_shutdown_is_requested(
    committed_sessions: async_sessionmaker[AsyncSession],
    clean_fulfilment_schema: None,
) -> None:
    shutdown = asyncio.Event()
    shutdown.set()
    receiver = FakeReceiver([[a_message()]])

    stats = await run_consumer_loop(
        receiver=receiver, session_factory=committed_sessions, shutdown=shutdown
    )

    assert stats == ConsumerStats()
    assert receiver.completed == []


@pytest.mark.asyncio
async def test_receive_failure_does_not_stop_the_loop(
    committed_sessions: async_sessionmaker[AsyncSession],
    clean_fulfilment_schema: None,
) -> None:
    """A broker hiccup must not end the process."""
    shutdown = asyncio.Event()
    receiver = FakeReceiver([[a_message()]], shutdown=shutdown, fail_receives=2)

    stats = await asyncio.wait_for(
        run_consumer_loop(
            receiver=receiver,
            session_factory=committed_sessions,
            shutdown=shutdown,
            error_backoff=timedelta(milliseconds=5),
        ),
        timeout=10,
    )

    assert stats.errors == 2
    assert stats.fulfilled == 1
    assert len(receiver.completed) == 1
