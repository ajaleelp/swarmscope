import asyncio
from datetime import timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from apps.orders.messaging import OutboundEvent
from apps.orders.models import OutboxEvent
from apps.orders.publisher_worker import LoopStats, run_publisher_loop
from packages.runtime import sleep_unless_shutdown


class ControllablePublisher:
    """A fake publisher the test can pace and observe."""

    def __init__(
        self,
        *,
        failures: int = 0,
        stop_after: int | None = None,
        shutdown: asyncio.Event | None = None,
        started: asyncio.Event | None = None,
        hold: asyncio.Event | None = None,
    ) -> None:
        self.attempted: list[OutboundEvent] = []
        self.delivered: list[OutboundEvent] = []
        self._remaining_failures = failures
        self._stop_after = stop_after
        self._shutdown = shutdown
        self._started = started
        self._hold = hold

    async def publish(self, event: OutboundEvent) -> None:
        self.attempted.append(event)
        if self._started is not None:
            self._started.set()
        if self._hold is not None:
            await self._hold.wait()
        if self._remaining_failures > 0:
            self._remaining_failures -= 1
            raise RuntimeError("broker unavailable")
        self.delivered.append(event)
        if (
            self._stop_after is not None
            and self._shutdown is not None
            and len(self.delivered) >= self._stop_after
        ):
            self._shutdown.set()


async def published_count(sessions: async_sessionmaker[AsyncSession]) -> int:
    async with sessions() as session:
        rows = await session.scalars(
            select(OutboxEvent).where(OutboxEvent.published_at.is_not(None))
        )
    return len(list(rows))


# --- draining the outbox ----------------------------------------------------


@pytest.mark.asyncio
async def test_loop_publishes_every_due_event(
    committed_sessions: async_sessionmaker[AsyncSession],
    seed_event,
) -> None:
    for _ in range(3):
        await seed_event()

    shutdown = asyncio.Event()
    publisher = ControllablePublisher(stop_after=3, shutdown=shutdown)

    stats = await asyncio.wait_for(
        run_publisher_loop(
            session_factory=committed_sessions,
            publisher=publisher,
            shutdown=shutdown,
            idle_sleep=timedelta(milliseconds=10),
        ),
        timeout=10,
    )

    assert stats.published == 3
    assert len(publisher.delivered) == 3
    assert await published_count(committed_sessions) == 3


@pytest.mark.asyncio
async def test_loop_goes_idle_when_the_outbox_is_empty(
    committed_sessions: async_sessionmaker[AsyncSession],
    clean_orders_schema: None,
) -> None:
    shutdown = asyncio.Event()
    publisher = ControllablePublisher()

    async def stop_soon() -> None:
        await asyncio.sleep(0.05)
        shutdown.set()

    _, stats = await asyncio.gather(
        stop_soon(),
        run_publisher_loop(
            session_factory=committed_sessions,
            publisher=publisher,
            shutdown=shutdown,
            idle_sleep=timedelta(milliseconds=5),
        ),
    )

    assert stats.idle > 0
    assert stats.published == 0
    assert publisher.attempted == []


# --- shutdown ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_shutdown_before_starting_does_no_work(
    committed_sessions: async_sessionmaker[AsyncSession],
    seed_event,
) -> None:
    await seed_event()
    shutdown = asyncio.Event()
    shutdown.set()
    publisher = ControllablePublisher()

    stats = await run_publisher_loop(
        session_factory=committed_sessions,
        publisher=publisher,
        shutdown=shutdown,
    )

    assert stats == LoopStats()
    assert publisher.attempted == []


@pytest.mark.asyncio
async def test_shutdown_interrupts_the_idle_sleep_immediately() -> None:
    """A long idle interval must not delay termination."""
    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    started = loop.time()
    loop.call_later(0.05, shutdown.set)

    await sleep_unless_shutdown(shutdown, timedelta(seconds=30))

    assert loop.time() - started < 5, "waited out the sleep instead of waking"


@pytest.mark.asyncio
async def test_event_already_being_sent_is_seen_through_to_completion(
    committed_sessions: async_sessionmaker[AsyncSession],
    seed_event,
) -> None:
    """Shutdown mid-send must not abandon the event unrecorded.

    Stopping between a successful send and recording it would produce exactly
    the duplicate the outbox exists to avoid.
    """
    event_id = await seed_event()
    shutdown = asyncio.Event()
    started = asyncio.Event()
    hold = asyncio.Event()
    publisher = ControllablePublisher(started=started, hold=hold)

    async def stop_mid_send() -> None:
        await started.wait()
        shutdown.set()
        hold.set()

    _, stats = await asyncio.gather(
        stop_mid_send(),
        asyncio.wait_for(
            run_publisher_loop(
                session_factory=committed_sessions,
                publisher=publisher,
                shutdown=shutdown,
            ),
            timeout=10,
        ),
    )

    async with committed_sessions() as session:
        stored = await session.scalar(
            select(OutboxEvent).where(OutboxEvent.event_id == event_id)
        )

    assert stats.published == 1
    assert stored is not None
    assert stored.published_at is not None
    assert stored.lease_owner is None


# --- resilience -------------------------------------------------------------


@pytest.mark.asyncio
async def test_broker_failure_does_not_stop_the_loop(
    committed_sessions: async_sessionmaker[AsyncSession],
    seed_event,
) -> None:
    await seed_event()
    shutdown = asyncio.Event()
    publisher = ControllablePublisher(failures=1, stop_after=1, shutdown=shutdown)

    stats = await asyncio.wait_for(
        run_publisher_loop(
            session_factory=committed_sessions,
            publisher=publisher,
            shutdown=shutdown,
            idle_sleep=timedelta(milliseconds=5),
            error_backoff=timedelta(milliseconds=5),
        ),
        timeout=10,
    )

    assert stats.failed == 1
    assert stats.published == 1
    assert len(publisher.attempted) == 2


@pytest.mark.asyncio
async def test_loop_pauses_after_a_failure_rather_than_hammering_the_broker(
    committed_sessions: async_sessionmaker[AsyncSession],
    seed_event,
) -> None:
    """A broker outage must not burn every event's retry budget at once."""
    for _ in range(5):
        await seed_event()

    shutdown = asyncio.Event()
    publisher = ControllablePublisher(failures=99)
    asyncio.get_running_loop().call_later(0.3, shutdown.set)

    stats = await asyncio.wait_for(
        run_publisher_loop(
            session_factory=committed_sessions,
            publisher=publisher,
            shutdown=shutdown,
            error_backoff=timedelta(seconds=30),
        ),
        timeout=10,
    )

    assert stats.failed == 1
    assert len(publisher.attempted) == 1, (
        f"looped without pausing: {len(publisher.attempted)} attempts"
    )


@pytest.mark.asyncio
async def test_unexpected_error_does_not_kill_the_loop(
    committed_sessions: async_sessionmaker[AsyncSession],
    seed_event,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A bug in the worker must not take the broker connection down with it."""
    await seed_event()
    shutdown = asyncio.Event()
    calls = 0

    async def explode(self) -> None:
        nonlocal calls
        calls += 1
        if calls >= 3:
            shutdown.set()
        raise ValueError("unexpected")

    monkeypatch.setattr("apps.orders.outbox.OutboxPublisher.run_once", explode)

    stats = await asyncio.wait_for(
        run_publisher_loop(
            session_factory=committed_sessions,
            publisher=ControllablePublisher(),
            shutdown=shutdown,
            error_backoff=timedelta(milliseconds=5),
        ),
        timeout=10,
    )

    assert stats.errors >= 3
    assert stats.published == 0
