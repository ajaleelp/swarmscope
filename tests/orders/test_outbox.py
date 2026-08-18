import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from apps.orders.messaging import OutboundEvent
from apps.orders.models import OutboxEvent
from apps.orders.outbox import (
    DEFAULT_MAX_BACKOFF,
    OutboxPublisher,
    PublishResult,
    backoff_delay,
    due_event_statement,
)


class FakePublisher:
    """In-memory EventPublisher used only by tests."""

    def __init__(self, *, failures: int = 0) -> None:
        self.attempted: list[OutboundEvent] = []
        self.delivered: list[OutboundEvent] = []
        self._remaining_failures = failures

    async def publish(self, event: OutboundEvent) -> None:
        self.attempted.append(event)
        if self._remaining_failures > 0:
            self._remaining_failures -= 1
            raise RuntimeError("broker unavailable")
        self.delivered.append(event)


async def load_event(
    sessions: async_sessionmaker[AsyncSession],
    event_id: UUID,
) -> OutboxEvent:
    async with sessions() as session:
        event = await session.scalar(
            select(OutboxEvent).where(OutboxEvent.event_id == event_id)
        )
    assert event is not None
    return event


# --- successful publication -------------------------------------------------


@pytest.mark.asyncio
async def test_run_once_publishes_due_event_and_marks_it_published(
    committed_sessions: async_sessionmaker[AsyncSession],
    seed_event,
) -> None:
    event_id = await seed_event()
    publisher = FakePublisher()
    worker = OutboxPublisher(
        session_factory=committed_sessions,
        publisher=publisher,
        owner=uuid4(),
    )

    outcome = await worker.run_once()
    stored = await load_event(committed_sessions, event_id)

    assert outcome.result is PublishResult.PUBLISHED
    assert outcome.event_id == event_id
    assert outcome.attempts == 1

    assert len(publisher.delivered) == 1
    assert publisher.delivered[0].event_id == event_id
    assert publisher.delivered[0].event_type == "OrderPlaced"

    assert stored.published_at is not None
    assert stored.publish_attempts == 1
    assert stored.last_attempt_at is not None
    assert stored.lease_owner is None
    assert stored.lease_expires_at is None
    assert stored.last_publish_error is None


@pytest.mark.asyncio
async def test_run_once_is_idle_when_nothing_is_due(
    committed_sessions: async_sessionmaker[AsyncSession],
    seed_event,
) -> None:
    await seed_event(next_attempt_at=datetime.now(UTC) + timedelta(hours=1))
    publisher = FakePublisher()
    worker = OutboxPublisher(
        session_factory=committed_sessions,
        publisher=publisher,
        owner=uuid4(),
    )

    outcome = await worker.run_once()

    assert outcome.result is PublishResult.IDLE
    assert publisher.attempted == []


# --- broker failure and retry scheduling ------------------------------------


def test_backoff_delay_doubles_and_is_capped() -> None:
    base = timedelta(seconds=1)
    maximum = timedelta(seconds=8)

    assert backoff_delay(1, base=base, maximum=maximum) == timedelta(seconds=1)
    assert backoff_delay(2, base=base, maximum=maximum) == timedelta(seconds=2)
    assert backoff_delay(3, base=base, maximum=maximum) == timedelta(seconds=4)
    assert backoff_delay(4, base=base, maximum=maximum) == timedelta(seconds=8)
    assert backoff_delay(99, base=base, maximum=maximum) == timedelta(seconds=8)
    assert backoff_delay(50) == DEFAULT_MAX_BACKOFF

    with pytest.raises(ValueError, match="at least 1"):
        backoff_delay(0)


@pytest.mark.asyncio
async def test_broker_failure_releases_lease_and_schedules_backoff(
    committed_sessions: async_sessionmaker[AsyncSession],
    seed_event,
) -> None:
    event_id = await seed_event()
    publisher = FakePublisher(failures=1)
    worker = OutboxPublisher(
        session_factory=committed_sessions,
        publisher=publisher,
        owner=uuid4(),
        base_backoff=timedelta(seconds=30),
    )

    outcome = await worker.run_once()
    stored = await load_event(committed_sessions, event_id)

    assert outcome.result is PublishResult.FAILED
    assert outcome.attempts == 1
    assert publisher.delivered == []

    assert stored.published_at is None
    assert stored.publish_attempts == 1
    assert stored.lease_owner is None
    assert stored.lease_expires_at is None
    assert stored.last_publish_error == "broker unavailable"

    assert stored.last_attempt_at is not None
    scheduled_gap = stored.next_attempt_at - stored.last_attempt_at
    assert timedelta(seconds=30) <= scheduled_gap <= timedelta(seconds=35)


@pytest.mark.asyncio
async def test_retry_reuses_the_same_event_id_and_succeeds(
    committed_sessions: async_sessionmaker[AsyncSession],
    seed_event,
) -> None:
    event_id = await seed_event()
    publisher = FakePublisher(failures=1)
    worker = OutboxPublisher(
        session_factory=committed_sessions,
        publisher=publisher,
        owner=uuid4(),
        base_backoff=timedelta(0),
    )

    first = await worker.run_once()
    second = await worker.run_once()
    stored = await load_event(committed_sessions, event_id)

    assert first.result is PublishResult.FAILED
    assert second.result is PublishResult.PUBLISHED
    assert [event.event_id for event in publisher.attempted] == [event_id, event_id]
    assert stored.publish_attempts == 2
    assert stored.published_at is not None


# --- safe claims by multiple workers ----------------------------------------


@pytest.mark.asyncio
async def test_claim_statement_skips_rows_locked_by_another_transaction(
    committed_sessions: async_sessionmaker[AsyncSession],
    seed_event,
) -> None:
    """Overlapping transactions must take different rows.

    One transaction holds a real row lock while a second runs the same
    statement. Without SKIP LOCKED the second call blocks and this fails on
    the timeout; without any row lock both calls return the same row.
    """
    older = await seed_event(occurred_at=datetime.now(UTC) - timedelta(minutes=2))
    newer = await seed_event(occurred_at=datetime.now(UTC) - timedelta(minutes=1))

    async with committed_sessions() as first, committed_sessions() as second:
        await first.begin()
        await second.begin()
        now = datetime.now(UTC)

        try:
            first_claim = await first.scalar(due_event_statement(now))
            second_claim = await asyncio.wait_for(
                second.scalar(due_event_statement(now)),
                timeout=5,
            )
            assert first_claim is not None
            assert second_claim is not None
            claimed = {first_claim.event_id, second_claim.event_id}
        finally:
            await first.rollback()
            await second.rollback()

    assert claimed == {older, newer}


@pytest.mark.asyncio
async def test_concurrent_workers_never_claim_the_same_event(
    committed_sessions: async_sessionmaker[AsyncSession],
    seed_event,
) -> None:
    """Two workers running together deliver each event exactly once.

    This covers the end-to-end path. It does not force the two claims to
    overlap; the statement-level test above does that.
    """
    first_event = await seed_event(occurred_at=datetime.now(UTC) - timedelta(minutes=2))
    second_event = await seed_event(occurred_at=datetime.now(UTC) - timedelta(minutes=1))

    publishers = [FakePublisher(), FakePublisher()]
    workers = [
        OutboxPublisher(
            session_factory=committed_sessions,
            publisher=publisher,
            owner=uuid4(),
        )
        for publisher in publishers
    ]

    outcomes = await asyncio.gather(*(worker.run_once() for worker in workers))

    published = {
        outcome.event_id
        for outcome in outcomes
        if outcome.result is PublishResult.PUBLISHED
    }
    delivered = [
        event.event_id for publisher in publishers for event in publisher.delivered
    ]

    assert published == {first_event, second_event}
    assert sorted(delivered) == sorted([first_event, second_event])
    assert len(delivered) == len(set(delivered))


@pytest.mark.asyncio
async def test_event_leased_by_another_worker_is_skipped(
    committed_sessions: async_sessionmaker[AsyncSession],
    seed_event,
) -> None:
    await seed_event(
        lease_owner=uuid4(),
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
    )
    publisher = FakePublisher()
    worker = OutboxPublisher(
        session_factory=committed_sessions,
        publisher=publisher,
        owner=uuid4(),
    )

    outcome = await worker.run_once()

    assert outcome.result is PublishResult.IDLE
    assert publisher.attempted == []


@pytest.mark.asyncio
async def test_expired_lease_is_reclaimed(
    committed_sessions: async_sessionmaker[AsyncSession],
    seed_event,
) -> None:
    event_id = await seed_event(
        lease_owner=uuid4(),
        lease_expires_at=datetime.now(UTC) - timedelta(minutes=5),
        publish_attempts=1,
    )
    publisher = FakePublisher()
    worker = OutboxPublisher(
        session_factory=committed_sessions,
        publisher=publisher,
        owner=uuid4(),
    )

    outcome = await worker.run_once()
    stored = await load_event(committed_sessions, event_id)

    assert outcome.result is PublishResult.PUBLISHED
    assert stored.publish_attempts == 2
    assert stored.published_at is not None


# --- the deliberate duplicate window ----------------------------------------


@pytest.mark.asyncio
async def test_crash_after_send_redelivers_the_same_event_id(
    committed_sessions: async_sessionmaker[AsyncSession],
    seed_event,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A crash between a successful send and recording it duplicates delivery.

    This is the accepted cost of never losing an event. The duplicate carries
    the same event_id so a consumer can discard it.
    """
    event_id = await seed_event()
    publisher = FakePublisher()
    crashing_worker = OutboxPublisher(
        session_factory=committed_sessions,
        publisher=publisher,
        owner=uuid4(),
        lease_duration=timedelta(0),
    )

    async def crash_before_recording(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("process died before recording success")

    monkeypatch.setattr(crashing_worker, "_mark_published", crash_before_recording)

    with pytest.raises(RuntimeError, match="died before recording"):
        await crashing_worker.run_once()

    after_crash = await load_event(committed_sessions, event_id)
    assert len(publisher.delivered) == 1
    assert after_crash.published_at is None
    assert after_crash.publish_attempts == 1

    recovered_worker = OutboxPublisher(
        session_factory=committed_sessions,
        publisher=publisher,
        owner=uuid4(),
    )
    outcome = await recovered_worker.run_once()
    after_recovery = await load_event(committed_sessions, event_id)

    assert outcome.result is PublishResult.PUBLISHED
    assert len(publisher.delivered) == 2
    assert publisher.delivered[0].event_id == publisher.delivered[1].event_id == event_id
    assert after_recovery.published_at is not None
    assert after_recovery.publish_attempts == 2
