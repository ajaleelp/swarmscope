from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from uuid import UUID, uuid4

from sqlalchemy import Select, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from apps.orders.messaging import EventPublisher, OutboundEvent
from apps.orders.models import OutboxEvent

DEFAULT_LEASE_DURATION = timedelta(seconds=30)
DEFAULT_BASE_BACKOFF = timedelta(seconds=1)
DEFAULT_MAX_BACKOFF = timedelta(minutes=5)
MAX_BACKOFF_EXPONENT = 16
MAX_ERROR_LENGTH = 1000


def backoff_delay(
    attempts: int,
    *,
    base: timedelta = DEFAULT_BASE_BACKOFF,
    maximum: timedelta = DEFAULT_MAX_BACKOFF,
) -> timedelta:
    """Return the capped exponential wait before retrying a failed attempt.

    The first failed attempt waits ``base``, and each later failure doubles
    that wait until it reaches ``maximum``.
    """
    if attempts < 1:
        raise ValueError("attempts must be at least 1")

    exponent = min(attempts - 1, MAX_BACKOFF_EXPONENT)
    return min(base * (2**exponent), maximum)


def due_event_statement(now: datetime) -> Select[tuple[OutboxEvent]]:
    """Select the next claimable event, locked against other workers.

    ``FOR UPDATE SKIP LOCKED`` makes two workers running this at the same
    instant take different rows instead of blocking or colliding.
    """
    return (
        select(OutboxEvent)
        .where(
            OutboxEvent.published_at.is_(None),
            OutboxEvent.next_attempt_at <= now,
            or_(
                OutboxEvent.lease_expires_at.is_(None),
                OutboxEvent.lease_expires_at <= now,
            ),
        )
        .order_by(OutboxEvent.next_attempt_at, OutboxEvent.occurred_at)
        .limit(1)
        .with_for_update(skip_locked=True)
    )


class PublishResult(StrEnum):
    """What one unit of worker effort accomplished."""

    PUBLISHED = "published"
    FAILED = "failed"
    IDLE = "idle"


@dataclass(frozen=True, slots=True)
class PublishOutcome:
    """The reportable result of a single ``run_once`` call."""

    result: PublishResult
    event_id: UUID | None = None
    attempts: int | None = None
    error: str | None = None


@dataclass(frozen=True, slots=True)
class _ClaimedEvent:
    """An outbox row this worker has committed a claim on."""

    event_id: UUID
    outbound: OutboundEvent
    attempts: int


class OutboxPublisher:
    """Move committed outbox rows to the broker, one at a time.

    Delivery is at-least-once. A crash after the broker accepts a message but
    before the row is marked published causes that message to be sent again
    with the same ``event_id``, so consumers must deduplicate on it.
    """

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        publisher: EventPublisher,
        owner: UUID | None = None,
        lease_duration: timedelta = DEFAULT_LEASE_DURATION,
        base_backoff: timedelta = DEFAULT_BASE_BACKOFF,
        max_backoff: timedelta = DEFAULT_MAX_BACKOFF,
    ) -> None:
        self._session_factory = session_factory
        self._publisher = publisher
        self._owner = owner or uuid4()
        self._lease_duration = lease_duration
        self._base_backoff = base_backoff
        self._max_backoff = max_backoff

    @property
    def owner(self) -> UUID:
        """This worker's lease identity."""
        return self._owner

    async def run_once(self) -> PublishOutcome:
        """Claim, publish, and settle at most one due event."""
        claimed = await self._claim_next_event()
        if claimed is None:
            return PublishOutcome(result=PublishResult.IDLE)

        try:
            await self._publisher.publish(claimed.outbound)
        except Exception as error:  # noqa: BLE001 - any transport failure retries
            await self._release_for_retry(claimed, error)
            return PublishOutcome(
                result=PublishResult.FAILED,
                event_id=claimed.event_id,
                attempts=claimed.attempts,
                error=str(error),
            )

        await self._mark_published(claimed)
        return PublishOutcome(
            result=PublishResult.PUBLISHED,
            event_id=claimed.event_id,
            attempts=claimed.attempts,
        )

    async def _claim_next_event(self) -> _ClaimedEvent | None:
        """Take ownership of one due event and commit that claim."""
        async with self._session_factory() as session, session.begin():
            now = await self._database_time(session)

            event = await session.scalar(due_event_statement(now))
            if event is None:
                return None

            event.lease_owner = self._owner
            event.lease_expires_at = now + self._lease_duration
            event.publish_attempts += 1
            event.last_attempt_at = now

            return _ClaimedEvent(
                event_id=event.event_id,
                outbound=OutboundEvent(
                    event_id=event.event_id,
                    event_type=event.event_type,
                    event_version=event.event_version,
                    occurred_at=event.occurred_at,
                    correlation_id=event.correlation_id,
                    payload=event.payload,
                ),
                attempts=event.publish_attempts,
            )

    async def _mark_published(self, claimed: _ClaimedEvent) -> None:
        """Record broker-confirmed delivery and drop the lease."""
        async with self._session_factory() as session, session.begin():
            now = await self._database_time(session)
            await session.execute(
                update(OutboxEvent)
                .where(
                    OutboxEvent.event_id == claimed.event_id,
                    OutboxEvent.lease_owner == self._owner,
                )
                .values(
                    published_at=now,
                    lease_owner=None,
                    lease_expires_at=None,
                    last_publish_error=None,
                )
            )

    async def _release_for_retry(
        self,
        claimed: _ClaimedEvent,
        error: Exception,
    ) -> None:
        """Drop the lease and schedule the next attempt after a backoff."""
        delay = backoff_delay(
            claimed.attempts,
            base=self._base_backoff,
            maximum=self._max_backoff,
        )

        async with self._session_factory() as session, session.begin():
            now = await self._database_time(session)
            await session.execute(
                update(OutboxEvent)
                .where(
                    OutboxEvent.event_id == claimed.event_id,
                    OutboxEvent.lease_owner == self._owner,
                )
                .values(
                    lease_owner=None,
                    lease_expires_at=None,
                    next_attempt_at=now + delay,
                    last_publish_error=str(error)[:MAX_ERROR_LENGTH],
                )
            )

    @staticmethod
    async def _database_time(session: AsyncSession) -> datetime:
        """Read the current time from PostgreSQL, the shared clock."""
        now = await session.scalar(select(func.now()))
        if now is None:  # pragma: no cover - PostgreSQL always returns a time
            raise RuntimeError("database did not return a current time")
        return now
