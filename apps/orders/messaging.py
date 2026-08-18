from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol
from uuid import UUID


@dataclass(frozen=True, slots=True)
class OutboundEvent:
    """A business event ready for delivery to a message broker."""

    event_id: UUID
    event_type: str
    event_version: int
    occurred_at: datetime
    correlation_id: UUID
    payload: Mapping[str, Any]


class EventPublisher(Protocol):
    """Transport boundary used by the outbox publisher."""

    async def publish(self, event: OutboundEvent) -> None:
        """Deliver one event or raise an exception."""
