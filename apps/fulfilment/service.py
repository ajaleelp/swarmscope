from enum import StrEnum
from uuid import uuid4

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from apps.fulfilment.models import Fulfilment, ProcessedEvent
from packages.contracts.order_placed import OrderPlacedV1

# Violating either of these means the event was already handled. Any other
# constraint failure is a defect and must not be mistaken for a duplicate.
DUPLICATE_CONSTRAINTS = frozenset({"pk_processed_events", "uq_fulfilments_order_id"})


class Handled(StrEnum):
    """What handling one delivery accomplished."""

    FULFILLED = "fulfilled"
    DUPLICATE = "duplicate"


def is_duplicate(error: IntegrityError) -> bool:
    """Distinguish an already-handled event from a genuine constraint failure.

    Treating every IntegrityError as a duplicate would mark a message complete
    and discard the event permanently the first time an unrelated constraint
    broke, so the specific constraint is checked by name.
    """
    diagnostic = getattr(error.orig, "diag", None)
    return getattr(diagnostic, "constraint_name", None) in DUPLICATE_CONSTRAINTS


async def fulfil_order(
    *,
    event: OrderPlacedV1,
    session: AsyncSession,
) -> Handled:
    """Fulfil one order, exactly once, however many times it is delivered.

    The processed-event record and the fulfilment are written in a single
    transaction. If it commits the work happened; if the event was already
    handled the insert violates a uniqueness constraint and nothing changes.
    That makes the database the arbiter rather than a read-then-write check,
    which two concurrent consumers could both pass.
    """
    try:
        async with session.begin():
            session.add(
                ProcessedEvent(
                    event_id=event.event_id,
                    event_type=event.event_type,
                    correlation_id=event.correlation_id,
                )
            )
            session.add(
                Fulfilment(
                    id=uuid4(),
                    order_id=event.payload.order_id,
                    customer_id=event.payload.customer_id,
                    sku=event.payload.sku,
                    quantity=event.payload.quantity,
                    correlation_id=event.correlation_id,
                )
            )
    except IntegrityError as error:
        if is_duplicate(error):
            return Handled.DUPLICATE
        raise

    return Handled.FULFILLED
