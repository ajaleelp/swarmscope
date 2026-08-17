from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest
from pydantic import ValidationError

from packages.contracts.order_placed import OrderPlacedV1


def valid_event_data() -> dict[str, Any]:
    return {
        "event_id": uuid4(),
        "event_type": "OrderPlaced",
        "event_version": 1,
        "occurred_at": datetime(2026, 8, 17, 12, 0, tzinfo=UTC),
        "correlation_id": uuid4(),
        "payload": {
            "order_id": uuid4(),
            "customer_id": "customer-123",
            "sku": "widget-blue",
            "quantity": 2,
        },
    }


def test_event_round_trips_through_json() -> None:
    event = OrderPlacedV1.model_validate(valid_event_data())

    serialized = event.model_dump_json()

    assert OrderPlacedV1.model_validate_json(serialized) == event


def test_unknown_version_is_rejected() -> None:
    data = valid_event_data()
    data["event_version"] = 2

    with pytest.raises(ValidationError):
        OrderPlacedV1.model_validate(data)


def test_naive_timestamp_is_rejected() -> None:
    data = valid_event_data()
    data["occurred_at"] = datetime(2026, 8, 17, 12, 0)

    with pytest.raises(ValidationError):
        OrderPlacedV1.model_validate(data)


def test_unexpected_field_is_rejected() -> None:
    data = valid_event_data()
    data["debug_details"] = "must not enter the public contract"

    with pytest.raises(ValidationError):
        OrderPlacedV1.model_validate(data)


def test_non_positive_quantity_is_rejected() -> None:
    data = valid_event_data()
    data["payload"]["quantity"] = 0

    with pytest.raises(ValidationError):
        OrderPlacedV1.model_validate(data)
