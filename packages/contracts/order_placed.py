from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field


class OrderPlacedPayloadV1(BaseModel):
    """Business data carried by an OrderPlaced version 1 event."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )

    order_id: UUID
    customer_id: str = Field(min_length=1, max_length=100)
    sku: str = Field(min_length=1, max_length=100)
    quantity: int = Field(ge=1)


class OrderPlacedV1(BaseModel):
    """Version 1 envelope for an order being placed."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: UUID
    event_type: Literal["OrderPlaced"] = "OrderPlaced"
    event_version: Literal[1] = 1
    occurred_at: AwareDatetime
    correlation_id: UUID
    payload: OrderPlacedPayloadV1
