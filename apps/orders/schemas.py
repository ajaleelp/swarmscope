from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class CreateOrder(BaseModel):
    """Validated input for creating an order."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )

    customer_id: str = Field(min_length=1, max_length=100)
    sku: str = Field(min_length=1, max_length=100)
    quantity: int = Field(ge=1)


class OrderAccepted(BaseModel):
    """Response returned after the order transaction commits."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    order_id: UUID
    correlation_id: UUID
    status: Literal["accepted"] = "accepted"
