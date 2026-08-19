from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, Header, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from apps.orders.database import close_database, get_session
from apps.orders.schemas import CreateOrder, OrderAccepted
from apps.orders.service import place_order


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    try:
        yield
    finally:
        await close_database()


app = FastAPI(title="Swarmscope Orders", lifespan=lifespan)


@app.post(
    "/orders",
    response_model=OrderAccepted,
    status_code=status.HTTP_201_CREATED,
)
async def create_order(
    request: CreateOrder,
    response: Response,
    session: Annotated[AsyncSession, Depends(get_session)],
    correlation_id: Annotated[
        UUID | None,
        Header(alias="X-Correlation-ID"),
    ] = None,
) -> OrderAccepted:
    resolved_correlation_id = correlation_id or uuid4()
    accepted = await place_order(
        request=request,
        correlation_id=resolved_correlation_id,
        session=session,
    )
    response.headers["X-Correlation-ID"] = str(resolved_correlation_id)
    return accepted
