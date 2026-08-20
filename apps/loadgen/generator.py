import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Protocol
from uuid import UUID, uuid4

import httpx

logger = logging.getLogger(__name__)

DEFAULT_RATE_PER_SECOND = 10.0
SKUS = tuple(f"SKU-{n:04d}" for n in range(1, 21))
CUSTOMERS = tuple(f"customer-{n:03d}" for n in range(1, 51))


class OrderSender(Protocol):
    """The one call the generator makes, declared so it can be tested offline."""

    async def place_order(self, order: dict[str, Any], correlation_id: UUID) -> int:
        """Submit one order and return its HTTP status code."""


@dataclass
class Outcome:
    """One request, as observed by the client."""

    status: int | None
    seconds: float
    correlation_id: UUID


@dataclass
class LoadStats:
    """What a run observed. Latencies are client-side, including queueing."""

    outcomes: list[Outcome] = field(default_factory=list)
    issued: int = 0

    @property
    def completed(self) -> int:
        return len(self.outcomes)

    @property
    def errors(self) -> int:
        return sum(1 for o in self.outcomes if o.status is None or o.status >= 400)

    def percentile(self, fraction: float) -> float:
        """Latency at the given fraction, nearest-rank. Zero if nothing finished."""
        if not self.outcomes:
            return 0.0
        ordered = sorted(o.seconds for o in self.outcomes)
        index = max(0, min(len(ordered) - 1, round(fraction * len(ordered)) - 1))
        return ordered[index]

    def summary(self) -> str:
        return (
            f"issued={self.issued} completed={self.completed} errors={self.errors} "
            f"p50={self.percentile(0.50) * 1000:.0f}ms "
            f"p99={self.percentile(0.99) * 1000:.0f}ms"
        )


class HttpOrderSender:
    """Places orders over HTTP against a running Orders API."""

    def __init__(self, client: httpx.AsyncClient, *, path: str = "/orders") -> None:
        self._client = client
        self._path = path

    async def place_order(self, order: dict[str, Any], correlation_id: UUID) -> int:
        response = await self._client.post(
            self._path,
            json=order,
            headers={"X-Correlation-ID": str(correlation_id)},
        )
        return response.status_code


def build_order(rng: random.Random) -> dict[str, Any]:
    """Compose one order. Seeding the generator makes a run repeatable."""
    return {
        "customer_id": rng.choice(CUSTOMERS),
        "sku": rng.choice(SKUS),
        "quantity": rng.randint(1, 5),
    }


async def issue_one(sender: OrderSender, order: dict[str, Any], stats: LoadStats) -> None:
    """Send one order and record what the client observed."""
    correlation_id = uuid4()
    started = time.perf_counter()
    status: int | None = None
    try:
        status = await sender.place_order(order, correlation_id)
    except Exception:
        logger.debug("request failed", exc_info=True)
    finally:
        stats.outcomes.append(
            Outcome(
                status=status,
                seconds=time.perf_counter() - started,
                correlation_id=correlation_id,
            )
        )


async def run_load(
    *,
    sender: OrderSender,
    shutdown: asyncio.Event,
    rate_per_second: float = DEFAULT_RATE_PER_SECOND,
    duration: timedelta | None = None,
    seed: int | None = None,
) -> LoadStats:
    """Issue orders at a steady rate until the duration elapses or shutdown.

    Requests are issued on a schedule and never wait for each other. A loop that
    awaited each response before pacing the next would send fewer requests as
    the system slowed down, so an injected fault would appear to make latency
    only slightly worse. Measuring that honestly is the point of this component.
    """
    rng = random.Random(seed)
    stats = LoadStats()
    interval = 1.0 / rate_per_second
    loop = asyncio.get_running_loop()
    started = loop.time()
    deadline = started + duration.total_seconds() if duration else None
    in_flight: set[asyncio.Task[None]] = set()
    slot = 0

    while not shutdown.is_set():
        if deadline is not None and loop.time() >= deadline:
            break

        # Strong references are kept deliberately: the event loop only holds a
        # weak one, so a task that nothing else references can be collected
        # while still in flight.
        task = asyncio.create_task(issue_one(sender, build_order(rng), stats))
        in_flight.add(task)
        task.add_done_callback(in_flight.discard)
        stats.issued += 1
        slot += 1

        # Sleep to the next scheduled slot rather than "interval from now", so a
        # slow request cannot push the whole schedule later.
        wait = (started + slot * interval) - loop.time()
        if wait > 0:
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=wait)
            except TimeoutError:
                pass

    if in_flight:
        await asyncio.gather(*in_flight, return_exceptions=True)

    return stats
