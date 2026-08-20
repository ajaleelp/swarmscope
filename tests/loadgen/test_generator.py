import asyncio
import random
from datetime import timedelta
from typing import Any
from uuid import UUID

import pytest

from apps.loadgen.__main__ import parse_args
from apps.loadgen.generator import (
    LoadStats,
    Outcome,
    build_order,
    run_load,
)


class RecordingSender:
    """Accepts orders, optionally slowly or unsuccessfully."""

    def __init__(self, *, latency: float = 0.0, status: int = 201, fail: bool = False) -> None:
        self.orders: list[dict[str, Any]] = []
        self.correlation_ids: list[UUID] = []
        self._latency = latency
        self._status = status
        self._fail = fail

    async def place_order(self, order: dict[str, Any], correlation_id: UUID) -> int:
        self.orders.append(order)
        self.correlation_ids.append(correlation_id)
        if self._latency:
            await asyncio.sleep(self._latency)
        if self._fail:
            raise RuntimeError("connection refused")
        return self._status


# --- rate ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_issues_at_approximately_the_configured_rate() -> None:
    sender = RecordingSender()

    stats = await run_load(
        sender=sender,
        shutdown=asyncio.Event(),
        rate_per_second=20,
        duration=timedelta(seconds=0.5),
    )

    assert 7 <= stats.issued <= 13, f"expected roughly 10, issued {stats.issued}"


@pytest.mark.asyncio
async def test_slow_responses_do_not_reduce_the_send_rate() -> None:
    """The guarantee this component exists for.

    A generator that awaited each response before pacing the next would issue
    about two requests here instead of ten, and would then report a latency
    distribution that barely noticed the slowdown. That is coordinated omission,
    and it hides exactly the faults this project injects on purpose.
    """
    slow = RecordingSender(latency=0.2)

    stats = await run_load(
        sender=slow,
        shutdown=asyncio.Event(),
        rate_per_second=20,
        duration=timedelta(seconds=0.5),
    )

    assert stats.issued >= 7, (
        f"send rate collapsed under latency: issued {stats.issued}, expected about 10"
    )
    assert stats.completed == stats.issued, "in-flight requests were not drained"


@pytest.mark.asyncio
async def test_the_schedule_does_not_drift_under_its_own_overhead() -> None:
    """Each slot is timed from the start, not from the previous iteration.

    Waiting a fixed interval after each send adds that iteration's own overhead
    to every subsequent slot, so the shortfall accumulates: measured at about
    13% over a second at this rate. Anchoring to absolute slot times is
    self-correcting, which is also why this threshold is not fragile. A late
    iteration shortens the next wait, whereas the drifting version can only
    fall further behind.
    """
    stats = await run_load(
        sender=RecordingSender(),
        shutdown=asyncio.Event(),
        rate_per_second=500,
        duration=timedelta(seconds=1.0),
    )

    assert stats.issued >= 450, (
        f"schedule drifted: issued {stats.issued} of an expected 500"
    )


@pytest.mark.asyncio
async def test_duration_bounds_the_run() -> None:
    loop = asyncio.get_running_loop()
    started = loop.time()

    await run_load(
        sender=RecordingSender(),
        shutdown=asyncio.Event(),
        rate_per_second=50,
        duration=timedelta(seconds=0.3),
    )

    assert loop.time() - started < 3, "run did not stop at its duration"


# --- measurement -----------------------------------------------------------


@pytest.mark.asyncio
async def test_every_issued_request_is_recorded_with_a_latency() -> None:
    sender = RecordingSender(latency=0.01)

    stats = await run_load(
        sender=sender,
        shutdown=asyncio.Event(),
        rate_per_second=40,
        duration=timedelta(seconds=0.3),
    )

    correlation_ids = [o.correlation_id for o in stats.outcomes]

    assert stats.completed == stats.issued
    assert all(o.seconds > 0 for o in stats.outcomes)
    assert all(o.status == 201 for o in stats.outcomes)
    assert len(set(correlation_ids)) == stats.completed, "correlation ids repeated"
    assert sender.correlation_ids == correlation_ids, "a request went out untracked"


def test_percentiles_use_nearest_rank() -> None:
    stats = LoadStats(
        outcomes=[Outcome(status=201, seconds=s / 100, correlation_id=UUID(int=s))
                  for s in range(1, 101)]
    )

    assert stats.percentile(0.50) == pytest.approx(0.50)
    assert stats.percentile(0.99) == pytest.approx(0.99)
    assert stats.percentile(1.0) == pytest.approx(1.00)


def test_percentiles_of_an_empty_run_are_zero_rather_than_raising() -> None:
    assert LoadStats().percentile(0.99) == 0.0


@pytest.mark.asyncio
async def test_failures_are_counted_and_do_not_stop_the_run() -> None:
    stats = await run_load(
        sender=RecordingSender(fail=True),
        shutdown=asyncio.Event(),
        rate_per_second=30,
        duration=timedelta(seconds=0.3),
    )

    assert stats.issued >= 3
    assert stats.completed == stats.issued, "a failed request was never recorded"
    assert stats.errors == stats.completed, "a raised request should count as an error"
    assert stats.errors > 0
    assert all(o.status is None for o in stats.outcomes)


def test_http_error_statuses_count_as_errors() -> None:
    stats = LoadStats(
        outcomes=[
            Outcome(status=201, seconds=0.01, correlation_id=UUID(int=1)),
            Outcome(status=422, seconds=0.01, correlation_id=UUID(int=2)),
            Outcome(status=500, seconds=0.01, correlation_id=UUID(int=3)),
        ]
    )

    assert stats.errors == 2


# --- repeatability ---------------------------------------------------------


def test_the_same_seed_produces_the_same_orders() -> None:
    """Two runs are only comparable if they send the same traffic."""
    a = random.Random(1337)
    b = random.Random(1337)

    assert [build_order(a) for _ in range(20)] == [build_order(b) for _ in range(20)]


def test_different_seeds_produce_different_orders() -> None:
    a = [build_order(random.Random(1)) for _ in range(20)]
    b = [build_order(random.Random(2)) for _ in range(20)]

    assert a != b


# --- shutdown --------------------------------------------------------------


@pytest.mark.asyncio
async def test_shutdown_stops_issuing_and_drains_in_flight() -> None:
    sender = RecordingSender(latency=0.05)
    shutdown = asyncio.Event()
    asyncio.get_running_loop().call_later(0.15, shutdown.set)

    stats = await asyncio.wait_for(
        run_load(sender=sender, shutdown=shutdown, rate_per_second=20),
        timeout=10,
    )

    assert stats.issued >= 1
    assert stats.completed == stats.issued, "shutdown abandoned in-flight requests"


@pytest.mark.asyncio
async def test_shutdown_before_starting_issues_nothing() -> None:
    shutdown = asyncio.Event()
    shutdown.set()
    sender = RecordingSender()

    stats = await run_load(sender=sender, shutdown=shutdown, rate_per_second=20)

    assert stats.issued == 0
    assert sender.orders == []


# --- the command line ------------------------------------------------------


def test_cli_defaults_and_overrides() -> None:
    defaults = parse_args([])
    assert defaults.rate == 10.0
    assert defaults.duration is None
    assert defaults.base_url == "http://localhost:8000"

    tuned = parse_args(["--rate", "25", "--duration", "60", "--seed", "1337"])
    assert tuned.rate == 25.0
    assert tuned.duration == 60.0
    assert tuned.seed == 1337
