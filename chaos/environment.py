import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Protocol, assert_never

import httpx
from azure.identity.aio import DefaultAzureCredential
from azure.servicebus.aio.management import ServiceBusAdministrationClient
from azure.servicebus.management import (
    EntityStatus,
    SubscriptionProperties,
    TopicProperties,
)
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from apps.orders.config import Settings, get_settings
from chaos.catalogue import (
    Check,
    ComposeStep,
    HttpCheck,
    SqlCheck,
    Step,
    SubscriptionStatusStep,
    TopicStatusStep,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILE = REPO_ROOT / "compose.yaml"
COMPOSE_PROJECT = "swarmscope"
DEFAULT_API_BASE_URL = "http://127.0.0.1:8000"

SqlScalarReader = Callable[[str], Awaitable[float]]
CommandRunner = Callable[[tuple[str, ...], Path], Awaitable[None]]


class AdministrationClient(Protocol):
    async def get_topic(self, topic_name: str) -> TopicProperties: ...

    async def update_topic(self, topic: TopicProperties) -> None: ...

    async def get_subscription(
        self,
        topic_name: str,
        subscription_name: str,
    ) -> SubscriptionProperties: ...

    async def update_subscription(
        self,
        topic_name: str,
        subscription: SubscriptionProperties,
    ) -> None: ...


@dataclass(frozen=True)
class DetectionResult:
    matched: bool
    observed: int | float
    description: str


async def run_command(command: tuple[str, ...], cwd: Path) -> None:
    """Run one argv-only command and surface bounded diagnostics on failure."""
    process = await asyncio.create_subprocess_exec(
        *command,
        cwd=cwd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    if process.returncode == 0:
        return

    detail = (stderr or stdout).decode(errors="replace").strip()
    if len(detail) > 2000:
        detail = detail[:2000] + "…"
    raise RuntimeError(
        f"command failed with exit {process.returncode}: {' '.join(command)}"
        + (f"\n{detail}" if detail else "")
    )


async def read_sql_scalar(engine: AsyncEngine, query: str) -> float:
    async with engine.connect() as connection:
        result = await connection.execute(text(query))
        return float(result.scalar_one())


def _matches(check: SqlCheck, observed: float) -> bool:
    if check.comparison == "gt":
        return observed > check.threshold
    return observed < check.threshold


def _status_value(status: str | EntityStatus | None) -> str | None:
    if isinstance(status, EntityStatus):
        return status.value
    return status


@dataclass
class FaultEnvironment:
    sql_scalar: SqlScalarReader
    http_client: httpx.AsyncClient
    administration: AdministrationClient
    command_runner: CommandRunner = run_command
    repo_root: Path = REPO_ROOT

    async def detect(self, check: Check) -> DetectionResult:
        if isinstance(check, SqlCheck):
            observed = await self.sql_scalar(check.query)
            return DetectionResult(
                matched=_matches(check, observed),
                observed=observed,
                description=check.describe,
            )

        if isinstance(check, HttpCheck):
            response = await self.http_client.request(
                check.method,
                check.path,
                json=check.body,
            )
            return DetectionResult(
                matched=response.status_code >= check.status_at_least,
                observed=response.status_code,
                description=check.describe,
            )

        assert_never(check)

    async def apply(self, step: Step) -> None:
        if isinstance(step, ComposeStep):
            verb = "stop" if step.action == "compose_stop" else "start"
            await self.command_runner(
                (
                    "docker",
                    "compose",
                    "--project-name",
                    COMPOSE_PROJECT,
                    "--file",
                    str(COMPOSE_FILE),
                    verb,
                    step.service,
                ),
                self.repo_root,
            )
            return

        if isinstance(step, TopicStatusStep):
            expected = EntityStatus(step.status)
            topic = await self.administration.get_topic(step.topic)
            topic.status = expected
            await self.administration.update_topic(topic)
            updated = await self.administration.get_topic(step.topic)
            if _status_value(updated.status) != expected.value:
                raise RuntimeError(
                    f"topic {step.topic!r} status is {_status_value(updated.status)!r}; "
                    f"expected {expected.value!r}"
                )
            return

        if isinstance(step, SubscriptionStatusStep):
            expected = EntityStatus(step.status)
            subscription = await self.administration.get_subscription(
                step.topic,
                step.subscription,
            )
            subscription.status = expected
            await self.administration.update_subscription(step.topic, subscription)
            updated = await self.administration.get_subscription(
                step.topic,
                step.subscription,
            )
            if _status_value(updated.status) != expected.value:
                raise RuntimeError(
                    f"subscription {step.topic!r}/{step.subscription!r} status is "
                    f"{_status_value(updated.status)!r}; expected {expected.value!r}"
                )
            return

        assert_never(step)


@asynccontextmanager
async def production_environment(
    *,
    api_base_url: str = DEFAULT_API_BASE_URL,
    settings: Settings | None = None,
) -> AsyncIterator[FaultEnvironment]:
    """Open the real local-and-Azure dependencies for one runner invocation."""
    resolved_settings = settings or get_settings()
    engine = create_async_engine(resolved_settings.database_url, pool_pre_ping=True)
    credential = DefaultAzureCredential()
    administration = ServiceBusAdministrationClient(
        fully_qualified_namespace=(resolved_settings.service_bus_fully_qualified_namespace),
        credential=credential,
    )
    http_client = httpx.AsyncClient(base_url=api_base_url, timeout=30.0)

    try:
        yield FaultEnvironment(
            sql_scalar=partial(read_sql_scalar, engine),
            http_client=http_client,
            administration=administration,
        )
    finally:
        await http_client.aclose()
        await administration.close()
        await credential.close()
        await engine.dispose()
