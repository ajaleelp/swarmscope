import json
from dataclasses import dataclass
from pathlib import Path

import httpx
import pytest
from azure.servicebus.management import EntityStatus

from chaos.catalogue import (
    ComposeStep,
    HttpCheck,
    SqlCheck,
    SubscriptionStatusStep,
    TopicStatusStep,
)
from chaos.environment import COMPOSE_FILE, FaultEnvironment


@dataclass
class Entity:
    name: str
    status: str | EntityStatus


class FakeAdministration:
    def __init__(self, *, ignore_updates: bool = False) -> None:
        self.topic_status: str | EntityStatus = EntityStatus.ACTIVE
        self.subscription_status: str | EntityStatus = EntityStatus.ACTIVE
        self.ignore_updates = ignore_updates
        self.calls: list[tuple] = []

    async def get_topic(self, topic_name: str) -> Entity:
        self.calls.append(("get_topic", topic_name))
        return Entity(topic_name, self.topic_status)

    async def update_topic(self, topic: Entity) -> None:
        self.calls.append(("update_topic", topic.name, topic.status))
        if not self.ignore_updates:
            self.topic_status = topic.status

    async def get_subscription(
        self,
        topic_name: str,
        subscription_name: str,
    ) -> Entity:
        self.calls.append(("get_subscription", topic_name, subscription_name))
        return Entity(subscription_name, self.subscription_status)

    async def update_subscription(
        self,
        topic_name: str,
        subscription: Entity,
    ) -> None:
        self.calls.append(
            ("update_subscription", topic_name, subscription.name, subscription.status)
        )
        if not self.ignore_updates:
            self.subscription_status = subscription.status


async def unused_sql(_: str) -> float:
    raise AssertionError("SQL was not expected")


def unused_http_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url="http://orders.test",
        transport=httpx.MockTransport(
            lambda _: (_ for _ in ()).throw(AssertionError("HTTP was not expected"))
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("comparison", "observed", "threshold", "matched"),
    [
        ("gt", 4.0, 3.0, True),
        ("gt", 3.0, 3.0, False),
        ("lt", 2.0, 3.0, True),
        ("lt", 3.0, 3.0, False),
    ],
)
async def test_sql_detection_uses_the_declared_strict_comparison(
    comparison: str,
    observed: float,
    threshold: float,
    matched: bool,
) -> None:
    queries: list[str] = []

    async def scalar(query: str) -> float:
        queries.append(query)
        return observed

    async with unused_http_client() as client:
        environment = FaultEnvironment(scalar, client, FakeAdministration())
        result = await environment.detect(
            SqlCheck(
                kind="sql",
                query="SELECT count(*) FROM orders.outbox_events",
                comparison=comparison,
                threshold=threshold,
                describe="outbox backlog",
            )
        )

    assert result.matched is matched
    assert result.observed == observed
    assert result.description == "outbox backlog"
    assert queries == ["SELECT count(*) FROM orders.outbox_events"]


@pytest.mark.asyncio
async def test_http_detection_sends_the_declared_request_and_reads_status() -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(503)

    async with httpx.AsyncClient(
        base_url="http://orders.test",
        transport=httpx.MockTransport(respond),
    ) as client:
        environment = FaultEnvironment(unused_sql, client, FakeAdministration())
        result = await environment.detect(
            HttpCheck(
                kind="http",
                method="POST",
                path="/orders",
                body={"sku": "SKU-0001", "quantity": 1},
                status_at_least=500,
                describe="checkout fails",
            )
        )

    assert result.matched is True
    assert result.observed == 503
    assert requests[0].method == "POST"
    assert requests[0].url.path == "/orders"
    assert json.loads(requests[0].content) == {"sku": "SKU-0001", "quantity": 1}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("action", "verb"),
    [("compose_stop", "stop"), ("compose_start", "start")],
)
async def test_compose_steps_use_argv_and_the_explicit_project(
    tmp_path: Path,
    action: str,
    verb: str,
) -> None:
    commands: list[tuple[tuple[str, ...], Path]] = []

    async def command_runner(command: tuple[str, ...], cwd: Path) -> None:
        commands.append((command, cwd))

    async with unused_http_client() as client:
        environment = FaultEnvironment(
            unused_sql,
            client,
            FakeAdministration(),
            command_runner=command_runner,
            repo_root=tmp_path,
        )
        await environment.apply(ComposeStep(action=action, service="postgres"))

    assert commands == [
        (
            (
                "docker",
                "compose",
                "--project-name",
                "swarmscope",
                "--file",
                str(COMPOSE_FILE),
                verb,
                "postgres",
            ),
            tmp_path,
        )
    ]


@pytest.mark.asyncio
async def test_topic_status_is_updated_and_read_back() -> None:
    administration = FakeAdministration()
    async with unused_http_client() as client:
        environment = FaultEnvironment(unused_sql, client, administration)
        await environment.apply(
            TopicStatusStep(
                action="topic_status",
                topic="orders",
                status="SendDisabled",
            )
        )

    assert administration.topic_status is EntityStatus.SEND_DISABLED
    assert administration.calls == [
        ("get_topic", "orders"),
        ("update_topic", "orders", EntityStatus.SEND_DISABLED),
        ("get_topic", "orders"),
    ]


@pytest.mark.asyncio
async def test_subscription_status_is_updated_and_read_back() -> None:
    administration = FakeAdministration()
    async with unused_http_client() as client:
        environment = FaultEnvironment(unused_sql, client, administration)
        await environment.apply(
            SubscriptionStatusStep(
                action="subscription_status",
                topic="orders",
                subscription="fulfilment",
                status="ReceiveDisabled",
            )
        )

    assert administration.subscription_status is EntityStatus.RECEIVE_DISABLED
    assert administration.calls == [
        ("get_subscription", "orders", "fulfilment"),
        (
            "update_subscription",
            "orders",
            "fulfilment",
            EntityStatus.RECEIVE_DISABLED,
        ),
        ("get_subscription", "orders", "fulfilment"),
    ]


@pytest.mark.asyncio
async def test_an_entity_update_that_did_not_stick_is_an_error() -> None:
    administration = FakeAdministration(ignore_updates=True)
    async with unused_http_client() as client:
        environment = FaultEnvironment(unused_sql, client, administration)

        with pytest.raises(RuntimeError, match="expected 'SendDisabled'"):
            await environment.apply(
                TopicStatusStep(
                    action="topic_status",
                    topic="orders",
                    status="SendDisabled",
                )
            )
