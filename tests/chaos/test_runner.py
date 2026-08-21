import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import pytest

import chaos.__main__ as chaos_cli
from chaos.catalogue import Check, Fault, Step, load_catalogue
from chaos.environment import DetectionResult
from chaos.runner import (
    FaultRunner,
    FaultRunResult,
    InjectionNotDetected,
    RecoveryReport,
    RecoveryRequired,
    RevertFailed,
    RunStatus,
    SymptomAlreadyPresent,
    describe_step,
    render_plan,
)
from chaos.state import FaultState, FaultStateStore

NOW = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)


def detection(matched: bool, observed: int = 0) -> DetectionResult:
    return DetectionResult(
        matched=matched,
        observed=observed,
        description="declared symptom",
    )


class ScriptedEnvironment:
    def __init__(
        self,
        detections: list[DetectionResult | Exception],
        *,
        fail_step: str | None = None,
        failure: BaseException | None = None,
        before_apply: Callable[[], None] | None = None,
    ) -> None:
        self.detections = detections
        self.fail_step = fail_step
        self.failure = failure or RuntimeError("injection exploded")
        self.before_apply = before_apply
        self.actions: list[str] = []
        self.detect_count = 0

    async def detect(self, _: Check) -> DetectionResult:
        self.detect_count += 1
        scripted = self.detections.pop(0)
        if isinstance(scripted, Exception):
            raise scripted
        return scripted

    async def apply(self, step: Step) -> None:
        if self.before_apply is not None:
            self.before_apply()
        described = describe_step(step)
        self.actions.append(described)
        if described == self.fail_step:
            raise self.failure


async def no_sleep(_: float) -> None:
    return None


def runner(
    environment: ScriptedEnvironment,
    state_path: Path,
) -> FaultRunner:
    return FaultRunner(
        environment,
        FaultStateStore(state_path),
        sleep=no_sleep,
        clock=lambda: NOW,
    )


def compound_fault() -> Fault:
    return Fault.model_validate(
        {
            "id": "compound-fault",
            "title": "Compound failure",
            "symptom": "Customers report failures.",
            "settle_seconds": 1,
            "inject": [
                {"action": "compose_stop", "service": "postgres"},
                {
                    "action": "topic_status",
                    "topic": "orders",
                    "status": "SendDisabled",
                },
            ],
            "revert": [
                {"action": "compose_start", "service": "postgres"},
                {
                    "action": "topic_status",
                    "topic": "orders",
                    "status": "Active",
                },
            ],
            "detects": {
                "kind": "sql",
                "query": "SELECT 1",
                "comparison": "gt",
                "threshold": 0,
                "describe": "declared symptom",
            },
            "ground_truth": {
                "summary": "Two dependencies were disabled.",
                "surface": "application",
                "evidence": ["two independent failures"],
            },
        }
    )


@pytest.mark.asyncio
async def test_run_checks_injects_verifies_reverts_and_clears_state(
    tmp_path: Path,
) -> None:
    fault = load_catalogue()["topic-send-disabled"]
    environment = ScriptedEnvironment([detection(False), detection(True, 4), detection(False)])

    result = await runner(environment, tmp_path / "state.json").run(fault)

    assert result.status is RunStatus.RECOVERED
    assert result.injected == detection(True, 4)
    assert environment.actions == [
        "set topic orders status to SendDisabled",
        "set topic orders status to Active",
    ]
    assert not (tmp_path / "state.json").exists()


@pytest.mark.asyncio
async def test_preexisting_symptom_refuses_injection(tmp_path: Path) -> None:
    fault = load_catalogue()["topic-send-disabled"]
    environment = ScriptedEnvironment([detection(True, 9)])

    with pytest.raises(SymptomAlreadyPresent, match="already true"):
        await runner(environment, tmp_path / "state.json").run(fault)

    assert environment.actions == []
    assert not (tmp_path / "state.json").exists()


@pytest.mark.asyncio
async def test_injection_that_does_nothing_is_reported_and_reverted(
    tmp_path: Path,
) -> None:
    fault = load_catalogue()["topic-send-disabled"]
    environment = ScriptedEnvironment([detection(False), detection(False), detection(False)])

    with pytest.raises(InjectionNotDetected, match="did not produce"):
        await runner(environment, tmp_path / "state.json").run(fault)

    assert environment.actions == [
        "set topic orders status to SendDisabled",
        "set topic orders status to Active",
    ]
    assert not (tmp_path / "state.json").exists()


@pytest.mark.asyncio
async def test_state_exists_before_the_first_injection_action(tmp_path: Path) -> None:
    fault = load_catalogue()["topic-send-disabled"]
    state_path = tmp_path / "state.json"
    store = FaultStateStore(state_path)
    observed_states: list[FaultState | None] = []
    environment = ScriptedEnvironment(
        [detection(False), detection(True), detection(False)],
        before_apply=lambda: observed_states.append(store.load()),
    )

    await runner(environment, state_path).run(fault)

    assert observed_states[0] == FaultState(
        fault_id=fault.id,
        injection_started_at=NOW,
    )


@pytest.mark.asyncio
async def test_mid_injection_failure_runs_every_revert(tmp_path: Path) -> None:
    fault = compound_fault()
    environment = ScriptedEnvironment(
        [detection(False), detection(False)],
        fail_step="set topic orders status to SendDisabled",
    )

    with pytest.raises(RuntimeError, match="injection exploded"):
        await runner(environment, tmp_path / "state.json").run(fault)

    assert environment.actions == [
        "compose stop postgres (project swarmscope)",
        "set topic orders status to SendDisabled",
        "compose start postgres (project swarmscope)",
        "set topic orders status to Active",
    ]
    assert not (tmp_path / "state.json").exists()


@pytest.mark.asyncio
async def test_interruption_still_reverts_before_propagating(tmp_path: Path) -> None:
    fault = load_catalogue()["topic-send-disabled"]
    environment = ScriptedEnvironment(
        [detection(False), detection(False)],
        fail_step="set topic orders status to SendDisabled",
        failure=asyncio.CancelledError(),
    )

    with pytest.raises(asyncio.CancelledError):
        await runner(environment, tmp_path / "state.json").run(fault)

    assert environment.actions == [
        "set topic orders status to SendDisabled",
        "set topic orders status to Active",
    ]
    assert not (tmp_path / "state.json").exists()


@pytest.mark.asyncio
async def test_backlog_after_revert_is_reported_and_state_is_retained(
    tmp_path: Path,
) -> None:
    fault = load_catalogue()["subscription-receive-disabled"]
    state_path = tmp_path / "state.json"
    environment = ScriptedEnvironment([detection(False), detection(True, 8), detection(True, 5)])

    result = await runner(environment, state_path).run(fault)

    assert result.status is RunStatus.RECOVERY_PENDING
    assert FaultStateStore(state_path).load() == FaultState(
        fault_id=fault.id,
        injection_started_at=NOW,
    )


@pytest.mark.asyncio
async def test_revert_only_uses_state_and_clears_after_verification(
    tmp_path: Path,
) -> None:
    fault = load_catalogue()["topic-send-disabled"]
    state_path = tmp_path / "state.json"
    store = FaultStateStore(state_path)
    store.record(FaultState(fault_id=fault.id, injection_started_at=NOW))
    environment = ScriptedEnvironment([detection(False)])

    result = await runner(environment, state_path).revert(fault)

    assert result.status is RunStatus.RECOVERED
    assert environment.actions == ["set topic orders status to Active"]
    assert store.load() is None


@pytest.mark.asyncio
async def test_revert_failure_attempts_remaining_steps_and_retains_state(
    tmp_path: Path,
) -> None:
    fault = compound_fault()
    state_path = tmp_path / "state.json"
    store = FaultStateStore(state_path)
    store.record(FaultState(fault_id=fault.id, injection_started_at=NOW))
    environment = ScriptedEnvironment(
        [detection(False)],
        fail_step="compose start postgres (project swarmscope)",
    )

    with pytest.raises(RevertFailed, match="state retained"):
        await runner(environment, state_path).revert(fault)

    assert environment.actions == [
        "compose start postgres (project swarmscope)",
        "set topic orders status to Active",
    ]
    assert store.load() is not None


@pytest.mark.asyncio
async def test_existing_state_blocks_a_new_run_before_checking_health(
    tmp_path: Path,
) -> None:
    fault = load_catalogue()["topic-send-disabled"]
    state_path = tmp_path / "state.json"
    FaultStateStore(state_path).record(
        FaultState(fault_id="database-unavailable", injection_started_at=NOW)
    )
    environment = ScriptedEnvironment([])

    with pytest.raises(RecoveryRequired, match="still requires revert"):
        await runner(environment, state_path).run(fault)

    assert environment.detect_count == 0
    assert environment.actions == []


def test_dry_run_prints_the_complete_plan_and_touches_no_runtime_state(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = tmp_path / "state.json"

    def forbidden_environment(**_):
        raise AssertionError("dry-run constructed the production environment")

    monkeypatch.setattr(chaos_cli, "production_environment", forbidden_environment)

    exit_code = chaos_cli.main(
        [
            "run",
            "topic-send-disabled",
            "--dry-run",
            "--state-file",
            str(state_path),
        ]
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "precheck (must be false)" in output
    assert "set topic orders status to SendDisabled" in output
    assert "set topic orders status to Active" in output
    assert "clear state only after verified recovery" in output
    assert "The orders topic was set to SendDisabled" not in output
    assert "ground_truth" not in output
    assert not state_path.exists()


def test_rendered_plan_never_contains_ground_truth(tmp_path: Path) -> None:
    fault = load_catalogue()["subscription-receive-disabled"]

    rendered = render_plan(fault, tmp_path / "state.json")

    assert fault.ground_truth.summary not in rendered
    assert "ground_truth" not in rendered


def test_a_failed_run_prints_the_notes_that_say_what_was_left_behind(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The recovery outcome travels as a note and must reach the operator.

    The runner attaches it with add_note. An f-string never renders __notes__,
    so printing only str(error) discards the one sentence that says whether the
    environment was restored or is still broken.
    """

    def failing_environment(**_):
        error = RuntimeError("fault did not produce its declared symptom")
        error.add_note("recovery is not verified; state retained: symptom remains present")
        raise error

    monkeypatch.setattr(chaos_cli, "production_environment", failing_environment)

    exit_code = chaos_cli.main(
        ["run", "topic-send-disabled", "--state-file", str(tmp_path / "state.json")]
    )
    reported = capsys.readouterr().err

    assert exit_code == 1
    assert "fault did not produce its declared symptom" in reported
    assert "recovery is not verified; state retained" in reported


def test_a_successful_run_reports_how_strongly_the_symptom_presented(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A pass that barely cleared the threshold is not the same as a clear one.

    The runner captures the injection observation and reporting only the
    recovery reading discards it, leaving every successful run looking alike.
    """
    fault = load_catalogue()["topic-send-disabled"]
    result = FaultRunResult(
        fault_id=fault.id,
        status=RunStatus.RECOVERED,
        injected=DetectionResult(
            matched=True, observed=137, description=fault.detects.describe
        ),
        recovery=RecoveryReport(
            observation=DetectionResult(
                matched=False, observed=0, description=fault.detects.describe
            )
        ),
    )

    exit_code = chaos_cli._report(result)
    reported = capsys.readouterr().out

    assert exit_code == 0
    assert "137" in reported
    assert "reverted and healthy" in reported
