import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Protocol

from chaos.catalogue import (
    Check,
    ComposeStep,
    Fault,
    Step,
    SubscriptionStatusStep,
    TopicStatusStep,
)
from chaos.environment import DetectionResult
from chaos.state import FaultState, FaultStateStore

Sleeper = Callable[[float], Awaitable[None]]
Clock = Callable[[], datetime]


class Environment(Protocol):
    async def detect(self, check: Check) -> DetectionResult: ...

    async def apply(self, step: Step) -> None: ...


class FaultRunnerError(RuntimeError):
    """A fault could not be run without violating a safety guarantee."""


class RecoveryRequired(FaultRunnerError):
    """A previous run still has an outstanding recovery obligation."""


class SymptomAlreadyPresent(FaultRunnerError):
    """The system was not healthy enough to begin a fault run."""


class InjectionNotDetected(FaultRunnerError):
    """The declared injection did not create its expected symptom."""


class RevertFailed(FaultRunnerError):
    """Revert actions or their verification failed."""


class RunStatus(StrEnum):
    RECOVERED = "recovered"
    RECOVERY_PENDING = "recovery_pending"


@dataclass(frozen=True)
class RecoveryReport:
    observation: DetectionResult | None
    failures: tuple[str, ...] = ()

    @property
    def verified(self) -> bool:
        return not self.failures and self.observation is not None and not self.observation.matched


@dataclass(frozen=True)
class FaultRunResult:
    fault_id: str
    status: RunStatus
    injected: DetectionResult | None
    recovery: RecoveryReport


def utc_now() -> datetime:
    return datetime.now(UTC)


def describe_step(step: Step) -> str:
    if isinstance(step, ComposeStep):
        verb = "stop" if step.action == "compose_stop" else "start"
        return f"compose {verb} {step.service} (project swarmscope)"
    if isinstance(step, TopicStatusStep):
        return f"set topic {step.topic} status to {step.status}"
    if isinstance(step, SubscriptionStatusStep):
        return f"set subscription {step.topic}/{step.subscription} status to {step.status}"
    raise TypeError(f"unsupported fault step: {type(step).__name__}")


def render_plan(fault: Fault, state_path: Path) -> str:
    """Render operator-visible behavior without reading or changing runtime state."""
    lines = [
        f"fault: {fault.id}",
        f"state file: {state_path}",
        f"precheck (must be false): {fault.detects.describe}",
        "record recovery obligation",
        "inject:",
    ]
    lines.extend(f"  {index}. {describe_step(step)}" for index, step in enumerate(fault.inject, 1))
    lines.extend(
        [
            f"wait: {fault.settle_seconds} seconds",
            f"injection check (must be true): {fault.detects.describe}",
            "revert:",
        ]
    )
    lines.extend(f"  {index}. {describe_step(step)}" for index, step in enumerate(fault.revert, 1))
    lines.extend(
        [
            f"recovery check (must be false): {fault.detects.describe}",
            "clear state only after verified recovery",
        ]
    )
    return "\n".join(lines) + "\n"


@dataclass
class FaultRunner:
    environment: Environment
    state_store: FaultStateStore
    sleep: Sleeper = asyncio.sleep
    clock: Clock = utc_now

    async def run(self, fault: Fault) -> FaultRunResult:
        outstanding = self.state_store.load()
        if outstanding is not None:
            raise RecoveryRequired(
                f"fault {outstanding.fault_id!r} still requires revert; "
                f"state is at {self.state_store.path}"
            )

        before = await self.environment.detect(fault.detects)
        if before.matched:
            raise SymptomAlreadyPresent(
                f"refusing to inject {fault.id!r}: {before.description} "
                f"is already true (observed {before.observed})"
            )

        self.state_store.record(
            FaultState(
                fault_id=fault.id,
                injection_started_at=self.clock(),
            )
        )

        try:
            for step in fault.inject:
                await self.environment.apply(step)
            await self.sleep(fault.settle_seconds)
            injected = await self.environment.detect(fault.detects)
            if not injected.matched:
                raise InjectionNotDetected(
                    f"fault {fault.id!r} did not produce its declared symptom: "
                    f"{injected.description} observed {injected.observed}"
                )
        except BaseException as error:
            recovery = await self._recover(fault)
            self._finish_failed_run(fault, recovery, error)
            raise

        recovery = await self._recover(fault)
        return self._finish_successful_run(fault, injected, recovery)

    async def revert(self, fault: Fault) -> FaultRunResult:
        outstanding = self.state_store.load()
        if outstanding is None:
            raise RecoveryRequired(f"no fault state exists at {self.state_store.path}")
        if outstanding.fault_id != fault.id:
            raise RecoveryRequired(f"state belongs to {outstanding.fault_id!r}, not {fault.id!r}")

        recovery = await self._recover(fault)
        return self._finish_successful_run(fault, None, recovery)

    async def _recover(self, fault: Fault) -> RecoveryReport:
        failures: list[str] = []
        for step in fault.revert:
            try:
                await self.environment.apply(step)
            except Exception as error:
                failures.append(f"{describe_step(step)} failed: {type(error).__name__}: {error}")

        observation: DetectionResult | None = None
        try:
            observation = await self.environment.detect(fault.detects)
        except Exception as error:
            failures.append(f"recovery check failed: {type(error).__name__}: {error}")
        return RecoveryReport(observation=observation, failures=tuple(failures))

    def _finish_failed_run(
        self,
        fault: Fault,
        recovery: RecoveryReport,
        error: BaseException,
    ) -> None:
        if recovery.verified:
            try:
                self.state_store.clear(fault.id)
            except Exception as state_error:
                error.add_note(
                    f"recovery was healthy but state could not be cleared: {state_error}"
                )
            return

        detail = "; ".join(recovery.failures) or "symptom remains present"
        error.add_note(f"recovery is not verified; state retained: {detail}")

    def _finish_successful_run(
        self,
        fault: Fault,
        injected: DetectionResult | None,
        recovery: RecoveryReport,
    ) -> FaultRunResult:
        if recovery.failures or recovery.observation is None:
            detail = "; ".join(recovery.failures) or "no recovery observation"
            raise RevertFailed(
                f"fault {fault.id!r} could not be safely reverted; state retained: {detail}"
            )

        if recovery.observation.matched:
            return FaultRunResult(
                fault_id=fault.id,
                status=RunStatus.RECOVERY_PENDING,
                injected=injected,
                recovery=recovery,
            )

        self.state_store.clear(fault.id)
        return FaultRunResult(
            fault_id=fault.id,
            status=RunStatus.RECOVERED,
            injected=injected,
            recovery=recovery,
        )
