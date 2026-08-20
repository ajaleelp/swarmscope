import logging
from datetime import timedelta
from pathlib import Path
from typing import Annotated, Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

logger = logging.getLogger(__name__)

FAULTS_DIRECTORY = Path(__file__).resolve().parent / "faults"
EntityStatus = Literal["Active", "Disabled", "SendDisabled", "ReceiveDisabled"]


class SqlCheck(BaseModel):
    """Detect a symptom by counting rows."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: Literal["sql"]
    query: str
    comparison: Literal["gt", "lt"]
    threshold: float
    describe: str


class HttpCheck(BaseModel):
    """Detect a symptom by calling the API and reading the status."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: Literal["http"]
    method: Literal["GET", "POST"] = "POST"
    path: str
    body: dict[str, Any] | None = None
    status_at_least: int
    describe: str


Check = Annotated[SqlCheck | HttpCheck, Field(discriminator="kind")]


class ComposeStep(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    action: Literal["compose_stop", "compose_start"]
    service: str


class TopicStatusStep(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    action: Literal["topic_status"]
    topic: str
    status: EntityStatus


class SubscriptionStatusStep(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    action: Literal["subscription_status"]
    topic: str
    subscription: str
    status: EntityStatus


Step = Annotated[
    ComposeStep | TopicStatusStep | SubscriptionStatusStep,
    Field(discriminator="action"),
]


class GroundTruth(BaseModel):
    """The answer. Reachable only through reveal_ground_truth."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    summary: str
    surface: Literal["database", "broker-send", "broker-receive", "application"]
    evidence: list[str] = Field(min_length=1)
    forbidden_in_symptom: list[str] = Field(default_factory=list)


class Symptom(BaseModel):
    """Everything an investigator is permitted to know.

    Deliberately has no field capable of carrying the cause. Widening this model
    is how ground truth would leak, so the test suite asserts its shape.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    fault_id: str
    title: str
    reported: str


class Fault(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    id: str = Field(pattern=r"^[a-z][a-z0-9-]{2,48}[a-z0-9]$")
    title: str
    symptom: str
    settle_seconds: int = Field(ge=1, le=600)
    inject: list[Step] = Field(min_length=1)
    revert: list[Step] = Field(min_length=1)
    detects: Check
    ground_truth: GroundTruth

    @property
    def settle(self) -> timedelta:
        return timedelta(seconds=self.settle_seconds)

    def public(self) -> Symptom:
        """What an investigator is given. Carries no path to the cause."""
        return Symptom(fault_id=self.id, title=self.title, reported=self.symptom)

    @model_validator(mode="after")
    def symptom_must_not_name_the_cause(self) -> "Fault":
        lowered = self.symptom.lower()
        leaked = [w for w in self.ground_truth.forbidden_in_symptom if w.lower() in lowered]
        if leaked:
            raise ValueError(f"{self.id}: symptom leaks {leaked}")
        return self

    @model_validator(mode="after")
    def revert_must_restore_every_disabled_entity(self) -> "Fault":
        """A fault that disables something must re-enable it.

        Left to review alone, this is the mistake that gets made once and leaves
        a namespace broken for a week.
        """
        restored = {
            (getattr(s, "topic", None), getattr(s, "subscription", None))
            for s in self.revert
            if getattr(s, "status", None) == "Active"
        }
        restarted = {s.service for s in self.revert if s.action == "compose_start"}

        for step in self.inject:
            status = getattr(step, "status", None)
            if status is not None and status != "Active":
                key = (getattr(step, "topic", None), getattr(step, "subscription", None))
                if key not in restored:
                    raise ValueError(
                        f"{self.id}: {key} is disabled but never set back to Active"
                    )
            if step.action == "compose_stop" and step.service not in restarted:
                raise ValueError(f"{self.id}: {step.service} is stopped but never started")
        return self


def load_catalogue(directory: Path = FAULTS_DIRECTORY) -> dict[str, Fault]:
    """Load every fault, keyed by id. Raises if a file disagrees with its name."""
    catalogue: dict[str, Fault] = {}
    for path in sorted(directory.glob("*.yaml")):
        fault = Fault.model_validate(yaml.safe_load(path.read_text()))
        if fault.id != path.stem:
            raise ValueError(f"{path.name} declares id {fault.id!r}")
        catalogue[fault.id] = fault
    return catalogue


def symptoms(directory: Path = FAULTS_DIRECTORY) -> list[Symptom]:
    """The investigator-facing view of the catalogue."""
    return [fault.public() for fault in load_catalogue(directory).values()]


def reveal_ground_truth(fault: Fault) -> GroundTruth:
    """Return the answer, for scoring only.

    Separated and logged so a call site that should not have it is visible in
    review and in the log, rather than being an ordinary attribute access.
    """
    logger.warning("ground truth revealed for %s", fault.id)
    return fault.ground_truth
