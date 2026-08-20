import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

DEFAULT_STATE_PATH = Path(__file__).resolve().parents[1] / ".swarmscope-chaos-state.json"


class FaultState(BaseModel):
    """A recovery obligation left before fault injection begins.

    Presence means the fault's complete revert must be applied and verified.
    It does not claim that every injection step finished successfully.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1] = 1
    fault_id: str = Field(pattern=r"^[a-z][a-z0-9-]{2,48}[a-z0-9]$")
    injection_started_at: AwareDatetime


class FaultStateError(RuntimeError):
    """The recovery state cannot be safely created or cleared."""


def _sync_directory(path: Path) -> None:
    directory_fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


@dataclass(frozen=True)
class FaultStateStore:
    path: Path = DEFAULT_STATE_PATH

    def load(self) -> FaultState | None:
        try:
            payload = self.path.read_bytes()
        except FileNotFoundError:
            return None
        return FaultState.model_validate_json(payload)

    def record(self, state: FaultState) -> None:
        """Create state atomically without replacing an earlier recovery record."""
        payload = (state.model_dump_json(indent=2) + "\n").encode()
        temporary_fd, temporary_name = tempfile.mkstemp(
            prefix=f".{self.path.name}.",
            suffix=".tmp",
            dir=self.path.parent,
        )
        temporary_path = Path(temporary_name)

        try:
            os.fchmod(temporary_fd, 0o600)
            with os.fdopen(temporary_fd, "wb") as handle:
                temporary_fd = -1
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())

            try:
                os.link(temporary_path, self.path)
            except FileExistsError as error:
                raise FaultStateError(
                    f"fault state already exists at {self.path}; "
                    "run revert before injecting another fault"
                ) from error

            _sync_directory(self.path.parent)
        finally:
            if temporary_fd != -1:
                os.close(temporary_fd)
            temporary_path.unlink(missing_ok=True)

    def clear(self, expected_fault_id: str) -> None:
        """Clear state after the caller has verified successful recovery."""
        current = self.load()
        if current is None:
            raise FaultStateError(f"no fault state exists at {self.path}")
        if current.fault_id != expected_fault_id:
            raise FaultStateError(
                f"state belongs to {current.fault_id!r}, not {expected_fault_id!r}"
            )

        try:
            self.path.unlink()
        except FileNotFoundError as error:
            raise FaultStateError(f"fault state changed while clearing {self.path}") from error

        _sync_directory(self.path.parent)
