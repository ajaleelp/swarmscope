from datetime import UTC, datetime
from pathlib import Path
from stat import S_IMODE

import pytest
from pydantic import ValidationError

from chaos.state import FaultState, FaultStateError, FaultStateStore

INJECTION_STARTED_AT = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)


def a_state(fault_id: str = "topic-send-disabled") -> FaultState:
    return FaultState(
        fault_id=fault_id,
        injection_started_at=INJECTION_STARTED_AT,
    )


def test_state_schema_contains_only_recovery_metadata() -> None:
    assert set(FaultState.model_fields) == {
        "schema_version",
        "fault_id",
        "injection_started_at",
    }


def test_state_round_trips_with_owner_only_permissions(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    store = FaultStateStore(state_path)
    state = a_state()

    store.record(state)

    assert store.load() == state
    assert S_IMODE(state_path.stat().st_mode) == 0o600


def test_injection_timestamp_must_include_a_timezone() -> None:
    with pytest.raises(ValidationError):
        FaultState(
            fault_id="topic-send-disabled",
            injection_started_at=datetime(2026, 8, 20, 12, 0),
        )


def test_existing_state_cannot_be_overwritten(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    store = FaultStateStore(state_path)
    original = a_state()
    store.record(original)

    with pytest.raises(FaultStateError, match="already exists"):
        store.record(a_state("database-unavailable"))

    assert store.load() == original
    assert set(tmp_path.iterdir()) == {state_path}


def test_wrong_fault_cannot_clear_state(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    store = FaultStateStore(state_path)
    original = a_state()
    store.record(original)

    with pytest.raises(FaultStateError, match="state belongs to"):
        store.clear("database-unavailable")

    assert store.load() == original


def test_matching_fault_clears_state(tmp_path: Path) -> None:
    store = FaultStateStore(tmp_path / "state.json")
    store.record(a_state())

    store.clear("topic-send-disabled")

    assert store.load() is None


def test_malformed_state_is_reported_and_preserved(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    state_path.write_text("{not-json")
    store = FaultStateStore(state_path)

    with pytest.raises(ValidationError):
        store.load()

    assert state_path.read_text() == "{not-json"
