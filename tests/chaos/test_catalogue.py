import logging
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from chaos.catalogue import (
    Fault,
    Symptom,
    load_catalogue,
    reveal_ground_truth,
    symptoms,
)


def a_fault(**overrides) -> dict:
    base = {
        "id": "example-fault",
        "title": "Something is wrong",
        "symptom": "Customers report failures since 14:00.",
        "settle_seconds": 30,
        "inject": [{"action": "topic_status", "topic": "orders", "status": "SendDisabled"}],
        "revert": [{"action": "topic_status", "topic": "orders", "status": "Active"}],
        "detects": {
            "kind": "sql",
            "query": "SELECT count(*) FROM orders.outbox_events",
            "comparison": "gt",
            "threshold": 1,
            "describe": "backlog",
        },
        "ground_truth": {
            "summary": "The topic was disabled.",
            "surface": "broker-send",
            "evidence": ["outbox grows"],
            "forbidden_in_symptom": ["topic"],
        },
    }
    base.update(overrides)
    return base


# --- the catalogue on disk --------------------------------------------------


def test_every_fault_loads_and_validates() -> None:
    catalogue = load_catalogue()

    assert len(catalogue) >= 3
    for fault_id, fault in catalogue.items():
        assert fault.id == fault_id
        assert fault.ground_truth.evidence


def test_file_name_must_match_declared_id(tmp_path: Path) -> None:
    (tmp_path / "wrong-name.yaml").write_text(yaml.safe_dump(a_fault()))

    with pytest.raises(ValueError, match="declares id"):
        load_catalogue(tmp_path)


# --- keeping the answer away from the investigator --------------------------


def test_the_public_view_carries_no_ground_truth() -> None:
    """Widening Symptom is how the answer would leak, so pin its shape.

    Adding a field here should be a deliberate act that fails this test first.
    """
    assert set(Symptom.model_fields) == {"fault_id", "title", "reported"}

    for symptom in symptoms():
        rendered = symptom.model_dump_json().lower()
        for banned in ("ground_truth", "surface", "evidence", "inject", "revert"):
            assert banned not in rendered


def test_a_symptom_that_names_its_cause_is_rejected() -> None:
    with pytest.raises(ValidationError, match="leaks"):
        Fault.model_validate(a_fault(symptom="The orders topic was disabled at 14:00."))


def test_no_shipped_symptom_names_its_own_cause() -> None:
    for fault in load_catalogue().values():
        lowered = fault.public().reported.lower()
        for banned in fault.ground_truth.forbidden_in_symptom:
            assert banned.lower() not in lowered, f"{fault.id} leaks {banned!r}"


def test_reveal_ground_truth_returns_the_answer_and_says_so(
    caplog: pytest.LogCaptureFixture,
) -> None:
    fault = load_catalogue()["topic-send-disabled"]

    with caplog.at_level(logging.WARNING):
        truth = reveal_ground_truth(fault)

    assert truth.surface == "broker-send"
    assert "ground truth revealed for topic-send-disabled" in caplog.text


# --- reverts ---------------------------------------------------------------


def test_a_disabled_entity_that_is_never_restored_is_rejected() -> None:
    with pytest.raises(ValidationError, match="never set back to Active"):
        Fault.model_validate(
            a_fault(
                revert=[{"action": "topic_status", "topic": "orders", "status": "SendDisabled"}]
            )
        )


def test_a_stopped_service_that_is_never_restarted_is_rejected() -> None:
    with pytest.raises(ValidationError, match="never started"):
        Fault.model_validate(
            a_fault(
                inject=[{"action": "compose_stop", "service": "postgres"}],
                revert=[{"action": "compose_stop", "service": "postgres"}],
            )
        )


def test_every_shipped_fault_restores_what_it_breaks() -> None:
    """Validation runs on load, so this asserts the catalogue was checked."""
    catalogue = load_catalogue()

    for fault in catalogue.values():
        assert fault.revert, f"{fault.id} has no revert"
        assert Fault.model_validate(fault.model_dump()) is not None


# --- the investigation has to be real ---------------------------------------


def test_the_two_broker_faults_present_the_same_way() -> None:
    """If the symptom gave the answer away, there would be nothing to investigate."""
    catalogue = load_catalogue()
    send = catalogue["topic-send-disabled"].public()
    receive = catalogue["subscription-receive-disabled"].public()

    assert send.reported == receive.reported
    assert send.title == receive.title


def test_but_their_evidence_differs() -> None:
    """Same complaint, different traces. That is what makes it solvable."""
    catalogue = load_catalogue()
    send = set(catalogue["topic-send-disabled"].ground_truth.evidence)
    receive = set(catalogue["subscription-receive-disabled"].ground_truth.evidence)

    assert send.isdisjoint(receive)
    assert (
        catalogue["topic-send-disabled"].ground_truth.surface
        != catalogue["subscription-receive-disabled"].ground_truth.surface
    )


def test_every_fault_is_detected_differently() -> None:
    catalogue = load_catalogue()
    described = [f.detects.describe for f in catalogue.values()]

    assert len(set(described)) == len(described), "two faults share a detection check"


def test_settle_is_exposed_as_a_duration() -> None:
    fault = load_catalogue()["database-unavailable"]

    assert fault.settle.total_seconds() == fault.settle_seconds
