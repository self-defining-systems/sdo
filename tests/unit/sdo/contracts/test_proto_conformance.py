"""Conformance ratchet for the canonical Python protobuf schema.

The proto messages in ``sdo/contracts/proto/sdodev/contracts/v1alpha1`` are the single schema
SOURCE for the controller<->responder tree, and the generated Python messages are what
the responder, reflection worker, and status command use directly. This test proves they
parse every value the Go controller writes, via the golden fixtures under
``tests/fixtures/sdo/contracts/go/`` (pinned by the Go ``TestGoContractFixtures``).

``json_format.Parse`` rejects unknown fields, so a field the Go encoding grows
without the proto schema fails here: drift is a test failure, not a run-time
surprise ten minutes into an experiment. The Go conformance test
(``sdo/operator/runtime/contract_conformance_test.go``) additionally checks the
protovalidate CEL mirrors the hand-written invariants.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from sdo.agents.responder.codex import _incident_result_schema
from sdo.contracts import OUTCOME_CLASSIFICATIONS, OutcomeClassification
from sdo.contracts import proto as contract_proto
from sdo.contracts.proto import (
    BrokerLedger,
    ControllerState,
    IncidentClosure,
    IncidentRequest,
    IncidentResult,
    IncidentView,
    OutcomeRecord,
    parse_json,
    to_canonical_json,
)

GO_FIXTURES = Path(__file__).resolve().parents[3] / "fixtures" / "sdo" / "contracts" / "go"


def test_outcomes_are_typed_across_the_reflection_boundary() -> None:
    assert hasattr(contract_proto, "OutcomeRecord")
    fields = contract_proto.ReflectionRequest.DESCRIPTOR.fields_by_name
    assert fields["outcome"].message_type.full_name == "sdodev.contracts.v1alpha1.OutcomeRecord"
    assert fields["prior_outcomes"].message_type.full_name == "sdodev.contracts.v1alpha1.OutcomeRecord"


def test_outcome_classification_is_read_from_proto_validation() -> None:
    from buf.validate import validate_pb2

    field = OutcomeRecord.DESCRIPTOR.fields_by_name["classification"]
    permitted = getattr(field.GetOptions().Extensions[validate_pb2.field].string, "in")
    assert set(OUTCOME_CLASSIFICATIONS) == set(permitted)
    assert vars(OutcomeClassification) == {value.upper(): value for value in permitted}


def test_outcome_round_trips_through_canonical_contract() -> None:
    record = OutcomeRecord(schema_version=1, incident_id="incident-1", classification="success")
    encoded = parse_json(to_canonical_json(record), OutcomeRecord())
    assert encoded.incident_id == record.incident_id
    assert encoded.schema_version == 1


@pytest.mark.parametrize(
    ("name", "message_type"),
    [
        ("incident_request.json", IncidentRequest),
        ("incident_view_empty.json", IncidentView),
        ("incident_view_open.json", IncidentView),
        ("incident_view_populated.json", IncidentView),
        ("closure_repaired.json", IncidentClosure),
        ("closure_own_edit.json", IncidentClosure),
        ("controller_state.json", ControllerState),
        ("broker_ledger.json", BrokerLedger),
    ],
)
def test_proto_schema_models_every_go_fixture(name: str, message_type: type) -> None:
    raw = (GO_FIXTURES / name).read_text(encoding="utf-8")
    # Raises on any field the proto schema does not model.
    parse_json(raw, message_type())


def _assert_schema_fields_exist(schema: dict[str, Any], descriptor: Any, path: str) -> None:
    """Every property of the model-output schema names a field of the proto message, recursively."""

    for name, spec in schema["properties"].items():
        field = descriptor.fields_by_name.get(name)
        assert field is not None, f"{path}.{name} is not a field of {descriptor.full_name}"
        child = spec["items"] if spec.get("type") == "array" else spec
        if child.get("type") == "object" and "properties" in child:
            assert field.message_type is not None, f"{path}.{name} is not a message field"
            _assert_schema_fields_exist(child, field.message_type, f"{path}.{name}")


def test_model_output_schema_is_pinned_to_the_incident_result_descriptor() -> None:
    schema = _incident_result_schema()

    _assert_schema_fields_exist(schema, IncidentResult.DESCRIPTOR, "$")
    # The model supplies everything except what the worker owns: schema version, usage, session, error, late pull.
    owned = {"schema_version", "usage", "responder_session_id", "error", "late_findings_pull"}
    unrequested = set(IncidentResult.DESCRIPTOR.fields_by_name) - set(schema["properties"]) - owned
    assert unrequested == {"final_detector_states", "proposed_artifact_ids"}
    assert set(schema["required"]) == set(schema["properties"])
