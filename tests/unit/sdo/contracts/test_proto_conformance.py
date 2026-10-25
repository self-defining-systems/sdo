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

import pytest

from sdo.contracts import OUTCOME_CLASSIFICATIONS, OutcomeClassification
from sdo.contracts import proto as contract_proto
from sdo.contracts.proto import (
    BrokerLedger,
    ControllerState,
    IncidentClosure,
    IncidentRequest,
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
        ("controller_state.json", ControllerState),
        ("broker_ledger.json", BrokerLedger),
    ],
)
def test_proto_schema_models_every_go_fixture(name: str, message_type: type) -> None:
    raw = (GO_FIXTURES / name).read_text(encoding="utf-8")
    # Raises on any field the proto schema does not model.
    parse_json(raw, message_type())


def test_detector_status_validation_includes_unconfirmed_pending() -> None:
    from buf.validate import validate_pb2

    field = contract_proto.DetectorEvaluation.DESCRIPTOR.fields_by_name["status"]
    permitted = getattr(field.GetOptions().Extensions[validate_pb2.field].string, "in")
    assert set(permitted) == {"firing", "clear", "error", "pending"}
