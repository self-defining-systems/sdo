"""Agent-view conformance ratchet for the canonical Python protobuf schema.

The proto messages in ``sdo/contracts/proto/sdodev/contracts/v1alpha1`` are the single schema
SOURCE for the controller<->responder tree. This test proves the generated
Python proto types faithfully model BOTH producers of the wire:

- the Go controller, via the golden fixtures under
  ``tests/fixtures/sdo/contracts/go/`` (pinned by the Go
  ``TestGoContractFixtures``), and
- the agent-local Pydantic views in ``sdo.agents.views``, via their JSON serialization.

``json_format.Parse`` rejects unknown fields, so a field either definition grows
without the proto schema fails here — drift is a test failure, not a run-time
surprise ten minutes into an experiment. The Go conformance test
(``sdo/operator/runtime/contract_conformance_test.go``) additionally checks the
protovalidate CEL mirrors the hand-written invariants.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from sdo.agents.views import (
    IncidentRequest as PydIncidentRequest,
)
from sdo.agents.views import (
    IncidentResult as PydIncidentResult,
)
from sdo.agents.views import (
    IncidentView as PydIncidentView,
)
from sdo.contracts import OutcomeClassification
from sdo.contracts import proto as contract_proto
from sdo.contracts.proto import (
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


def test_outcome_classification_type_matches_proto_validation() -> None:
    from buf.validate import validate_pb2

    field = OutcomeRecord.DESCRIPTOR.fields_by_name["classification"]
    permitted = getattr(field.GetOptions().Extensions[validate_pb2.field].string, "in")
    assert {item.value for item in OutcomeClassification} == set(permitted)


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
    ],
)
def test_proto_schema_models_every_go_fixture(name: str, message_type: type) -> None:
    raw = (GO_FIXTURES / name).read_text(encoding="utf-8")
    # Raises on any field the proto schema does not model.
    parse_json(raw, message_type())


def test_proto_schema_models_pydantic_request_serialization() -> None:
    # Load a real request through the strict Pydantic model, re-serialize it the
    # way Python writes the wire, and parse that into the proto type.
    raw = json.loads((GO_FIXTURES / "incident_request.json").read_text(encoding="utf-8"))
    pyd = PydIncidentRequest.model_validate(raw)
    parse_json(pyd.model_dump_json(), IncidentRequest())


def test_proto_schema_models_pydantic_view_serialization() -> None:
    raw = json.loads((GO_FIXTURES / "incident_view_populated.json").read_text(encoding="utf-8"))
    pyd = PydIncidentView.model_validate(raw)
    parse_json(pyd.model_dump_json(), IncidentView())


def test_proto_schema_models_pydantic_result_serialization() -> None:
    # IncidentResult is produced by the Python responder; no standalone Go
    # fixture carries it, so build a minimal valid instance via the Pydantic
    # model and confirm the proto type reads its serialization.
    pyd = PydIncidentResult.model_validate(
        {
            "incident_id": "demo-1",
            "status": "completed",
            "usage": {"llm_calls": 1, "input_tokens": 10, "output_tokens": 4, "reasoning_output_tokens": 1},
            "timing": {"started_at": "1970-01-01T00:00:01Z", "completed_at": "1970-01-01T00:00:02Z"},
            "fast_path_relation_covered_findings": [
                {
                    "finding_fingerprint": "health/service/api",
                    "program_id": "repair",
                    "bindings": [
                        {"name": "workload", "object": {"kind": "Deployment", "namespace": "app", "name": "api"}}
                    ],
                }
            ],
            "repair_actions": [
                {
                    "action_id": "a1",
                    "kind": "kubectl",
                    "target": "Service/frontend",
                    "summary": "s",
                    "details": "d",
                    "started_at": "1970-01-01T00:00:01Z",
                    "completed_at": "1970-01-01T00:00:02Z",
                    "success": True,
                    "reversible": True,
                }
            ],
        }
    )
    result = parse_json(pyd.model_dump_json(), IncidentResult())
    assert result.incident_id == "demo-1"
    assert result.repair_actions[0].action_id == "a1"
    assert result.fast_path_relation_covered_findings[0].bindings[0].object.name == "api"
