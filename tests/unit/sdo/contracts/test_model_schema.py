"""Model-facing output schemas are generated from the proto messages Go loads."""

from __future__ import annotations

from typing import Any

import pytest
from google.protobuf import json_format

from sdo.agents.responder.codex import _incident_result_schema
from sdo.agents.responder.reflection import ReflectionTurn, reflection_output_schema
from sdo.contracts import IncidentResult, ReflectionResult
from sdo.contracts.model_schema import model_output_schema


def _minimal_instance(schema: dict[str, Any]) -> Any:
    """The smallest document the schema accepts."""

    kind = schema["type"]
    if kind == "object":
        return {name: _minimal_instance(child) for name, child in schema["properties"].items()}
    if kind == "array":
        return [_minimal_instance(schema["items"])] * schema.get("minItems", 0)
    if "enum" in schema:
        return schema["enum"][0]
    if kind == "string":
        if schema.get("format") == "date-time":
            return "2026-10-22T00:00:00Z"
        return "x" * schema.get("minLength", 0)
    return {"boolean": False, "integer": 0, "number": 0.0}[kind]


def _property_names(schema: dict[str, Any]) -> set[str]:
    return set(schema["properties"])


def test_incident_schema_is_the_proto_message_minus_declared_omissions() -> None:
    schema = _incident_result_schema()
    descriptor = IncidentResult.DESCRIPTOR

    assert _property_names(schema) <= set(descriptor.fields_by_name)
    assert set(schema["required"]) == _property_names(schema)
    assert schema["additionalProperties"] is False
    # A new IncidentResult field is requested from the model unless it is deliberately omitted.
    unrequested = set(descriptor.fields_by_name) - _property_names(schema)
    assert unrequested == {
        "schema_version",
        "final_detector_states",
        "proposed_artifact_ids",
        "usage",
        "responder_session_id",
        "error",
        "late_findings_pull",
        "program_tool_calls",
        "program_runs",
        "plan_applications",
        "response_loops_used",
        "response_loop_exhausted",
        "program_failure_signals",
        "resolution_path",
        "fast_path_programs",
        "fast_path_skipped_reason",
        "fast_path_relation_covered_findings",
    }


def test_incident_schema_carries_the_proto_field_rules() -> None:
    schema = _incident_result_schema()
    cause = schema["properties"]["confirmed_root_causes"]["items"]
    evidence = cause["properties"]["evidence"]["items"]

    assert schema["properties"]["status"]["enum"] == ["completed", "failed", "cancelled"]
    assert set(evidence["properties"]["kind"]["enum"]) == {
        "detector-finding",
        "synthetic-traffic",
        "state-change",
        "live-observation",
    }
    assert evidence["properties"]["source"]["minLength"] == 1
    assert cause["properties"]["resources"]["minItems"] == 1
    assert schema["properties"]["timing"]["properties"]["started_at"] == {"type": "string", "format": "date-time"}


def test_a_minimal_schema_instance_parses_as_the_proto_message() -> None:
    incident = json_format.ParseDict(_minimal_instance(_incident_result_schema()), IncidentResult())
    reflection = json_format.ParseDict(_minimal_instance(reflection_output_schema()), ReflectionResult())

    assert incident.status == "completed"
    assert reflection.learning_decision == "updated"


def test_reflection_schema_names_the_fields_go_loads_as_the_decision_file() -> None:
    schema = reflection_output_schema()

    assert _property_names(schema) == {
        "summary",
        "learning_decision",
        "no_change_reason",
        "proposed_artifact_ids",
        "sdk_gaps",
    }
    # The deprecated proposal message is never requested from the model.
    assert "artifact_proposal" in ReflectionResult.DESCRIPTOR.fields_by_name


def test_reflection_reply_in_the_old_turn_vocabulary_is_rejected() -> None:
    with pytest.raises(ValueError, match="playbook_ids"):
        ReflectionTurn.from_model_output({"summary": "s", "learning_decision": "updated", "playbook_ids": ["a"]})


def test_reflection_reply_is_validated_by_the_proto_rules() -> None:
    no_reason = {"summary": "s", "learning_decision": "no_change", "proposed_artifact_ids": []}
    with pytest.raises(ValueError, match="no_change_reason|concrete reason"):
        ReflectionTurn.from_model_output(no_reason)
    gap = {"fault_class": "", "primitive": "p", "reason": "r", "scratch_script": "s"}
    with pytest.raises(ValueError, match="fault_class"):
        ReflectionTurn.from_model_output(
            {"summary": "s", "learning_decision": "sdk_gap", "no_change_reason": "r", "sdk_gaps": [gap]}
        )
    turn = ReflectionTurn.from_model_output(
        {"summary": "s", "learning_decision": "updated", "proposed_artifact_ids": ["a-b"]}
    )
    assert turn.playbook_ids == ["a-b"]


def test_unknown_omission_is_an_error_not_silently_ignored() -> None:
    with pytest.raises(ValueError, match="no fields"):
        model_output_schema(ReflectionResult.DESCRIPTOR, omit={"sdodev.contracts.v1alpha1.ReflectionResult": ("nope",)})
    with pytest.raises(ValueError, match="not in the schema"):
        model_output_schema(ReflectionResult.DESCRIPTOR, omit={"sdodev.contracts.v1alpha1.Finding": ("summary",)})


def test_fields_with_no_model_form_must_be_omitted() -> None:
    from sdo.contracts import AppliedPlaybook

    with pytest.raises(ValueError, match="map"):
        model_output_schema(AppliedPlaybook.DESCRIPTOR)
    schema = model_output_schema(
        AppliedPlaybook.DESCRIPTOR, omit={AppliedPlaybook.DESCRIPTOR.full_name: ("parameter_bindings",)}
    )
    assert _property_names(schema) == {"playbook_id", "script_names"}
