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
        "unresolved_findings",
        "proposed_artifact_ids",
        "usage",
        "responder_session_id",
        "investigation_report",
        "error",
        "program_tool_calls",
        "repair_actions",
        "program_runs",
        "plan_applications",
        "response_loops_used",
        "response_loop_exhausted",
        "program_failure_signals",
        "resolution_path",
        "drift_reprompt",
        "incident_id",
        "timing",
        "confirmed_root_causes",
        "applied_playbooks",
        "repair_changes",
        "verification_evidence",
        "drift_events",
    }


def test_incident_schema_carries_the_proto_field_rules() -> None:
    changes = _incident_result_schema()["properties"]["agent_changes"]["items"]

    assert changes["properties"]["at"] == {"type": "string", "format": "date-time"}
    assert changes["properties"]["summary"]["minLength"] == 1


def test_every_incident_result_must_carry_findings_the_agent_authors() -> None:
    schema = _incident_result_schema()
    finding = schema["properties"]["findings"]["items"]

    assert "findings" in schema["required"]
    assert set(finding["properties"]) == {"kind", "statement", "seen_in", "reasoning", "verdict"}
    assert finding["properties"]["kind"]["enum"] == ["observation", "ruled_out"]
    assert "note" not in finding["properties"]


def test_a_minimal_schema_instance_parses_as_the_proto_message() -> None:
    incident = json_format.ParseDict(_minimal_instance(_incident_result_schema()), IncidentResult())

    assert not incident.findings
    assert _minimal_instance(reflection_output_schema()) == {"note": ""}


def test_the_closing_turn_replies_with_one_line() -> None:
    assert _property_names(reflection_output_schema()) == {"note"}
    turn = ReflectionTurn.from_model_output({"note": " restarts the stale process "})
    assert turn.note == "restarts the stale process"
    for bad in ({}, {"note": ""}, {"note": 3}, {"summary": "s", "learning_decision": "updated"}):
        with pytest.raises(ValueError, match="one-line note|extra"):
            ReflectionTurn.from_model_output(bad)


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


@pytest.mark.parametrize("provider", ["codex", "claude"])
def test_no_controller_owned_field_is_reachable_in_provider_schema(provider: str, tmp_path) -> None:
    from sdo.agents.responder.session_turn import session_turn_options
    from sdo.contracts.model_schema import controller_owned

    def check(descriptor: Any, schema: dict[str, Any]) -> None:
        for field in descriptor.fields:
            if controller_owned(field):
                assert field.name not in schema["properties"], field.full_name
            elif field.name in schema["properties"] and field.message_type is not None:
                child = schema["properties"][field.name]
                if field.is_repeated:
                    child = child["items"]
                if "properties" in child:
                    check(field.message_type, child)

    schema = session_turn_options(provider, tmp_path)["output_schema"]
    for branch, message in (("incident_result", IncidentResult), ("reflection", ReflectionResult)):
        check(message.DESCRIPTOR, schema["properties"][branch]["anyOf"][0])


def test_worker_projection_drops_every_annotated_field_recursively() -> None:
    from sdo.contracts.model_schema import controller_owned, worker_result_payload

    def sweep(descriptor: Any, ancestors: frozenset[str] = frozenset()) -> None:
        if descriptor.full_name in ancestors:
            return
        ancestors = ancestors | {descriptor.full_name}
        for field in descriptor.fields:
            for key in {field.name, field.json_name}:
                if controller_owned(field):
                    assert worker_result_payload({key: {"invalid": None}}, descriptor) == {}
                elif field.message_type is not None and not field.message_type.GetOptions().map_entry:
                    sweep(field.message_type, ancestors)
                    nested = {
                        child.name: {"invalid": None} for child in field.message_type.fields if controller_owned(child)
                    }
                    if nested:
                        value = [nested] if field.is_repeated else nested
                        expected = [{}] if field.is_repeated else {}
                        assert worker_result_payload({key: value}, descriptor) == {key: expected}

    sweep(IncidentResult.DESCRIPTOR)
    sweep(ReflectionResult.DESCRIPTOR)


@pytest.mark.parametrize(("field", "value"), [("cause_class", "gremlins"), ("reach", "outside")])
def test_confirmed_root_cause_rejects_values_outside_the_vocabulary(field: str, value: str) -> None:
    from sdo.contracts import ConfirmedRootCause, ObjectRef, validate

    resources = [ObjectRef(api_version="v1", kind="Service", namespace="ns", name="db")]
    validate(ConfirmedRootCause(summary="s", resources=resources))
    validate(ConfirmedRootCause(summary="s", resources=resources, cause_class="network-delay", reach="out-of-reach"))
    cause = ConfirmedRootCause(summary="s", resources=resources)
    setattr(cause, field, value)
    with pytest.raises(ValueError, match=field):
        validate(cause)
