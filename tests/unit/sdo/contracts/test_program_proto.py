"""Program messages crossing Go and Python use the shared ProtoJSON schema."""

from __future__ import annotations

import pytest
from google.protobuf import duration_pb2, json_format, struct_pb2

from sdo.contracts import proto as contracts


def test_program_messages_round_trip_as_protojson() -> None:
    messages = [
        contracts.ProgramSpec(
            id="service-targetport-mismatch",
            params=[contracts.ParamSpec(name="service", kind="Service", required=True)],
        ),
        contracts.Binding(name="service", object=contracts.ObjectRef(kind="Service", namespace="app", name="frontend")),
        contracts.Verdict(result=contracts.Verdict.RESULT_NEEDS_JUDGEMENT, question="Which port is intended?"),
        contracts.Plan(
            actions=[
                contracts.Action(
                    reconcile_from_source=contracts.ReconcileFromSource(
                        ref=contracts.ObjectRef(kind="Service", name="frontend")
                    )
                ),
                contracts.Action(
                    rollback=contracts.Rollback(
                        ref=contracts.ObjectRef(kind="Service", name="frontend"), to_revision="abc123"
                    )
                ),
                contracts.Action(
                    set_field=contracts.SetField(
                        ref=contracts.ObjectRef(kind="Service", name="frontend"),
                        path="spec.ports[0].targetPort",
                        value=struct_pb2.Value(number_value=8080),
                        reason="restore source port",
                    )
                ),
                contracts.Action(
                    restart_rollout=contracts.RestartRollout(
                        ref=contracts.ObjectRef(kind="Deployment", name="frontend"), reason="reload configuration"
                    )
                ),
            ],
            rationale="restore service",
        ),
        contracts.ProgramRun(
            program_id="p",
            verdict=contracts.Verdict(result=contracts.Verdict.RESULT_UNKNOWN),
            duration=duration_pb2.Duration(seconds=1),
        ),
        contracts.PlanApplication(plan=contracts.Plan(), dry_run=True),
    ]
    for message in messages:
        encoded = contracts.to_canonical_json(message)
        decoded = contracts.parse_json(encoded, type(message)())
        assert decoded == message


@pytest.mark.parametrize(
    ("message_type", "payload"),
    [
        ("ProgramSpec", '{"id":"p","unmodeled":true}'),
        ("Verdict", '{"result":"NOT_A_RESULT"}'),
        ("Action", '{"rollback":{},"restart_rollout":{}}'),
    ],
)
def test_program_protojson_rejects_invalid_wire_shapes(message_type: str, payload: str) -> None:
    with pytest.raises(json_format.ParseError):
        contracts.parse_json(payload, getattr(contracts, message_type)())


def test_program_validation_rules_are_in_python_descriptors() -> None:
    from buf.validate import validate_pb2

    id_rule = contracts.ProgramSpec.DESCRIPTOR.fields_by_name["id"].GetOptions().Extensions[validate_pb2.field]
    assert id_rule.string.min_len == 1

    result_rule = contracts.Verdict.DESCRIPTOR.fields_by_name["result"].GetOptions().Extensions[validate_pb2.field]
    assert result_rule.enum.defined_only
    assert list(result_rule.enum.not_in) == [0]

    verdict_rules = contracts.Verdict.DESCRIPTOR.GetOptions().Extensions[validate_pb2.message]
    assert any(rule.id == "verdict.question_for_judgement" for rule in verdict_rules.cel)

    action_rule = contracts.Action.DESCRIPTOR.oneofs_by_name["kind"].GetOptions().Extensions[validate_pb2.oneof]
    assert action_rule.required
