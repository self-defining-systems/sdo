"""The responder's model-output edge: generated messages plus the invariants they cannot carry."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from google.protobuf import json_format

from sdo.agents.responder.result_output import normalize_usage, parse_incident_result, validate_incident_result
from sdo.contracts import IncidentRequest, IncidentResult, UsageMetrics, parse_json, to_canonical_json, validate

FIXTURE_DIR = Path(__file__).resolve().parents[4] / "fixtures" / "sdo" / "contracts"
USAGE = {"llm_calls": 1, "input_tokens": 10, "output_tokens": 4, "reasoning_output_tokens": 1}


def _fixture(name: str) -> dict[str, Any]:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def _model_reply() -> dict[str, Any]:
    """The fixture result as the model would return it: no worker-owned fields."""

    reply = _fixture("incident_result.json")
    for owned in ("schema_version", "usage", "responder_session_id", "error", "late_findings_pull"):
        reply.pop(owned, None)
    return reply


@pytest.mark.parametrize(
    ("fixture_name", "message_type"),
    [
        ("incident_request.json", IncidentRequest),
        ("incident_request_state_changes.json", IncidentRequest),
        ("incident_result.json", IncidentResult),
    ],
)
def test_contract_golden_fixture_round_trip(fixture_name: str, message_type: type) -> None:
    text = (FIXTURE_DIR / fixture_name).read_text(encoding="utf-8")

    message = parse_json(text, message_type())

    assert parse_json(to_canonical_json(message), message_type()) == message


def test_contract_messages_reject_unknown_and_legacy_fields() -> None:
    payload = _fixture("incident_request.json")
    payload["unexpected"] = True
    with pytest.raises(json_format.ParseError, match="unexpected"):
        json_format.ParseDict(payload, IncidentRequest())

    payload = _fixture("incident_request.json")
    payload["findings"][0]["playbooks"] = payload["findings"][0].pop("playbook_ids", [])
    with pytest.raises(json_format.ParseError, match="playbooks"):
        json_format.ParseDict(payload, IncidentRequest())


def test_a_model_reply_becomes_the_canonical_result() -> None:
    result = parse_incident_result(_model_reply(), USAGE)

    assert result.schema_version == "sdo.dev/v1alpha1"
    assert result.usage.input_tokens == 10
    assert [item.kind for item in result.confirmed_root_causes[0].evidence] == ["detector-finding", "live-observation"]
    assert list(result.confirmed_root_causes[0].explained_detectors) == ["missing-configmap"]


def test_root_cause_evidence_must_be_live_and_static_artifacts_are_only_context() -> None:
    reply = _model_reply()
    reply["confirmed_root_causes"][0]["evidence"] = [
        {"kind": "static-artifact", "source": "arch.md", "observation": "x"}
    ]
    with pytest.raises(ValueError, match=r"evidence\.kind"):
        parse_incident_result(reply, USAGE)

    reply["confirmed_root_causes"][0]["evidence"] = [{"kind": "state-change", "source": "Service/x"}]
    with pytest.raises(ValueError, match=r"evidence\.observation"):
        parse_incident_result(reply, USAGE)


def test_the_reply_may_not_carry_fields_outside_the_contract() -> None:
    reply = _model_reply()
    reply["notes"] = "extra"

    with pytest.raises(ValueError, match="notes"):
        parse_incident_result(reply, USAGE)


def test_a_result_needs_an_incident_status_and_its_accounting() -> None:
    reply = _model_reply()
    reply["status"] = "done"
    with pytest.raises(ValueError, match="status"):
        parse_incident_result(reply, USAGE)

    with pytest.raises(ValueError, match="timing"):
        validate_incident_result(IncidentResult(schema_version="sdo.dev/v1alpha1", incident_id="i", status="failed"))


def test_incident_result_rejects_duplicate_or_non_chronological_repair_actions() -> None:
    reply = _model_reply()
    reply["repair_actions"].append(dict(reply["repair_actions"][0]))
    with pytest.raises(ValueError, match="repair action IDs must be unique"):
        parse_incident_result(reply, USAGE)

    reply["repair_actions"] = [reply["repair_actions"][0]]
    reply["repair_actions"][0]["completed_at"] = "2026-07-09T17:59:00Z"
    with pytest.raises(ValueError, match="completed_at must not be before started_at"):
        parse_incident_result(reply, USAGE)


def test_the_worker_owned_usage_is_validated_with_the_result() -> None:
    with pytest.raises(ValueError, match="reasoning_output_tokens"):
        parse_incident_result(_model_reply(), {**USAGE, "reasoning_output_tokens": 5})


class TestUsageMetrics:
    def _usage(self, **fields: int) -> UsageMetrics:
        return json_format.ParseDict(
            {"llm_calls": 1, "input_tokens": 1_000, "output_tokens": 80, **fields}, UsageMetrics()
        )

    def test_a_normalized_breakdown_validates_and_aliases_cached_input(self) -> None:
        usage = self._usage(
            cache_read_input_tokens=600,
            cache_write_input_tokens=300,
            cache_write_1h_input_tokens=100,
            uncached_input_tokens=100,
            reasoning_output_tokens=20,
            model_requests=7,
        )

        normalize_usage(usage)

        assert usage.cache_read_input_tokens == 600
        assert usage.cached_input_tokens == 600

    def test_a_pre_split_claude_record_still_validates(self) -> None:
        # Before agentshim 0.7, cached_input_tokens counted cache writes too.
        usage = json_format.ParseDict(
            {
                "llm_calls": 4,
                "input_tokens": 800,
                "output_tokens": 75,
                "cached_input_tokens": 300,
                "cache_write_input_tokens": 100,
            },
            UsageMetrics(),
        )

        normalize_usage(usage)

        assert not usage.HasField("cache_read_input_tokens")

    @pytest.mark.parametrize(
        ("fields", "match"),
        [
            ({"cached_input_tokens": 600, "cache_read_input_tokens": 500}, "alias"),
            ({"cache_read_input_tokens": 800, "cache_write_input_tokens": 300}, "exceed input_tokens"),
            ({"cache_read_input_tokens": 600, "uncached_input_tokens": 1}, "uncached_input_tokens"),
            (
                {"cache_read_input_tokens": 0, "cache_write_input_tokens": 10, "cache_write_1h_input_tokens": 11},
                "cache_write_1h_input_tokens",
            ),
            ({"reasoning_output_tokens": 81}, "reasoning_output_tokens"),
        ],
    )
    def test_an_inconsistent_breakdown_is_rejected(self, fields: dict[str, int], match: str) -> None:
        usage = self._usage(**fields)
        normalize_usage(usage)
        with pytest.raises(ValueError, match=match):
            validate(usage)


def test_state_change_source_must_name_exactly_one_object() -> None:
    reply = _model_reply()
    reply["confirmed_root_causes"][0]["evidence"] = [
        {"kind": "state-change", "source": "Service/geo; Service/recommendation", "observation": "ports changed"}
    ]
    with pytest.raises(ValueError, match=r"exactly one object reference.*own evidence entry"):
        parse_incident_result(reply, USAGE)


@pytest.mark.parametrize("source", ["Service/geo", "Service/hotel/geo"])
def test_state_change_source_accepts_one_object_reference(source: str) -> None:
    reply = _model_reply()
    reply["confirmed_root_causes"][0]["evidence"] = [
        {"kind": "state-change", "source": source, "observation": "port changed"}
    ]
    assert parse_incident_result(reply, USAGE).confirmed_root_causes[0].evidence[0].source == source


def test_two_object_cause_cites_each_object_in_its_own_entry() -> None:
    reply = _model_reply()
    reply["confirmed_root_causes"][0]["evidence"] = [
        {"kind": "state-change", "source": "Service/geo", "observation": "targetPort changed"},
        {"kind": "state-change", "source": "Service/recommendation", "observation": "targetPort changed"},
    ]
    sources = [e.source for e in parse_incident_result(reply, USAGE).confirmed_root_causes[0].evidence]
    assert sources == ["Service/geo", "Service/recommendation"]
