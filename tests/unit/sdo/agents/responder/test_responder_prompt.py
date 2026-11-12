"""The responder's first message names the incident and its symptom and sends it to the skills and `sdo detect`."""

from __future__ import annotations

import pytest
from google.protobuf import timestamp_pb2

from sdo.agents.responder.codex import _responder_prompt
from sdo.agents.responder.credentials import SKILLS_ROOT
from tests.proto_messages import evolve
from tests.unit.sdo.agents.prompts.cases import incident


def _opened_by_a_test(*scenarios: str):
    request = incident(len(scenarios))
    for finding, scenario in zip(request.findings, scenarios, strict=True):
        finding.detector_id = "traffic-health"
        finding.rule_id = f"scenario-slo.{scenario}"
    request.detected_at.CopyFrom(timestamp_pb2.Timestamp(seconds=1_700_000_000))
    return request


def test_the_first_message_names_the_incident_and_the_failing_tests() -> None:
    prompt = str(_responder_prompt(_opened_by_a_test("login", "checkout")))

    assert prompt.startswith(
        "Incident toy-incident in namespace toy.\n"
        "Failing user tests; counts are failed/total samples in the window:\n"
        "- checkout, login failed with an unrecorded error\n"
    )
    assert f"{SKILLS_ROOT}/incident-response/SKILL.md" in prompt
    assert f"{SKILLS_ROOT}/sdo-operating/SKILL.md" in prompt
    assert "Start with `sdo detect`." in prompt


def test_the_first_message_is_small_and_holds_no_finding() -> None:
    request = _opened_by_a_test("login")
    prompt = str(_responder_prompt(request))

    assert len(prompt.encode()) <= 1900
    for finding in request.findings:
        assert finding.summary not in prompt
        assert finding.fingerprint not in prompt
    assert "incident_data" not in prompt
    assert "Incident request" not in prompt


def test_a_condition_names_the_object_and_the_condition() -> None:
    request = incident(1)
    request.findings[0].rule_id = "crash-looping"
    request.findings[0].primary_resource.kind = "Deployment"
    request.findings[0].primary_resource.name = "api"

    assert "Opened on Deployment/api crash-looping" in str(_responder_prompt(request))


def test_an_external_alert_says_to_return_when_it_is_fixed() -> None:
    request = incident(1)
    del request.findings[:]
    del request.detector_history[:]
    request.external_message = "disk full on the batch node"

    prompt = str(_responder_prompt(request))
    assert "external alert: disk full on the batch node; return when it is fixed" in prompt


def test_the_repair_policy_stays_in_the_request_not_the_prompt() -> None:
    prompt = str(_responder_prompt(evolve(incident(1), repair_policy="recorded-actions")))

    assert "recorded-actions" not in prompt


@pytest.mark.parametrize("skill", ["sdo-incident-response", "sdo-memory"])
def test_the_old_skill_pointers_are_gone(skill: str) -> None:
    assert skill not in str(_responder_prompt(incident(1)))
