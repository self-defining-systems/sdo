"""The responder's first message names the incident and its page; it requires no skill and no command."""

from __future__ import annotations

import pytest
from google.protobuf import timestamp_pb2

from sdo.agents.responder.codex import _responder_prompt, recent_changes_block
from sdo.agents.responder.incident_opening import learned_playbooks_block, symptom
from tests.proto_messages import evolve
from tests.unit.sdo.agents.prompts.cases import incident

#: The text around the page, with the telemetry sentence and a long skills path: about 520 bytes today.
FIXED_BYTES = 800


def _opened_by_a_test(*scenarios: str):
    request = incident(len(scenarios))
    for finding, scenario in zip(request.findings, scenarios, strict=True):
        finding.detector_id = "traffic-health"
        finding.rule_id = f"scenario-slo.{scenario}"
    request.detected_at.CopyFrom(timestamp_pb2.Timestamp(seconds=1_700_000_000))
    return request


def test_the_first_message_names_the_incident_and_the_failing_tests() -> None:
    prompt = str(_responder_prompt(_opened_by_a_test("login", "checkout")))

    assert prompt.startswith("Incident toy-incident in namespace toy: investigate and fix it.")
    assert (
        "\n2 failing user tests; each name is followed by failed/total samples in the window."
        " Shared symptom per group:\n"
        "- an unrecorded error (2): checkout, login\n"
    ) in prompt


def test_the_first_message_requires_no_skill_and_no_command() -> None:
    prompt = str(_responder_prompt(_opened_by_a_test("login")))

    for forced in ("Read your skills", "SKILL.md", "Start with", "subagent", "orchestrat"):
        assert forced not in prompt
    assert "return only to give up" in prompt


def test_the_fixed_part_of_the_first_message_cannot_grow_back() -> None:
    """Everything the responder is told besides the page (symptom, playbooks, candidates)."""
    request = _opened_by_a_test("login")
    full = str(_responder_prompt(request))
    # Each block is removed on its own: the template and the other blocks sit between them.
    for block in (symptom(request), learned_playbooks_block(request), recent_changes_block(request)):
        assert block in full
        full = full.replace(block, "", 1)

    fixed = len(full.encode())
    assert fixed <= FIXED_BYTES, f"{fixed} > {FIXED_BYTES}: the first-try prompt grew; cut text or justify the raise"


def test_the_first_message_is_small_and_holds_no_finding() -> None:
    request = _opened_by_a_test("login")
    prompt = str(_responder_prompt(request))

    assert len(prompt.encode()) <= 1500
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
    assert "return when it is fixed:\ndisk full on the batch node" in prompt


def test_the_repair_policy_stays_in_the_request_not_the_prompt() -> None:
    prompt = str(_responder_prompt(evolve(incident(1), repair_policy="recorded-actions")))

    assert "recorded-actions" not in prompt


@pytest.mark.parametrize("skill", ["sdo-incident-response", "sdo-memory"])
def test_the_old_skill_pointers_are_gone(skill: str) -> None:
    assert skill not in str(_responder_prompt(incident(1)))


def test_the_first_message_carries_the_candidate_block_then_the_live_pointer() -> None:
    request = _opened_by_a_test("login")
    request.recent_changes.extend(
        [
            "CANDIDATES (on every failing path unless marked k of n, plus one hop upstream)",
            "db  on every failing path; serves passing ops",
            "  store db: schema changed  (read 06:52:40Z, change untimed)",
            "live: sdo detect (this view); sdo changes and sdo timeline list every row",
        ]
    )
    prompt = str(_responder_prompt(request))

    assert (
        "CANDIDATES (on every failing path unless marked k of n, plus one hop upstream)\n"
        "db  on every failing path; serves passing ops\n"
        "  store db: schema changed  (read 06:52:40Z, change untimed)\n"
        "live: sdo detect (this view); sdo changes and sdo timeline list every row\n"
    ) in prompt
    assert "Changes nearest onset" not in prompt
    assert "timeline.csv" not in prompt
    assert "changes.csv" not in prompt


def test_the_first_message_names_the_commands_when_no_change_is_listed() -> None:
    prompt = str(_responder_prompt(_opened_by_a_test("login")))

    assert "`sdo detect`" in prompt
    assert "`sdo changes`" in prompt
    assert "`sdo timeline`" in prompt
    assert "CANDIDATES" not in prompt


def test_the_operating_skill_forbids_printing_credentials_but_allows_use_in_place() -> None:
    skill = " ".join((__import__("pathlib").Path("sdo/skills/sdo-operating/SKILL.md")).read_text().split())

    assert "Never print, copy, log or write a credential value" in skill
    assert "may be used in place" in skill
    assert "Never read Secret values" not in skill
