"""The responder's first message carries its method, the incident and its page; it requires no skill and no command."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from google.protobuf import timestamp_pb2

from sdo.agents.context_output import budget_for
from sdo.agents.prompt_templates import render_prompt
from sdo.agents.responder import page_only
from sdo.agents.responder.codex import _responder_prompt, recent_changes_block
from sdo.agents.responder.incident_opening import symptom
from sdo.agents.telemetry_context import telemetry_context
from tests.proto_messages import evolve
from tests.unit.sdo.agents.prompts.cases import incident

if TYPE_CHECKING:
    from pathlib import Path

#: The text around the method and the page, with the telemetry sentence and a long skills path.
FIXED_BYTES = 800
METHOD = str(render_prompt("responder", "incident_method", part="responder.method"))


def _opened_by_a_test(*scenarios: str):
    request = incident(len(scenarios))
    for finding, scenario in zip(request.findings, scenarios, strict=True):
        finding.detector_id = "traffic-health"
        finding.rule_id = f"scenario-slo.{scenario}"
    request.detected_at.CopyFrom(timestamp_pb2.Timestamp(seconds=1_700_000_000))
    return request


def test_the_first_message_names_the_incident_and_the_failing_tests() -> None:
    prompt = str(_responder_prompt(_opened_by_a_test("login", "checkout")))

    assert "\nIncident toy-incident in namespace toy: investigate and fix it." in prompt
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


def test_the_method_is_whole_and_first_so_the_first_request_already_has_it() -> None:
    """Two incidents share the whole method as a byte-identical prefix; nothing incident-specific precedes it."""
    first = str(_responder_prompt(_opened_by_a_test("login")))
    second = str(_responder_prompt(evolve(incident(2), incident_id="another-incident")))

    assert first.startswith(METHOD)
    assert second.startswith(METHOD)
    assert first.count("**Record.**") == 1


def test_the_page_only_responder_gets_no_method() -> None:
    assert "**Record.**" not in str(page_only.prompt(_opened_by_a_test("login"), ""))


def test_telemetry_and_environment_instructions_stay_in_the_first_message(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Two operator-provided sources and a transport appendix: nothing the responder must read back from a file."""
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    monkeypatch.setenv("SDO_RESPONDER_EXTRA_INSTRUCTIONS", "x" * 676)
    request = _opened_by_a_test("login")
    metrics = request.telemetry.sources.add(
        id="prom",
        kinds=["metrics"],
        dialect="PromQL (Prometheus HTTP API v1)",
        default=True,
        capabilities=["alerts", "range"],
        endpoint="http://prometheus.application-operator.svc:9090",
        origin="operator",
    )
    metrics.publish.scrape_annotations = True
    metrics.publish.otlp_metrics_url = "http://prometheus.application-operator.svc:9090/api/v1/otlp/v1/metrics"
    traces = request.telemetry.sources.add(
        id="traces",
        kinds=["traces"],
        dialect="Jaeger query HTTP API (/api/services, /api/traces)",
        default=True,
        endpoint="http://jaeger.application-operator.svc:16686",
        origin="operator",
    )
    traces.publish.otlp_traces_url = "http://jaeger.application-operator.svc:4318"
    request.telemetry.capabilities.extend(["pod-discovery", "node-proxy"])
    telemetry = telemetry_context(request.telemetry, "responder")

    prompt = _responder_prompt(request)

    assert len(telemetry) > 800
    assert telemetry in prompt
    assert "x" * 676 in prompt
    assert not prompt.context_files
    assert not list(tmp_path.rglob("*.txt"))


def test_oversized_environment_instructions_spill_to_one_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    monkeypatch.setenv("SDO_RESPONDER_EXTRA_INSTRUCTIONS", "long " * 2000)

    prompt = str(_responder_prompt(_opened_by_a_test("login")))

    assert prompt.count("Full: $SDO_SESSION_SCRATCH/context/") == 1
    assert len(list((tmp_path / "context").iterdir())) == 1
    assert len(prompt) < len(METHOD) + 1500 + budget_for("template_data.responder-incident_01-extra_instructions")


def test_the_fixed_part_of_the_first_message_cannot_grow_back() -> None:
    """Everything the responder is told besides the page (symptom, candidates) and the branch line."""
    request = _opened_by_a_test("login")
    full = str(_responder_prompt(request))
    page = symptom(request) + recent_changes_block(request)

    fixed = len(full.replace(page, "").replace(METHOD, "").encode())
    assert fixed <= FIXED_BYTES, f"{fixed} > {FIXED_BYTES}: the first-try prompt grew; cut text or justify the raise"


def test_the_first_message_is_small_and_holds_no_finding() -> None:
    request = _opened_by_a_test("login")
    prompt = str(_responder_prompt(request))

    assert len(prompt.replace(METHOD, "").encode()) <= 1500
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
