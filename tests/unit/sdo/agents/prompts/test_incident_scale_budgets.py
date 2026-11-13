"""An incident on a large application never fails on a prompt budget; it degrades to counts and files."""

from pathlib import Path

import pytest
from google.protobuf import json_format

from sdo.agents.context_output import budget_for
from sdo.agents.prompt_templates import RenderedPrompt
from sdo.agents.responder import codex
from sdo.agents.responder.session_recovery import SessionRecovery
from sdo.contracts import MitigationReport, MitigationSession
from tests.context_helpers import expand_context
from tests.unit.sdo.agents.prompts.cases import incident, responder_prompt

WORKLOADS = 60


@pytest.fixture(autouse=True)
def _scratch(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    # The prompt embeds the skills path; a fixed root keeps byte caps independent of the checkout's location.
    monkeypatch.setattr(codex, "SKILLS_ROOT", Path("/opt/sdo/skills"))


def _fits(prompt: RenderedPrompt) -> None:
    assert prompt.parts
    for part in prompt.parts:
        assert len(part.text) <= budget_for(part.name), part.name


def test_responder_first_message_for_many_findings_and_stage_actions_fits() -> None:
    request = incident(WORKLOADS, oversized=True, pending=True)
    report = request.prior_stages.add(stage="deterministic", remaining_finding_fingerprints=["f" * 500] * WORKLOADS)
    for index in range(WORKLOADS):
        report.mitigations.add(
            playbook_id=f"repair-{index}-" + "p" * 200, program_revision="rev", result="apply_error", error="e" * 2000
        )
    request.external_message = "operator note " * 400
    rendered = str(responder_prompt(request))
    assert len(rendered.encode()) <= 800
    assert request.incident_id in rendered
    assert "repair-0-" not in rendered


def test_responder_first_message_names_every_failing_test_within_a_measured_bound() -> None:
    """The 800-byte bound above is for an incident opened on cluster findings. Failing user tests are the
    exception: each is named so the agent sees which operations fail and what they share; the symptom is
    bounded (48 failing names, 8 groups, 24 passing names) and never spills to a context file."""
    request = incident(0)
    for index in range(WORKLOADS):
        finding = request.findings.add(
            detector_id="traffic-probe-call",
            rule_id=f"scenario-slo.operation-{index:02d}",
            fingerprint=f"fp-{index}",
        )
        json_format.ParseDict(
            {"failure_status": "429", "failure_observed": 'text "Retry later"', "errors": 3, "samples": 6},
            finding.metadata,
        )
        request.opening_keys.append(f"{finding.detector_id}\x00{finding.fingerprint}")
    request.passing_tests.extend(f"fixture-{index}" for index in range(WORKLOADS))
    rendered = str(responder_prompt(request))
    assert len(rendered.encode()) <= 3000
    assert "operation-00 3/6" in rendered and "operation-47 3/6" in rendered
    assert 'HTTP 429 text "Retry later" (48)' in rendered
    assert "(+12 more failing tests not listed" in rendered
    assert request.incident_id in rendered


def test_pending_reports_for_a_large_incident_spill_instead_of_failing(tmp_path: Path) -> None:
    session = MitigationSession(incident_id="big", session_id="logical")
    for index in range(WORKLOADS):
        session.pending_reports.append(
            MitigationReport(
                version=1,
                incident_id="big",
                session_id="logical",
                request_id="request",
                attempt_id=f"attempt-{index}",
                acceptance_epoch=1,
                status=MitigationReport.STATUS_INTERRUPTED,
            )
        )
    prompt = SessionRecovery(session, "codex", tmp_path, lambda _: None).recovered_prompt()
    _fits(prompt)
    assert prompt.context_files
    assert "attempt-0" in expand_context(prompt)
