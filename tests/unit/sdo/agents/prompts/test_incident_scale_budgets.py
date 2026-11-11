"""An incident on a large application never fails on a prompt budget; it degrades to counts and files."""

import json
from pathlib import Path

import pytest

from sdo.agents.context_output import budget_for
from sdo.agents.prompt_templates import RenderedPrompt
from sdo.agents.responder.continuation import continuation_prompt
from sdo.agents.responder.incident_context import prepared_incident_data
from sdo.agents.responder.session_recovery import SessionRecovery
from sdo.contracts import MitigationReport, MitigationSession
from tests.context_helpers import expand_context
from tests.unit.sdo.agents.prompts.cases import incident

WORKLOADS = 60


@pytest.fixture(autouse=True)
def _scratch(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))


def _fits(prompt: RenderedPrompt) -> None:
    assert prompt.parts
    for part in prompt.parts:
        assert len(part.text) <= budget_for(part.name), part.name


def _continuation_payload() -> dict[str, object]:
    resources = [
        {"api_version": "apps/v1", "kind": "Deployment", "namespace": "big", "name": f"workload-{i}"}
        for i in range(WORKLOADS)
    ]
    return {
        "attempt": 2,
        "healthy": False,
        "summary": "rollout partly applied; many workloads pending " * 3,
        "error": "verification failed " * 40,
        "still_broken": [
            {
                "primary_resource": resource,
                "fingerprint": f"workload-health/unavailable/{resource['name']}-" + "a" * 40,
                "detector_id": "workload-health",
                "rule_id": "unavailable",
                "summary": f"{resource['name']} has no ready replicas; " * 6,
                "evidence": "pods pending " * 50,
            }
            for resource in resources
        ],
        "already_done": [
            {
                "action_id": f"act-{i:04d}",
                "kind": "recorded-action",
                "summary": "restart workload " * 10,
                "receipt": {
                    "action_id": f"act-{i:04d}",
                    "success": True,
                    "status": "REPAIR_ACTION_STATUS_APPLIED",
                    "resources": [resources[i]],
                },
            }
            for i in range(WORKLOADS)
        ],
        "verification_blockers": [
            {"detector_id": f"health-{i}", "reason": "observation window is incomplete " * 8} for i in range(20)
        ],
    }


def test_continuation_for_dozens_of_workloads_fits_and_keeps_essentials() -> None:
    payload = _continuation_payload()
    prompt = continuation_prompt(json.dumps(payload))
    _fits(prompt)
    assert "still_broken: (60)" in prompt
    assert "already_done:" in prompt
    assert "verification_blockers: (20)" in prompt
    assert "Omitted" in prompt
    # Still-broken findings are the last rows to go: the first fingerprint survives.
    assert payload["still_broken"][0]["fingerprint"] in prompt  # type: ignore[index]
    evidence = Path(expand_context(prompt).split("Full JSON: ", 1)[1].splitlines()[0])
    assert json.loads(evidence.read_text()) == payload


def test_responder_incident_data_for_many_findings_and_stage_actions_fits() -> None:
    request = incident(WORKLOADS, oversized=True, pending=True)
    report = request.prior_stages.add(stage="deterministic", remaining_finding_fingerprints=["f" * 500] * WORKLOADS)
    for index in range(WORKLOADS):
        report.mitigations.add(
            playbook_id=f"repair-{index}-" + "p" * 200, program_revision="rev", result="apply_error", error="e" * 2000
        )
    request.external_message = "operator note " * 400
    with prepared_incident_data(request) as rendered:
        assert len(rendered) <= budget_for("responder.incident_data")
        document = json.loads(rendered[rendered.index("{") :])
        assert document["incident_id"] == request.incident_id
        assert document["namespace"] == request.namespace
        assert document["details_file"]
        assert document["findings"] or document["findings_omitted"]
        assert Path(document["details_file"]).exists()


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
