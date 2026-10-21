from __future__ import annotations

import json
from pathlib import Path

from google.protobuf import json_format

from sdo.agents.responder import codex
from sdo.contracts import IncidentRequest
from tests.proto_messages import evolve


def _request() -> IncidentRequest:
    fixture = Path(__file__).resolve().parents[5] / "tests/fixtures/sdo/contracts/incident_request.json"
    raw = json.loads(fixture.read_text(encoding="utf-8"))
    for finding in raw["findings"]:
        finding["playbook_ids"] = ["missing-configmap"]
    for playbook in raw["surfaced_playbooks"]:
        playbook["playbook_id"] = "missing-configmap"
    raw["memory_snapshot"] = {
        "accepted_commit": raw["repository_base_commit"],
        "goal_markdown": "The checkout-independent health objective.",
        "architecture_markdown": "The checkout-independent architecture.",
        "playbook_index_markdown": "Incident playbook index",
        "detectors": [
            {
                "id": "learned-detector",
                "detector_class": "incident",
                "owner": "responder",
                "watches": [{"api_version": "apps/v1", "kind": "Deployment"}],
                "possible_playbook_ids": ["missing-configmap"],
                "originating_incident": "prior-1",
            }
        ],
        "playbooks": [
            {
                "id": "missing-configmap",
                "markdown": "Restore the ConfigMap safely.",
                "scripts": [{"name": "verify.sh", "content": "kubectl get configmap"}],
            }
        ],
    }
    raw["relevant_outcomes"] = [
        {
            "incident_id": "prior-1",
            "match_reason": "exact-fingerprint",
            "source_commit": raw["source_commit"],
            "exact_source_match": True,
            "applied_playbook_ids": ["missing-configmap"],
        }
    ]
    return json_format.ParseDict(raw, IncidentRequest())


def test_responder_uses_logical_snapshot_without_loading_memory_files() -> None:
    request = _request()
    prompt = codex._responder_prompt(request)
    assert "checkout-independent health objective" in prompt
    assert "checkout-independent architecture" in prompt
    assert "Restore the ConfigMap safely" in prompt
    assert ".sdo/playbooks/" not in prompt.split("Warm path:", 1)[1].split("During this response", 1)[0]
    assert ".sdo/" not in prompt


def test_reflection_brief_uses_logical_snapshot_without_loading_memory_files(tmp_path: Path) -> None:
    from sdo.agents.responder import reflection_brief
    from sdo.agents.responder.reflection import SessionReflector
    from tests.unit.sdo.agents.responder.test_reflection import _CapturingBackend, _fresh_closure, _outcome

    request = _request()
    closure = _fresh_closure(tmp_path)
    closure.request.CopyFrom(evolve(request, repository_worktree=str(tmp_path)))
    for playbook in closure.result.applied_playbooks:
        playbook.playbook_id = "missing-configmap"
        playbook.script_names[:] = ["verify.sh"]
    del closure.result.proposed_artifact_ids[:]

    brief = reflection_brief.incident_brief(closure, worktree=tmp_path, detector_detail=True)
    assert "trusted logical snapshot" in brief
    assert "Restore the ConfigMap safely" in brief
    assert "missing-configmap" in brief
    assert ".sdo/" not in brief

    backend = _CapturingBackend()
    outcome = _outcome()
    outcome.incident_id = request.incident_id
    SessionReflector(backend).resume(
        session_id="fresh",
        incident_id=request.incident_id,
        worktree=tmp_path,
        outcome=outcome,
        history=[],
        outcome_commit=request.repository_base_commit,
        session_mode="fresh",
        closure=closure,
    )
    prompt = str(backend.calls[-1]["prompt"])
    assert "Edit the final files directly" in prompt
    assert "sdo-operator memory check" in prompt
    assert "draft-test" not in prompt
