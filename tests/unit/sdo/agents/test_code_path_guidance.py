from __future__ import annotations

import pytest

from sdo.agents.code_paths import agent_guidance
from sdo.agents.responder.codex import _responder_prompt
from sdo.agents.responder.credentials import AGENT_SKILLS, skills_for_role
from sdo.agents.responder.reflection import _logical_learning_request
from sdo.contracts import IncidentClosure, IncidentRequest, OperationalMemorySnapshot, OutcomeRecord


def test_default_code_path_keeps_existing_skills(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SDO_CODE_PATH", raising=False)
    monkeypatch.delenv("SDO_AGENT_GUIDANCE", raising=False)
    assert agent_guidance() == "go"
    assert skills_for_role("responder") == AGENT_SKILLS["responder"]


def test_shell_guidance_is_free_form_and_retains_incident_facts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SDO_CODE_PATH", "shell")
    monkeypatch.delenv("SDO_AGENT_GUIDANCE", raising=False)
    snapshot = OperationalMemorySnapshot(accepted_commit="accepted", goal_markdown="Serve traffic")
    request = IncidentRequest(incident_id="incident-1", memory_snapshot=snapshot)
    prompt = _responder_prompt(request, incident_data="FIRED SCRIPT EVIDENCE")
    assert "shell" in prompt
    assert "FIRED SCRIPT EVIDENCE" in prompt
    assert "Go SDK" not in prompt
    assert "sdo-shell-authoring" in skills_for_role("responder")
    assert "sdo-detector-authoring" not in skills_for_role("reflector")
    reflection = _logical_learning_request(
        outcome=OutcomeRecord(incident_id="incident-1"),
        history=[],
        outcome_commit="outcome",
        closure=IncidentClosure(request=request),
        guidance="baseline",
    )
    assert "notes and shell scripts" in reflection
    assert "incident-1" in reflection
    assert "required structure" not in reflection


def test_guidance_override_and_invalid_selection(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SDO_CODE_PATH", "shell")
    monkeypatch.setenv("SDO_AGENT_GUIDANCE", "go")
    assert agent_guidance() == "go"
    monkeypatch.setenv("SDO_AGENT_GUIDANCE", "unknown")
    with pytest.raises(ValueError, match="agent guidance"):
        agent_guidance()
