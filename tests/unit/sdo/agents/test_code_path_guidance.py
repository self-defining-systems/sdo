from __future__ import annotations

import pytest

from sdo.agents.code_paths import agent_guidance
from sdo.agents.responder.codex import _responder_prompt
from sdo.agents.responder.credentials import AGENT_SKILLS, skills_for_role
from sdo.contracts import IncidentRequest, OperationalMemorySnapshot


def test_default_code_path_keeps_existing_skills(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SDO_CODE_PATH", raising=False)
    monkeypatch.delenv("SDO_AGENT_GUIDANCE", raising=False)
    assert agent_guidance() == "go"
    assert skills_for_role("responder") == AGENT_SKILLS["responder"]


def test_shell_guidance_is_free_form_and_points_at_sdo_detect(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SDO_CODE_PATH", "shell")
    monkeypatch.delenv("SDO_AGENT_GUIDANCE", raising=False)
    snapshot = OperationalMemorySnapshot(accepted_commit="accepted", goal_markdown="Serve traffic")
    request = IncidentRequest(incident_id="incident-1", memory_snapshot=snapshot)
    prompt = _responder_prompt(request)
    assert "shell" in prompt
    assert "sdo detect" in prompt
    assert "Go SDK" not in prompt
    assert "sdo-shell-authoring" in skills_for_role("responder")
    assert "sdo-detectors" not in skills_for_role("reflector")
    assert skills_for_role("reflector") == ("playbook-writing",)


def test_guidance_override_and_invalid_selection(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SDO_CODE_PATH", "shell")
    monkeypatch.setenv("SDO_AGENT_GUIDANCE", "go")
    assert agent_guidance() == "go"
    monkeypatch.setenv("SDO_AGENT_GUIDANCE", "unknown")
    with pytest.raises(ValueError, match="agent guidance"):
        agent_guidance()
