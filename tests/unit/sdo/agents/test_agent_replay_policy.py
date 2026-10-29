from pathlib import Path

import pytest

from sdo.agents.replay import replay_mode, selected_skills_root
from sdo.agents.responder.credentials import SKILLS_ROOT, prepare_claude_home
from sdo.agents.responder.reflection_validation import VALIDATION_ENVIRONMENT, validation_server


def test_replay_defaults_on_and_rejects_invalid_policy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SDO_REPLAY", raising=False)
    assert replay_mode() == "on"
    monkeypatch.setenv("SDO_REPLAY", "false")
    with pytest.raises(ValueError, match="on or off"):
        replay_mode()


@pytest.mark.asyncio
async def test_replay_off_projects_guidance_and_tool_surface(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SDO_REPLAY", "off")
    root = selected_skills_root(SKILLS_ROOT)
    authoring = (root / "sdo-playbook-authoring/SKILL.md").read_text()
    assert "Label one corpus case" not in authoring
    assert "## Corpus labels" not in authoring
    assert "RunScript" in authoring
    assert "mitigate.go" in authoring
    prepare_claude_home(claude_home=tmp_path, credentials_root=tmp_path / "absent", skills=("sdo-playbook-authoring",))
    assert (tmp_path / "skills/sdo-playbook-authoring/SKILL.md").read_text() == authoring
    tools = await validation_server(tmp_path).list_tools()
    names = {tool.name for tool in tools}
    assert "check_memory" in names
    assert "create_corpus" not in names
    assert "capture_fields" not in names
    assert "SDO_REPLAY" in VALIDATION_ENVIRONMENT
