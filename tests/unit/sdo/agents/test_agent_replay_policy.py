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


def test_reflection_instructions_select_only_enabled_replay_guidance(monkeypatch: pytest.MonkeyPatch) -> None:
    from sdo.agents.responder.reflection import render_reflection_prompt
    from tests.unit.sdo.agents.prompts.cases import live_shaped_reflection

    data = live_shaped_reflection()
    for mode in ("on", "off"):
        monkeypatch.setenv("SDO_REPLAY", mode)
        prompt = render_reflection_prompt(
            outcome=data.outcome,
            history=[],
            outcome_commit="a" * 40,
            closure=data.closure,
            guidance="baseline",
            cause_work=data.cause_work,
            unassigned_work=data.unassigned_work,
        )
        instructions = "".join(part.text for part in prompt.parts if part.name == "reflection.instructions")
        assert ("Replay is informational" in instructions) == (mode == "on")
        if mode == "off":
            assert "corpus" not in instructions.lower()
            assert "replay" not in instructions.lower()


@pytest.mark.parametrize(
    ("relative", "instruction"),
    [
        ("references/rules.md", "Build replay fixtures"),
        ("references/rules.md", "Preserve every accepted case"),
        ("references/general-actions.md", "constructing a new replay case"),
        ("references/general-actions.md", "Do not replay a write"),
    ],
)
def test_optional_references_only_request_replay_when_enabled(
    relative: str, instruction: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    for mode in ("on", "off"):
        monkeypatch.setenv("SDO_REPLAY", mode)
        text = (selected_skills_root(SKILLS_ROOT) / "sdo-playbook-authoring" / relative).read_text()
        assert (instruction in text) == (mode == "on")
