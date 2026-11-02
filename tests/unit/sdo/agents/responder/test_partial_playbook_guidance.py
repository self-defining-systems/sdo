"""Keep the shipped reflection guidance aligned on partial learning."""

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[5]


@pytest.mark.parametrize(
    "relative",
    [
        "sdo/agents/responder/prompts/reflection.md",
        "sdo/skills/sdo-playbook-authoring/SKILL.md",
        "sdo/skills/sdo-playbook-authoring/references/rules.md",
    ],
)
@pytest.mark.parametrize("replay", ["on", "off"])
def test_partial_learning_is_explicit(relative: str, replay: str, monkeypatch: pytest.MonkeyPatch) -> None:
    from sdo.agents.replay import selected_skills_root
    from sdo.agents.responder.credentials import SKILLS_ROOT

    monkeypatch.setenv("SDO_REPLAY", replay)
    if relative.endswith("prompts/reflection.md"):
        from sdo.agents.responder.reflection import render_reflection_prompt
        from tests.unit.sdo.agents.prompts.cases import live_shaped_reflection

        data = live_shaped_reflection()
        prompt = render_reflection_prompt(
            outcome=data.outcome,
            history=[],
            outcome_commit="a" * 40,
            closure=data.closure,
            guidance="baseline",
            cause_work=data.cause_work,
            unassigned_work=data.unassigned_work,
        )
        text = "".join(part.text for part in prompt.parts if part.name == "reflection.instructions").lower()
    else:
        text = (selected_skills_root(SKILLS_ROOT) / relative.removeprefix("sdo/skills/")).read_text().lower()
    assert "partial" in text
    assert "readme" in text
    assert "judgement" in text
    if replay == "on":
        assert "replay" in text
        assert "information" in text
    else:
        assert "create_corpus" not in text
        assert "root_cause_program_ids" not in text
    if relative != "sdo/agents/responder/prompts/reflection.md":
        assert "runscript" in text
        if relative.endswith("SKILL.md"):
            reference = selected_skills_root(SKILLS_ROOT) / "sdo-playbook-authoring/references/script-receipts.md"
            assert "script_receipts" in reference.read_text()
        else:
            assert "receipt" in text
        assert "relation-derived env" in text
    if relative.endswith(("SKILL.md", "reflection.md")):
        assert "no_change" in text
        assert "hand repair" in text
        assert "executable" in text
        assert "not encoded yet" in text
        assert "left findings unresolved" in text
        assert "difficulty never justifies saving nothing" not in text
    assert "a readme alone does not establish learning" not in text
    assert "a readme note alone does not capture an executable repair" not in text


def test_memory_skill_delegates_reflection_to_one_entry_point() -> None:
    """Reflection guidance lives once, in the playbook-authoring entry point."""
    text = (ROOT / "sdo/skills/sdo-memory/SKILL.md").read_text()
    assert "`sdo-playbook-authoring`" in text
    assert "reflection entry point" in text
    assert "not encoded yet" not in text
