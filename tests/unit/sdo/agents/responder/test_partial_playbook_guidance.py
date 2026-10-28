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
def test_partial_learning_is_explicit(relative: str) -> None:
    text = (ROOT / relative).read_text().lower()
    assert "partial" in text
    assert "readme" in text
    assert "judgement" in text
    assert "replay" in text
    assert "information" in text
    if relative != "sdo/agents/responder/prompts/reflection.md":
        assert "runscript" in text
        assert "digest-to-receipt" in text
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
