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
        "sdo/skills/sdo-memory/SKILL.md",
    ],
)
def test_partial_learning_is_explicit(relative: str) -> None:
    text = (ROOT / relative).read_text().lower()
    assert "partial" in text
    assert "readme-only" in text
    assert "judgement" in text
    assert "runscript" in text
    assert "digest-to-receipt" in text
    assert "relation-derived env" in text
    assert "difficulty never justifies saving nothing" in text
    assert "a readme alone does not establish learning" not in text
    assert "a readme note alone does not capture an executable repair" not in text
