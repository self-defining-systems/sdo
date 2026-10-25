"""Byte goldens captured from production builders before template extraction."""

from pathlib import Path

from tests.unit.sdo.agents.prompts.cases import rendered_cases

GOLDENS = Path(__file__).resolve().parents[5] / "tests/fixtures/sdo/prompts"


def test_prompt_goldens() -> None:
    prompts = rendered_cases()
    assert set(prompts) == {path.stem for path in GOLDENS.glob("*.md")}
    for name, prompt in prompts.items():
        assert prompt.encode("utf-8") == (GOLDENS / f"{name}.md").read_bytes(), name


def test_fixture_skill_root_is_independent_of_checkout(monkeypatch, tmp_path: Path) -> None:
    from sdo.agents.responder import codex, reflection

    monkeypatch.setattr(codex, "SKILLS_ROOT", tmp_path / "other-checkout/skills")
    monkeypatch.setattr(reflection, "SKILLS_ROOT", tmp_path / "other-checkout/skills")
    prompts = rendered_cases()
    assert "Skill index: /opt/sdo/skills/sdo-memory/SKILL.md" in prompts["responder_cold_two"]
    assert "guide starts at /opt/sdo/skills/sdo-memory/SKILL.md" in prompts["reflection_resume_2"]
