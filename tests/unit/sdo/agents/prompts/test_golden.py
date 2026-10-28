"""Byte goldens captured from production builders before template extraction."""

import re
import tempfile
from pathlib import Path

from tests.unit.sdo.agents.prompts.cases import rendered_cases

GOLDENS = Path(__file__).resolve().parents[5] / "tests/fixtures/sdo/prompts"


def normalize_prompt(prompt: str) -> str:
    prompt = re.sub(r"(?:\$SDO_SESSION_SCRATCH/context|/tmp/sdo-context)/[0-9a-f]{24}\.txt", "<context file>", prompt)
    prompt = re.sub(r'"/tmp/sdo/[^"\n]+-incident\.json"', '"<temporary incident facts file>"', prompt)
    prompt = re.sub(r'"[^"\n]*/sdo-playbooks-[^/"\n]+/(\d+\.md)"', r'"<temporary playbook documents>/\1"', prompt)
    prompt = re.sub(
        r'("full_outcome_path": ")[^"]*/sdo-reflection-history-[^/"]+/([^"]+)(")',
        r"\1<temporary reflection history>/\2\3",
        prompt,
    )
    return re.sub(r"(?m)^Full incident facts: .+$", "Full incident facts: <temporary incident facts file>", prompt)


def test_prompt_goldens() -> None:
    prompts = rendered_cases()
    assert set(prompts) == {path.stem for path in GOLDENS.glob("*.md")}
    for name, prompt in prompts.items():
        prompt = normalize_prompt(prompt)
        assert prompt.encode("utf-8") == (GOLDENS / f"{name}.md").read_bytes(), name


def test_prompt_goldens_are_independent_of_temp_root(monkeypatch, tmp_path: Path) -> None:
    """Prompt size caps count temp-path characters, so the fixture owns the temp root."""
    expected = {name: normalize_prompt(prompt) for name, prompt in rendered_cases().items()}
    long_root = tmp_path / ("x" * 80)
    long_root.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(long_root))
    assert {name: normalize_prompt(prompt) for name, prompt in rendered_cases().items()} == expected


def test_fixture_skill_root_is_independent_of_checkout(monkeypatch, tmp_path: Path) -> None:
    from sdo.agents.responder import codex, reflection

    monkeypatch.setattr(codex, "SKILLS_ROOT", tmp_path / "other-checkout/skills")
    monkeypatch.setattr(reflection, "SKILLS_ROOT", tmp_path / "other-checkout/skills")
    prompts = rendered_cases()
    assert "Read /opt/sdo/skills/sdo-incident-response/SKILL.md" in prompts["responder_cold_two"]
    assert "guide starts at /opt/sdo/skills/sdo-memory/SKILL.md" in prompts["reflection_resume_2"]
