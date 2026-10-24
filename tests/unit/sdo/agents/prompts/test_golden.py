"""Byte goldens captured from production builders before template extraction."""

from pathlib import Path

from tests.unit.sdo.agents.prompts.cases import rendered_cases

GOLDENS = Path(__file__).resolve().parents[5] / "tests/fixtures/sdo/prompts"


def test_prompt_goldens() -> None:
    prompts = rendered_cases()
    assert set(prompts) == {path.stem for path in GOLDENS.glob("*.md")}
    for name, prompt in prompts.items():
        assert prompt.encode("utf-8") == (GOLDENS / f"{name}.md").read_bytes(), name
