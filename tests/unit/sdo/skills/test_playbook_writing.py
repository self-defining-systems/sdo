"""The reflector's one skill stays short, names only SDK identifiers that exist, and teaches from two examples."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
SKILL = ROOT / "sdo/skills/playbook-writing"
PROGRAM = ROOT / "sdo/operator/sdk/program"
BUDGET_BYTES = 1536


def _declared() -> set[str]:
    names: set[str] = set()
    for path in PROGRAM.glob("*.go"):
        if path.name.endswith("_test.go"):
            continue
        text = path.read_text(encoding="utf-8")
        names.update(re.findall(r"^(?:type|func)\s+([A-Z]\w*)", text, flags=re.MULTILINE))
        names.update(re.findall(r"^\t([A-Z]\w*)\s+(?:=|[A-Za-z.]+\s*=)", text, flags=re.MULTILINE))
    return names


def test_the_skill_fits_its_budget() -> None:
    assert len((SKILL / "SKILL.md").read_bytes()) <= BUDGET_BYTES


def test_the_directory_holds_only_the_skill_and_two_examples() -> None:
    entries = sorted(path.name for path in SKILL.iterdir())
    assert entries == ["SKILL.md", "examples"]
    examples = sorted(path.name for path in (SKILL / "examples").iterdir())
    assert len(examples) == 2
    for example in examples:
        files = sorted(path.name for path in (SKILL / "examples" / example).iterdir())
        assert files == ["diagnose.go", "mitigate.go"], example
        for path in (SKILL / "examples" / example).iterdir():
            assert len(path.read_text(encoding="utf-8").splitlines()) <= 60, path


def test_every_program_identifier_the_skill_and_examples_name_exists_in_the_sdk() -> None:
    declared = _declared()
    texts = [(SKILL / "SKILL.md").read_text(encoding="utf-8")]
    texts += [path.read_text(encoding="utf-8") for path in (SKILL / "examples").rglob("*.go")]
    named = {name for text in texts for name in re.findall(r"\bprogram\.([A-Z]\w*)", text)}
    # `program.X` placeholders in prose stand for "the action you ran"; only real identifiers are checked.
    named.discard("X")
    missing = sorted(name for name in named if name not in declared)
    assert not missing, f"the skill names SDK identifiers that do not exist: {missing}"
