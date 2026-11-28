"""The reflector's one skill stays short, names only SDK identifiers that exist, and teaches from two examples."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
SKILL = ROOT / "sdo/skills/playbook-writing"
PROGRAM = ROOT / "sdo/operator/sdk/program"
BUDGET_BYTES = 3072


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
    assert entries == ["SKILL.md", "detector.md", "examples"]
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


def test_the_skill_names_the_executable_alternative_to_a_sole_escalate() -> None:
    text = (SKILL / "SKILL.md").read_text(encoding="utf-8")
    assert "program.EnsureExec" in text
    assert "sdk-card.md" in text
    assert "known-good value directly" in text


def test_a_missing_object_is_read_from_its_list_never_from_a_failed_source_diff() -> None:
    """An object that is gone is the fault's evidence. Diffing it against source errors, and an error is Unknown."""
    text = (SKILL / "SKILL.md").read_text(encoding="utf-8")
    assert "not a failed read" in text
    assert "DiffAgainstSource" in text
    template = (SKILL / "examples/template-drift/diagnose.go").read_text(encoding="utf-8")
    # The example diffs only objects its own list holds, and says why an error there is a failed read.
    assert "unavailable evidence is Unknown, never NoMatch" not in template
    assert "a missing object is absent from the list, not an error" in template


STORESTATE = ROOT / "sdo/operator/sdk/storestate"


def test_the_detector_guidance_offers_the_store_state_readers_with_real_identifiers() -> None:
    """A cause in a store (an account, role or setting) has a detector-readable signal; the guidance must say so."""
    text = (SKILL / "detector.md").read_text(encoding="utf-8")
    assert "storestate.Changes" in text
    assert "only signal" not in text
    declared = "\n".join(
        path.read_text(encoding="utf-8") for path in STORESTATE.glob("*.go") if not path.name.endswith("_test.go")
    )
    named = set(re.findall(r"\bstorestate\.([A-Z]\w*)", text))
    missing = sorted(
        name
        for name in named
        if not re.search(rf"^(?:func|type|const|var)\s+{name}\b|^\t{name}\b", declared, re.MULTILINE)
    )
    assert not missing, f"detector.md names storestate identifiers that do not exist: {missing}"


def test_a_playbook_for_app_managed_state_encodes_the_apps_own_path() -> None:
    """A learned playbook must not turn a hand schema edit into the default repair for state the app maintains."""
    text = (SKILL / "SKILL.md").read_text(encoding="utf-8")
    assert "A playbook that writes app internals by hand says so in its README" in text


def test_a_matched_but_unfit_playbook_is_generalized() -> None:
    """A playbook that matched a repeat but did not fix it is widened, not shadowed by a narrower sibling."""
    text = (SKILL / "SKILL.md").read_text(encoding="utf-8")
    assert "generalize it (parameterize what varied) rather than adding a narrower sibling" in text
