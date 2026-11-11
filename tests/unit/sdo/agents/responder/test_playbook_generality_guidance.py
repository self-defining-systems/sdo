"""Keep the shipped playbook-authoring guidance aligned on generality."""

import pytest

GENERALITY_PRINCIPLES = (
    "each bound by an opening finding",
    "diagnose matches a deviation from accepted source or captured evidence, never a value seen in",
    "match its origin incident's captured evidence without `needsjudgement`",
)


@pytest.mark.parametrize("replay", ["on", "off"])
def test_learned_playbooks_are_told_to_generalize(replay: str, monkeypatch: pytest.MonkeyPatch) -> None:
    from sdo.agents.replay import selected_skills_root
    from sdo.agents.responder.credentials import SKILLS_ROOT

    monkeypatch.setenv("SDO_REPLAY", replay)
    text = " ".join((selected_skills_root(SKILLS_ROOT) / "sdo-playbook-authoring/SKILL.md").read_text().lower().split())
    for principle in GENERALITY_PRINCIPLES:
        assert principle in text
    example = " ".join(
        (selected_skills_root(SKILLS_ROOT) / "sdo-playbook-authoring/references/programs.md").read_text().split()
    )
    assert "restore it `FromSource`" in example
