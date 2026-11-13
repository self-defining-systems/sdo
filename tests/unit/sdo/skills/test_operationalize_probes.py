"""The detection designer is told to probe the records users own, not only fixtures the probe wrote."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
SKILL = ROOT / "sdo/skills/sdo-operationalize"


def _probes() -> str:
    return (SKILL / "references/probes.md").read_text(encoding="utf-8")


def test_probes_guidance_covers_records_users_own() -> None:
    text = _probes()
    assert "## Users' own records" in text
    section = text.split("## Users' own records", 1)[1].split("\n## ", 1)[0]
    # A state flip on a record the probe never wrote is invisible to the probe's own fixtures.
    assert "never wrote" in section
    # Reads happen on every iteration, over every such record, and remember what existed at Setup.
    assert "every iteration" in section
    assert "Setup" in section
    # A write the grader's users make (a commit, an upload) is probed in the users' own containers too, on a marked
    # record removed afterwards, and the probe identity gets the lowest role that can do it.
    assert "marked" in section
    assert "lowest role" in section
    assert "never admin" in section


def test_skill_asks_for_an_operation_per_objective_verb() -> None:
    text = (SKILL / "SKILL.md").read_text(encoding="utf-8")
    assert "health objective" in text
    assert "Users' own records" in text
