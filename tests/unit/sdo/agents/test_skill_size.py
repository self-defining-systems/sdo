"""The responder's skills stay short enough to be read in full on every incident."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from sdo.agents.code_paths import AGENT_SKILLS, SKILLS_ROOT
from tests.skill_sizes import SKILL_BYTE_CAPS, SKILL_BYTE_FLOOR

REPO_ROOT = Path(__file__).resolve().parents[4]
SHORT_SKILLS = tuple(SKILL_BYTE_CAPS)


def test_the_responder_reads_the_two_short_skills_patching_and_the_investigator_skill() -> None:
    assert AGENT_SKILLS["responder"] == (*SHORT_SKILLS, "sdo-patching", "sdo-investigator")


@pytest.mark.parametrize("name", SHORT_SKILLS)
def test_a_short_skill_is_one_small_file(name: str) -> None:
    directory = SKILLS_ROOT / name
    assert [path.name for path in directory.iterdir()] == ["SKILL.md"]
    size = (directory / "SKILL.md").stat().st_size
    assert SKILL_BYTE_FLOOR <= size <= SKILL_BYTE_CAPS[name], size


def test_the_skills_name_no_application() -> None:
    result = subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts/check_app_names.py")], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stdout + result.stderr
