"""Provisioned skills are read from outside the application checkout and must not trip the escape audit."""

from __future__ import annotations

from pathlib import Path

import pytest

from sdo.agents.lifecycle.agents import _command_escapes_repository

REPOSITORY = Path("/work/app")


@pytest.mark.parametrize(
    "command",
    [
        "cat /tmp/.codex/skills/sdo-detector-authoring/references/sdk.md",
        "sed -n 1,80p /home/agent/.claude/skills/sdo-memory/SKILL.md",
        "ls /root/.cache/sdo/codex-homes/turn-ab12/skills/sdo-playbook-authoring/examples",
    ],
)
def test_reading_a_sdo_skill_is_not_an_escape(command: str) -> None:
    assert not _command_escapes_repository(command, REPOSITORY)


@pytest.mark.parametrize(
    "command",
    [
        "cat /tmp/.codex/skills/other-skill/SKILL.md",
        "cat /tmp/.codex/skills/sdo-memory/../../auth.json",
        "cat /tmp/.codex/auth.json",
        "cat /tmp/skills/sdo-memory/../secrets",
    ],
)
def test_other_paths_stay_outside_the_repository(command: str) -> None:
    assert _command_escapes_repository(command, REPOSITORY)
