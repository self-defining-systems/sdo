"""Provisioned skills are read from outside the application checkout and must not trip the escape audit."""

from __future__ import annotations

from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from sdo.agents.code_paths import SKILLS_ROOT
from sdo.agents.lifecycle.agents import _command_escapes_repository

REPOSITORY = Path("/work/app")


@given(st.text(alphabet="abcdefghijklmnopqrstuvwxyz0123456789_-", min_size=1, max_size=40))
@settings(max_examples=50, deadline=500)
def test_cli_home_credential_canaries_stay_excluded(name: str) -> None:
    skills = Path("/sdo/codex-homes/turn-owned/skills")
    for variable in ("CODEX_HOME", "CLAUDE_CONFIG_DIR"):
        for suffix in (f"{name}.json", f"credentials/{name}", f"skills/../{name}"):
            assert _command_escapes_repository(
                f"cat ${{{variable}}}/{suffix}", REPOSITORY, skill_directories={variable: skills}
            )


def test_hotel_skill_root_lookup_is_readable(monkeypatch) -> None:
    monkeypatch.setenv("CODEX_HOME", "/sdo/codex-homes/turn-owned")
    command = "find /sdo/codex-homes/turn-owned/skills -path '*/sdo-detector-authoring/references/sdk.md' -print"
    assert not _command_escapes_repository(command, REPOSITORY)


def test_cli_home_auth_variable_is_an_escape() -> None:
    assert _command_escapes_repository("cat $CODEX_HOME/auth.json", REPOSITORY)


@pytest.mark.parametrize(
    "command",
    [
        "cat /tmp/.codex/skills/sdo-detector-authoring/references/sdk.md",
        "sed -n 1,80p /home/agent/.claude/skills/sdo-memory/SKILL.md",
        "ls /root/.cache/sdo/codex-homes/turn-ab12/skills/sdo-playbook-authoring/examples",
    ],
)
def test_reading_a_sdo_skill_is_not_an_escape(command: str) -> None:
    home = command.split("/skills/", 1)[0].split()[-1]
    assert not _command_escapes_repository(command, REPOSITORY, skill_directories={"CODEX_HOME": Path(home) / "skills"})


@pytest.mark.parametrize("variable", ["CODEX_HOME", "CLAUDE_CONFIG_DIR"])
def test_session_installed_skill_root_can_be_discovered(variable: str) -> None:
    skills = Path("/sdo/codex-homes/turn-owned/skills")
    command = (
        '/usr/bin/bash -lc "find .sdo -maxdepth 6 -type f -print 2>/dev/null; '
        f"find {skills} -path '*/sdo-detector-authoring/references/sdk.md' -print; "
        "rg -n 'HandleFunc|Handle\\\\(|Path|GET\\\\(' cmd\""
    )
    assert not _command_escapes_repository(command, REPOSITORY, skill_directories={variable: skills})
    assert not _command_escapes_repository(
        f"cat ${variable}/skills/sdo-memory/SKILL.md", REPOSITORY, skill_directories={variable: skills}
    )


@pytest.mark.parametrize(
    "command",
    [
        "cat $CODEX_HOME/auth.json",
        "cat ${CODEX_HOME}/auth.json",
        "cat $CLAUDE_CONFIG_DIR/credentials.json",
        "cat /sdo/codex-homes/turn-other/skills/sdo-memory/SKILL.md",
        "cat /tmp/skills/sdo-memory/SKILL.md",
        "cat /sdo/codex-homes/turn-owned/skills/../auth.json",
        "cat ../source.txt",
        "cat /work/sregym/solution.txt",
        "cat /work/benchmarks/verdict.json",
        "echo changed > /sdo/codex-homes/turn-owned/skills/sdo-memory/SKILL.md",
        "sed -i x /sdo/codex-homes/turn-owned/skills/sdo-memory/SKILL.md",
        "find /sdo/codex-homes/turn-owned/skills -delete",
    ],
)
def test_skill_allowance_does_not_widen_other_read_or_write_access(command: str) -> None:
    assert _command_escapes_repository(
        command,
        REPOSITORY,
        skill_directories={"CODEX_HOME": Path("/sdo/codex-homes/turn-owned/skills")},
    )


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


@pytest.mark.parametrize(
    "relative", ["sdo-shell-authoring/SKILL.md", "sdo-memory/SKILL.md", "sdo-detector-authoring/references/sdk.md"]
)
def test_shipped_skill_prompt_paths_are_readable_with_a_private_turn_home(relative: str) -> None:
    assert not _command_escapes_repository(
        f"cat {SKILLS_ROOT / relative}",
        REPOSITORY,
        skill_directories={"CODEX_HOME": Path("/sdo/codex-homes/turn-owned/skills")},
    )


@pytest.mark.parametrize(
    "command",
    [
        "cat {root}/../code_paths.py",
        "cat {root}-other/auth.json",
        "sed -i x {root}/sdo-shell-authoring/SKILL.md",
        "echo changed > {root}/sdo-shell-authoring/SKILL.md",
        "find {root} -exec cat /work/verdict.json ;",
    ],
)
def test_shipped_skill_root_remains_read_only_and_narrow(command: str) -> None:
    assert _command_escapes_repository(command.format(root=SKILLS_ROOT), REPOSITORY, skill_directories={})
