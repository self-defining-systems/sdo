from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from sdo.agents.responder.credentials import (
    AGENT_SKILLS,
    SKILLS_ROOT,
    prepare_claude_home,
    prepare_codex_home,
)

if TYPE_CHECKING:
    from pathlib import Path


def test_prepare_codex_home_copies_auth_into_writable_runtime_directory(tmp_path: Path) -> None:
    credentials = tmp_path / "credentials"
    codex_home = tmp_path / "home" / ".codex"
    credentials.mkdir()
    (credentials / "auth.json").write_text('{"token":"secret"}', encoding="utf-8")

    prepare_codex_home(credentials_root=credentials, codex_home=codex_home)

    copied = codex_home / "auth.json"
    assert copied.read_text(encoding="utf-8") == '{"token":"secret"}'
    assert copied.stat().st_mode & 0o777 == 0o600


def test_prepare_claude_home_copies_credentials_into_writable_home(tmp_path: Path) -> None:
    credentials = tmp_path / "credentials"
    claude_home = tmp_path / "home" / ".claude"
    credentials.mkdir()
    (credentials / ".credentials.json").write_text('{"token":"secret"}', encoding="utf-8")

    prepare_claude_home(credentials_root=credentials, claude_home=claude_home)

    copied = claude_home / ".credentials.json"
    assert copied.read_text(encoding="utf-8") == '{"token":"secret"}'
    assert copied.stat().st_mode & 0o777 == 0o600


def _skills(root: Path, *names: str) -> Path:
    for name in names:
        (root / name / "references").mkdir(parents=True)
        (root / name / "SKILL.md").write_text(f"---\nname: {name}\ndescription: d\n---\nbody\n", encoding="utf-8")
        (root / name / "references" / "rules.md").write_text("rules\n", encoding="utf-8")
    return root


def test_prepare_codex_home_installs_only_the_requested_skills_without_credentials(tmp_path: Path) -> None:
    source = _skills(tmp_path / "skills", "sdo-memory", "sdo-detector-authoring")
    codex_home = tmp_path / "home" / ".codex"

    prepare_codex_home(
        credentials_root=tmp_path / "none", codex_home=codex_home, skills=("sdo-memory",), skills_root=source
    )

    assert sorted(path.name for path in (codex_home / "skills").iterdir()) == ["sdo-memory"]
    assert (codex_home / "skills" / "sdo-memory" / "references" / "rules.md").read_text(encoding="utf-8") == "rules\n"


def test_prepare_claude_home_installs_skills_and_replaces_a_stale_copy(tmp_path: Path) -> None:
    source = _skills(tmp_path / "skills", "sdo-memory")
    claude_home = tmp_path / "home" / ".claude"
    stale = claude_home / "skills" / "sdo-memory" / "stale.md"
    stale.parent.mkdir(parents=True)
    stale.write_text("old", encoding="utf-8")

    prepare_claude_home(
        credentials_root=tmp_path / "none", claude_home=claude_home, skills=("sdo-memory",), skills_root=source
    )

    assert not stale.exists()
    assert (claude_home / "skills" / "sdo-memory" / "SKILL.md").is_file()


def test_an_unknown_skill_fails_loudly(tmp_path: Path) -> None:
    source = _skills(tmp_path / "skills", "sdo-memory")

    with pytest.raises(ValueError, match="sdo-missing"):
        prepare_codex_home(codex_home=tmp_path / "home", skills=("sdo-missing",), skills_root=source)


def test_no_skills_leaves_the_home_without_a_skills_directory(tmp_path: Path) -> None:
    prepare_claude_home(credentials_root=tmp_path / "none", claude_home=tmp_path / "home")

    assert not (tmp_path / "home" / "skills").exists()


@pytest.mark.parametrize(
    ("agent", "skills"),
    [
        ("responder", {"sdo-memory", "sdo-incident-response"}),
        ("reflector", {"sdo-memory", "sdo-detector-authoring", "sdo-playbook-authoring"}),
        ("health-judge", {"sdo-memory", "sdo-detector-authoring", "sdo-playbook-authoring"}),
        ("deployer", {"sdo-memory"}),
    ],
)
def test_each_agent_worker_declares_the_skills_it_installs(agent: str, skills: set[str]) -> None:
    assert set(AGENT_SKILLS[agent]) == skills
    for name in skills:
        document = (SKILLS_ROOT / name / "SKILL.md").read_text(encoding="utf-8")
        assert document.startswith(f"---\nname: {name}\ndescription: ")
