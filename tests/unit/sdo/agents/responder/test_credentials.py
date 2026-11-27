from __future__ import annotations

import json
import os
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


@pytest.mark.parametrize("name", ["CODEX_API_KEY", "OPENAI_API_KEY"])
def test_mounted_api_key_becomes_a_private_codex_login(tmp_path: Path, name: str) -> None:
    (tmp_path / name).write_text("synthetic-key\n", encoding="utf-8")
    prepare_codex_home(credentials_root=tmp_path, codex_home=tmp_path / "codex")
    login = tmp_path / "codex" / "auth.json"
    assert json.loads(login.read_text(encoding="utf-8"))["OPENAI_API_KEY"] == "synthetic-key"
    assert login.stat().st_mode & 0o777 == 0o600


def test_mounted_claude_token_becomes_a_private_login(tmp_path: Path) -> None:
    (tmp_path / "CLAUDE_CODE_OAUTH_TOKEN").write_text("synthetic-token\n", encoding="utf-8")
    prepare_claude_home(credentials_root=tmp_path, claude_home=tmp_path / "claude")
    login = tmp_path / "claude" / ".credentials.json"
    assert json.loads(login.read_text(encoding="utf-8"))["claudeAiOauth"]["accessToken"] == "synthetic-token"
    assert login.stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize("name", ["ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN"])
def test_mounted_claude_key_is_read_through_a_helper(tmp_path: Path, name: str) -> None:
    (tmp_path / name).write_text("synthetic-key\n", encoding="utf-8")
    prepare_claude_home(credentials_root=tmp_path, claude_home=tmp_path / "claude")
    settings = (tmp_path / "claude" / "settings.json").read_text(encoding="utf-8")
    key = tmp_path / "claude" / "api_key.credential"
    assert json.loads(settings) == {"apiKeyHelper": f"cat {key}"}
    assert key.read_text(encoding="utf-8") == "synthetic-key"
    assert key.stat().st_mode & 0o777 == 0o600


def test_client_settings_of_the_secret_reach_the_environment_and_credentials_do_not(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(os, "environ", dict(os.environ))
    (tmp_path / "ANTHROPIC_BASE_URL").write_text("https://gateway.example.test\n", encoding="utf-8")
    (tmp_path / "CLOUD_ML_REGION").write_text("region-1", encoding="utf-8")
    (tmp_path / "ANTHROPIC_AUTH_TOKEN").write_text("synthetic-token", encoding="utf-8")
    prepare_claude_home(credentials_root=tmp_path, claude_home=tmp_path / "claude")
    assert os.environ["ANTHROPIC_BASE_URL"] == "https://gateway.example.test"
    assert os.environ["CLOUD_ML_REGION"] == "region-1"
    assert "synthetic-token" not in os.environ.values()

    del os.environ["ANTHROPIC_BASE_URL"]
    (tmp_path / "ANTHROPIC_BASE_URL").write_text("https://user:synthetic@gateway.example.test", encoding="utf-8")
    with pytest.raises(ValueError, match="must not embed a credential"):
        prepare_claude_home(credentials_root=tmp_path, claude_home=tmp_path / "claude")


def _skills(root: Path, *names: str) -> Path:
    for name in names:
        (root / name / "references").mkdir(parents=True)
        (root / name / "SKILL.md").write_text(f"---\nname: {name}\ndescription: d\n---\nbody\n", encoding="utf-8")
        (root / name / "references" / "rules.md").write_text("rules\n", encoding="utf-8")
    return root


def test_prepare_codex_home_installs_only_the_requested_skills_without_credentials(tmp_path: Path) -> None:
    source = _skills(tmp_path / "skills", "sdo-memory", "sdo-detectors")
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
        (
            "responder",
            {
                "incident-response",
                "sdo-operating",
                "sdo-serving",
                "sdo-observability",
                "sdo-operations-map",
            },
        ),
        ("onboarding", {"sdo-onboarding", "sdo-serving", "sdo-observability", "sdo-operations-map", "sdo-detectors"}),
        ("reflector", {"playbook-writing"}),
        (
            "health-judge",
            {"sdo-memory", "sdo-detectors", "sdo-playbook-authoring", "sdo-operations-map", "sdo-observability"},
        ),
        ("source_deployment", {"sdo-memory", "sdo-serving"}),
    ],
)
def test_each_agent_worker_declares_the_skills_it_installs(agent: str, skills: set[str]) -> None:
    assert set(AGENT_SKILLS[agent]) == skills
    for name in skills:
        document = (SKILLS_ROOT / name / "SKILL.md").read_text(encoding="utf-8")
        assert document.startswith(f"---\nname: {name}\ndescription: ")


def test_responder_home_ships_no_investigator_skill_and_the_method_names_no_subagent(tmp_path: Path) -> None:
    claude_home = tmp_path / ".claude"

    prepare_claude_home(credentials_root=tmp_path / "none", claude_home=claude_home, skills=AGENT_SKILLS["responder"])

    method = (claude_home / "skills" / "incident-response" / "SKILL.md").read_text(encoding="utf-8")
    assert not (claude_home / "skills" / "sdo-investigator").exists()
    assert "sdo-investigator" not in method
