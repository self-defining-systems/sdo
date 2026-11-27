"""Materialize read-only runtime credentials and agent skills into the CLIs' writable homes."""

from __future__ import annotations

import json
import os
import shlex
import shutil
from pathlib import Path

from libs.agent_cli.claude_accounts import write_claude_login
from libs.agent_cli.provider_capacity import CLAUDE_API_KEY_FILE

#: The skills shipped with SDO: ``sdo/skills`` in the repository and in the agent image.
from sdo.agents.code_paths import AGENT_SKILLS as AGENT_SKILLS
from sdo.agents.code_paths import SKILLS_ROOT as SKILLS_ROOT
from sdo.agents.code_paths import skills_for_role as skills_for_role
from sdo.agents.replay import selected_skills_root

#: Secret keys that configure the model client and hold no credential (Go: ``evidencesink.plainSettings``).
CLIENT_SETTINGS = (
    "ANTHROPIC_BASE_URL",
    "OPENAI_BASE_URL",
    "CLAUDE_CODE_USE_VERTEX",
    "CLOUD_ML_REGION",
    "ANTHROPIC_VERTEX_PROJECT_ID",
    "GOOGLE_APPLICATION_CREDENTIALS",
)


def install_skills(destination: Path, skills: tuple[str, ...], *, skills_root: Path = SKILLS_ROOT) -> None:
    """Replace ``destination/skills/<name>`` with a copy of each named skill.

    Codex reads ``$CODEX_HOME/skills`` and Claude Code reads
    ``$CLAUDE_CONFIG_DIR/skills``. A missing skill is an image-build error and
    fails the worker instead of silently running without its instructions.
    """
    skills_root = selected_skills_root(skills_root)
    for name in skills:
        source = skills_root / name
        if not (source / "SKILL.md").is_file():
            raise ValueError(f"skill {name!r} not found under {skills_root}")
    for name in skills:
        target = destination / "skills" / name
        shutil.rmtree(target, ignore_errors=True)
        shutil.copytree(skills_root / name, target)


def prepare_codex_home(
    *,
    credentials_root: Path = Path("/sdo/credentials"),
    codex_home: Path | None = None,
    skills: tuple[str, ...] = (),
    skills_root: Path = SKILLS_ROOT,
) -> None:
    destination_root = codex_home or Path(os.getenv("CODEX_HOME", str(Path.home() / ".codex")))
    if skills:
        destination_root.mkdir(parents=True, exist_ok=True)
        install_skills(destination_root, skills, skills_root=skills_root)
    _export_client_settings(credentials_root)
    source = credentials_root / "auth.json"
    if source.is_file():
        login = source.read_bytes()
    elif key := _mounted_value(credentials_root, "CODEX_API_KEY", "OPENAI_API_KEY"):
        login = json.dumps({"auth_mode": "apikey", "OPENAI_API_KEY": key}).encode()
    else:
        return
    _write_private(destination_root / "auth.json", login)


def prepare_claude_home(
    *,
    credentials_root: Path = Path("/sdo/credentials"),
    claude_home: Path | None = None,
    skills: tuple[str, ...] = (),
    skills_root: Path = SKILLS_ROOT,
) -> None:
    destination_root = claude_home or Path(os.getenv("CLAUDE_CONFIG_DIR", str(Path.home() / ".claude")))
    if skills:
        destination_root.mkdir(parents=True, exist_ok=True)
        install_skills(destination_root, skills, skills_root=skills_root)
    _export_client_settings(credentials_root)
    source = credentials_root / ".credentials.json"
    if source.is_file():
        _write_private(destination_root / ".credentials.json", source.read_bytes())
    elif token := _mounted_value(credentials_root, "CLAUDE_CODE_OAUTH_TOKEN"):
        write_claude_login(destination_root, token)
    if key := _mounted_value(credentials_root, "ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_API_KEY"):
        copy = destination_root / CLAUDE_API_KEY_FILE
        _write_private(copy, key.encode())
        _write_private(
            destination_root / "settings.json",
            json.dumps({"apiKeyHelper": f"cat {shlex.quote(str(copy))}"}).encode(),
        )


def _mounted_value(root: Path, *names: str) -> str:
    """The first non-empty bare credential among the Secret keys *names*."""
    for name in names:
        if (path := root / name).is_file() and (value := path.read_text(encoding="utf-8").strip()):
            return value
    return ""


def _write_private(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_name(f".{path.name}.{os.getpid()}.new")
    staged.unlink(missing_ok=True)
    with os.fdopen(os.open(staged, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "wb") as stream:
        stream.write(content)
    staged.replace(path)


def _export_client_settings(root: Path) -> None:
    """Put the Secret's non-credential client settings in the environment, where the CLIs read them."""
    for name in CLIENT_SETTINGS:
        value = _mounted_value(root, name)
        if not value or name in os.environ:
            continue
        if name.endswith("_BASE_URL") and "@" in value:
            raise ValueError(f"{name} in the credentials Secret must not embed a credential")
        os.environ[name] = value
