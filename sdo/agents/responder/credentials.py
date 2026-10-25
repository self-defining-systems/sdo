"""Materialize read-only runtime credentials and agent skills into the CLIs' writable homes."""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from types import MappingProxyType

#: The skills shipped with SDO: ``sdo/skills`` in the repository and in the agent image.
SKILLS_ROOT = Path(__file__).resolve().parents[2] / "skills"

#: The skills each agent worker installs into its CLI home. The registration
#: manifests (``sdo/agents/registrations``) are protobuf ``AgentRegistration``
#: messages with no skills field, so the list is a per-worker constant here.
AGENT_SKILLS = MappingProxyType(
    {
        "responder": ("sdo-memory", "sdo-incident-response"),
        "reflector": ("sdo-memory", "sdo-detector-authoring", "sdo-playbook-authoring"),
        "health-judge": ("sdo-memory", "sdo-detector-authoring", "sdo-playbook-authoring"),
        "deployer": ("sdo-memory",),
    }
)


def install_skills(destination: Path, skills: tuple[str, ...], *, skills_root: Path = SKILLS_ROOT) -> None:
    """Replace ``destination/skills/<name>`` with a copy of each named skill.

    Codex reads ``$CODEX_HOME/skills`` and Claude Code reads
    ``$CLAUDE_CONFIG_DIR/skills``. A missing skill is an image-build error and
    fails the worker instead of silently running without its instructions.
    """
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
    source = credentials_root / "auth.json"
    if not source.is_file():
        return
    destination_root.mkdir(parents=True, exist_ok=True)
    destination = destination_root / "auth.json"
    shutil.copyfile(source, destination)
    destination.chmod(0o600)


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
    source = credentials_root / ".credentials.json"
    if not source.is_file():
        return
    destination_root.mkdir(parents=True, exist_ok=True)
    destination = destination_root / ".credentials.json"
    shutil.copyfile(source, destination)
    destination.chmod(0o600)
