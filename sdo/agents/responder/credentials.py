"""Materialize read-only runtime credentials into Codex's writable home."""

from __future__ import annotations

import os
import shutil
from pathlib import Path


def prepare_codex_home(
    *,
    credentials_root: Path = Path("/sdo/credentials"),
    codex_home: Path | None = None,
) -> None:
    source = credentials_root / "auth.json"
    if not source.is_file():
        return
    destination_root = codex_home or Path(os.getenv("CODEX_HOME", str(Path.home() / ".codex")))
    destination_root.mkdir(parents=True, exist_ok=True)
    destination = destination_root / "auth.json"
    shutil.copyfile(source, destination)
    destination.chmod(0o600)


def prepare_claude_home(
    *,
    credentials_root: Path = Path("/sdo/credentials"),
    claude_home: Path | None = None,
) -> None:
    source = credentials_root / ".credentials.json"
    if not source.is_file():
        return
    destination_root = claude_home or Path(os.getenv("CLAUDE_CONFIG_DIR", str(Path.home() / ".claude")))
    destination_root.mkdir(parents=True, exist_ok=True)
    destination = destination_root / ".credentials.json"
    shutil.copyfile(source, destination)
    destination.chmod(0o600)
