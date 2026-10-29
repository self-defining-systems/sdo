"""Command-local Git trust for explicitly selected controller-shared checkouts."""

from __future__ import annotations

import shlex
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


def host_git_command(repository: Path, *args: str) -> list[str]:
    """Trust exactly this resolved checkout for one command, irrespective of its uid."""
    path = str(repository.resolve())
    return ["git", "-c", f"safe.directory={path}", "-C", path, *args]


def host_clone_command(source: Path, destination: Path, *options: str) -> list[str]:
    """Clone a shared source, including upload-pack's ownership check on its Git directory."""
    path = source.resolve()
    return [
        "git",
        "-c",
        f"safe.directory={path}",
        "-c",
        f"safe.directory={path / '.git'}",
        "clone",
        "--no-local",
        *options,
        "--upload-pack="
        + shlex.join(["git", "-c", f"safe.directory={path}", "-c", f"safe.directory={path / '.git'}", "upload-pack"]),
        "--",
        str(path),
        str(destination),
    ]
