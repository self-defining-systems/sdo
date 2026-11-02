"""Git-only benchmark provenance over a concurrently published accepted tree."""

from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING

from benchmarks.sregym.adapter import WaitKind, wait_for
from benchmarks.sregym.repository_git import host_git_command

if TYPE_CHECKING:
    from pathlib import Path


def _git(repository: Path, *args: str) -> str:
    return subprocess.check_output(
        host_git_command(repository, *args),
        text=True,
        timeout=wait_for(WaitKind.PROBE),
        stderr=subprocess.PIPE,
    ).strip()


def memory_head(repository: Path) -> str:
    from benchmarks.sregym.adapter import refresh_memory_mirror

    refresh_memory_mirror(repository)
    return _git(repository, "rev-parse", "--verify", "HEAD^{commit}")


def memory_ancestry(repository: Path, before: str, after: str) -> list[str]:
    result = subprocess.run(
        host_git_command(repository, "merge-base", "--is-ancestor", before, after),
        timeout=wait_for(WaitKind.PROBE),
        capture_output=True,
        check=False,
    )
    if result.returncode:
        raise ValueError("eval: accepted memory rewound or lost its encounter ancestry")
    return _git(repository, "rev-list", "--reverse", "--ancestry-path", f"{before}..{after}").splitlines()
