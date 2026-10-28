"""Commit synthetic manifests as a workspace's accepted source (the one input restore reads)."""

from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING, Any

import yaml

from benchmarks.sregym.fastloop.accepted_source import AcceptedSource

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

REAL_RUN = subprocess.run


def commit_accepted_source(
    workspace: Path,
    manifests: Sequence[dict[str, Any]] = (),
    *,
    namespace: str | None = None,
    path: str = "kubernetes/manifests.yaml",
) -> AcceptedSource:
    """Commit ``manifests`` into ``workspace`` (a git repo is created) and return how restore reads them."""
    workspace.mkdir(parents=True, exist_ok=True)

    def git(*args: str) -> None:
        REAL_RUN(
            ["git", "-C", str(workspace), "-c", "user.name=Fixture", "-c", "user.email=fixture@example.com", *args],
            check=True,
            capture_output=True,
        )

    if not (workspace / ".git").exists():
        git("init", "-q")
    if manifests:
        (workspace / path).parent.mkdir(parents=True, exist_ok=True)
        (workspace / path).write_text(yaml.safe_dump_all(manifests))
    if not (workspace / "kubernetes").exists():
        # An explicitly empty synthetic inventory still selects a real source
        # file. Production rendering must never treat missing paths as empty.
        (workspace / "kubernetes").mkdir()
        (workspace / "kubernetes/manifests.yaml").write_text("# Synthetic empty inventory.\n")
    git("add", "-A")
    git("commit", "-q", "--allow-empty", "-m", "accepted source")
    if namespace is None:
        namespace = next((m["metadata"]["namespace"] for m in manifests if m["metadata"].get("namespace")), "app")
    return AcceptedSource(workspace, namespace)


def fixed_source_provider(baseline, namespace):
    """An immutable synthetic renderer boundary for external-I/O unit fixtures."""
    from copy import deepcopy
    from dataclasses import dataclass

    @dataclass(frozen=True)
    class SourceFixture:
        namespace: str
        manifests: tuple[dict[str, Any], ...]

    baselines = baseline if isinstance(baseline, tuple) else (baseline,)
    source = SourceFixture(namespace, tuple(deepcopy(obj) for row in baselines for obj in row.objects))
    return lambda: source
