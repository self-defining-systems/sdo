"""Read per-detector manifests from an operational repository for analysis and assurance.

Each detector owns ``.sdo/diagnostics/detectors/<health|incidents>/<id>/manifest.yaml``. These readers
are the benchmark-side counterpart of the Go ``memory/layout`` package and exist only because the
benchmarks inspect finished repositories; production code never imports them.
"""

from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING, Any

import yaml

if TYPE_CHECKING:
    from pathlib import Path

DETECTORS_DIR = ".sdo/diagnostics/detectors"
MANIFEST_NAME = "manifest.yaml"
CLASS_DIRS = ("health", "incidents")


def _parse(text: str) -> dict[str, Any] | None:
    document = yaml.safe_load(text)
    return document if isinstance(document, dict) else None


def workspace_detector_manifests(workspace: Path) -> list[dict[str, Any]]:
    """Every detector manifest in a checked-out workspace, ordered by class then ID."""

    manifests: list[dict[str, Any]] = []
    for class_dir in CLASS_DIRS:
        for path in sorted((workspace / DETECTORS_DIR / class_dir).glob(f"*/{MANIFEST_NAME}")):
            document = _parse(path.read_text(encoding="utf-8"))
            if document is not None:
                manifests.append(document)
    return manifests


def git_detector_manifests(repo: Path, rev: str) -> list[dict[str, Any]] | None:
    """Every detector manifest committed at ``rev``; ``None`` when the commit registers no detectors."""

    listing = subprocess.run(
        ["git", "-C", str(repo), "ls-tree", "-r", "--name-only", rev, DETECTORS_DIR],
        capture_output=True,
        text=True,
        check=False,
    ).stdout.splitlines()
    paths = sorted(p for p in listing if p.endswith(f"/{MANIFEST_NAME}") and p.count("/") == 5)
    manifests: list[dict[str, Any]] = []
    for path in paths:
        shown = subprocess.run(
            ["git", "-C", str(repo), "show", f"{rev}:{path}"], capture_output=True, text=True, check=False
        )
        document = _parse(shown.stdout) if shown.returncode == 0 else None
        if document is not None:
            manifests.append(document)
    return manifests or None
