"""Offline, benchmark-owned projections of accepted operational memory."""

from __future__ import annotations

import shutil
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

ARMS = ("full", "detectors-only", "playbooks-only")


def transform(root: Path, arm: str) -> list[str]:
    """Project an isolated checkout in place; return an auditable metadata edit log.

    Retained artifacts are preserved byte for byte. Detector playbook hints
    remain advisory when the optional playbook catalog is absent.
    """
    if arm not in ARMS:
        raise ValueError(f"unknown ablation arm: {arm}")
    memory = root / ".sdo"
    changes: list[str] = []
    if arm == "detectors-only":
        for directory in ("playbooks", "lib", "corpus"):
            if (memory / directory).exists():
                shutil.rmtree(memory / directory)
    elif arm == "playbooks-only":
        detectors = memory / "diagnostics/detectors"
        if detectors.exists():
            shutil.rmtree(detectors)
    return changes
