"""Read saved reflection ledgers and checkout deltas without executing agents."""

from __future__ import annotations

import json
import subprocess
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

from benchmarks.sregym.analysis.incident_cost import RECEIPT_NAME


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(cwd), *args], check=True, capture_output=True, text=True).stdout


def find_ledger(stage: Path, suffix: str) -> dict[str, Any]:
    """The ``native_ledger`` of the incident ending in *suffix*, from the stage's strict receipts."""

    for path in sorted((stage / "runs").rglob(RECEIPT_NAME)):
        receipt = json.loads(path.read_text(encoding="utf-8"))
        ledger = receipt.get("native_ledger") if isinstance(receipt, dict) else None
        if isinstance(ledger, dict) and str(ledger.get("incident_id", "")).endswith(suffix):
            return ledger
    raise SystemExit(f"no native ledger ending in {suffix!r} under {stage}")


def memory_delta(clone: Path) -> dict[str, list[str]]:
    """Incident detectors and playbooks the reflection added or modified, relative to the outcome commit."""

    added: list[str] = []
    modified: list[str] = []
    for line in _git(clone, "status", "--porcelain", "--untracked-files=all").splitlines():
        status, path = line[:2].strip(), line[3:]
        (added if status in {"??", "A"} else modified).append(path)

    def kinds(paths: list[str], marker: str) -> list[str]:
        return sorted({p.split(marker, 1)[1].split("/", 1)[0] for p in paths if marker in p})

    return {
        "new_detectors": kinds(added, "detectors/incidents/"),
        "modified_detectors": kinds(modified, "detectors/incidents/"),
        "new_playbooks": kinds(added, ".sdo/playbooks/"),
        "modified_playbooks": kinds(modified, ".sdo/playbooks/"),
        "changed_paths": sorted(added + modified),
    }
