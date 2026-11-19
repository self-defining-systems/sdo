"""The row's live injection marker: the one fact a harness outside the row process needs before the row ends.

A fastloop row injects its own fault and publishes its record only at the online end, so a watcher in another process
(the eval runner's fallback page) cannot otherwise know when the clock started.
"""

from __future__ import annotations

import json
import os
from datetime import datetime  # noqa: TC003
from pathlib import Path  # noqa: TC003

INJECTION_MARKER = "injection.json"


def write_injection_marker(results_dir: Path, started_at: datetime) -> None:
    """Publish the injection start (atomic); called right after the fault is injected."""
    if started_at.tzinfo is None:
        raise ValueError("the injection start must be timezone-aware")
    results_dir.mkdir(parents=True, exist_ok=True)
    temporary = results_dir / f"{INJECTION_MARKER}.tmp"
    temporary.write_text(json.dumps({"started_at": started_at.timestamp()}) + "\n", encoding="utf-8")
    os.replace(temporary, results_dir / INJECTION_MARKER)


def read_injection_marker(results_dir: Path) -> float | None:
    """The injection start in epoch seconds, or ``None`` while the row has not injected."""
    try:
        document = json.loads((results_dir / INJECTION_MARKER).read_text(encoding="utf-8"))
        return float(document["started_at"])
    except (OSError, ValueError, KeyError, TypeError):
        return None
