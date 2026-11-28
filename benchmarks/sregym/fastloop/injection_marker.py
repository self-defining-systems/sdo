"""The row's live injection marker: the facts a harness outside the row process needs before the row ends.

A fastloop row injects its own fault and publishes its record only at the online end, so a watcher in another process
(the eval runner's page) cannot otherwise know when the clock started, nor which incidents SDO already had.

The marker is written *before* the injector call, with the row's own clock, so it exists while the call is still
running and the fault may already be live. Its start is therefore slightly earlier than the injector's reported
``started_at`` (the row record's ``t_inject``).
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime  # noqa: TC003
from pathlib import Path  # noqa: TC003

INJECTION_MARKER = "injection.json"


@dataclass(frozen=True)
class InjectionMarker:
    """When the injection started and the incident IDs the controller state named just before it.

    Any other incident ID in the state later is an incident SDO opened since injection, open or already closed.
    """

    started_at: float
    known_incident_ids: frozenset[str]


def write_injection_marker(results_dir: Path, started_at: datetime, known_incident_ids: frozenset[str]) -> None:
    """Publish the injection start and the known incidents (atomic); called right before the injector call."""
    if started_at.tzinfo is None:
        raise ValueError("the injection start must be timezone-aware")
    results_dir.mkdir(parents=True, exist_ok=True)
    temporary = results_dir / f"{INJECTION_MARKER}.tmp"
    document = {"started_at": started_at.timestamp(), "known_incident_ids": sorted(known_incident_ids)}
    temporary.write_text(json.dumps(document) + "\n", encoding="utf-8")
    os.replace(temporary, results_dir / INJECTION_MARKER)


def clear_injection_marker(results_dir: Path) -> None:
    """Withdraw the marker of an injector call that failed."""
    (results_dir / INJECTION_MARKER).unlink(missing_ok=True)


def read_injection_marker(results_dir: Path) -> InjectionMarker | None:
    """The injection marker, or ``None`` while the row has not started injecting."""
    try:
        document = json.loads((results_dir / INJECTION_MARKER).read_text(encoding="utf-8"))
        known = document["known_incident_ids"]
        if not isinstance(known, list):
            return None
        return InjectionMarker(float(document["started_at"]), frozenset(str(item) for item in known))
    except (OSError, ValueError, KeyError, TypeError):
        return None
