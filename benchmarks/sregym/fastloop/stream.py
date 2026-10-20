"""Ordered incident streams for one warm application deployment."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


def load_stream(path: Path) -> tuple[tuple[str, ...], ...]:
    """Read a JSON array of problem IDs or arrays of IDs injected together."""

    entries = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(entries, list) or not entries:
        raise ValueError("stream must contain a non-empty JSON array")
    incidents: list[tuple[str, ...]] = []
    for index, entry in enumerate(entries):
        ids = [entry] if isinstance(entry, str) else entry
        if (
            not isinstance(ids, list)
            or not ids
            or any(not isinstance(item, str) or not item.strip() for item in ids)
            or len(ids) != len(set(ids))
        ):
            raise ValueError(f"stream incident {index} must contain distinct non-empty problem IDs")
        incidents.append(tuple(ids))
    return tuple(incidents)
