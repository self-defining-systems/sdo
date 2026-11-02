"""Worker-owned, atomic accounting snapshots; each process turn has one identity."""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from uuid import uuid4


@dataclass(frozen=True)
class UsageCheckpoint:
    directory: Path
    incident_id: str
    task_id: str
    phase: str
    turn_id: str = field(default_factory=lambda: uuid4().hex)

    def __post_init__(self) -> None:
        if not self.incident_id or not self.task_id or self.phase != "responder":
            raise ValueError("usage checkpoint requires incident, task and responder phase")

    def publish(self, usage: dict[str, int | float], complete: bool) -> None:
        """Replace this turn's cumulative counters; absence means unknown, never zero."""
        self.directory.mkdir(parents=True, exist_ok=True)
        document = {
            "version": 1,
            "incident_id": self.incident_id,
            "task_id": self.task_id,
            "phase": self.phase,
            "turn_id": self.turn_id,
            "complete": complete,
            "usage": usage,
        }
        fd, temporary = tempfile.mkstemp(prefix=".usage-", dir=self.directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump(document, stream, separators=(",", ":"))
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.directory / f"{self.turn_id}.json")
        finally:
            Path(temporary).unlink(missing_ok=True)
