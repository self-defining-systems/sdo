"""Worker-owned, atomic accounting snapshots; each process turn has one identity.

Accounting never fails the agent it measures: ``UsageCheckpoint.open`` and ``publish`` log a failure once and
return, and whatever was published last stays on disk marked incomplete.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from time import monotonic
from uuid import uuid4

logger = logging.getLogger(__name__)

#: Phases of one incident, identified by incident and task. "responder" is the older label of "respond".
INCIDENT_PHASES = frozenset({"responder", "respond", "investigate", "reflect", "source_repair"})
#: Phases outside any incident, identified by their task alone (their incident id is empty). Go sets them through
#: ``SDO_SETUP_USAGE_PHASE``.
SETUP_PHASES = frozenset({"deploy", "design"})


@dataclass(frozen=True)
class UsageCheckpoint:
    directory: Path
    incident_id: str
    task_id: str
    phase: str
    turn_id: str = field(default_factory=lambda: uuid4().hex)

    started: float = field(default_factory=monotonic, init=False)
    _failures: list[Exception] = field(default_factory=list, init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.phase not in INCIDENT_PHASES | SETUP_PHASES:
            raise ValueError(f"usage checkpoint phase {self.phase!r} is not recognized")
        if not self.task_id:
            raise ValueError(f"usage checkpoint for phase {self.phase!r} requires a task")
        if (self.phase in INCIDENT_PHASES) != bool(self.incident_id):
            raise ValueError(f"usage checkpoint for phase {self.phase!r} has the wrong incident id")

    @classmethod
    def open(cls, directory: Path, incident_id: str, task_id: str, phase: str) -> UsageCheckpoint | None:
        """Start one turn's accounting with an incomplete record, or log why there is none."""
        try:
            checkpoint = cls(directory, incident_id, task_id, phase)
        except Exception as exc:
            logger.warning(
                "usage checkpoint unavailable, usage is partial: path=%s phase=%s: %s", directory, phase, exc
            )
            return None
        checkpoint.publish({}, False)
        return checkpoint

    def publish(self, usage: dict[str, int | float], complete: bool) -> None:
        """Replace this turn's cumulative counters; absence means unknown, never zero. Never raises."""
        try:
            self._write(usage, complete)
        except Exception as exc:
            if not self._failures:
                logger.warning(
                    "usage checkpoint not written, usage is partial: path=%s phase=%s: %s",
                    self.directory,
                    self.phase,
                    exc,
                )
            self._failures.append(exc)

    def _write(self, usage: dict[str, int | float], complete: bool) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        document = {
            "version": 1,
            "incident_id": self.incident_id,
            "task_id": self.task_id,
            "phase": self.phase,
            "turn_id": self.turn_id,
            "complete": complete,
            "duration_seconds": monotonic() - self.started,
            "partial_reason": None if complete else "turn_accounting_unfinished",
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
