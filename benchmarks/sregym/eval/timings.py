"""Per-cell phase timings, written in a flat format other tools (the lane-overhead work) can read directly.

``timings.json`` holds a ``phases`` map in the fixed order of ``PHASES`` (seconds, or ``null`` when the phase did not
run), the ``spans`` that produced it, and ``overhead_seconds``: everything in the cell outside the agent's own work.
"""

from __future__ import annotations

import json
import os
import time
from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path
    from types import TracebackType

TIMINGS_SCHEMA = "sdo.sregym-eval-timings/v1"
PHASES = ("claim_reset", "deploy", "inject", "page", "agent", "settle", "grade", "collect")
#: The one phase that is the agent's own work; the rest of a cell is overhead to cut.
AGENT_PHASE = "agent"


class _Span:
    def __init__(self, timings: PhaseTimings, name: str) -> None:
        self._timings = timings
        self._name = name
        self._start = 0.0

    def __enter__(self) -> Self:
        self._start = self._timings._now()
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        self._timings._record(self._name, self._start, self._timings._now(), failed=exc_type is not None)


class PhaseTimings:
    def __init__(self, now: Callable[[], float] = time.time) -> None:
        self._now = now
        self._spans: list[dict[str, Any]] = []

    def phase(self, name: str) -> _Span:
        if name not in PHASES:
            raise ValueError(f"unknown phase {name!r}; phases are {', '.join(PHASES)}")
        return _Span(self, name)

    def _record(self, name: str, start: float, end: float, *, failed: bool) -> None:
        span: dict[str, Any] = {"phase": name, "start": start, "end": end}
        if failed:
            span["failed"] = True
        self._spans.append(span)

    def to_dict(self) -> dict[str, Any]:
        phases: dict[str, float | None] = dict.fromkeys(PHASES)
        for span in self._spans:
            phases[span["phase"]] = (phases[span["phase"]] or 0) + span["end"] - span["start"]
        total = sum(value for value in phases.values() if value is not None)
        agent = phases[AGENT_PHASE] or 0
        return {
            "schema": TIMINGS_SCHEMA,
            "phases": phases,
            "spans": self._spans,
            "total_seconds": total,
            "overhead_seconds": total - agent,
        }

    def write(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".tmp")
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(json.dumps(self.to_dict(), indent=2) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
