"""The closing record: what happens on the held lane after a row's online end.

A row ends once its outcome is committed and graded (``row.json``). The closing turn (SDO's reflection and its cleanup
turns), the end-of-row fault recovery and the process exit then drain on the same still-held lane, and ``closing.json``
records them: offline tokens and USD, ``closing_s``, ``drain_s``, ``lane_occupancy_s``, the learning delta and the
closing outcome. The offline tokens are summed from the closing turn's own usage log in the collected evidence, never
from the arm's own estimate, so the table and the evidence cannot disagree.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

from benchmarks.sregym.analysis.incident_cost import logged_reflection_cost
from benchmarks.sregym.eval.results import TokenRow

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping
    from pathlib import Path

CLOSING_FILE = "closing.json"
CLOSING_SCHEMA = "sdo.sregym-eval-closing/v1"
#: ``drained``: the process exited with the closing turn finished. ``not_drained``: the drain deadline passed and the
#: process was stopped, so the cell's learning is excluded by name. ``not_applicable``: the arm has no closing turn
#: (or the cell never started one). ``error``: the drain itself failed. ``pending`` is never terminal.
ClosingStatus = Literal["drained", "not_drained", "not_applicable", "error"]
TERMINAL_STATUSES = frozenset({"drained", "not_drained", "not_applicable", "error"})


@dataclass(frozen=True)
class ClosingResult:
    """What an arm reports once its closing turn, recovery and exit are over (or were stopped)."""

    status: ClosingStatus
    incident_id: str | None = None
    #: The closing turn's own wall time (the reflection and its cleanup turns), as the arm measured it.
    closing_s: float | None = None
    #: The harness fault recovery and cleanup the arm paid at the row's end; the same step for both arms.
    recovery_s: float | None = None
    #: Head of the lane's accepted memory after the closing turn: the pass-1 delta. ``None`` unless the closing turn
    #: finished and was accepted; set on an ``error`` status too when only the lane's end-of-row recovery failed.
    memory_commit_after: str | None = None
    #: The closing outcome: the reflection's learning decision (``updated``, ``failed``, ...) or ``not_drained``.
    decision: str | None = None
    failure_reason: str | None = None
    #: Playbooks, detectors, lib and corpus the closing turn added, with the commit.
    learning: Mapping[str, Any] = field(default_factory=dict)
    source_repair: str | None = None
    source_repair_uncaptured: str | None = None
    cleanup_turns_used: int | None = None
    #: The closure's phase timestamps from the ledger (reflection dispatch, commit, source repair, cleanup).
    phases: Mapping[str, str] = field(default_factory=dict)
    #: The end-of-row recovery facts, for the arm-fairness comparison.
    recovery: Mapping[str, Any] = field(default_factory=dict)
    #: The arm's own offline estimate, used only when the collected evidence holds no usage log.
    offline_fallback: TokenRow | None = None
    error: str | None = None


def offline_tokens_from_evidence(directory: Path, incident_id: str) -> TokenRow | None:
    """The closing turn's tokens and USD, summed from its ``reflection-usage.jsonl`` in the collected evidence."""
    logged = logged_reflection_cost(directory, incident_id)
    if logged is None:
        return None
    usage = logged.usage
    return TokenRow(
        uncached_input=usage.uncached_input_tokens,
        cache_read=usage.cached_input_tokens,
        cache_write=usage.cache_write_input_tokens,
        output=usage.output_tokens,
        requests=usage.requests,
        usd=logged.cost_usd,
    )


def read_closing(directory: Path) -> dict[str, Any] | None:
    path = directory / CLOSING_FILE
    if not path.is_file():
        return None
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    return document if isinstance(document, dict) else None


def closing_is_terminal(document: Mapping[str, Any] | None) -> bool:
    return document is not None and document.get("status") in TERMINAL_STATUSES


def pending_closings(directories: Iterable[Path]) -> list[Path]:
    """The cell directories that have no terminal ``closing.json`` yet: a merge barrier waits for this to be empty."""
    return [directory for directory in directories if not closing_is_terminal(read_closing(directory))]
