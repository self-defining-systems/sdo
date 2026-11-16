"""One cell/run model for the analysis reports: loaders over a finished bench cell's evidence.

A *cell* directory holds ``runs/<run>/`` with the grader's ``record.json`` and ``result.json`` and the
operator evidence under ``operator/evidence/incidents/<id>/`` (``ledger.json``, ``reflection-usage.jsonl``).
Every report reads cells through :func:`load_cell`; the per-cell evidence manifest
(``benchmarks.fuzzops.cellevidence``) will replace the ledger reads here and nowhere else.

All timestamps are RFC3339 UTC; every derived duration is ``None`` when its source is absent, and the
loader names the absent facts in ``Incident.missing`` instead of guessing.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path


def ts(value: Any) -> float | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.timestamp() if parsed.tzinfo is not None and parsed.year > 1 else None


def delta(later: float | None, earlier: float | None) -> float | None:
    return None if later is None or earlier is None else later - earlier


def _num(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


@dataclass(frozen=True)
class Usage:
    """Normalized model usage: ``input_tokens`` includes cache reads and writes, ``output_tokens`` reasoning."""

    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float | None = None

    @property
    def tokens(self) -> int:
        return self.input_tokens + self.output_tokens

    @classmethod
    def from_mapping(cls, raw: Any) -> Usage:
        if not isinstance(raw, dict) or not raw:
            return cls()
        cost = raw.get("total_cost_usd", raw.get("cost_usd"))
        return cls(
            int(_num(raw.get("input_tokens"))),
            int(_num(raw.get("output_tokens"))),
            None if cost is None else _num(cost),
        )

    def __add__(self, other: Usage) -> Usage:
        cost = (
            None
            if self.cost_usd is None and other.cost_usd is None
            else (self.cost_usd or 0.0) + (other.cost_usd or 0.0)
        )
        return Usage(self.input_tokens + other.input_tokens, self.output_tokens + other.output_tokens, cost)


@dataclass
class Incident:
    incident_id: str
    detected_at: float | None
    dispatched_at: float | None
    health_cleared_at: float | None
    verified_at: float | None
    first_agent_change_at: float | None
    agent_changes: int
    classification: str
    resolution_path: str
    applied_playbook_ids: list[str]
    applied_started_at: float | None
    applied_completed_at: float | None
    stage_exits: list[str]
    opened_by: list[str]
    responder: Usage
    reflection: Usage
    reflection_decision: str
    learned_playbook_ids: list[str]
    reflection_started_at: float | None
    reflection_ended_at: float | None
    reflection_seconds: float | None
    first_activations: list[tuple[str, str, float]] = field(default_factory=list)  # detector, rule, first activated_at
    missing: list[str] = field(default_factory=list)

    @property
    def used_responder(self) -> bool:
        return self.responder.tokens > 0

    @property
    def page_to_first_change(self) -> float | None:
        return delta(self.first_agent_change_at, self.detected_at)

    @property
    def page_to_recovered(self) -> float | None:
        return delta(self.verified_at, self.detected_at)

    @property
    def page_to_cleared(self) -> float | None:
        return delta(self.health_cleared_at, self.detected_at)

    @property
    def page_to_applied(self) -> float | None:
        return delta(self.applied_completed_at, self.detected_at)

    @property
    def applied_to_recovered(self) -> float | None:
        return delta(self.verified_at, self.applied_completed_at)


@dataclass
class Cell:
    name: str
    run_dir: Path | None
    inject_at: float | None
    recovered_in_window: bool | None
    window_bad_seconds: int | None
    tail_bad_seconds: int | None
    incidents: list[Incident]
    first_bad_second: float | None = None  # seconds after inject of the first failing user second
    run_ok: bool | None = None  # the bench run finished (False: capped, harness failure)
    attempts: int = 1

    @property
    def harm_onset(self) -> float | None:
        return (
            None if self.inject_at is None or self.first_bad_second is None else self.inject_at + self.first_bad_second
        )

    def transcripts(self) -> list[Path]:
        """Agent session JSONL files (responder, its subagents and resumed closing turns) of the cell's incidents."""
        if self.run_dir is None:
            return []
        root = self.run_dir / "operator" / "evidence" / "incidents"
        return sorted(root.glob("*/claude/projects/**/*.jsonl"))

    @property
    def first_incident(self) -> Incident | None:
        return self.incidents[0] if self.incidents else None


def _json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _reflection_turns(ledger_path: Path) -> list[dict[str, Any]]:
    path = ledger_path.with_name("reflection-usage.jsonl")
    if not path.is_file():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def parse_ledger(ledger: dict[str, Any], turns: list[dict[str, Any]] | None = None) -> Incident:
    closure = ledger.get("closure") or {}
    outcome = ledger.get("outcome_document") or {}
    phases = ledger.get("closure_phase_timestamps") or {}
    decision = ledger.get("reflection_decision") or {}
    missing: list[str] = []

    changes = outcome.get("agent_changes") or (closure.get("result") or {}).get("agent_changes") or []
    mitigations = [m for s in outcome.get("prior_stages") or [] for m in s.get("mitigations") or []]
    receipts = [m.get("apply_receipt") or {} for m in mitigations]
    applied_started = min((t for t in (ts(r.get("started_at")) for r in receipts) if t is not None), default=None)
    applied_done = min((t for t in (ts(r.get("completed_at")) for r in receipts) if t is not None), default=None)
    if mitigations and applied_done is None:
        missing.append("playbook applied_at")

    responder = (
        Usage.from_mapping(outcome.get("usage"))
        if outcome.get("usage")
        else Usage.from_mapping((closure.get("result") or {}).get("usage"))
    )
    reflection = Usage.from_mapping(decision.get("usage"))
    turn_usage = Usage()
    for turn in turns or []:
        turn_usage += Usage.from_mapping({"input_tokens": 0, "total_cost_usd": turn.get("cost_usd")})
    if reflection.cost_usd is None and turn_usage.cost_usd is not None:
        reflection = Usage(reflection.input_tokens, reflection.output_tokens, turn_usage.cost_usd)

    started = ts(phases.get("reflection_dispatch")) or ts(ledger.get("reflection_dispatch_at"))
    ended = ts(phases.get("reflection_committed")) or ts(phases.get("reflection_without_commit"))
    seconds = sum(_num(t.get("duration_seconds")) for t in turns or []) if turns else delta(ended, started)
    if turns and not seconds:
        seconds = delta(ended, started)

    first_change = ts(changes[0].get("at")) if changes else None
    keys = (closure.get("recovery_verification") or {}).get("opening_keys") or []
    opened = sorted(
        {
            k.split("\x00")[0] + ":" + k.split("\x00")[-1].split("/")[1]
            if "\x00" in k and "/" in k.split("\x00")[-1]
            else k
            for k in keys
        }
    )
    if not ledger.get("reflection_decision"):
        if outcome.get("classification") == "success":  # only a success closure learns
            missing.append("reflection decision")
    elif decision.get("playbook_ids") and not reflection.tokens and not reflection.cost_usd:
        missing.append("reflection usage")
    if responder.tokens and responder.cost_usd is None:
        missing.append("responder cost")

    return Incident(
        incident_id=str(ledger.get("incident_id") or ""),
        detected_at=ts(closure.get("detected_at")),
        dispatched_at=ts(closure.get("dispatched_at")),
        health_cleared_at=ts(closure.get("health_cleared_at")),
        verified_at=ts(closure.get("verified_at")),
        first_agent_change_at=first_change,
        agent_changes=len(changes),
        classification=str(outcome.get("classification") or ""),
        resolution_path=str(outcome.get("resolution_path") or ""),
        applied_playbook_ids=[str(m.get("playbook_id")) for m in mitigations if m.get("playbook_id")],
        applied_started_at=applied_started,
        applied_completed_at=applied_done,
        stage_exits=[
            str((s.get("recovery_wait") or {}).get("exit_reason", "")).removeprefix("EXIT_REASON_")
            for s in outcome.get("prior_stages") or []
        ],
        opened_by=opened,
        responder=responder,
        reflection=reflection,
        reflection_decision=str(decision.get("learning_decision") or ""),
        learned_playbook_ids=[str(p) for p in decision.get("playbook_ids") or []],
        reflection_started_at=started,
        reflection_ended_at=ended,
        reflection_seconds=seconds,
        first_activations=[
            (str(d.get("detector_id")), str(d.get("rule_id")), t)
            for d in closure.get("detector_timeline") or []
            if (t := ts(d.get("first_activated_at"))) is not None
        ],
        missing=missing,
    )


def pick_run(cell_dir: Path) -> Path:
    """The cell's last successful run (``result.json`` ``ok``), else its last run: earlier attempts may be capped."""
    runs = sorted((cell_dir / "runs").glob("*")) if (cell_dir / "runs").is_dir() else [cell_dir]
    for run in reversed(runs):
        try:
            if _json(run / "result.json").get("ok") is True:
                return run
        except (OSError, ValueError):
            continue
    return runs[-1] if runs else cell_dir


def load_cell(cell_dir: Path) -> Cell:
    """Load the first run under ``cell_dir/runs`` (or ``cell_dir`` itself when it is a run directory)."""
    run = pick_run(cell_dir)
    record = _json(run / "record.json") if (run / "record.json").is_file() else {}
    result = _json(run / "result.json") if (run / "result.json").is_file() else {}
    ledgers = list((run / "operator" / "evidence" / "incidents").glob("*/ledger.json"))
    incidents = [parse_ledger(_json(p), _reflection_turns(p)) for p in ledgers]
    inject = ts(result.get("inject_at"))
    incidents = [
        i for i in incidents if inject is None or i.detected_at is None or i.detected_at >= inject
    ]  # lanes keep old ledgers
    incidents.sort(key=lambda i: i.detected_at or 0.0)
    window = record.get("window") or {}
    tail = record.get("tail") or {}
    return Cell(
        name=cell_dir.name,
        run_dir=run,
        inject_at=ts(result.get("inject_at")),
        recovered_in_window=record.get("recovered_in_window"),
        window_bad_seconds=window.get("BadSeconds"),
        tail_bad_seconds=tail.get("BadSeconds"),
        incidents=incidents,
        first_bad_second=(record.get("lifecycle") or {}).get("first_bad_second"),
        run_ok=result.get("ok") if "ok" in result else None,
        attempts=len(list((cell_dir / "runs").glob("*"))) if (cell_dir / "runs").is_dir() else 1,
    )
