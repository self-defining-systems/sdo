"""SDO's results table for fuzzops-bench experiments: first try, repeats on learned memory, and false alarms.

fuzzops-bench grades every experiment itself (``fuzzops-bench results``); this table joins those grades with SDO's
own outcome records, which the ``collect`` hook (``benchmarks.fuzzops.hooks``) copied after every run. One experiment
is one column. Bench layout read here (``<exp>`` = ``<root>/<experiment>``)::

    <exp>/runs/<run-id>/run.json            bench run: fault, sequence, position, seed, attempt, status, reason, lane,
                                            host, operator_label, inject_at, settle_end
    <exp>/runs/<run-id>/record.json         the fuzzops run record (fault, operator, seed, valid, window, lifecycle)
    <exp>/runs/<run-id>/operator/memory/.sdo/outcomes/*.json   SDO's accepted outcome records after the run
    <exp>/runs/<run-id>/operator/evidence/incidents/*/ledger.json   broker ledgers of the run's interval, copied from
                                            the lane's host evidence sink (also carry reflection usage)
    <exp>/results/grades.json               fuzzops grade cells (Fault, Operator, Grade, NoopBad, Life, ...)
    <exp>/results/runs.csv                  per run: fault, position, seed, lane, host, status, reason, grade
    <exp>/gates.jsonl                       failed ready gates (``alarm``: SDO had an incident open while gating)

A run owns ``[inject_at, settle_end]``; SDO incidents belong to it when detected in that interval or open at the
inject and naming the faulted component (``window.belongs``). Only ``done`` runs are graded and counted. Healthy runs
(fault ``healthy``) give the false alarms; a fault run's incidents only on objects its background churn touched are
false alarms after churn.

``uv run python -m benchmarks.fuzzops.table <exp> [<exp> ...] [--out DIR]``
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from agentshim import TokenUsage, cost_usd, default_pricing, price_for

from benchmarks.fuzzops.window import belongs, fault_targets

HEALTHY = "healthy"
#: Seconds after the inject an incident may still belong to a run that has no settle end.
DEFAULT_SPAN_S = 1800.0


@dataclass
class Row:
    experiment: str  # the column
    run: str
    fault: str
    sequence: int
    position: int
    seed: int
    lane: str = ""
    host: str = ""
    status: str = ""
    valid: bool = True
    grade: str = ""
    bad: float | None = None
    noop_bad: float | None = None
    best_bad: float | None = None
    toward_best: float | None = None  # share of the way from noop to best-fix (1 = as good as best-fix)
    lost: int | None = None
    noop_lost: int | None = None
    harmful: list[str] = field(default_factory=list)
    detect_s: float | None = None
    localize_s: float | None = None
    diagnose_s: float | None = None
    mitigate_s: float | None = None  # users recovered (seconds from inject)
    escalate_s: float | None = None
    closed_s: float | None = None  # SDO closed the incident
    localized: str = ""
    diagnosed: str = ""
    expected_reach: str = ""
    sdo_reach: str = ""
    reach_ok: str = ""
    cause_class: str = ""
    classification: str = ""
    tokens_in: int = 0
    tokens_cached: int = 0
    tokens_out: int = 0
    cost_usd: float | None = 0.0  # None: a model without a known price
    auto_applied: str = ""
    reflection: str = ""
    reflection_cost_usd: float | None = 0.0
    incidents: list[str] = field(default_factory=list)
    #: Distinct (detector, resource) findings on objects the run's background churn touched, whether they joined
    #: the open fault incident or opened their own; None when the run had no churn.
    churn_findings: int | None = None
    note: str = ""

    @property
    def round(self) -> str:
        return "first" if self.position <= 1 else "repeat"


def _opt(v: Any) -> float | None:
    if isinstance(v, dict):
        return round(float(v.get("V", 0)), 1) if v.get("OK") else None
    return None


def _ts(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def toward_best(bad: float | None, noop: float | None, best: float | None) -> float | None:
    """Where ``bad`` falls between noop (0) and best-fix (1); None when the two references are too close to tell."""
    if bad is None or noop is None or best is None or noop - best < 5:
        return None
    return round((noop - bad) / (noop - best), 2)


def harmful_of(cell: dict[str, Any]) -> list[str]:
    out: list[str] = []
    for run in (cell.get("Life") or {}).get("Runs") or []:
        out += [str(h) for h in (run.get("Harmful") or [])]
    for key, label in (
        ("Safeguards", "weakened"),
        ("DBRestarts", "restarted data"),
        ("PVCChanges", "pvc"),
        ("Security", "security"),
        ("Admission", "admission"),
    ):
        out += [f"{label}: {x}" for x in (cell.get(key) or [])]
    if cell.get("Grade") == "unsafe":
        out.append("unsafe: lost writes")
    return sorted(set(out))


def outcome_cost(usage: dict[str, Any], outcome: dict[str, Any]) -> tuple[int, int, int, float | None]:
    """Tokens and USD of *usage*, priced as the model the outcome records (``responder_backend``/``responder_model``).

    The cost is ``None`` when that model has no known price: a column never borrows another model's price.
    """
    tokens = TokenUsage(
        input_tokens=int(usage.get("input_tokens") or 0),
        cached_input_tokens=int(usage.get("cached_input_tokens") or 0),
        output_tokens=int(usage.get("output_tokens") or 0),
    )
    backend = str(outcome.get("responder_backend") or "codex")
    price = price_for(backend, str(outcome.get("responder_model") or ""), default_pricing())
    cost = round(cost_usd(tokens, price), 4) if price is not None else None
    return tokens.input_tokens, tokens.cached_input_tokens, tokens.output_tokens, cost


def outcomes_in(
    outcomes: list[dict[str, Any]],
    start: datetime,
    end: datetime,
    inject: datetime | None = None,
    targets: frozenset[str] = frozenset(),
) -> list[dict[str, Any]]:
    """Outcome records of the run's incidents, by detection time.

    An incident detected inside [start, end] belongs to the run; so does one detected before the inject and still open
    at it when its findings, causes or repair actions name the faulted component (see ``window.belongs``).
    """
    picked = [
        (_ts((o.get("timestamps") or {}).get("detected_at")), o)
        for o in outcomes
        if belongs(o, start, end, inject, targets)
    ]
    return [o for _, o in sorted(picked, key=lambda p: p[0] or start)]


def auto_applied(outcome: dict[str, Any]) -> str:
    """yes: the deterministic stage resolved it with a learned playbook; partial: a learned plan applied and the
    responder still ran; no: nothing learned applied."""
    if outcome.get("resolution_path") == "deterministic":
        return "yes"
    if any(p.get("applied") and not p.get("dry_run") for p in outcome.get("plan_applications") or []):
        return "partial"
    for stage in outcome.get("prior_stages") or []:
        if stage.get("stage") == "deterministic" and stage.get("mitigations"):
            return "partial"
    return "no"


def fill_from_outcomes(row: Row, picked: list[dict[str, Any]], inject: datetime | None) -> None:
    row.incidents = [str(o.get("incident_id", "")) for o in picked]
    if not picked:
        row.auto_applied = "n/a"
        return
    first = picked[0]
    for o in picked:
        tin, tc, tout, cost = outcome_cost(o.get("usage") or {}, o)
        row.tokens_in += tin
        row.tokens_cached += tc
        row.tokens_out += tout
        row.cost_usd = None if cost is None or row.cost_usd is None else round(row.cost_usd + cost, 4)
    row.reflection = str(first.get("_reflection_decision") or "")
    row.reflection_cost_usd = outcome_cost(first.get("_reflection_usage") or {}, first)[3]
    row.classification = str(first.get("classification") or "")
    causes = first.get("confirmed_root_causes") or []
    row.cause_class = ",".join(sorted({str(c.get("cause_class") or "") for c in causes} - {""}))
    row.sdo_reach = ",".join(sorted({str(c.get("reach") or "") for c in causes} - {""}))
    row.auto_applied = auto_applied(first)
    done = _ts((first.get("timestamps") or {}).get("completed_at"))
    if inject is not None and done is not None:
        row.closed_s = round((done - inject).total_seconds(), 1)


def _churn_touched(rec: dict[str, Any]) -> set[str]:
    touched = {
        str(x).split("/")[-1] for ev in (rec.get("churn") or {}).get("events") or [] for x in ev.get("objects") or []
    }
    touched.discard(_root_name(rec))
    return touched


def _root_name(rec: dict[str, Any]) -> str:
    return str(((rec.get("lifecycle") or {}).get("root_cause") or {}).get("component") or "").split("/")[-1]


def churn_findings(outcomes: list[dict[str, Any]], rec: dict[str, Any]) -> int | None:
    """Findings caused by the run's background churn: distinct (detector, resource name) pairs, over every incident
    of the run, whose primary resource is an object a churn action touched (the faulted component excluded).

    A responder that closes the fault incident sooner turns churn findings that would have joined it into separate
    incidents (counted as false alarms after churn); this count is the same either way, so columns compare fairly.
    """
    if rec.get("churn") is None:
        return None
    touched = _churn_touched(rec)
    seen = set()
    for o in outcomes:
        for f in o.get("findings") or []:
            name = str((f.get("primary_resource") or {}).get("name") or "")
            if name in touched:
                seen.add((str(f.get("detector_id") or ""), name))
    return len(seen)


def reach_ok(expected: str, said: str) -> str:
    if not expected:
        return ""
    if not said:
        return "not stated"
    return "yes" if expected in said.split(",") else "no"


def _read(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


# ---- the bench layout ----------------------------------------------------------------------------------------


@dataclass(frozen=True)
class BenchRun:
    """One ``<exp>/runs/<run-id>/`` directory."""

    path: Path
    info: dict[str, Any]
    record: dict[str, Any] | None

    @property
    def id(self) -> str:
        return str(self.info.get("id") or self.path.name)

    @property
    def fault(self) -> str:
        return str(self.info.get("fault") or "")

    @property
    def healthy(self) -> bool:
        return self.fault == HEALTHY

    @property
    def done(self) -> bool:
        return self.info.get("status") == "done"

    @property
    def inject(self) -> datetime | None:
        rec = self.record or {}
        return (
            _ts(self.info.get("inject_at"))
            or _ts((rec.get("lifecycle") or {}).get("inject_at"))
            or _ts(rec.get("started"))
        )

    @property
    def end(self) -> datetime | None:
        """The end of the run's interval: the settle end, else the run's end, else a default span after the inject."""
        end = _ts(self.info.get("settle_end")) or _ts(self.info.get("ended"))
        if end is None and self.inject is not None:
            end = self.inject + timedelta(seconds=DEFAULT_SPAN_S)
        return end

    @property
    def operator_label(self) -> str:
        return str((self.record or {}).get("operator") or self.info.get("operator_label") or "")

    @property
    def record_fault(self) -> str:
        """The fault as fuzzops grades it (the instance ID; ``none`` for a healthy run)."""
        return str((self.record or {}).get("fault") or self.fault)

    def outcomes(self) -> list[dict[str, Any]]:
        """SDO's outcome records after this run: the accepted memory the collect hook copied, plus incident ledgers
        (which also carry reflection usage) when the run's operator output holds an evidence sink tree."""
        by_id: dict[str, dict[str, Any]] = {}
        for ledger_path in sorted((self.path / "operator").glob("**/incidents/*/ledger.json")):
            ledger = _read(ledger_path) or {}
            doc = ledger.get("outcome_document")
            if isinstance(doc, dict) and doc.get("incident_id"):
                doc = dict(doc)
                doc["_reflection_usage"] = (ledger.get("reflection_decision") or {}).get("usage") or {}
                doc["_reflection_decision"] = (ledger.get("reflection_decision") or {}).get("learning_decision", "")
                by_id[doc["incident_id"]] = doc
        for p in sorted((self.path / "operator" / "memory" / ".sdo" / "outcomes").glob("*.json")):
            doc = _read(p)
            if doc and doc.get("incident_id") and doc["incident_id"] not in by_id:
                by_id[doc["incident_id"]] = doc
        return list(by_id.values())

    def picked(self) -> list[dict[str, Any]]:
        """The outcome records of the incidents that belong to this run's interval."""
        inject, end = self.inject, self.end
        if inject is None or end is None:
            return []
        root = str((((self.record or {}).get("lifecycle") or {}).get("root_cause") or {}).get("component") or "")
        targets = fault_targets(self.record_fault, root) if not self.healthy else frozenset()
        return outcomes_in(self.outcomes(), inject, end, inject, targets)


def bench_runs(exp: Path) -> list[BenchRun]:
    """Every run directory with a ``run.json``, by fault, sequence, position and attempt."""
    runs = []
    for run_dir in sorted((exp / "runs").iterdir()) if (exp / "runs").is_dir() else []:
        info = _read(run_dir / "run.json")
        if info is None:
            continue
        runs.append(BenchRun(run_dir, info, _read(run_dir / "record.json")))

    def key(r: BenchRun) -> tuple[str, int, int, int]:
        return (
            r.fault,
            int(r.info.get("sequence") or 0),
            int(r.info.get("position") or 0),
            int(r.info.get("attempt") or 0),
        )

    return sorted(runs, key=key)


def _cell_get(cell: dict[str, Any], name: str) -> Any:
    """A grade cell field under fuzzops' Go name (``Fault``) or a lower-case JSON name (``fault``)."""
    if name in cell:
        return cell[name]
    return cell.get(name.lower())


def read_grades(exp: Path) -> list[dict[str, Any]]:
    try:
        data = json.loads((exp / "results" / "grades.json").read_text())
    except (OSError, json.JSONDecodeError):
        return []
    return [c for c in data if isinstance(c, dict)] if isinstance(data, list) else []


def read_run_grades(exp: Path) -> dict[tuple[str, int, int, str], str]:
    """``runs.csv`` grades by (fault, position, seed, lane)."""
    out: dict[tuple[str, int, int, str], str] = {}
    try:
        with (exp / "results" / "runs.csv").open(newline="") as f:
            for r in csv.DictReader(f):
                try:
                    out[
                        (r.get("fault", ""), int(r.get("position") or 0), int(r.get("seed") or 0), r.get("lane", ""))
                    ] = r.get("grade") or ""
                except ValueError:
                    continue
    except OSError:
        pass
    return out


def build_rows(exp: Path, grades: list[dict[str, Any]], run_grades: dict[tuple[str, int, int, str], str]) -> list[Row]:
    """One row per fault run of the experiment (healthy runs give false alarms instead)."""
    cells = {(str(_cell_get(c, "Fault")), str(_cell_get(c, "Operator"))): c for c in grades}
    rows = []
    for run in bench_runs(exp):
        if run.healthy:
            continue
        info, rec = run.info, run.record
        row = Row(
            experiment=str(info.get("experiment") or exp.name),
            run=run.id,
            fault=run.fault,
            sequence=int(info.get("sequence") or 0),
            position=int(info.get("position") or 0),
            seed=int(info.get("seed") or 0),
            lane=str(info.get("lane") or ""),
            host=str(info.get("host") or ""),
            status=str(info.get("status") or ""),
        )
        if not run.done:
            row.valid, row.note = False, str(info.get("reason") or info.get("status") or "not done")[:120]
            rows.append(row)
            continue
        if rec is None:
            row.valid, row.note = False, "no record"
            rows.append(row)
            continue
        row.valid = bool(rec.get("valid"))
        if not row.valid:
            row.note = str(rec.get("invalid_reason") or rec.get("error") or "invalid")[:120]
        cell = cells.get((run.record_fault, run.operator_label)) or {}
        best = cells.get((run.record_fault, "best-fix")) or {}
        bad = (
            (rec.get("window") or {}).get("BadSeconds", 0)
            + (rec.get("bad_seconds_added") or 0)
            + (rec.get("tail") or {}).get("BadSeconds", 0)
        )
        row.bad = float(bad) if row.valid else None
        row.grade = run_grades.get((row.fault, row.position, row.seed, row.lane), "") or str(
            _cell_get(cell, "Grade") or ""
        )
        if cell:
            row.noop_bad = round(_cell_get(cell, "NoopBad") or 0.0, 1) if _cell_get(cell, "NoopRuns") else None
            row.lost, row.noop_lost = _cell_get(cell, "Lost"), _cell_get(cell, "NoopLost")
            row.harmful = harmful_of(cell)
            life = _cell_get(cell, "Life") or {}
            row.expected_reach = str(life.get("Reach") or "")
            life_runs = life.get("Runs") or []
            mine = next((r for r in life_runs if r.get("Seed") == rec.get("seed")), life_runs[0] if life_runs else {})
            row.detect_s, row.localize_s = _opt(mine.get("DetectSec")), _opt(mine.get("LocalSec"))
            row.diagnose_s, row.escalate_s = _opt(mine.get("DiagSec")), _opt(mine.get("ReachSec"))
            row.mitigate_s = _opt(mine.get("RecoverySec")) if mine.get("UsersHurt") else None
            row.localized, row.diagnosed = str(mine.get("Localized") or ""), str(mine.get("Diagnosed") or "")
        if _cell_get(best, "Runs"):
            row.best_bad = round(_cell_get(best, "Bad") or 0.0, 1)
        row.toward_best = toward_best(row.bad, row.noop_bad, row.best_bad)
        picked = run.picked()
        fill_from_outcomes(row, picked, run.inject)
        row.churn_findings = churn_findings(picked, rec)
        row.reach_ok = reach_ok(row.expected_reach, row.sdo_reach)
        rows.append(row)
    return rows


@dataclass
class Ratio:
    experiment: str
    fault: str
    sequence: int
    position: int
    time_ratio: float | None
    cost_ratio: float | None
    repeat_time_s: float | None
    auto_applied: str


def _time(row: Row) -> float | None:
    """The run's response time: users recovered, else SDO escalated, else SDO closed."""
    for v in (row.mitigate_s, row.escalate_s, row.closed_s):
        if v is not None:
            return v
    return None


def _counted(row: Row) -> bool:
    return row.valid and row.status == "done"


def ratios(rows: list[Row]) -> list[Ratio]:
    """Each repeat against the first run of its sequence (same lane, the memory the earlier runs left)."""
    first = {(r.experiment, r.fault, r.sequence): r for r in rows if r.position == 1 and _counted(r)}
    out = []
    for r in rows:
        if r.position <= 1 or not _counted(r):
            continue
        f = first.get((r.experiment, r.fault, r.sequence))
        if f is None:
            continue
        ft, rt = _time(f), _time(r)
        tr = round(rt / ft, 2) if ft and rt is not None else None
        cr = round(r.cost_usd / f.cost_usd, 2) if f.cost_usd and r.cost_usd is not None else None
        out.append(Ratio(r.experiment, r.fault, r.sequence, r.position, tr, cr, rt, r.auto_applied))
    return out


def _fmt(v: Any) -> str:
    if v is None or v == "":
        return "-"
    if isinstance(v, float):
        return f"{v:.0f}" if abs(v) >= 10 else f"{v:.2f}".rstrip("0").rstrip(".")
    if isinstance(v, list):
        return "; ".join(v) if v else "none"
    return str(v)


COLUMNS = [
    ("fault", "fault"),
    ("sequence", "q"),
    ("position", "r"),
    ("grade", "grade"),
    ("bad", "bad s"),
    ("noop_bad", "noop"),
    ("best_bad", "best-fix"),
    ("toward_best", "noop->best"),
    ("lost", "lost writes"),
    ("harmful", "harmful"),
    ("detect_s", "detect"),
    ("localize_s", "component"),
    ("diagnose_s", "diagnosis"),
    ("mitigate_s", "users ok"),
    ("escalate_s", "escalated"),
    ("closed_s", "closed"),
    ("cause_class", "cause class"),
    ("diagnosed", "cause right"),
    ("reach_ok", "reach right"),
    ("churn_findings", "churn findings"),
    ("tokens_in", "tokens in"),
    ("cost_usd", "cost $"),
    ("auto_applied", "auto-applied"),
    ("reflection", "learned"),
    ("reflection_cost_usd", "reflection $"),
    ("lane", "lane"),
    ("note", "note"),
]


def markdown(column: str, rows: list[Row], rat: list[Ratio]) -> str:
    lines = [
        f"## Column {column}",
        "",
        "Times are seconds from the inject. `noop->best`: 0 = no better than noop, 1 = as good as the reference fix. "
        "`q`/`r`: sequence and position (r1 = first try, later positions repeat on the memory the earlier left).",
        "",
        "| " + " | ".join(h for _, h in COLUMNS) + " |",
        "|" + "---|" * len(COLUMNS),
    ]
    for r in rows:
        d = asdict(r)
        lines.append("| " + " | ".join(_fmt(d[k]) for k, _ in COLUMNS) + " |")
    if rat:
        lines += [
            "",
            "### Recurrence: repeats against the first try of their sequence",
            "",
            "| fault | q | r | time ratio | cost ratio | time s | auto-applied |",
            "|---|---|---|---|---|---|---|",
        ]
        for q in rat:
            lines.append(
                f"| {q.fault} | {q.sequence} | {q.position} | {_fmt(q.time_ratio)} | {_fmt(q.cost_ratio)} | "
                f"{_fmt(q.repeat_time_s)} | {q.auto_applied} |"
            )
    return "\n".join(lines) + "\n"


@dataclass
class FalseAlarm:
    column: str
    run: str
    lane: str
    incident: str
    detectors: list[str]
    detected_at: str
    kind: str  # "healthy run" or "after churn"


def _churn_only(o: dict[str, Any], rec: dict[str, Any], at: datetime) -> bool:
    """Whether every finding of the incident is on an object a churn action touched within two minutes before it."""
    touched: set[str] = set()
    for ev in (rec.get("churn") or {}).get("events") or []:
        ended = _ts(ev.get("end")) or _ts(ev.get("start"))
        if ended is not None and timedelta(0) <= at - ended <= timedelta(minutes=2):
            touched |= {str(x).split("/")[-1] for x in (ev.get("objects") or [])}
    names = {str((f.get("primary_resource") or {}).get("name") or "") for f in o.get("findings") or []} - {""}
    return bool(touched and names and names <= touched and _root_name(rec) not in names)


def false_alarms(exp: Path) -> list[FalseAlarm]:
    """Incidents SDO opened while no fault was active: in a healthy run's interval, or in a fault run's interval on
    churned objects only, within two minutes of a churn action (after churn). Only done runs count."""
    out = []
    for run in bench_runs(exp):
        if not run.done:
            continue
        rec = run.record or {}
        column = str(run.info.get("experiment") or exp.name)
        for o in run.picked():
            at = _ts((o.get("timestamps") or {}).get("detected_at"))
            if at is None:
                continue
            if run.healthy:
                kind = "healthy run"
            elif _churn_only(o, rec, at):
                kind = "after churn"
            else:
                continue
            detectors = sorted({str(f.get("detector_id") or "") for f in o.get("findings") or []} - {""})
            out.append(
                FalseAlarm(
                    column,
                    run.id,
                    str(run.info.get("lane") or ""),
                    str(o.get("incident_id") or ""),
                    detectors,
                    at.isoformat(),
                    kind,
                )
            )
    return out


@dataclass
class GateAlarm:
    column: str
    run: str
    lane: str
    at: str
    reasons: list[str]


def gate_alarms(exp: Path) -> list[GateAlarm]:
    """Failed ready gates during which SDO had an incident open (``gates.jsonl`` lines with ``alarm: true``)."""
    out = []
    try:
        lines = (exp / "gates.jsonl").read_text().splitlines()
    except OSError:
        return []
    for line in lines:
        try:
            g = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(g, dict) and g.get("alarm"):
            reasons = [str(r) for r in g.get("reasons") or []]
            out.append(
                GateAlarm(exp.name, str(g.get("run") or ""), str(g.get("lane") or ""), str(g.get("at") or ""), reasons)
            )
    return out


def collect(experiments: list[Path]) -> tuple[dict[str, list[Row]], list[FalseAlarm], list[GateAlarm]]:
    by_column: dict[str, list[Row]] = {}
    alarms: list[FalseAlarm] = []
    gates: list[GateAlarm] = []
    for exp in experiments:
        rows = build_rows(exp, read_grades(exp), read_run_grades(exp))
        by_column.setdefault(exp.name, []).extend(rows)
        alarms += false_alarms(exp)
        gates += gate_alarms(exp)
    return by_column, alarms, gates


def _rate(n: int, d: int) -> str:
    return f"{n}/{d}" if d else "-"


def _mean(xs: list[float]) -> float | None:
    return round(sum(xs) / len(xs), 1) if xs else None


def _median(xs: list[float]) -> float | None:
    xs = sorted(xs)
    return xs[len(xs) // 2] if xs else None


def summary(by_column: dict[str, list[Row]], alarms: list[FalseAlarm], gates: list[GateAlarm] | None = None) -> str:
    """Per question: first try, recurrence, churn findings, false alarms; one line per column."""
    gates = gates or []
    lines = [
        "## Summary",
        "",
        "### First try (position 1)",
        "",
        "| column | runs | detected | right component | right cause | fixed or escalated | bad s | noop | best-fix "
        "| noop->best |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    rec_lines = [
        "",
        "### Recurrence (repeat / first, same sequence)",
        "",
        "| column | pairs | median time ratio | median cost ratio | auto-applied | median repeat time s |",
        "|---|---|---|---|---|---|",
    ]
    churn = [
        "",
        "### Churn findings per run (false alarms compared fairly)",
        "",
        "A faster close turns churn findings that would have joined the fault incident into their own incidents (false "
        "alarms after churn); churn findings count them either way. Per run with churn.",
        "",
        "| column | runs | runs with churn | churn findings | per churned run | false alarms after churn "
        "| per churned run |",
        "|---|---|---|---|---|---|---|",
    ]
    for key in sorted(by_column):
        rows = [r for r in by_column[key] if _counted(r)]
        first = [r for r in rows if r.position == 1]
        if first:
            n = len(first)
            det = sum(r.detect_s is not None for r in first)
            comp = sum(r.localized == "yes" for r in first)
            cause = sum(r.diagnosed == "yes" for r in first)
            fixed = sum(
                r.mitigate_s is not None or (r.reach_ok == "yes" and r.expected_reach == "out-of-reach") for r in first
            )
            lines.append(
                f"| {key} | {n} | {_rate(det, n)} | {_rate(comp, n)} | {_rate(cause, n)} | {_rate(fixed, n)} | "
                f"{_fmt(_mean([r.bad for r in first if r.bad is not None]))} | "
                f"{_fmt(_mean([r.noop_bad for r in first if r.noop_bad is not None]))} | "
                f"{_fmt(_mean([r.best_bad for r in first if r.best_bad is not None]))} | "
                f"{_fmt(_mean([r.toward_best for r in first if r.toward_best is not None]))} |"
            )
        rat = ratios(rows)
        if rat:
            auto = sum(q.auto_applied == "yes" for q in rat)
            rec_lines.append(
                f"| {key} | {len(rat)} | {_fmt(_median([q.time_ratio for q in rat if q.time_ratio is not None]))} | "
                f"{_fmt(_median([q.cost_ratio for q in rat if q.cost_ratio is not None]))} | {_rate(auto, len(rat))} | "
                f"{_fmt(_median([q.repeat_time_s for q in rat if q.repeat_time_s is not None]))} |"
            )
        churned = [r for r in rows if r.churn_findings is not None]
        found = sum(r.churn_findings or 0 for r in churned)
        after = sum(a.kind == "after churn" for a in alarms if a.column == key)
        n_churned = len(churned)
        per_found = _fmt(found / n_churned) if n_churned else "-"
        per_after = _fmt(after / n_churned) if n_churned else "-"
        churn.append(f"| {key} | {len(rows)} | {n_churned} | {found} | {per_found} | {after} | {per_after} |")
    fa = [
        "",
        "### False alarms (incidents while no fault was active)",
        "",
        "| column | detector | healthy runs | after churn |",
        "|---|---|---|---|",
    ]
    counts: dict[tuple[str, str], list[int]] = {}
    for a in alarms:
        for d in a.detectors or ["(none)"]:
            c = counts.setdefault((a.column, d), [0, 0])
            c[1 if a.kind == "after churn" else 0] += 1
    for (col, det), (healthy, churned_n) in sorted(counts.items()):
        fa.append(f"| {col} | {det} | {healthy} | {churned_n} |")
    if not counts:
        fa.append("| - | none | 0 | 0 |")
    gate_lines = ["", "### Alarms while gating (an incident open before an inject)", ""]
    per_gate: dict[str, int] = {}
    for g in gates:
        per_gate[g.column] = per_gate.get(g.column, 0) + 1
    gate_lines += [f"- {col}: {n}" for col, n in sorted(per_gate.items())] or ["- none"]
    return "\n".join(lines + rec_lines + churn + fa + gate_lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("experiments", type=Path, nargs="+", help="bench experiment directories (<root>/<experiment>)")
    p.add_argument("--out", type=Path, help="output directory (default: <experiment>/sdo-table for one experiment)")
    a = p.parse_args(argv)
    if a.out is None and len(a.experiments) > 1:
        p.error("--out is required for several experiments")
    out: Path = a.out or a.experiments[0] / "sdo-table"
    out.mkdir(parents=True, exist_ok=True)
    by_column, alarms, gates = collect([e.resolve() for e in a.experiments])
    parts = [summary(by_column, alarms, gates)]
    all_rows: list[Row] = []
    for key in sorted(by_column):
        rows = by_column[key]
        all_rows += rows
        parts.append(markdown(key, rows, ratios(rows)))
    (out / "table.md").write_text("# SDO under fuzzops-bench\n\n" + "\n".join(parts))
    with (out / "table.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(asdict(all_rows[0]).keys()) if all_rows else ["experiment"])
        w.writeheader()
        for r in all_rows:
            w.writerow({k: (json.dumps(v) if isinstance(v, list) else v) for k, v in asdict(r).items()})
    (out / "false-alarms.json").write_text(
        json.dumps({"incidents": [asdict(x) for x in alarms], "gates": [asdict(g) for g in gates]}, indent=2) + "\n"
    )
    print(f"{len(all_rows)} rows, {len(alarms)} false alarms, {len(gates)} gate alarms -> {out}/table.md")
    return 0


if __name__ == "__main__":
    sys.exit(main())
