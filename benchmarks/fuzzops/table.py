"""Per-SDO-version results table of a fuzzops campaign: first try, exact repeat and same-app variant.

Campaign layout (all under one sdo-exp directory)::

    <campaign>/<app>/baseline*/runs.jsonl         fuzzops suites: noop and best-fix runs (and screens) per instance
    <campaign>/<app>/lanes/<lane>/lane.json       benchmarks.fuzzops.campaign lane metadata (sdo_version, plan)
    <campaign>/<app>/lanes/<lane>/runs/*/         record.json (fuzzops record), run.json (round, instance)
    <campaign>/<app>/lanes/<lane>/memory/.sdo/outcomes/*.json   SDO's accepted outcome records

For each app and SDO version the baselines and that version's SDO runs are merged into one ``runs.jsonl`` and graded
by ``fuzzops report --plain`` (SDO runs on another cluster than its baselines, so no interference rule); the table
joins those grades (``grades.json``) with SDO's outcome records: tokens and cost, cause class, reach, and whether the
learned playbook applied automatically. ``python -m benchmarks.fuzzops.table <campaign> --fuzzops <checkout>``.
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from agentshim import TokenUsage, cost_usd, default_pricing, price_for

from benchmarks.fuzzops.window import belongs, fault_targets

SDO_PREFIX = "exec:sdo-"


@dataclass
class Row:
    app: str
    version: str  # the column key: SDO version, arm and churn setting
    lane: str
    template: str
    round: str
    instance: str
    valid: bool = True
    verdict: str = ""
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
    #: Distinct (detector, resource) findings on objects the round's background churn touched, whether they joined
    #: the open fault incident or opened their own; None when the round ran without churn.
    churn_findings: int | None = None
    note: str = ""


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


def churn_findings(outcomes: list[dict[str, Any]], rec: dict[str, Any]) -> int | None:
    """Findings caused by the round's background churn: distinct (detector, resource name) pairs, over every incident
    of the round, whose primary resource is an object a churn action touched (the faulted component excluded).

    A responder that closes the fault incident sooner turns churn findings that would have joined it into separate
    incidents (counted as false alarms after churn); this count is the same either way, so columns compare fairly.
    """
    events = (rec.get("churn") or {}).get("events")
    if rec.get("churn") is None:
        return None
    touched = {str(x).split("/")[-1] for ev in events or [] for x in ev.get("objects") or []}
    root = str(((rec.get("lifecycle") or {}).get("root_cause") or {}).get("component") or "").split("/")[-1]
    touched.discard(root)
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


def lane_outcomes(lane: Path) -> list[dict[str, Any]]:
    """The lane's outcome records: the evidence sink's incident ledgers when the lane had a sink (they also carry
    reflection usage), and the accepted memory copy for incidents the sink does not hold."""
    by_id: dict[str, dict[str, Any]] = {}
    meta = _read(lane / "lane.json") or {}
    sink = Path(meta["evidence_dir"]) if meta.get("evidence_dir") else None
    if sink is not None and (sink / "incidents").is_dir():
        for ledger_path in sorted((sink / "incidents").glob("*/ledger.json")):
            ledger = _read(ledger_path) or {}
            doc = ledger.get("outcome_document")
            if isinstance(doc, dict) and doc.get("incident_id"):
                doc = dict(doc)
                doc["_reflection_usage"] = (ledger.get("reflection_decision") or {}).get("usage") or {}
                doc["_reflection_decision"] = (ledger.get("reflection_decision") or {}).get("learning_decision", "")
                by_id[doc["incident_id"]] = doc
    for p in sorted((lane / "memory" / ".sdo" / "outcomes").glob("*.json")):
        doc = _read(p)
        if doc and doc.get("incident_id") and doc["incident_id"] not in by_id:
            by_id[doc["incident_id"]] = doc
    return list(by_id.values())


def _read(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def build_rows(app: str, version: str, lanes: list[Path], grades: list[dict[str, Any]]) -> list[Row]:
    cells = {(c["Fault"], c["Operator"]): c for c in grades}
    rows = []
    for lane in lanes:
        outcomes = lane_outcomes(lane)
        for run_dir in sorted((lane / "runs").iterdir()):
            info_path, rec_path = run_dir / "run.json", run_dir / "record.json"
            if not info_path.is_file() or run_dir.name.endswith("-unhealthy"):
                continue
            info = json.loads(info_path.read_text())
            row = Row(app, version, lane.name, info["template"], info["round"], info["instance"])
            try:
                rec = json.loads(rec_path.read_text())
            except (OSError, json.JSONDecodeError):
                row.valid, row.note = False, "no record"
                rows.append(row)
                continue
            row.valid = bool(rec.get("valid"))
            if not row.valid:
                row.note = str(rec.get("invalid_reason") or rec.get("error") or "invalid")[:120]
            cell = cells.get((row.instance, info.get("operator") or SDO_PREFIX + row.round)) or {}
            best = cells.get((row.instance, "best-fix")) or {}
            bad = (
                (rec.get("window") or {}).get("BadSeconds", 0)
                + (rec.get("bad_seconds_added") or 0)
                + (rec.get("tail") or {}).get("BadSeconds", 0)
            )
            row.bad = float(bad) if row.valid else None
            if cell:
                row.verdict = str(cell.get("Grade") or "")
                row.noop_bad = round(cell.get("NoopBad", 0.0), 1) if cell.get("NoopRuns") else None
                row.lost, row.noop_lost = cell.get("Lost"), cell.get("NoopLost")
                row.harmful = harmful_of(cell)
                life = cell.get("Life") or {}
                row.expected_reach = str(life.get("Reach") or "")
                runs = life.get("Runs") or []
                mine = next((r for r in runs if r.get("Seed") == rec.get("seed")), runs[0] if runs else {})
                row.detect_s, row.localize_s = _opt(mine.get("DetectSec")), _opt(mine.get("LocalSec"))
                row.diagnose_s, row.escalate_s = _opt(mine.get("DiagSec")), _opt(mine.get("ReachSec"))
                row.mitigate_s = _opt(mine.get("RecoverySec")) if mine.get("UsersHurt") else None
                row.localized, row.diagnosed = str(mine.get("Localized") or ""), str(mine.get("Diagnosed") or "")
            if best.get("Runs"):
                row.best_bad = round(best.get("Bad", 0.0), 1)
            row.toward_best = toward_best(row.bad, row.noop_bad, row.best_bad)
            started = _ts(rec.get("started"))
            root = (rec.get("lifecycle") or {}).get("root_cause") or {}
            inject = _ts((rec.get("lifecycle") or {}).get("inject_at")) or started
            if started is not None:
                end_s = float(rec.get("seconds") or 0) + 120
                targets = fault_targets(str(rec.get("fault") or ""), str(root.get("component") or ""))
                picked = outcomes_in(
                    outcomes,
                    started,
                    started.fromtimestamp(started.timestamp() + end_s, started.tzinfo),
                    inject,
                    targets,
                )
                fill_from_outcomes(row, picked, inject)
                row.churn_findings = churn_findings(picked, rec)
            row.reach_ok = reach_ok(row.expected_reach, row.sdo_reach)
            rows.append(row)
    return rows


@dataclass
class Ratio:
    app: str
    version: str
    template: str
    round: str
    time_ratio: float | None
    cost_ratio: float | None
    repeat_mitigate_s: float | None
    auto_applied: str


def _time(row: Row) -> float | None:
    """The round's response time: users recovered, else SDO escalated, else SDO closed."""
    for v in (row.mitigate_s, row.escalate_s, row.closed_s):
        if v is not None:
            return v
    return None


def ratios(rows: list[Row]) -> list[Ratio]:
    first = {(r.app, r.version, r.lane, r.template): r for r in rows if r.round == "first" and r.valid}
    out = []
    for r in rows:
        if r.round == "first" or not r.valid:
            continue
        f = first.get((r.app, r.version, r.lane, r.template))
        if f is None:
            continue
        ft, rt = _time(f), _time(r)
        tr = round(rt / ft, 2) if ft and rt is not None else None
        cr = round(r.cost_usd / f.cost_usd, 2) if f.cost_usd and r.cost_usd is not None else None
        out.append(Ratio(r.app, r.version, r.template, r.round, tr, cr, rt, r.auto_applied))
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
    ("app", "app"),
    ("template", "fault"),
    ("round", "round"),
    ("verdict", "vs noop"),
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
    ("note", "note"),
]


def markdown(version: str, rows: list[Row], rat: list[Ratio]) -> str:
    lines = [
        f"## Column {version}",
        "",
        "Times are seconds from the inject. `noop->best`: 0 = no better than noop, 1 = as good as the reference fix.",
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
            "### Recurrence: repeat and variant against first try",
            "",
            "| app | fault | round | time ratio | cost ratio | time s | auto-applied |",
            "|---|---|---|---|---|---|---|",
        ]
        for q in rat:
            lines.append(
                f"| {q.app} | {q.template} | {q.round} | {_fmt(q.time_ratio)} | {_fmt(q.cost_ratio)} | "
                f"{_fmt(q.repeat_mitigate_s)} | {q.auto_applied} |"
            )
    return "\n".join(lines) + "\n"


def grade(fuzzops: Path, out: Path, records: list[str]) -> list[dict[str, Any]]:
    out.mkdir(parents=True, exist_ok=True)
    (out / "runs.jsonl").write_text("".join(line if line.endswith("\n") else line + "\n" for line in records))
    subprocess.run(
        [str(fuzzops / "bin" / "fuzzops"), "report", "--plain", str(out)], check=True, capture_output=True, text=True
    )
    return json.loads((out / "grades.json").read_text()) or []


def _churned(line: str) -> bool:
    try:
        return json.loads(line).get("churn") is not None
    except json.JSONDecodeError:
        return False


def column_key(meta: dict[str, Any], churn: bool) -> str:
    """One table column: the SDO version, the arm and whether fuzzops' background churn was on."""
    arm = f"-{meta['arm']}" if meta.get("arm") else ""
    return f"{meta['sdo_version'][:9]}{arm}-{'churn' if churn else 'nochurn'}"


@dataclass
class FalseAlarm:
    app: str
    column: str
    lane: str
    incident: str
    detectors: list[str]
    detected_at: str
    after_churn: bool


def false_alarms(app: str, column: str, lane: Path) -> list[FalseAlarm]:
    """Incidents SDO opened while no fault was active: outside every round's inject..run end (window, undo and
    tail), or inside one but
    on churned workloads only, within two minutes of a churn action (after churn)."""
    windows: list[tuple[datetime, datetime, dict[str, Any]]] = []
    for rec_path in sorted((lane / "runs").glob("*/record.json")):
        rec = _read(rec_path) or {}
        life = rec.get("lifecycle") or {}
        start, end = _ts(life.get("inject_at")), _ts(life.get("window_end"))
        began = _ts(rec.get("started"))
        if began is not None and end is not None:
            # The undo and the tail follow the window; what they restart is the round's, not a false alarm.
            end = max(end, began + timedelta(seconds=float(rec.get("seconds") or 0)))
        if start is not None and end is not None:
            windows.append((start, end, rec))
    out = []
    for o in lane_outcomes(lane):
        at = _ts((o.get("timestamps") or {}).get("detected_at"))
        if at is None:
            continue
        findings = o.get("findings") or []
        detectors = sorted({str(f.get("detector_id") or "") for f in findings} - {""})
        inside = [rec for start, end, rec in windows if start <= at <= end]
        churn_hit = False
        for rec in inside:
            touched: set[str] = set()
            for ev in (rec.get("churn") or {}).get("events") or []:
                ended = _ts(ev.get("end")) or _ts(ev.get("start"))
                if ended is not None and timedelta(0) <= at - ended <= timedelta(minutes=2):
                    touched |= {str(x).split("/")[-1] for x in (ev.get("objects") or [])}
            names = {str((f.get("primary_resource") or {}).get("name") or "") for f in findings} - {""}
            root = str(((rec.get("lifecycle") or {}).get("root_cause") or {}).get("component") or "").split("/")[-1]
            if touched and names and names <= touched and root not in names:
                churn_hit = True
        if inside and not churn_hit:
            continue
        out.append(
            FalseAlarm(app, column, lane.name, str(o.get("incident_id") or ""), detectors, at.isoformat(), churn_hit)
        )
    return out


def collect(campaign: Path, fuzzops: Path) -> tuple[dict[str, list[Row]], list[FalseAlarm]]:
    by_column: dict[str, list[Row]] = {}
    alarms: list[FalseAlarm] = []
    for app_dir in sorted(p for p in campaign.iterdir() if (p / "lanes").is_dir()):
        base = [line for f in sorted(app_dir.glob("baseline*/runs.jsonl")) for line in f.read_text().splitlines()]
        lanes_by: dict[str, list[Path]] = {}
        records_by: dict[str, list[str]] = {}
        for lane in sorted((app_dir / "lanes").iterdir()):
            meta = _read(lane / "lane.json")
            if meta is None:
                continue
            lines = []
            for rec in sorted((lane / "runs").glob("*/record.json")):
                text = rec.read_text().strip()
                if text and not rec.parent.name.endswith("-unhealthy"):
                    lines.append(json.dumps(json.loads(text)))
            # A round whose inject failed records no churn; the valid rounds say whether the lane ran with churn.
            basis = [x for x in lines if json.loads(x).get("valid")] or lines
            churn = sum(_churned(x) for x in basis) * 2 > len(basis) if basis else bool(meta.get("churn"))
            key = column_key(meta, churn)
            lanes_by.setdefault(key, []).append(lane)
            records_by.setdefault(key, []).extend(lines)
            alarms.extend(false_alarms(app_dir.name, key, lane))
        for key, lanes in lanes_by.items():
            churn = key.endswith("-churn")
            records = [x for x in base if x.strip() and _churned(x) == churn] + records_by[key]
            grades = grade(fuzzops, app_dir / f"graded-{key}", records)
            by_column.setdefault(key, []).extend(build_rows(app_dir.name, key, lanes, grades))
    return by_column, alarms


def _rate(n: int, d: int) -> str:
    return f"{n}/{d}" if d else "-"


def _mean(xs: list[float]) -> float | None:
    return round(sum(xs) / len(xs), 1) if xs else None


def summary(by_column: dict[str, list[Row]], alarms: list[FalseAlarm]) -> str:
    """Per question: first try, recurrence, false alarms; one line per app and column (arms side by side)."""
    lines = [
        "## Summary",
        "",
        "### First try",
        "",
        "| app | column | rounds | detected | right component | right cause | fixed or escalated | bad s | noop "
        "| best-fix | noop->best |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    rec_lines = [
        "",
        "### Recurrence (repeat / first, same lane)",
        "",
        "| app | column | pairs | median time ratio | median cost ratio | auto-applied | repeat time s |",
        "|---|---|---|---|---|---|---|",
    ]
    for key in sorted(by_column):
        rows = by_column[key]
        for app in sorted({r.app for r in rows}):
            first = [r for r in rows if r.app == app and r.round == "first" and r.valid]
            if first:
                n = len(first)
                det = sum(r.detect_s is not None for r in first)
                comp = sum(r.localized == "yes" for r in first)
                cause = sum(r.diagnosed == "yes" for r in first)
                fixed = sum(
                    r.mitigate_s is not None or (r.reach_ok == "yes" and r.expected_reach == "out-of-reach")
                    for r in first
                )
                lines.append(
                    f"| {app} | {key} | {n} | {_rate(det, n)} | {_rate(comp, n)} | {_rate(cause, n)} | "
                    f"{_rate(fixed, n)} | "
                    f"{_fmt(_mean([r.bad for r in first if r.bad is not None]))} | "
                    f"{_fmt(_mean([r.noop_bad for r in first if r.noop_bad is not None]))} | "
                    f"{_fmt(_mean([r.best_bad for r in first if r.best_bad is not None]))} | "
                    f"{_fmt(_mean([r.toward_best for r in first if r.toward_best is not None]))} |"
                )
            rat = [q for q in ratios([r for r in rows if r.app == app]) if q.round == "repeat"]
            if rat:
                tr = sorted(q.time_ratio for q in rat if q.time_ratio is not None)
                cr = sorted(q.cost_ratio for q in rat if q.cost_ratio is not None)
                auto = sum(q.auto_applied == "yes" for q in rat)
                rt = sorted(q.repeat_mitigate_s for q in rat if q.repeat_mitigate_s is not None)
                rec_lines.append(
                    f"| {app} | {key} | {len(rat)} | {_fmt(tr[len(tr) // 2] if tr else None)} | "
                    f"{_fmt(cr[len(cr) // 2] if cr else None)} | {_rate(auto, len(rat))} | "
                    f"{_fmt(rt[len(rt) // 2] if rt else None)} |"
                )
    churn = [
        "",
        "### Churn findings per round (false alarms compared fairly)",
        "",
        "A faster close turns churn findings that would have joined the fault incident into their own incidents (false "
        "alarms after churn); churn findings count them either way. Per round with churn.",
        "",
        "| app | column | rounds | rounds with churn | churn findings | per churned round | false alarms after churn "
        "| per churned round |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for key in sorted(by_column):
        for app in sorted({r.app for r in by_column[key]}):
            valid = [r for r in by_column[key] if r.app == app and r.valid]
            churned = [r for r in valid if r.churn_findings is not None]
            found = sum(r.churn_findings or 0 for r in churned)
            after = sum(a.after_churn for a in alarms if a.app == app and a.column == key)
            n_churned = len(churned)
            per_found = _fmt(found / n_churned) if n_churned else "-"
            per_after = _fmt(after / n_churned) if n_churned else "-"
            churn.append(
                f"| {app} | {key} | {len(valid)} | {n_churned} | {found} | {per_found} | {after} | {per_after} |"
            )
    fa = [
        "",
        "### False alarms (incidents while no fault was active)",
        "",
        "| app | column | detector | between rounds | after churn |",
        "|---|---|---|---|---|",
    ]
    counts: dict[tuple[str, str, str], list[int]] = {}
    for a in alarms:
        for d in a.detectors or ["(none)"]:
            c = counts.setdefault((a.app, a.column, d), [0, 0])
            c[1 if a.after_churn else 0] += 1
    for (app, col, det), (between, churned) in sorted(counts.items()):
        fa.append(f"| {app} | {col} | {det} | {between} | {churned} |")
    if not counts:
        fa.append("| - | - | none | 0 | 0 |")
    return "\n".join(lines + rec_lines + churn + fa) + "\n"


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("campaign", type=Path)
    p.add_argument("--fuzzops", type=Path, required=True, help="fuzzops checkout with bin/fuzzops built")
    a = p.parse_args(argv)
    by_column, alarms = collect(a.campaign, a.fuzzops.resolve())
    parts = [summary(by_column, alarms)]
    all_rows: list[Row] = []
    for key in sorted(by_column):
        rows = by_column[key]
        all_rows += rows
        parts.append(markdown(key, rows, ratios(rows)))
    (a.campaign / "table.md").write_text("# SDO under fuzzops\n\n" + "\n".join(parts))
    with (a.campaign / "table.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(asdict(all_rows[0]).keys()) if all_rows else ["app"])
        w.writeheader()
        for r in all_rows:
            w.writerow({k: (json.dumps(v) if isinstance(v, list) else v) for k, v in asdict(r).items()})
    (a.campaign / "false-alarms.json").write_text(json.dumps([asdict(x) for x in alarms], indent=2) + "\n")
    print(f"{len(all_rows)} rows, {len(alarms)} false alarms -> {a.campaign}/table.md")
    return 0


if __name__ == "__main__":
    sys.exit(main())
