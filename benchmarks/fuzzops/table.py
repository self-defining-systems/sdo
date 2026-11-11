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
from datetime import datetime
from pathlib import Path
from typing import Any

from agentshim import TokenUsage, cost_usd, default_pricing, price_for

SDO_PREFIX = "exec:sdo-"


@dataclass
class Row:
    app: str
    version: str
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
    cost_usd: float = 0.0
    auto_applied: str = ""
    reflection: str = ""
    reflection_cost_usd: float = 0.0
    incidents: list[str] = field(default_factory=list)
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


def outcome_cost(usage: dict[str, Any]) -> tuple[int, int, int, float]:
    tokens = TokenUsage(
        input_tokens=int(usage.get("input_tokens") or 0),
        cached_input_tokens=int(usage.get("cached_input_tokens") or 0),
        output_tokens=int(usage.get("output_tokens") or 0),
    )
    price = price_for("codex", "gpt-6-luna", default_pricing())
    cost = cost_usd(tokens, price) if price is not None else 0.0
    return tokens.input_tokens, tokens.cached_input_tokens, tokens.output_tokens, round(cost, 4)


def outcomes_in(outcomes: list[dict[str, Any]], start: datetime, end: datetime) -> list[dict[str, Any]]:
    """Outcome records whose incident SDO detected inside [start, end], by detection time."""
    picked = []
    for o in outcomes:
        at = _ts((o.get("timestamps") or {}).get("detected_at"))
        if at is not None and start <= at <= end:
            picked.append((at, o))
    return [o for _, o in sorted(picked, key=lambda p: p[0])]


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
        tin, tc, tout, cost = outcome_cost(o.get("usage") or {})
        row.tokens_in += tin
        row.tokens_cached += tc
        row.tokens_out += tout
        row.cost_usd = round(row.cost_usd + cost, 4)
    row.reflection = str(first.get("_reflection_decision") or "")
    row.reflection_cost_usd = outcome_cost(first.get("_reflection_usage") or {})[3]
    row.classification = str(first.get("classification") or "")
    causes = first.get("confirmed_root_causes") or []
    row.cause_class = ",".join(sorted({str(c.get("cause_class") or "") for c in causes} - {""}))
    row.sdo_reach = ",".join(sorted({str(c.get("reach") or "") for c in causes} - {""}))
    row.auto_applied = auto_applied(first)
    done = _ts((first.get("timestamps") or {}).get("completed_at"))
    if inject is not None and done is not None:
        row.closed_s = round((done - inject).total_seconds(), 1)


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
            cell = cells.get((row.instance, SDO_PREFIX + row.round)) or {}
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
            inject = _ts((rec.get("lifecycle") or {}).get("inject_at")) or started
            if started is not None:
                end_s = float(rec.get("seconds") or 0) + 120
                picked = outcomes_in(
                    outcomes, started, started.fromtimestamp(started.timestamp() + end_s, started.tzinfo)
                )
                fill_from_outcomes(row, picked, inject)
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
        cr = round(r.cost_usd / f.cost_usd, 2) if f.cost_usd else None
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
    ("tokens_in", "tokens in"),
    ("cost_usd", "cost $"),
    ("auto_applied", "auto-applied"),
    ("reflection", "learned"),
    ("reflection_cost_usd", "reflection $"),
    ("note", "note"),
]


def markdown(version: str, rows: list[Row], rat: list[Ratio]) -> str:
    lines = [
        f"## SDO {version}",
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


def collect(campaign: Path, fuzzops: Path) -> dict[str, list[Row]]:
    by_version: dict[str, list[Row]] = {}
    for app_dir in sorted(p for p in campaign.iterdir() if (p / "lanes").is_dir()):
        base = [line for f in sorted(app_dir.glob("baseline*/runs.jsonl")) for line in f.read_text().splitlines()]
        lanes_by: dict[str, list[Path]] = {}
        for lane in sorted((app_dir / "lanes").iterdir()):
            meta = lane / "lane.json"
            if meta.is_file():
                lanes_by.setdefault(json.loads(meta.read_text())["sdo_version"], []).append(lane)
        for version, lanes in lanes_by.items():
            records = list(base)
            for lane in lanes:
                for rec in sorted((lane / "runs").glob("*/record.json")):
                    text = rec.read_text().strip()
                    if text and not rec.parent.name.endswith("-unhealthy"):
                        records.append(json.dumps(json.loads(text)))
            grades = grade(fuzzops, app_dir / f"graded-{version}", records)
            by_version.setdefault(version, []).extend(build_rows(app_dir.name, version, lanes, grades))
    return by_version


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("campaign", type=Path)
    p.add_argument("--fuzzops", type=Path, required=True, help="fuzzops checkout with bin/fuzzops built")
    a = p.parse_args(argv)
    for version, rows in collect(a.campaign, a.fuzzops.resolve()).items():
        rat = ratios(rows)
        (a.campaign / f"table-{version}.md").write_text(markdown(version, rows, rat))
        with (a.campaign / f"table-{version}.csv").open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(asdict(rows[0]).keys()) if rows else ["app"])
            w.writeheader()
            for r in rows:
                w.writerow({k: (json.dumps(v) if isinstance(v, list) else v) for k, v in asdict(r).items()})
        (a.campaign / f"ratios-{version}.json").write_text(json.dumps([asdict(q) for q in rat], indent=2) + "\n")
        print(f"{version}: {len(rows)} rows -> {a.campaign}/table-{version}.md")
    return 0


if __name__ == "__main__":
    sys.exit(main())
