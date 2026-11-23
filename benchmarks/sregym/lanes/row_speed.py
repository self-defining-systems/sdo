"""Per-row speed of a finished SREGym lanes plan against each row's first-principles floor.

``python -m benchmarks.sregym.lanes.row_speed --root ROOT --plan NAME --out DIR [--lanes-bin BIN]``

Reads the plan under ``ROOT/plans/NAME`` and each final attempt's run directory (``run.json``, ``record.json``,
``runner.log``, ``exec/fastloop-results/``), splits each row's wall into stages and writes ``DIR/final-speed.md``
(one table row per plan row, medians, per-arm medians) and ``DIR/final-speed.json`` (what
``benchmarks.sregym.lanes.learning_plan --timing-from`` reads).

Floor of a row = inject + detect + agent or playbook + verify (all measured in the row, they are the experiment) +
the closing turn on p1 only (the only arm whose learning is used) + a restart-from-memory ready gate
(``--gate-floor-s``, 90 s). Rows of the untimed warm-up arm ``w`` are listed but kept out of the medians.

With ``--lanes-bin`` the final attempts and the reseed column come from ``lanes plan results --tsv --final``;
without it the last attempt of each node is final and the reseed class comes from the row's record.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import statistics
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

STAGES = ("gate", "inject", "detect", "agent", "verify", "tail", "closing", "reset")
NAMES = {
    "gate": "ready gate",
    "inject": "inject",
    "detect": "detect (+dispatch)",
    "agent": "agent / playbook",
    "verify": "verify (window end)",
    "tail": "tail",
    "closing": "closing turn",
    "reset": "reset (settle/collect)",
}
ARM_ORDER = {"cold": 0, "p1": 1, "p2": 2}
LOG_MARKS = {
    "wipe": r"Tier-1 wipe",
    "controller_ready": r"seeded controller ready",
    "leave": r"Leaving .* active|incident closure work",
    "reflection": r"waiting for reflection drain",
    "end_loop": r"incident \d+ .*oracle=",
}
LOG_STAMP = re.compile(r"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d)")


def epoch(value: Any) -> float | None:
    """ISO or ``YYYY-mm-dd HH:MM:SS,mmm`` log time (naive = UTC) -> epoch seconds."""
    if value in (None, ""):
        return None
    text = str(value).replace("Z", "+00:00").replace(",", ".")
    if " " in text and "T" not in text:
        text = text.replace(" ", "T", 1)
    stamp = datetime.fromisoformat(re.sub(r"(\.\d{6})\d+", r"\1", text))
    return (stamp if stamp.tzinfo else stamp.replace(tzinfo=UTC)).timestamp()


def read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None


def log_marks(path: Path) -> dict[str, float | None]:
    marks: dict[str, float | None] = dict.fromkeys(LOG_MARKS)
    try:
        lines = path.read_text(errors="replace").splitlines()
    except OSError:
        return marks
    for line in lines:
        stamp = LOG_STAMP.match(line)
        if not stamp:
            continue
        for key, pattern in LOG_MARKS.items():
            if marks[key] is None and re.search(pattern, line):
                marks[key] = epoch(stamp.group(1))
    return marks


def reseed_class(record: dict[str, Any], tsv_row: dict[str, str] | None) -> str:
    """``reseeded``, ``fallback``, ``fresh`` or ``n/a``: the grade's ``reseed`` when known, else the record's."""
    if tsv_row:
        try:
            grade = json.loads(tsv_row.get("grade") or "")
        except ValueError:
            grade = None
        if isinstance(grade, dict) and isinstance(grade.get("reseed"), str):
            return grade["reseed"]
    reseed = record.get("reseed")
    if isinstance(reseed, dict):
        if not reseed.get("attempted"):
            return "fresh"
        return "reseeded" if reseed.get("reused") else "fallback"
    return "n/a"


def lanes_results(root: Path, plan: str, lanes_bin: str) -> dict[tuple[str, str], dict[str, str]]:
    try:
        out = subprocess.run(
            [lanes_bin, "plan", "results", plan, "--tsv", "--final", "--root", str(root)],
            capture_output=True,
            text=True,
            timeout=60,
            check=True,
        ).stdout
    except (OSError, subprocess.SubprocessError) as error:
        detail = getattr(error, "stderr", "") or error
        raise SystemExit(f"lanes plan results {plan} failed: {detail}") from error
    return {(row["exp"], row["run"]): row for row in csv.DictReader(out.splitlines(), delimiter="\t")}


def load_row(
    run_dir: Path, run: dict[str, Any], node: dict[str, Any], tsv_row: dict[str, str] | None
) -> dict[str, Any]:
    record = read_json(run_dir / "record.json") or {}
    sregym = record.get("sregym") or {}
    results = run_dir / "exec" / "fastloop-results"
    injection = (read_json(results / "incidents.online" / "000000.json") or {}).get("injection") or {}
    admission = results / "admission"
    row: dict[str, Any] = {
        "node": node["id"],
        "arm": node.get("arm"),
        "item": node.get("item"),
        "lane": run.get("lane"),
        "start": epoch(run.get("started")),
        "end": epoch(run.get("ended")),
        "inject_start": epoch(injection.get("started_at")) or epoch(run.get("inject_at")),
        "inject_end": epoch(injection.get("finished_at")),
        "onboarding": (read_json(admission / "onboarding.json") or {}).get("seconds") or 0,
        "prior_closure": (read_json(admission / "prior-closure.json") or {}).get("seconds") or 0,
        "reseed": reseed_class(record, tsv_row),
    }
    for key in ("detected_at", "dispatched_at", "resolved_at", "reflection_started_at", "paged_at"):
        row[key] = epoch(sregym.get(key))
    row["resolution"] = sregym.get("resolution_path")
    row["page"] = sregym.get("page_source")
    row.update(log_marks(run_dir / "runner.log"))
    return row


def load_rows(root: Path, plan: str, tsv: dict[tuple[str, str], dict[str, str]] | None) -> list[dict[str, Any]]:
    state = read_json(root / "plans" / plan / "state.json")
    if state is None:
        raise SystemExit(f"no plan state at {root / 'plans' / plan / 'state.json'}")
    rows = []
    for node in state.get("nodes", []):
        attempts = node.get("attempts") or [{"exp": node.get("exp")}]
        if tsv is None:
            attempts = attempts[-1:]
        for attempt in attempts:
            exp = attempt.get("exp")
            if not exp:
                continue
            for run_dir in sorted((root / exp / "runs").glob("*")):
                run = read_json(run_dir / "run.json")
                if not run or not run.get("ended"):
                    continue
                key = (run.get("experiment") or exp, run_dir.name)
                if tsv is not None and (key not in tsv or tsv[key].get("status") != "done"):
                    continue
                rows.append(load_row(run_dir, run, node, tsv.get(key) if tsv else None))
    return rows


def stages(row: dict[str, Any]) -> dict[str, float]:
    """Seconds per stage group; a stage whose bounds the row did not record counts 0."""
    seconds: dict[str, float] = dict.fromkeys(STAGES, 0.0)

    def add(stage: str, start: float | None, end: float | None) -> None:
        if start is not None and end is not None:
            seconds[stage] += end - start

    start, inject = row["start"], row["inject_start"]
    if inject is None:
        return seconds
    add("gate", start, inject)
    inject_end = row["inject_end"] or inject
    add("inject", inject, inject_end)
    detected = row["detected_at"] or row["paged_at"]
    if row["page"] == "fallback":
        detected = row["dispatched_at"] or detected
    add("detect", inject_end, detected)
    resolved = row["resolved_at"]
    if row["resolution"] != "deterministic":
        dispatched = row["dispatched_at"] or detected
        add("detect", detected, dispatched)
        detected = dispatched
    add("agent", detected, resolved)
    add("verify", resolved, row["leave"])
    closing_start = row["reflection_started_at"] or row["reflection"]
    add("tail", row["leave"], closing_start or row["end_loop"])
    if closing_start:
        add("closing", closing_start, row["end_loop"])
    add("reset", row["end_loop"], row["end"])
    return seconds


def measure(rows: list[dict[str, Any]], gate_floor_s: float) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        seconds = stages(row)
        closing = seconds["closing"] if row["arm"] == "p1" else 0.0
        floor = sum(seconds[s] for s in ("inject", "detect", "agent", "verify")) + closing + gate_floor_s
        out.append(
            {
                "node": row["node"],
                "arm": row["arm"],
                "item": row["item"],
                "lane": row["lane"],
                "reseed": row["reseed"],
                "resolution": row["resolution"],
                "page": row["page"],
                "wall": row["end"] - row["start"],
                "stages": seconds,
                "floor": floor,
            }
        )
    out.sort(key=lambda x: (x["item"] or "", ARM_ORDER.get(x["arm"], 3)))
    return out


def report(plan: str, measured: list[dict[str, Any]], gate_floor_s: float) -> str:
    warmups = [x for x in measured if x["arm"] == "w"]
    rows = [x for x in measured if x["arm"] != "w"]
    header = ["row", "lane", "start", *(NAMES[s] for s in STAGES), "row wall", "floor", "wall/floor"]
    lines = [
        f"# Per-row stages: plan {plan}",
        "",
        "Seconds. Floor = inject + detect + agent/playbook + verify + closing turn on p1 only"
        f" + {gate_floor_s:.0f} s gate.",
        "",
        "| " + " | ".join(header) + " |",
        "|" + "---|" * len(header),
    ]
    for x in rows:
        cells = [f"{x['arm']}/{x['item']} ({x['resolution']})", str(x["lane"]), str(x["reseed"])]
        cells += [f"{x['stages'][s]:.0f}" for s in STAGES]
        cells += [f"{x['wall']:.0f}", f"{x['floor']:.0f}", f"{x['wall'] / x['floor']:.2f}"]
        lines.append("| " + " | ".join(cells) + " |")
    if rows:
        lines += ["", f"Medians over {len(rows)} rows:", "", "| stage | median s |", "|---|---|"]
        lines += [f"| {NAMES[s]} | {statistics.median(x['stages'][s] for x in rows):.0f} |" for s in STAGES]
        lines.append(f"| **row wall** | {statistics.median(x['wall'] for x in rows):.0f} |")
        lines.append(f"| **wall / floor** | {statistics.median(x['wall'] / x['floor'] for x in rows):.2f} |")
        lines += [
            "",
            "| arm | rows | median wall | median floor | median wall/floor | reseeded |",
            "|---|---|---|---|---|---|",
        ]
        for arm in sorted({x["arm"] for x in rows}, key=lambda a: (ARM_ORDER.get(a, 3), a)):
            mine = [x for x in rows if x["arm"] == arm]
            if mine:
                lines.append(
                    f"| {arm} | {len(mine)} | {statistics.median(x['wall'] for x in mine):.0f} | "
                    f"{statistics.median(x['floor'] for x in mine):.0f} | "
                    f"{statistics.median(x['wall'] / x['floor'] for x in mine):.2f} | "
                    f"{sum(x['reseed'] == 'reseeded' for x in mine)} |"
                )
    if warmups:
        lines += ["", "Untimed warm-up rows (arm w, not in the medians):", ""]
        lines += [
            f"- {x['item']} on {x['lane']}: wall {x['wall']:.0f} s, ready gate {x['stages']['gate']:.0f} s"
            for x in warmups
        ]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", required=True, type=Path, help="lanes root (holds plans/ and the experiments)")
    ap.add_argument("--plan", required=True)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--lanes-bin", default="", help="lanes binary: final attempts and reseed from plan results")
    ap.add_argument("--gate-floor-s", type=float, default=90.0)
    args = ap.parse_args(argv)
    root = args.root.resolve()
    tsv = lanes_results(root, args.plan, args.lanes_bin) if args.lanes_bin else None
    rows = [row for row in load_rows(root, args.plan, tsv) if row["start"] is not None and row["end"] is not None]
    measured = measure(rows, args.gate_floor_s)
    args.out.mkdir(parents=True, exist_ok=True)
    text = report(args.plan, measured, args.gate_floor_s)
    (args.out / "final-speed.md").write_text(text)
    (args.out / "final-speed.json").write_text(json.dumps([x for x in measured if x["arm"] != "w"], indent=1) + "\n")
    print(text, end="")


if __name__ == "__main__":
    main()
