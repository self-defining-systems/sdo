"""SDO's table over the fuzzops-bench results layout: grades joined with SDO's outcome records per run."""

from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import pytest
from agentshim import TokenUsage, cost_usd, default_pricing, price_for

from benchmarks.fuzzops import table
from benchmarks.fuzzops.table import (
    auto_applied,
    bench_runs,
    build_rows,
    churn_findings,
    collect,
    false_alarms,
    gate_alarms,
    outcome_cost,
    outcomes_in,
    ratios,
    reach_ok,
    read_run_grades,
    summary,
    toward_best,
)

if TYPE_CHECKING:
    from pathlib import Path

FAULT = "t@deploy/web"
USAGE = {"input_tokens": 1_000_000, "cached_input_tokens": 800_000, "output_tokens": 10_000}


def _run(
    exp: Path,
    run_id: str,
    *,
    fault: str = FAULT,
    sequence: int = 1,
    position: int = 1,
    seed: int = 1,
    hour: int = 12,
    status: str = "done",
    record: dict[str, Any] | None = None,
) -> Path:
    run = exp / "runs" / run_id
    (run / "operator" / "memory" / ".sdo" / "outcomes").mkdir(parents=True)
    inject = f"2026-01-01T{hour}:00:00Z"
    info = {
        "id": run_id,
        "experiment": exp.name,
        "fault": fault,
        "sequence": sequence,
        "position": position,
        "seed": seed,
        "attempt": 1,
        "status": status,
        "reason": "" if status == "done" else "capped: busy: reflection queued",
        "lane": "lane1",
        "host": "host1",
        "operator_label": f"exec:sdo-{exp.name}-r{position}",
        "inject_at": inject,
        "settle_end": f"2026-01-01T{hour}:20:00Z",
    }
    (run / "run.json").write_text(json.dumps(info))
    rec = {
        "fault": "none" if fault == "healthy" else fault,
        "operator": info["operator_label"],
        "seed": seed,
        "valid": True,
        "started": inject,
        "window": {"BadSeconds": 100 // position},
        "tail": {"BadSeconds": 0},
        "lifecycle": {"inject_at": inject},
    }
    rec.update(record or {})
    (run / "record.json").write_text(json.dumps(rec))
    return run


def _outcome(run: Path, incident: str, detected: str, **fields: Any) -> None:
    doc = {
        "incident_id": incident,
        "classification": "success",
        "responder_backend": "codex",
        "responder_model": "gpt-6-luna",
        "timestamps": {"detected_at": detected, "completed_at": fields.pop("completed", detected)},
    }
    doc.update(fields)
    (run / "operator" / "memory" / ".sdo" / "outcomes" / f"{incident}.json").write_text(json.dumps(doc))


def _cell(op: str, seed: int, recovery: float) -> dict[str, Any]:
    return {
        "Fault": FAULT,
        "Operator": op,
        "Grade": "better: cause removed",
        "Runs": 1,
        "NoopRuns": 3,
        "Bad": 100.0,
        "NoopBad": 600.0,
        "Lost": 0,
        "NoopLost": 0,
        "Life": {
            "Reach": "in-scope",
            "Runs": [
                {
                    "Seed": seed,
                    "DetectSec": {"V": 5, "OK": True},
                    "RecoverySec": {"V": recovery, "OK": True},
                    "UsersHurt": True,
                    "Diagnosed": "yes",
                    "Localized": "yes",
                }
            ],
        },
    }


def _experiment(tmp: Path) -> Path:
    """One sequence: r1 at 12:00 (responder), r2 at 13:00 (learned playbook) on the memory r1 left."""
    exp = tmp / "exp1"
    first = _run(exp, "t-q1-r1-a1", position=1, seed=1, hour=12)
    repeat = _run(exp, "t-q1-r2-a1", position=2, seed=2, hour=13)
    causes = [{"cause_class": "config-value-wrong", "reach": "in-scope"}]
    for run in (first, repeat):
        _outcome(
            run,
            "inc-first",
            "2026-01-01T12:00:10Z",
            completed="2026-01-01T12:02:00Z",
            usage=USAGE,
            resolution_path="responder",
            confirmed_root_causes=causes,
        )
    _outcome(
        repeat,
        "inc-repeat",
        "2026-01-01T13:00:10Z",
        completed="2026-01-01T13:00:50Z",
        resolution_path="deterministic",
        confirmed_root_causes=causes,
    )
    (exp / "results").mkdir()
    (exp / "results" / "grades.json").write_text(
        json.dumps(
            [
                _cell("exec:sdo-exp1-r1", 1, 120),
                _cell("exec:sdo-exp1-r2", 2, 48),
                {"Fault": FAULT, "Operator": "best-fix", "Runs": 1, "Bad": 40.0},
            ]
        )
    )
    with (exp / "results" / "runs.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["fault", "position", "seed", "lane", "host", "status", "reason", "grade"])
        w.writerow([FAULT, 1, 1, "lane1", "host1", "done", "", "better"])
        w.writerow([FAULT, 2, 2, "lane1", "host1", "done", "", "much better"])
    return exp


def _rows(exp: Path) -> list[table.Row]:
    return build_rows(exp, table.read_grades(exp), read_run_grades(exp))


def test_rows_join_bench_grades_and_the_runs_own_outcomes(tmp_path: Path) -> None:
    first, repeat = _rows(_experiment(tmp_path))
    assert (first.run, first.round, first.grade) == ("t-q1-r1-a1", "first", "better")
    assert (first.bad, first.noop_bad, first.best_bad) == (100.0, 600.0, 40.0)
    assert (first.incidents, first.cause_class, first.reach_ok) == (["inc-first"], "config-value-wrong", "yes")
    assert first.cost_usd > 0
    assert (first.auto_applied, first.closed_s) == ("no", 120)
    # The repeat's memory copy still holds the first incident; only its own interval's incident is its.
    assert (repeat.round, repeat.grade, repeat.incidents) == ("repeat", "much better", ["inc-repeat"])
    assert (repeat.auto_applied, repeat.mitigate_s) == ("yes", 48)
    (r,) = ratios([first, repeat])
    assert (r.position, r.time_ratio, r.cost_ratio) == (2, 0.4, 0.0)


def test_grade_cells_with_lower_case_fields_join_too(tmp_path: Path) -> None:
    exp = _experiment(tmp_path)
    cells = json.loads((exp / "results" / "grades.json").read_text())
    (exp / "results" / "grades.json").write_text(json.dumps([{k.lower(): v for k, v in c.items()} for c in cells]))
    first, _ = _rows(exp)
    assert (first.noop_bad, first.best_bad, first.detect_s) == (600.0, 40.0, 5)


def test_runs_that_are_not_done_are_kept_but_not_counted(tmp_path: Path) -> None:
    exp = _experiment(tmp_path)
    _run(exp, "t-q2-r1-a1", sequence=2, hour=14, status="invalid")
    rows = _rows(exp)
    capped = next(r for r in rows if r.sequence == 2)
    assert (capped.valid, capped.note) == (False, "capped: busy: reflection queued")
    text = summary({"exp1": rows}, [])
    assert "| exp1 | 1 |" in text  # one first-try run counted


def test_a_done_run_without_a_record_is_invalid(tmp_path: Path) -> None:
    exp = _experiment(tmp_path)
    (exp / "runs" / "t-q1-r1-a1" / "record.json").unlink()
    first = _rows(exp)[0]
    assert (first.valid, first.note) == (False, "no record")


def test_bench_runs_order_by_sequence_and_position(tmp_path: Path) -> None:
    exp = tmp_path / "exp1"
    _run(exp, "b-q1-r2-a1", fault="b", position=2)
    _run(exp, "b-q1-r1-a1", fault="b", position=1)
    _run(exp, "a-q2-r1-a1", fault="a", sequence=2)
    assert [r.id for r in bench_runs(exp)] == ["a-q2-r1-a1", "b-q1-r1-a1", "b-q1-r2-a1"]


def test_incidents_of_healthy_runs_are_false_alarms(tmp_path: Path) -> None:
    exp = _experiment(tmp_path)
    healthy = _run(exp, "healthy-q1-r1-a1", fault="healthy", hour=15)
    _outcome(healthy, "quiet", "2026-01-01T14:59:00Z")  # before the run: not its
    _outcome(healthy, "noisy", "2026-01-01T15:05:00Z", findings=[{"detector_id": "traffic-probe-call"}])
    (alarm,) = false_alarms(exp)
    assert (alarm.incident, alarm.detectors, alarm.kind, alarm.run) == (
        "noisy",
        ["traffic-probe-call"],
        "healthy run",
        "healthy-q1-r1-a1",
    )
    assert all(r.fault != "healthy" for r in _rows(exp))


def test_gate_alarms_come_from_the_gates_file(tmp_path: Path) -> None:
    exp = _experiment(tmp_path)
    (exp / "gates.jsonl").write_text(
        json.dumps(
            {"run": "t-q1-r2-a1", "lane": "lane1", "at": "x", "reasons": ["alarm: incident open i"], "alarm": True}
        )
        + "\n"
        + json.dumps({"run": "t-q1-r2-a1", "lane": "lane1", "at": "y", "reasons": ["bad second"], "alarm": False})
        + "\n"
    )
    (gate,) = gate_alarms(exp)
    assert gate.reasons == ["alarm: incident open i"]
    by_column, alarms, gates = collect([exp])
    assert "- exp1: 1" in summary(by_column, alarms, gates)


def _churned(run: Path) -> None:
    """A node drain at 12:03 deletes a toolbox Pod; the fault is on deploy/web."""
    rec = json.loads((run / "record.json").read_text())
    rec["lifecycle"]["root_cause"] = {"component": "deploy/web"}
    rec["churn"] = {
        "events": [
            {
                "kind": "node-drain",
                "start": "2026-01-01T12:03:00Z",
                "end": "2026-01-01T12:03:30Z",
                "objects": ["pod/toolbox-abc"],
            }
        ]
    }
    (run / "record.json").write_text(json.dumps(rec))


def _finding(detector: str, name: str) -> dict[str, Any]:
    return {"detector_id": detector, "primary_resource": {"kind": "Pod", "name": name}}


def test_churn_findings_and_false_alarms_after_churn(tmp_path: Path) -> None:
    exp = tmp_path / "exp1"
    run = _run(exp, "t-q1-r1-a1")
    _churned(run)
    absorbed = [_finding("workload-health", "toolbox-abc"), _finding("store-state", "web")]
    separate = [_finding("service-endpoints", "toolbox-abc"), _finding("workload-health", "toolbox-abc")]
    _outcome(run, "fault", "2026-01-01T12:01:00Z", findings=absorbed)
    _outcome(run, "churn", "2026-01-01T12:04:00Z", findings=separate)
    rec = json.loads((run / "record.json").read_text())
    picked = bench_runs(exp)[0].picked()
    assert churn_findings(picked, rec) == 2  # (workload-health, toolbox Pod) once, (service-endpoints, toolbox Pod)
    (row,) = _rows(exp)
    assert row.churn_findings == 2
    (alarm,) = false_alarms(exp)
    assert (alarm.incident, alarm.kind) == ("churn", "after churn")
    assert "| exp1 | 1 | 1 | 2 | 2 | 1 | 1 |" in summary({"exp1": [row]}, [alarm])


def test_a_run_without_churn_has_no_churn_findings(tmp_path: Path) -> None:
    assert {r.churn_findings for r in _rows(_experiment(tmp_path))} == {None}


def test_main_writes_the_table_beside_the_experiment(tmp_path: Path) -> None:
    exp = _experiment(tmp_path)
    assert table.main([str(exp)]) == 0
    text = (exp / "sdo-table" / "table.md").read_text()
    assert "## Column exp1" in text
    assert "### Recurrence" in text
    with (exp / "sdo-table" / "table.csv").open() as f:
        assert [r["run"] for r in csv.DictReader(f)] == ["t-q1-r1-a1", "t-q1-r2-a1"]


def test_toward_best_places_a_run_between_noop_and_best_fix() -> None:
    assert toward_best(100, 600, 50) == pytest.approx(0.91, abs=0.01)
    assert toward_best(600, 600, 50) == 0
    assert toward_best(10, 12, 10) is None  # references too close to tell


def test_auto_applied_reads_the_resolution_path() -> None:
    assert auto_applied({"resolution_path": "deterministic"}) == "yes"
    assert auto_applied({"resolution_path": "responder", "plan_applications": [{"applied": True}]}) == "partial"
    assert auto_applied({"resolution_path": "responder"}) == "no"


def test_reach_ok() -> None:
    assert reach_ok("out-of-reach", "out-of-reach") == "yes"
    assert reach_ok("in-scope", "") == "not stated"
    assert reach_ok("in-scope", "out-of-reach") == "no"


def test_outcomes_in_picks_incidents_detected_in_the_run() -> None:
    o = [
        {"incident_id": i, "timestamps": {"detected_at": t}}
        for i, t in (
            ("before", "2026-01-01T11:59:00Z"),
            ("in", "2026-01-01T12:01:00Z"),
            ("after", "2026-01-01T13:00:00Z"),
        )
    ]
    start, end = datetime(2026, 1, 1, 12, tzinfo=UTC), datetime(2026, 1, 1, 12, 30, tzinfo=UTC)
    assert [x["incident_id"] for x in outcomes_in(o, start, end)] == ["in"]


@pytest.mark.parametrize(
    ("model", "priced"),
    [("gpt-6-luna", True), ("gpt-6-sol", True), ("not-a-priced-model", False)],
)
def test_incident_cost_uses_the_model_the_outcome_records(model: str, priced: bool) -> None:
    outcome = {"responder_backend": "codex", "responder_model": model, "usage": USAGE}
    tin, tc, tout, cost = outcome_cost(USAGE, outcome)
    assert (tin, tc, tout) == (1_000_000, 800_000, 10_000)
    price = price_for("codex", model, default_pricing())
    if priced:
        assert price is not None
        assert cost == round(
            cost_usd(TokenUsage(input_tokens=tin, cached_input_tokens=tc, output_tokens=tout), price), 4
        )
    else:
        assert cost is None


def test_a_column_on_an_unpriced_model_reports_tokens_and_no_cost(tmp_path: Path) -> None:
    exp = _experiment(tmp_path)
    for f in (exp / "runs").glob("*/operator/memory/.sdo/outcomes/*.json"):
        doc = json.loads(f.read_text())
        doc["responder_model"] = "not-a-priced-model"
        f.write_text(json.dumps(doc))
    first, _ = _rows(exp)
    assert first.tokens_in == 1_000_000
    assert first.cost_usd is None


def test_ledgers_in_the_operator_output_carry_reflection(tmp_path: Path) -> None:
    exp = _experiment(tmp_path)
    ledger_dir = exp / "runs" / "t-q1-r1-a1" / "operator" / "evidence" / "incidents" / "abc"
    ledger_dir.mkdir(parents=True)
    (ledger_dir / "ledger.json").write_text(
        json.dumps(
            {
                "outcome_document": {
                    "incident_id": "inc-first",
                    "timestamps": {"detected_at": "2026-01-01T12:00:10Z"},
                },
                "reflection_decision": {"learning_decision": "updated", "usage": {"input_tokens": "10"}},
            }
        )
    )
    first, _ = _rows(exp)
    assert first.reflection == "updated"
