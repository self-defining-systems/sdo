"""Detection latency, miss classes, onboarding summary, wall-time over cells, and the ``all`` hook."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from benchmarks.sregym.analysis.__main__ import main
from benchmarks.sregym.analysis.cells import load_cell
from benchmarks.sregym.analysis.detection import detect
from tests.unit.benchmarks.sregym.analysis.test_learning_economics import at, make_cell

if TYPE_CHECKING:
    from pathlib import Path


def decorate(
    cell: Path, *, first_bad: int | None, timeline: list[dict], ok: bool = True, recovered: bool = True
) -> Path:
    run = next((cell / "runs").iterdir())
    (run / "result.json").write_text(json.dumps({"ok": ok, "inject_at": at(0)}))
    record = json.loads((run / "record.json").read_text())
    record["recovered_in_window"] = recovered
    record["lifecycle"] = {"first_bad_second": first_bad}
    (run / "record.json").write_text(json.dumps(record))
    ledger_path = next(run.glob("operator/evidence/incidents/*/ledger.json"))
    ledger = json.loads(ledger_path.read_text())
    ledger["closure"]["detector_timeline"] = timeline
    ledger_path.write_text(json.dumps(ledger))
    return cell


def entry(detector: str, rule: str, offset: float) -> dict:
    return {"detector_id": detector, "rule_id": rule, "first_activated_at": at(offset)}


def test_on_time_operation_page_is_ok(tmp_path: Path) -> None:
    cell = make_cell(tmp_path, "f-a-abcdef12", detected=40, verified=200, responder_cost=1.0, bad=60)
    decorate(cell, first_bad=2, timeline=[entry("request-metrics", "request-metrics.errors", 40)])
    d = detect(load_cell(cell))
    assert d.harm_onset_s == 2
    assert d.page_after_harm_s == 38
    assert d.operation_after_harm_s == 38
    assert d.miss_class == "ok"


def test_rollout_page_before_operation_alert_is_a_detection_miss(tmp_path: Path) -> None:
    cell = make_cell(tmp_path, "f-a-abcdef12", detected=30, verified=300, responder_cost=1.0, bad=80)
    decorate(
        cell,
        first_bad=1,
        timeline=[entry("workload-health", "x", 30), entry("traffic-probe-write", "p", 250)],
    )
    d = detect(load_cell(cell))
    assert d.page_after_harm_s == 29
    assert d.operation_after_harm_s == 249
    assert d.miss_class == "detection"


def test_timely_page_without_recovery_is_a_response_miss(tmp_path: Path) -> None:
    cell = make_cell(tmp_path, "f-a-abcdef12", detected=20, verified=400, responder_cost=1.0, bad=90)
    decorate(cell, first_bad=1, timeline=[entry("request-metrics", "request-metrics.errors", 20)], recovered=False)
    assert detect(load_cell(cell)).miss_class == "response"


def test_unfinished_run_is_a_harness_miss(tmp_path: Path) -> None:
    cell = make_cell(tmp_path, "f-a-abcdef12", detected=20, verified=100, responder_cost=1.0, bad=90)
    decorate(cell, first_bad=1, timeline=[], ok=False)
    assert detect(load_cell(cell)).miss_class == "harness"


def test_all_hook_expands_a_pool_and_writes_every_report(tmp_path: Path) -> None:
    pool = tmp_path / "pool"
    for name, kwargs in (
        (
            "f-a-abcdef12",
            {"detected": 20, "verified": 300, "responder_cost": 2.0, "learned": ["pb"], "reflection_cost": 0.5},
        ),
        ("f-b-abcdef12", {"detected": 20, "verified": 100, "responder_cost": None, "applied": 30}),
    ):
        cell = make_cell(pool, name, **kwargs)
        decorate(cell, first_bad=1, timeline=[entry("request-metrics", "request-metrics.errors", 20)])
    transcript = (
        pool
        / "f-a-abcdef12"
        / "runs"
        / "r1"
        / "operator"
        / "evidence"
        / "incidents"
        / "i1"
        / "claude"
        / "projects"
        / "p"
    )
    transcript.mkdir(parents=True)
    lines = [
        {"type": "assistant", "timestamp": at(0), "message": {"content": [{"type": "text", "text": "hi"}]}},
        {"type": "assistant", "timestamp": at(5), "message": {"content": [{"type": "text", "text": "done"}]}},
    ]
    (transcript / "s.jsonl").write_text("\n".join(json.dumps(x) for x in lines))
    card = tmp_path / "cards" / "scorecard.json"
    card.parent.mkdir()
    card.write_text(
        json.dumps(
            {"memory": "m", "started": at(0), "finished": at(600), "recall": {"recall": 1.0, "breaks": 5, "graded": 4}}
        )
    )
    out = tmp_path / "out"
    assert main(["all", str(pool), str(card.parent), "--out", str(out)]) == 0
    for name in ("learning-economics", "sweep", "detection", "wall-time", "onboarding"):
        assert (out / f"{name}.md").is_file(), name
    assert json.loads((out / "onboarding.json").read_text())[0]["wall_s"] == 600
    assert json.loads((out / "wall-time.json").read_text())["main"]["wall_s"] == 5
