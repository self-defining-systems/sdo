"""Row speed against the floor: stages from a synthetic plan root, the closing turn counted in the floor on p1 only,
warm-up rows kept out of the medians, final attempts from the plan results, and the JSON the plan generator reads."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from benchmarks.sregym.lanes import learning_plan, row_speed

if TYPE_CHECKING:
    from pathlib import Path

    import pytest

T0 = "2026-01-01T00:00:00Z"


def at(seconds: int) -> str:
    return f"2026-01-01T00:{seconds // 60:02d}:{seconds % 60:02d}Z"


def log_at(seconds: int) -> str:
    return f"2026-01-01 00:{seconds // 60:02d}:{seconds % 60:02d},000"


def write_run(
    root: Path,
    exp: str,
    *,
    closing: bool,
    resolution: str = "responder",
    page: str = "own",
    ended: bool = True,
) -> None:
    run = root / exp / "runs" / "001"
    results = run / "exec" / "fastloop-results"
    (results / "incidents.online").mkdir(parents=True)
    run_json = {"lane": "l1", "started": T0, "experiment": exp}
    if ended:
        run_json["ended"] = at(600)
    (run / "run.json").write_text(json.dumps(run_json))
    sregym = {
        "detected_at": at(140),
        "dispatched_at": at(150),
        "resolved_at": at(250),
        "resolution_path": resolution,
        "page_source": page,
    }
    if closing:
        sregym["reflection_started_at"] = at(300)
    (run / "record.json").write_text(json.dumps({"sregym": sregym, "reseed": {"attempted": True, "reused": True}}))
    (results / "incidents.online" / "000000.json").write_text(
        json.dumps({"injection": {"started_at": at(120), "finished_at": at(130)}})
    )
    (run / "runner.log").write_text(
        f"{log_at(260)} Leaving incident 1 active\n{log_at(500 if closing else 320)} incident 1 done oracle=ok\n"
    )


def plan_root(tmp_path: Path, runs: dict[str, dict]) -> Path:
    """One node per arm, attempt experiments ``synthetic-<arm>-a-1-r1`` (and ``-x1`` retries when listed)."""
    root = tmp_path / "root"
    nodes = []
    for arm, options in runs.items():
        attempts = []
        for exp in options.pop("exps", [f"synthetic-{arm}-a-1-r1"]):
            write_run(root, exp, **options)
            attempts.append({"exp": exp})
        nodes.append({"id": f"{arm}/a-1/r1", "arm": arm, "item": "a-1", "attempts": attempts})
    (root / "plans" / "synthetic").mkdir(parents=True)
    (root / "plans" / "synthetic" / "state.json").write_text(json.dumps({"nodes": nodes}))
    return root


def run(root: Path, out: Path) -> dict[str, dict]:
    row_speed.main(["--root", str(root), "--plan", "synthetic", "--out", str(out)])
    return {row["arm"]: row for row in json.loads((out / "final-speed.json").read_text())}


def test_rows_split_into_stages_and_only_p1_floor_counts_its_closing_turn(tmp_path: Path) -> None:
    root = plan_root(tmp_path, {"w": {"closing": True}, "p1": {"closing": True}, "cold": {"closing": True}})

    rows = run(root, tmp_path / "out")

    assert set(rows) == {"p1", "cold"}
    p1, cold = rows["p1"], rows["cold"]
    assert p1["stages"] == {
        "gate": 120,
        "inject": 10,
        "detect": 20,
        "agent": 100,
        "verify": 10,
        "tail": 40,
        "closing": 200,
        "reset": 100,
    }
    assert p1["floor"] == 10 + 20 + 100 + 10 + 200 + 90
    assert cold["stages"]["closing"] == 200
    assert cold["floor"] == 10 + 20 + 100 + 10 + 90
    assert p1["reseed"] == "reseeded"
    assert p1["wall"] == 600
    text = (tmp_path / "out" / "final-speed.md").read_text()
    assert "Untimed warm-up rows" in text
    assert "a-1 on l1" in text


def test_a_playbook_row_has_no_dispatch_wait_and_a_fallback_page_waits_for_dispatch(tmp_path: Path) -> None:
    root = plan_root(
        tmp_path,
        {"p2": {"closing": False, "resolution": "deterministic"}, "cold": {"closing": False, "page": "fallback"}},
    )

    rows = run(root, tmp_path / "out")

    assert (rows["p2"]["stages"]["detect"], rows["p2"]["stages"]["agent"]) == (10, 110)
    assert (rows["cold"]["stages"]["detect"], rows["cold"]["stages"]["agent"]) == (20, 100)


def test_unfinished_runs_are_skipped(tmp_path: Path) -> None:
    root = plan_root(tmp_path, {"p1": {"closing": True}, "cold": {"closing": False, "ended": False}})

    assert set(run(root, tmp_path / "out")) == {"p1"}


def test_plan_results_choose_the_final_done_attempt(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = plan_root(
        tmp_path,
        {
            "p1": {"closing": True, "exps": ["synthetic-p1-a-1-r1", "synthetic-p1-a-1-r1-x1"]},
            "cold": {"closing": False},
        },
    )
    tsv = {
        ("synthetic-p1-a-1-r1-x1", "001"): {"status": "done", "grade": json.dumps({"reseed": "fallback"})},
        ("synthetic-cold-a-1-r1", "001"): {"status": "failed", "grade": ""},
    }
    monkeypatch.setattr(row_speed, "lanes_results", lambda *_: tsv)

    row_speed.main(["--root", str(root), "--plan", "synthetic", "--out", str(tmp_path / "out"), "--lanes-bin", "x"])

    rows = json.loads((tmp_path / "out" / "final-speed.json").read_text())
    assert [(row["arm"], row["reseed"]) for row in rows] == [("p1", "fallback")]


def test_measured_rows_feed_plan_deadlines(tmp_path: Path) -> None:
    root = plan_root(tmp_path, {"p1": {"closing": True}, "cold": {"closing": False}})
    run(root, tmp_path / "out")

    timing = learning_plan.Timing.from_speed_report(tmp_path / "out" / "final-speed.json")

    assert timing == learning_plan.Timing("5m", "20m")
