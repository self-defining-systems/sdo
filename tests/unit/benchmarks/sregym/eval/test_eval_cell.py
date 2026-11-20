"""The cell state machine against scripted fakes."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from benchmarks.sregym.eval.cell import run_cell
from benchmarks.sregym.eval.plan import build_plan
from benchmarks.sregym.eval.results import verify_manifest
from tests.unit.benchmarks.sregym.eval.eval_fakes import (
    Clock,
    FakeBackend,
    FakeExecutor,
    FakeGate,
    make_config,
    make_context,
)

if TYPE_CHECKING:
    from pathlib import Path


def first(config, pass_: str):
    return next(c for c in build_plan(config).cells if c.pass_ == pass_)


def test_a_graded_cell_has_clock_times_timings_and_a_verified_manifest(tmp_path: Path) -> None:
    clock = Clock()
    config = make_config(tmp_path)
    ctx, backend = make_context(config, clock)
    cell = first(config, "pass1")
    row = run_cell(cell, ctx, "hotel-lane")
    assert backend.calls == ["claim", "deploy", "inject", "grade", "collect"]
    assert backend.snapshots == ["commit-base"]
    assert row.status == "graded"
    assert row.success is True
    assert row.t_inject == 1003.0
    assert row.t_page == 1008.0
    assert row.page_source == "own"
    assert row.no_page is False
    # healthy from inject+20s; page at inject+5 s
    assert row.t_healthy == 1023.0
    assert row.time_to_mitigate_s == 15.0
    assert row.inject_to_healthy_s == 20.0
    assert row.confirmation_s == 1033.0 + 70 - 1023.0
    assert row.online.output == 10
    assert row.offline.output == 4
    assert row.memory_commit_after == "head-1"
    assert row.manifest_sha256
    directory = cell.directory(config)
    assert verify_manifest(directory) == []
    timings = json.loads((directory / "timings.json").read_text())
    assert timings["phases"]["claim_reset"] == 2
    assert timings["phases"]["inject"] == 1
    assert timings["phases"]["page"] == 5
    assert timings["phases"]["agent"] == 25
    assert timings["phases"]["grade"] == 0
    assert (directory / "transcript.jsonl").is_file()
    assert backend.released == [("hotel-lane", True)]


def test_a_cell_with_no_page_is_recorded_and_has_no_time_to_mitigate(tmp_path: Path) -> None:
    clock = Clock()
    config = make_config(tmp_path)
    ctx, _ = make_context(config, clock, executor=FakeExecutor(clock, page_after=None))
    row = run_cell(first(config, "pass1"), ctx, "hotel-lane")
    assert row.no_page is True
    assert row.t_page is None
    assert row.time_to_mitigate_s is None
    assert row.inject_to_healthy_s == 20.0
    assert row.status == "graded"  # still counts in fix rate


def test_missing_transcripts_make_the_cell_not_gradable(tmp_path: Path) -> None:
    clock = Clock()
    config = make_config(tmp_path)
    ctx, _ = make_context(config, clock, gate=FakeGate(gradable=False))
    row = run_cell(first(config, "pass1"), ctx, "hotel-lane")
    assert row.status == "not_graded"
    assert row.success is None
    assert "transcript missing" in (row.reason or "")


def test_a_backend_failure_is_a_row_not_an_exception_and_holds_the_lane(tmp_path: Path) -> None:
    clock = Clock()
    config = make_config(tmp_path)
    backend = FakeBackend(clock, fail_at="inject")
    ctx, _ = make_context(config, clock, backend=backend)
    row = run_cell(first(config, "pass1"), ctx, "hotel-lane")
    assert row.status == "error"
    assert "inject failed" in (row.reason or "")
    assert backend.released == [("hotel-lane", False)]
    assert "collect" in backend.calls  # evidence is still collected after a failure


def test_a_claim_failure_has_no_lane_to_release(tmp_path: Path) -> None:
    clock = Clock()
    config = make_config(tmp_path)
    backend = FakeBackend(clock, fail_at="claim")
    ctx, _ = make_context(config, clock, backend=backend)
    row = run_cell(first(config, "pass1"), ctx, "hotel-lane")
    assert row.status == "error"
    assert row.lane is None
    assert backend.released == []


def test_the_cell_keeps_probing_until_the_hold_is_satisfied(tmp_path: Path) -> None:
    clock = Clock()
    config = make_config(tmp_path, hold_seconds=30)
    ctx, _ = make_context(config, clock, executor=FakeExecutor(clock, work=1), healthy_after_inject=10)
    row = run_cell(first(config, "pass1"), ctx, "hotel-lane")
    # agent ends at inject+1 s, but users are only known healthy after the 30 s hold from inject+10 s
    assert row.t_healthy == 1014.0  # first 3 s sample at or after inject+10 s
    timings = json.loads((first(config, "pass1").directory(config) / "timings.json").read_text())
    assert timings["phases"]["settle"] >= 39


def test_settle_gives_up_at_its_cap_and_reports_no_healthy_time(tmp_path: Path) -> None:
    clock = Clock()
    config = make_config(tmp_path, hold_seconds=30, settle_cap_seconds=12)
    ctx, _ = make_context(config, clock, executor=FakeExecutor(clock, work=1), healthy_after_inject=1000)
    row = run_cell(first(config, "pass1"), ctx, "hotel-lane")
    assert row.t_healthy is None
    assert row.time_to_mitigate_s is None


def test_vanilla_cells_resolve_the_base_snapshot(tmp_path: Path) -> None:
    """Source-equal baseline: a vanilla cell runs the application source of the SDO base snapshot."""
    clock = Clock()
    config = make_config(tmp_path)
    ctx, backend = make_context(config, clock)
    run_cell(first(config, "vanilla"), ctx, "hotel-lane")
    assert backend.snapshots == ["commit-base"]
