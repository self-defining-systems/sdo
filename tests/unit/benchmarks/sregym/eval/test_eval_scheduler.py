"""Scheduler: dependency order, lane pools, load gate, resume."""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

from benchmarks.sregym.eval.plan import AppConfig, build_plan
from benchmarks.sregym.eval.results import ResultsTable
from benchmarks.sregym.eval.scheduler import LoadGate, Scheduler
from tests.unit.benchmarks.sregym.eval.eval_fakes import Clock, FakeBackend, make_config, make_context

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    import pytest


class Store:
    def __init__(self) -> None:
        self.events: list[str] = []
        self.lock = threading.Lock()

    def resolve(self, app: str, key: str) -> str:
        return f"{app}:{key}"

    def record_first_try(self, app: str, problem: str, memory_commit_after: str | None) -> None:
        with self.lock:
            self.events.append(f"first:{app}/{problem}")

    def merge(self, app: str, problems: Sequence[str]) -> object:
        with self.lock:
            self.events.append(f"merge:{app}:{len(problems)}")
        return None


def no_wait_gate() -> LoadGate:
    return LoadGate(1e9, 0, loadavg=lambda: 0.0, sleep=lambda s: None)


def run(tmp_path: Path, **overrides: object) -> tuple[list, Store, FakeBackend, Scheduler]:
    clock = Clock()
    config = make_config(tmp_path, **overrides)
    plan = build_plan(config)
    context, backend = make_context(config, clock)
    store = Store()
    scheduler = Scheduler(plan, context, store, ResultsTable(tmp_path), gate=no_wait_gate())
    return scheduler.run(), store, backend, scheduler


def test_every_cell_runs_once_and_the_table_is_written(tmp_path: Path) -> None:
    rows, _, _, _ = run(tmp_path)
    assert len(rows) == 2 * 4  # two problems x four passes
    assert all(r.status == "graded" for r in rows)
    assert (tmp_path / "results.csv").is_file()
    assert len(ResultsTable(tmp_path).rows()) == 8


def test_merge_waits_for_every_pass_one_cell_and_pass_two_waits_for_the_merge(tmp_path: Path) -> None:
    rows, store, backend, _ = run(tmp_path)
    assert store.events.index("merge:hotel:2") > max(store.events.index(f"first:hotel/{p}") for p in ("a", "b"))
    by_pass = {}
    for row in rows:
        by_pass.setdefault(row.pass_, []).append(row)
    assert {r.snapshot_commit for r in by_pass["pass2"]} == {"commit-merged"}
    assert {r.snapshot_commit for r in by_pass["pass2-own"]} == {"commit-own:a", "commit-own:b"}


def test_vanilla_and_sdo_cells_use_their_own_lanes(tmp_path: Path) -> None:
    rows, _, _, _ = run(tmp_path)
    assert {r.lane for r in rows if r.arm == "vanilla"} == {"v1"}
    assert {r.lane for r in rows if r.arm == "sdo"} <= {"h1", "h2"}


def test_a_cell_that_errors_holds_its_lane_and_the_rest_continue(tmp_path: Path) -> None:
    clock = Clock()
    config = make_config(tmp_path, passes=("pass1",))
    backend = FakeBackend(clock, fail_at="inject")
    context, _ = make_context(config, clock, backend=backend)
    scheduler = Scheduler(build_plan(config), context, Store(), ResultsTable(tmp_path), gate=no_wait_gate())
    rows = scheduler.run()
    assert len(rows) == 2
    # one error is a bad cell, not a bad lane: no lane is held until it errors twice in a row
    assert {r.status for r in rows} == {"error"}
    assert scheduler.pools.held == []


def test_with_every_lane_held_remaining_cells_fail_fast(tmp_path: Path) -> None:
    clock = Clock()
    config = make_config(
        tmp_path, passes=("pass1",), apps=(AppConfig(name="hotel", problems=("a", "b", "c"), sdo_lanes=("h1",)),)
    )
    backend = FakeBackend(clock, fail_at="claim")
    context, _ = make_context(config, clock, backend=backend)
    rows = Scheduler(build_plan(config), context, Store(), ResultsTable(tmp_path), gate=no_wait_gate()).run()
    assert sorted(r.reason or "" for r in rows).count("no lane left: every lane of the pool is held") == 1


def test_finished_cells_are_skipped_on_restart(tmp_path: Path) -> None:
    first, _, _, _ = run(tmp_path)
    again, _, backend, _ = run(tmp_path)
    assert len(first) == 8
    assert again == []
    assert backend.calls == []


def test_load_gate_waits_while_load_is_high_and_spaces_starts() -> None:
    loads = iter([50.0, 50.0, 10.0, 10.0])
    clock = Clock(0)
    gate = LoadGate(40, 5, loadavg=lambda: next(loads), now=clock.time, sleep=clock.sleep)
    waited = gate.admit()
    assert waited >= 2  # slept through the two high readings
    start = clock.now
    gate.admit()
    assert clock.now - start >= 5  # the next start waited out the spacing


def test_load_gate_pauses_new_starts_while_host_memory_refuses_and_logs_the_reason(
    caplog: pytest.LogCaptureFixture,
) -> None:
    refusals = iter(
        ["host memory pressure some avg60=25.00 >= 10.00", "host memory pressure some avg60=25.00 >= 10.00", None]
    )
    clock = Clock(0)
    gate = LoadGate(40, 0, loadavg=lambda: 0.0, now=clock.time, sleep=clock.sleep, host_check=lambda: next(refusals))
    with caplog.at_level("WARNING", logger="benchmarks.sregym.eval.scheduler"):
        waited = gate.admit()
    assert waited > 0
    assert gate.memory_waited == waited
    reasons = [r.getMessage() for r in caplog.records if "avg60=25.00" in r.getMessage()]
    assert len(reasons) == 1, "the same reason is logged once, not on every poll"


def test_load_gate_admits_at_once_when_host_memory_has_room() -> None:
    clock = Clock(0)
    gate = LoadGate(40, 0, loadavg=lambda: 0.0, now=clock.time, sleep=clock.sleep, host_check=lambda: None)
    assert gate.admit() == 0
    assert gate.memory_waited == 0


def test_scheduler_does_not_start_a_cell_until_the_host_check_passes(tmp_path: Path) -> None:
    clock = Clock()
    config = make_config(tmp_path, passes=("pass1",))
    plan = build_plan(config)
    context, backend = make_context(config, clock)
    checks: list[int] = []

    def refuse_twice() -> str | None:
        checks.append(len(backend.calls))
        return "low" if len(checks) <= 2 else None

    gate = LoadGate(1e9, 0, loadavg=lambda: 0.0, sleep=lambda s: None, host_check=refuse_twice)
    Scheduler(plan, context, Store(), ResultsTable(tmp_path), gate=gate).run()
    assert checks[:2] == [0, 0], "no backend call happened while the host refused"
