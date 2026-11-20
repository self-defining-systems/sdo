"""Row end at the committed outcome, the closing drain on the held lane, and the closing record."""

from __future__ import annotations

import json
import threading
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.eval.cell import run_cell
from benchmarks.sregym.eval.closing import (
    ClosingResult,
    closing_is_terminal,
    offline_tokens_from_evidence,
    pending_closings,
    read_closing,
)
from benchmarks.sregym.eval.plan import AppConfig, build_plan
from benchmarks.sregym.eval.results import ResultsTable, TokenRow, verify_manifest
from benchmarks.sregym.eval.scheduler import LoadGate, Scheduler
from tests.unit.benchmarks.sregym.eval.eval_fakes import (
    Clock,
    DrainingExecutor,
    FakeBackend,
    FakeExecutor,
    make_config,
    make_context,
)

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path


def first(config, pass_: str):
    return next(c for c in build_plan(config).cells if c.pass_ == pass_)


def run_draining(tmp_path: Path, **executor: object):
    clock = Clock()
    config = make_config(tmp_path)
    backend = FakeBackend(clock)
    drainer = DrainingExecutor(clock, backend=backend, **executor)  # type: ignore[arg-type]
    ctx, _ = make_context(config, clock, backend=backend, executor=drainer)
    cell = first(config, "pass1")
    return run_cell(cell, ctx, "hotel-lane"), cell, config, backend, drainer


def test_the_row_is_on_disk_before_the_drain_and_the_lane_stays_held_through_it(tmp_path: Path) -> None:
    row, cell, config, backend, drainer = run_draining(tmp_path)
    (seen,) = drainer.during_drain
    assert seen == {"row_json": True, "manifest": False, "released": []}  # online row written; lane still held
    assert backend.released == [("hotel-lane", True)]  # released only after the drain
    assert backend.calls.count("collect") == 2  # row evidence, then closing evidence
    assert verify_manifest(cell.directory(config)) == []  # the manifest seals the cell after the drain


def test_row_json_is_the_online_row_and_closing_json_the_offline_record(tmp_path: Path) -> None:
    row, cell, config, _, _ = run_draining(tmp_path)
    directory = cell.directory(config)
    online = json.loads((directory / "row.json").read_text())
    assert online["closing_status"] == "pending"
    assert online["row_wall_s"] == pytest.approx(row.row_wall_s)
    assert online["offline_output"] is None  # the closing turn has not happened at the row's end
    assert online["memory_commit_after"] is None
    closing = json.loads((directory / "closing.json").read_text())
    assert closing["status"] == "drained"
    assert closing["incident_id"] == "incident-1"
    assert closing["closing_s"] == 560.0
    assert closing["memory_commit_after"] == "closing-head"
    assert closing["learning"] == {"playbooks": {"added": ["p-new"]}, "commit": "closing-head"}
    assert closing["decision"] == "updated"


def test_lane_occupancy_is_row_wall_plus_drain(tmp_path: Path) -> None:
    row, cell, config, _, _ = run_draining(tmp_path)
    closing = json.loads((cell.directory(config) / "closing.json").read_text())
    assert closing["drain_s"] >= 600.0
    assert closing["row_wall_s"] == row.row_wall_s
    assert abs(closing["lane_occupancy_s"] - (closing["row_wall_s"] + closing["drain_s"])) <= 2.0
    assert row.lane_occupancy_s == closing["lane_occupancy_s"]
    assert row.closing_s == 560.0
    assert row.drain_s == closing["drain_s"]


def test_the_table_row_takes_the_delta_and_offline_cost_from_the_closing(tmp_path: Path) -> None:
    row, _, _, _, _ = run_draining(tmp_path)
    assert row.memory_commit_after == "closing-head"  # never the online value
    assert row.closing_status == "drained"
    assert row.closing_decision == "updated"
    assert row.recovery_s == 25.0
    assert (row.offline.uncached_input, row.offline.cache_read, row.offline.cache_write, row.offline.output) == (
        1,
        2,
        3,
        4,
    )
    assert (row.offline.requests, row.offline.usd) == (5, 0.5)


def test_offline_tokens_are_the_sums_of_the_reflection_usage_log_in_the_collected_evidence(tmp_path: Path) -> None:
    incident = "incident-1"
    import hashlib

    session = hashlib.sha256(incident.encode()).hexdigest()[:32]
    log = tmp_path / "artifacts" / "sdo_runtime" / "sessions" / session / "reflection-usage.jsonl"
    log.parent.mkdir(parents=True)
    turns = [
        {
            "cost_usd": 0.25,
            "model_requests": 7,
            "usage": {
                "input_tokens": 1000,
                "cache_read_input_tokens": 600,
                "cached_input_tokens": 600,
                "cache_write_input_tokens": 100,
                "output_tokens": 50,
                "model_requests": 7,
            },
        },
        {
            "cost_usd": 0.125,
            "model_requests": 3,
            "usage": {
                "input_tokens": 500,
                "cache_read_input_tokens": 400,
                "cached_input_tokens": 400,
                "cache_write_input_tokens": 0,
                "output_tokens": 20,
                "model_requests": 3,
            },
        },
    ]
    log.write_text("\n".join(json.dumps(t) for t in turns) + "\n")
    tokens = offline_tokens_from_evidence(tmp_path, incident)
    assert tokens == TokenRow(
        uncached_input=300 + 100, cache_read=1000, cache_write=100, output=70, requests=10, usd=0.375
    )


def test_missing_reflection_usage_is_none_never_zero(tmp_path: Path) -> None:
    assert offline_tokens_from_evidence(tmp_path, "incident-1") is None


def test_a_cell_whose_closing_has_no_usage_log_falls_back_and_says_so(tmp_path: Path) -> None:
    _, cell, config, _, _ = run_draining(tmp_path)
    closing = json.loads((cell.directory(config) / "closing.json").read_text())
    assert closing["offline_source"] == "incident-record"
    assert closing["evidence_has_closing_usage"] is False


def test_an_executor_without_a_drain_has_a_terminal_not_applicable_closing(tmp_path: Path) -> None:
    clock = Clock()
    config = make_config(tmp_path)
    ctx, backend = make_context(config, clock, executor=FakeExecutor(clock))
    cell = first(config, "vanilla")
    row = run_cell(cell, ctx, "hotel-lane")
    closing = read_closing(cell.directory(config))
    assert closing is not None
    assert closing["status"] == "not_applicable"
    assert closing_is_terminal(closing)
    assert row.closing_status == "not_applicable"
    assert row.memory_commit_after == "head-1"  # no drain: the arm's own value stands
    assert backend.calls.count("collect") == 1
    assert row.lane_occupancy_s is not None
    assert row.row_wall_s is not None


def test_a_closing_that_never_drained_holds_the_lane_and_excludes_the_delta(tmp_path: Path) -> None:
    row, cell, config, backend, _ = run_draining(tmp_path, closing_status="not_drained")
    assert row.status == "graded"  # the online row stands
    assert row.closing_status == "not_drained"
    assert row.memory_commit_after is None
    assert backend.released == [("hotel-lane", False)]
    assert closing_is_terminal(read_closing(cell.directory(config)))


def test_a_failing_drain_is_a_closing_error_not_an_exception_and_holds_the_lane(tmp_path: Path) -> None:
    row, cell, config, backend, _ = run_draining(tmp_path, drain_error="row process vanished")
    assert row.closing_status == "error"
    assert row.status == "graded"
    assert backend.released == [("hotel-lane", False)]
    closing = read_closing(cell.directory(config))
    assert closing is not None
    assert "row process vanished" in closing["error"]
    assert verify_manifest(cell.directory(config)) == []


def test_a_cell_that_never_got_a_lane_has_a_terminal_closing(tmp_path: Path) -> None:
    clock = Clock()
    config = make_config(tmp_path)
    backend = FakeBackend(clock, fail_at="claim")
    ctx, _ = make_context(config, clock, backend=backend, executor=DrainingExecutor(clock))
    cell = first(config, "pass1")
    run_cell(cell, ctx, "hotel-lane")
    assert closing_is_terminal(read_closing(cell.directory(config)))


def test_pending_closings_names_the_cells_without_a_terminal_closing(tmp_path: Path) -> None:
    done, running, absent = tmp_path / "a", tmp_path / "b", tmp_path / "c"
    for directory in (done, running, absent):
        directory.mkdir()
    (done / "closing.json").write_text(json.dumps({"status": "drained"}))
    (running / "closing.json").write_text(json.dumps({"status": "pending"}))
    assert pending_closings([done, running, absent]) == [running, absent]


def test_a_finished_closing_result_is_terminal_only_for_its_terminal_statuses() -> None:
    assert closing_is_terminal({"status": "not_drained"})
    assert not closing_is_terminal({"status": "pending"})
    assert not closing_is_terminal(None)
    assert ClosingResult(status="drained").status == "drained"


class _Store:
    def __init__(self) -> None:
        self.events: list[str] = []
        self.lock = threading.Lock()

    def resolve(self, app: str, key: str) -> str:
        return f"{app}:{key}"

    def record_first_try(self, app: str, problem: str, memory_commit_after: str | None) -> None:
        with self.lock:
            self.events.append(f"first:{problem}:{memory_commit_after}")

    def merge(self, app: str, problems: Sequence[str]) -> object:
        with self.lock:
            self.events.append("merge")
        return None


def test_the_scheduler_records_the_pass_one_delta_from_the_closing_and_merges_only_after_every_drain(
    tmp_path: Path,
) -> None:
    clock = Clock()
    config = make_config(
        tmp_path,
        passes=("pass1", "pass2"),
        apps=(AppConfig(name="hotel", problems=("a", "b"), sdo_lanes=("h1", "h2")),),
    )
    backend = FakeBackend(clock)
    drainer = DrainingExecutor(clock, backend=backend)
    ctx, _ = make_context(config, clock, backend=backend, executor=drainer)
    store = _Store()
    gate = LoadGate(1e9, 0, loadavg=lambda: 0.0, sleep=lambda s: None)
    rows = Scheduler(build_plan(config), ctx, store, ResultsTable(tmp_path), gate=gate).run()
    firsts = [e for e in store.events if e.startswith("first:")]
    assert sorted(firsts) == ["first:a:closing-head", "first:b:closing-head"]
    assert store.events.index("merge") > max(store.events.index(e) for e in firsts)
    # pass 2 cells see a merged snapshot, and every cell's closing is terminal before its lane returns to the pool
    assert all(closing_is_terminal(read_closing(build_cell_dir(config, r))) for r in rows)


def build_cell_dir(config, row):
    cell = next(c for c in build_plan(config).cells if (c.app, c.pass_, c.problem) == (row.app, row.pass_, row.problem))
    return cell.directory(config)


def test_a_lane_whose_closing_errored_is_never_handed_to_the_next_cell(tmp_path: Path) -> None:
    clock = Clock()
    config = make_config(
        tmp_path, passes=("pass1",), apps=(AppConfig(name="hotel", problems=("a", "b"), sdo_lanes=("h1",)),)
    )
    backend = FakeBackend(clock)
    drainer = DrainingExecutor(clock, backend=backend, closing_status="not_drained")
    ctx, _ = make_context(config, clock, backend=backend, executor=drainer)
    gate = LoadGate(1e9, 0, loadavg=lambda: 0.0, sleep=lambda s: None)
    scheduler = Scheduler(build_plan(config), ctx, _Store(), ResultsTable(tmp_path), gate=gate)
    rows = scheduler.run()
    assert scheduler.pools.held == [("hotel", "h1")]
    assert sorted(r.reason or "" for r in rows).count("no lane left: every lane of the pool is held") == 1


class _EvidenceAfterDrainGate:
    """The evidence an SDO cell is judged on (final record, evidence copy) exists only after the closing turn."""

    def check(self, cell, directory):
        from benchmarks.sregym.eval.cell import GateVerdict

        ok = (directory / "closing-evidence.json").is_file()
        return GateVerdict(ok, () if ok else ("no verified evidence copy (evidence-copy.json)",))


class _LateEvidenceBackend(FakeBackend):
    def collect(self, lane, cell, directory):
        super().collect(lane, cell, directory)
        if self.calls.count("collect") >= 2:  # the second collect is the closing turn's
            (directory / "closing-evidence.json").write_text("{}\n")


def _run_with_gate(tmp_path: Path, backend_cls, gate):
    clock = Clock()
    config = make_config(tmp_path)
    backend = backend_cls(clock)
    drainer = DrainingExecutor(clock, backend=backend)
    ctx, _ = make_context(config, clock, backend=backend, executor=drainer, gate=gate)
    cell = first(config, "pass1")
    return run_cell(cell, ctx, "hotel-lane")


def test_the_evidence_gate_judges_a_draining_cell_after_its_closing_evidence_is_collected(tmp_path: Path) -> None:
    row = _run_with_gate(tmp_path, _LateEvidenceBackend, _EvidenceAfterDrainGate())
    assert row.status == "graded"
    assert row.success is True
    assert row.reason is None


def test_a_draining_cell_whose_evidence_never_completes_is_not_graded_after_the_drain(tmp_path: Path) -> None:
    row = _run_with_gate(tmp_path, FakeBackend, _EvidenceAfterDrainGate())
    assert row.status == "not_graded"
    assert row.success is None
    assert row.reason == "no verified evidence copy (evidence-copy.json)"


def test_a_closing_turn_that_made_no_model_call_costs_exactly_zero_not_unknown(tmp_path: Path) -> None:
    """The learning gate can skip the closing turn's agent call; the incident record then says zero requests."""
    none = TokenRow(uncached_input=0, cache_read=0, cache_write=0, output=0, requests=0)
    row, cell, config, _, _ = run_draining(tmp_path, offline_fallback=none)
    assert row.offline == TokenRow(uncached_input=0, cache_read=0, cache_write=0, output=0, requests=0, usd=0.0)
    closing = json.loads((cell.directory(config) / "closing.json").read_text())
    assert closing["offline_source"] == "no-closing-turn"


def test_a_closing_turn_with_requests_but_no_usage_log_stays_unknown(tmp_path: Path) -> None:
    some = TokenRow(uncached_input=0, cache_read=0, cache_write=0, output=0, requests=3)
    row, cell, config, _, _ = run_draining(tmp_path, offline_fallback=some)
    assert row.offline.usd is None
    closing = json.loads((cell.directory(config) / "closing.json").read_text())
    assert closing["offline_source"] == "incident-record"


def test_row_json_names_why_its_cost_is_pending_and_the_sealed_row_carries_the_cost(tmp_path: Path) -> None:
    row, cell, config, _, _ = run_draining(tmp_path)
    online = json.loads((cell.directory(config) / "row.json").read_text())
    assert online["cost_note"].startswith("pending")
    sealed = json.loads((cell.directory(config) / "manifest.json").read_text())  # sealed after the drain
    assert sealed["status"] == row.status
    assert row.online.usd  # the sealed row has a positive cost and so needs no reason
    assert row.cost_note is None


def test_sealed_row_recovers_responder_cost_when_the_final_sink_is_missing(tmp_path: Path) -> None:
    from dataclasses import replace
    from hashlib import sha256

    from benchmarks.sregym.eval.cell import Accounting, ArmResult
    from benchmarks.sregym.eval.plan import Cell

    class MissingAccountant:
        def account(self, cell: Cell, directory: Path, result: ArmResult | None) -> Accounting:
            return Accounting(TokenRow(), TokenRow())

    clock = Clock()
    config = make_config(tmp_path)
    drainer = DrainingExecutor(clock, closing_status="not_drained")
    ctx, _ = make_context(config, clock, executor=drainer)
    ctx = replace(ctx, accountant=MissingAccountant())
    cell = first(config, "pass1")
    sessions = cell.directory(config) / "artifacts/sdo_runtime/sessions"
    current = sessions / sha256(b"incident-1").hexdigest()[:32]
    current.mkdir(parents=True)
    for root in (current, sessions / "unrelated"):
        projects = root / "claude/projects/example"
        projects.mkdir(parents=True)
        frames = [
            {
                "type": "assistant",
                "timestamp": f"2026-01-01T00:00:{second:02d}Z",
                "message": {
                    "id": f"msg-{second}",
                    "model": "claude-opus-5-5",
                    "usage": {"input_tokens": 1000, "output_tokens": 100},
                },
            }
            for second in (1, 3)
        ]
        (projects / "session.jsonl").write_text("".join(json.dumps(frame) + "\n" for frame in frames))
    (current / "claude-turns.jsonl").write_text(
        json.dumps({"role": "reflection", "event": "start", "recorded_at_epoch": 1767225602}) + "\n"
    )

    row = run_cell(cell, ctx, "hotel-lane")
    assert row.status == "graded"
    assert row.closing_status == "not_drained"
    assert row.online.requests == 1
    assert row.online.usd == pytest.approx(0.006)
    assert row.cost_note is None
