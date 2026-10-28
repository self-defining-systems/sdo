"""Real producers feed the scheduler: fastloop's parallel rows and the prepare worker's ready record."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.fastloop.environment import FastloopEnvironment
from benchmarks.sregym.fastloop.parallel import Encounter, ParallelPlan, run_parallel
from benchmarks.sregym.fastloop.records import IncidentRecord, Injected, NotInjected
from benchmarks.sregym.pool.endpoints import MemoryEndpoint
from benchmarks.sregym.pool.scheduler import DeploySpec, PairSpec, Ready, pair_disposition
from benchmarks.sregym.pool.worker import pair, prepare

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

NOW = datetime(2026, 10, 28, tzinfo=timezone.utc)


def record(index: int, problem: str, **changes: object) -> IncidentRecord:
    values: dict[str, object] = {
        "run_id": "r",
        "index": index,
        "agent": "sdo",
        "problem_id": problem,
        "model": "scripted",
        "injection": Injected(started_at=NOW, finished_at=NOW),
    }
    values.update(changes)
    return IncidentRecord.model_validate(values)


def parallel_rows(tmp_path: Path, rows: list[IncidentRecord]) -> str:
    plan = ParallelPlan(
        run_id="r",
        accepted_repository=tmp_path / "accepted",
        lanes={"c5-l0": tmp_path / "lane"},
        encounters=[Encounter(problem="p", lane="c5-l0")] * 2,
    )

    def execute(
        _name: str, _directory: Path, _encounters: Sequence[Encounter], _plan: ParallelPlan
    ) -> list[IncidentRecord]:
        return rows

    output = tmp_path / "out"
    try:
        run_parallel(plan, output, preflight=lambda *_: None, execute=execute)
    except RuntimeError:
        pass  # a failed lane still leaves its rows; the pool decides from them
    return (output / "incidents.jsonl").read_text()


def test_parallel_output_decides_the_pair(tmp_path: Path) -> None:
    done = parallel_rows(tmp_path / "a", [record(0, "p"), record(1, "p", error="oracle reported failure")])
    assert pair_disposition(done, "p", ["first", "repeat"], []).kind == "done"
    retry = parallel_rows(
        tmp_path / "b",
        [
            record(0, "p"),
            record(1, "p", injection=NotInjected(stage="pre_injection"), infra_error="never_injected_retry"),
        ],
    )
    assert pair_disposition(retry, "p", ["first", "repeat"], []).model_dump() == {
        "kind": "retry",
        "remaining": ["repeat"],
        "detail": "never_injected_retry",
    }
    held = parallel_rows(tmp_path / "c", [record(0, "p", recovery_error="namespace left dirty")])
    assert pair_disposition(held, "p", ["first", "repeat"], []).kind == "held"


def toy_lane(tmp_path: Path) -> Path:
    run_dir = tmp_path / "lane"
    FastloopEnvironment(
        run_dir=run_dir,
        cluster="kind-c5-l0",
        kubeconfig=run_dir / "kubeconfig",
        namespace="hotel",
        application="hotel-reservation",
        workspace=tmp_path / "accepted",
        sregym_dir=tmp_path / "sregym",
        private_tmp=None,
        seed_commit="a" * 40,
    ).save()
    return run_dir


def test_prepare_worker_ready_record_is_what_the_scheduler_reads(tmp_path: Path) -> None:
    run_dir = toy_lane(tmp_path)
    spec = DeploySpec(
        lane="c5-l0",
        app="hotel_reservation",
        problem="p",
        build="h1",
        run_dir=run_dir,
        ready=tmp_path / "ready" / "unit.json",
        steps=[["sh", "-c", 'test "$SDO_MEMORY_BRANCH" = live/t']],
        memory=MemoryEndpoint(repo="o/hotel-reservation-sdo", branch="live/t"),
    )
    prepare(spec)
    ready = Ready.model_validate_json(spec.ready.read_text())
    assert (ready.app, ready.build, ready.kubeconfig) == ("hotel_reservation", "h1", run_dir / "kubeconfig")
    assert ready.source_digest.startswith("sha256:")


def test_pair_refuses_a_lane_whose_memory_differs_from_the_run(tmp_path: Path) -> None:
    spec = PairSpec(
        run_id="r",
        lane="c5-l0",
        problem="p",
        app="hotel_reservation",
        encounters=["first", "repeat"],
        run_dir=toy_lane(tmp_path),
        output=tmp_path / "out",
        provider="codex",
        model="scripted",
        timeout_seconds=60,
        diagnosis_grading="off",
        capability_mode="permissive",
        lane_memory="shared",
        memory=MemoryEndpoint(repo="o/hotel-reservation-sdo", branch="live/t"),
    )
    with pytest.raises(ValueError, match="lane_memory=shared"):
        pair(spec)
    assert not spec.output.exists()


def test_attach_lane_prepares_exactly_that_lane_into_the_app_root(tmp_path: Path) -> None:
    from benchmarks.sregym.pool.worker import attach_lane

    seen: list[tuple[ParallelPlan, Path]] = []
    attach_lane(
        tmp_path / "lane",
        "c5-l3",
        "mf__hotel_reservation__n1__s1__reliable__v_",
        tmp_path / "memory" / "hotel_reservation" / "accepted",
        tmp_path / "attach.json",
        prepare_shared=lambda plan, report: seen.append((plan, report)),
    )
    [(plan, report)] = seen
    assert plan.lanes == {"c5-l3": tmp_path / "lane"}
    assert plan.accepted_repository == tmp_path / "memory" / "hotel_reservation" / "accepted"
    assert [e.problem for e in plan.encounters] == ["mf__hotel_reservation__n1__s1__reliable__v_"]
    assert report == tmp_path / "attach.json"
