"""The fastloop-row adapter: command, record mapping, lane fetch and failure handling (no cluster, no subprocess)."""

from __future__ import annotations

import json
import subprocess
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.sregym.eval.cell import ArmContext, Injection, Lane, NoPageHook
from benchmarks.sregym.eval.fastloop_row import FastloopRowRunner, RowSettings, map_record, row_command, run_id
from benchmarks.sregym.eval.plan import Arm, Cell, PlanPass
from benchmarks.sregym.eval.timings import PHASES
from benchmarks.sregym.fastloop.records import IncidentRecord
from tests.unit.benchmarks.sregym.eval.eval_fakes import make_config

if TYPE_CHECKING:
    from pathlib import Path

T0 = datetime(2026, 11, 18, 12, 0, 0, tzinfo=timezone.utc)


def at(seconds: float) -> str:
    return (T0 + timedelta(seconds=seconds)).isoformat()


def cell(arm: Arm = "sdo", pass_: PlanPass = "pass1") -> Cell:
    return Cell(app="hotel", problem="prob_a", arm=arm, pass_=pass_, snapshot="base" if arm == "sdo" else None, order=0)


def record(**overrides: Any) -> IncidentRecord:
    values: dict[str, Any] = {
        "run_id": "r",
        "index": 0,
        "agent": "sdo",
        "problem_id": "prob_a",
        "model": "m",
        "injection": {"state": "injected", "started_at": at(0), "finished_at": at(3)},
        "detected_at": at(40),
        "dispatched_at": at(45),
        "responder_started_at": at(46),
        "responder_finished_at": at(200),
        "resolved_at": at(260),
        "reflection_started_at": at(262),
        "oracle": {"success": True, "kind": "sregym-mitigation-oracle"},
        "resolution_path": "responder",
        "binding_sources": ["finding"],
        "run_prelude_phase_seconds": {"worker_start": 5.0, "worker_deploy": 7.0, "baseline_and_driver_setup": 2.0},
        "responder_tokens": {"input_tokens": 100, "cached_input_tokens": 60, "output_tokens": 10},
        "reflection_tokens": {"input_tokens": 30, "output_tokens": 3},
    }
    return IncidentRecord.model_validate({**values, **overrides})


def settings(tmp_path: Path, **overrides: Any) -> RowSettings:
    values: dict[str, Any] = {
        "code_root": tmp_path,
        "lane_environments": {"h1": tmp_path / "lane-h1"},
        "snapshot_repos": {"hotel": tmp_path / "store"},
    }
    return RowSettings(**{**values, **overrides})


def environment(tmp_path: Path) -> Any:
    return SimpleNamespace(
        run_dir=tmp_path / "lane-h1",
        cluster="c",
        kubeconfig=tmp_path / "kc",
        namespace="hotel-reservation",
        workspace=tmp_path / "ws",
        seed_commit="seed",
        evidence_root=None,
        save=lambda: None,
    )


def test_run_id_is_a_valid_unique_name() -> None:
    assert run_id("small", cell(), 2) == "small-hotel-pass1-prob_a-a2"
    assert run_id(
        "s", Cell(app="a", problem="x/y z", arm="sdo", pass_="pass1", snapshot="base", order=0), 0
    ).startswith("s-a-pass1-x-y-z")


def test_row_command_pins_the_snapshot_and_names_the_arm_agent(tmp_path: Path) -> None:
    command = row_command(settings(tmp_path), environment(tmp_path), cell(), "rid", tmp_path / "s.json", "abc123")
    assert command[:3] == [command[0], "-m", "benchmarks.sregym.fastloop"]
    assert command[command.index("--planned-commit") + 1] == "abc123"
    assert command[command.index("--agent") + 1] == "sdo"
    assert command[command.index("--run-id") + 1] == "rid"
    assert "--healthy-baseline" in command


def test_vanilla_command_has_no_planned_commit_and_its_own_agent(tmp_path: Path) -> None:
    command = row_command(
        settings(tmp_path, extra_args={"vanilla": ("--x", "1")}),
        environment(tmp_path),
        cell("vanilla", "vanilla"),
        "rid",
        tmp_path / "s.json",
        None,
    )
    assert command[command.index("--agent") + 1] == "claudecode"
    assert "--planned-commit" not in command
    assert command[-2:] == ["--x", "1"]


def test_coord_wraps_the_row_in_the_lane_guard(tmp_path: Path) -> None:
    command = row_command(
        settings(tmp_path, coord=tmp_path / "coord"),
        environment(tmp_path),
        cell(),
        "rid",
        tmp_path / "s.json",
        "abc",
    )
    assert "benchmarks.sregym.fastloop.lane_guard" in command
    assert command[command.index("--") + 1 :][:2] == ["benchmarks.sregym.fastloop", "run"]


def test_sdo_record_maps_own_page_recovery_and_phases() -> None:
    result = map_record(record(), cell(), "head")
    assert result.injected_at == T0.timestamp()
    assert result.paged_at == T0.timestamp() + 40
    assert result.page_source == "own"
    assert result.agent_ended_at == T0.timestamp() + 200
    assert result.recovered_at == T0.timestamp() + 260
    assert result.memory_commit_after == "head"
    assert result.phase_seconds == {"claim_reset": 7.0, "deploy": 7.0}
    assert set(result.phase_seconds) <= set(PHASES)


def test_a_late_sdo_incident_is_no_page_because_the_fallback_hook_is_not_built() -> None:
    result = map_record(record(detected_at=at(400)), cell(), None)
    assert result.paged_at is None
    assert result.page_source == "none"


def test_vanilla_page_is_the_instruction_handoff_and_has_no_sdo_recovery() -> None:
    result = map_record(record(agent="codex", detected_at=None), cell("vanilla", "vanilla"), None)
    assert result.page_source == "instruction"
    assert result.paged_at == T0.timestamp() + 46
    assert result.recovered_at is None
    assert result.resolution_path == "responder"


def test_infra_error_is_an_error_arm_result() -> None:
    result = map_record(record(infra_error="never_injected_retry"), cell(), None)
    assert result.status == "error"
    assert result.error == "never_injected_retry"


def test_execute_runs_the_row_and_reads_its_single_record(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    env = environment(tmp_path)
    monkeypatch.setattr("benchmarks.sregym.eval.fastloop_row.FastloopEnvironment.load", lambda directory: env)
    fetched: list[list[str]] = []
    monkeypatch.setattr(
        "benchmarks.sregym.eval.fastloop_row.subprocess.run", lambda command, **kw: fetched.append(command) or None
    )
    imported: list[tuple[str, str, str]] = []

    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        row = command[command.index("--run-id") + 1]
        results = env.run_dir / "results" / row
        results.mkdir(parents=True)
        (results / "incidents.jsonl").write_text(record().model_dump_json() + "\n")
        (results / "transcript.jsonl").write_text("{}\n")
        return subprocess.CompletedProcess(command, 0)

    runner = FastloopRowRunner(
        settings(tmp_path),
        "small",
        run=fake_run,
        import_memory=lambda app, problem, workspace, commit: imported.append((app, problem, commit)),
        head=lambda workspace: "newhead",
    )
    config = make_config(tmp_path)
    context = ArmContext(config=config, page_hook=NoPageHook(), now=lambda: 0.0, sleep=lambda s: None)
    lane = runner.claim(cell(), "h1", "snapcommit")
    assert fetched
    assert "snapcommit" in fetched[0]
    result = runner.execute(lane, cell(), Injection(at=0), tmp_path, context)
    assert result.memory_commit_after == "newhead"
    assert imported == [("hotel", "prob_a", "newhead")]
    assert (tmp_path / "stream.json").is_file()
    assert json.loads((tmp_path / "row-exit.json").read_text())["returncode"] == 0
    verdict = runner.grade(lane, cell())
    assert verdict.success is True
    runner.collect(lane, cell(), tmp_path)
    assert (tmp_path / "fastloop" / "transcript.jsonl").is_file()
    runner.release(lane, reusable=True)


def test_a_row_without_results_is_an_error_result(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    env = environment(tmp_path)
    monkeypatch.setattr("benchmarks.sregym.eval.fastloop_row.FastloopEnvironment.load", lambda directory: env)
    runner = FastloopRowRunner(
        settings(tmp_path),
        "small",
        run=lambda command, **kw: subprocess.CompletedProcess(command, 3),
        head=lambda w: "h",
    )
    lane = runner.claim(cell("vanilla", "vanilla"), "h1", None)
    context = ArmContext(config=make_config(tmp_path), page_hook=NoPageHook(), now=lambda: 5.0, sleep=lambda s: None)
    result = runner.execute(lane, cell("vanilla", "vanilla"), Injection(at=0), tmp_path, context)
    assert result.status == "error"
    assert "exited 3" in (result.error or "")
    with pytest.raises(RuntimeError, match="no oracle verdict"):
        runner.grade(lane, cell("vanilla", "vanilla"))


def test_lane_info_names_the_environment() -> None:
    assert Lane(id="h1", app="hotel").info == {}


def test_an_sdo_row_rejected_by_admission_before_injection_is_retried(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env = environment(tmp_path)
    monkeypatch.setattr("benchmarks.sregym.eval.fastloop_row.FastloopEnvironment.load", lambda directory: env)
    monkeypatch.setattr("benchmarks.sregym.eval.fastloop_row.subprocess.run", lambda command, **kw: None)
    runs: list[str] = []

    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        row = command[command.index("--run-id") + 1]
        runs.append(row)
        results = env.run_dir / "results" / row
        results.mkdir(parents=True)
        data = json.loads(record().model_dump_json())
        if len(runs) < 3:
            data["injection"] = {"state": "not_injected", "stage": "pre_injection"}
            data["error"] = "ValueError: eval: SDO rejected onboarding after 232s (install observation window expired)"
        (results / "incidents.jsonl").write_text(json.dumps(data) + "\n")
        return subprocess.CompletedProcess(command, 0)

    runner = FastloopRowRunner(settings(tmp_path), "small", run=fake_run, head=lambda w: "h")
    context = ArmContext(config=make_config(tmp_path), page_hook=NoPageHook(), now=lambda: 0.0, sleep=lambda s: None)
    lane = runner.claim(cell(), "h1", "snapcommit")
    runner.execute(lane, cell(), Injection(at=0), tmp_path, context)
    assert len(runs) == 3
    assert runs[0].endswith("-a0") and runs[2].endswith("-a2")
