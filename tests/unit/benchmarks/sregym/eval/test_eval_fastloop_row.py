"""The fastloop-row adapter: command, record mapping, lane fetch and failure handling (no cluster, no subprocess)."""

from __future__ import annotations

import json
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


class FakeRow:
    """A scripted ``fastloop run`` process: running for ``polls`` polls, then exited with ``returncode``."""

    def __init__(self, returncode: int = 0, polls: int = 0, on_poll: Any = None) -> None:
        self._code, self._polls, self._on_poll = returncode, polls, on_poll
        self.returncode: int | None = None
        self.calls = 0
        self.terminated = False
        self.killed = False

    def poll(self) -> int | None:
        self.calls += 1
        if self._on_poll is not None:
            self._on_poll(self.calls)
        if self.calls > self._polls and not (self.terminated or self.killed):
            self.returncode = self._code
        return self.returncode

    def wait(self, timeout: float | None = None) -> int:
        return self.returncode if self.returncode is not None else self._code

    def terminate(self) -> None:
        self.terminated = True
        self.returncode = -15

    def kill(self) -> None:
        self.killed = True
        self.returncode = -9


@pytest.fixture(autouse=True)
def _no_docker(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("benchmarks.sregym.eval.fastloop_row.apply_lane_cpu", lambda *args, **kwargs: None)


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
        application="hotel-reservation",
        description="A synthetic hotel application.",
        workspace=tmp_path / "ws",
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


def test_a_late_sdo_incident_without_a_fallback_page_is_no_page() -> None:
    result = map_record(record(detected_at=at(400)), cell(), None)
    assert result.paged_at is None
    assert result.page_source == "none"


def test_a_fallback_page_is_the_clock_even_though_the_incident_it_opened_is_late() -> None:
    page = T0.timestamp() + 121
    result = map_record(record(detected_at=at(125)), cell(), None, fallback_page_at=page)
    assert result.paged_at == page
    assert result.page_source == "fallback"


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

    def fake_start(command: list[str], **kwargs: Any) -> FakeRow:
        row = command[command.index("--run-id") + 1]
        results = env.run_dir / "results" / row
        results.mkdir(parents=True)
        (results / "incidents.jsonl").write_text(record().model_dump_json() + "\n")
        (results / "transcript.jsonl").write_text("{}\n")
        return FakeRow(returncode=0)

    runner = FastloopRowRunner(
        settings(tmp_path),
        "small",
        start=fake_start,
        sleep=lambda seconds: None,
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
        start=lambda command, **kw: FakeRow(returncode=3),
        sleep=lambda seconds: None,
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


# --- the row ends at the online checkpoint; the closing turn drains on the held lane -------------------------------


class Time:
    def __init__(self) -> None:
        self.t = 100.0

    def now(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.t += seconds


def git(repo: Path, *args: str) -> str:
    import os
    import subprocess

    identity = {
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@x",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@x",
    }
    done = subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True, text=True, env={**os.environ, **identity}
    )
    return done.stdout.strip()


def memory_repo(tmp_path: Path) -> tuple[Path, str, str]:
    """A synthetic accepted-memory repository: a base commit and the closing turn's commit on top."""
    repo = tmp_path / "ws"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "main")
    for name in (".sdo/playbooks/old/p.go", ".sdo/lib/helpers/h.go"):
        (repo / name).parent.mkdir(parents=True, exist_ok=True)
        (repo / name).write_text("package x\n")
    git(repo, "add", "-A")
    git(repo, "commit", "-q", "-m", "base")
    base = git(repo, "rev-parse", "HEAD")
    for name, body in (
        (".sdo/playbooks/new/p.go", "package x\n"),
        (".sdo/diagnostics/detectors/learned/d1/main.go", "package x\n"),
        (".sdo/corpus/case1/case.json", "{}\n"),
        (".sdo/lib/helpers/h.go", "package y\n"),
    ):
        (repo / name).parent.mkdir(parents=True, exist_ok=True)
        (repo / name).write_text(body)
    git(repo, "add", "-A")
    git(repo, "commit", "-q", "-m", "closing turn")
    return repo, base, git(repo, "rev-parse", "HEAD")


def write_receipt(directory: Path, **ledger: Any) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "sdo_production_receipt_strict.json").write_text(json.dumps({"native_ledger": ledger}))


def usage_log(directory: Path, incident_id: str) -> None:
    import hashlib

    session = hashlib.sha256(incident_id.encode()).hexdigest()[:32]
    log = directory / "sdo_runtime" / "sessions" / session / "reflection-usage.jsonl"
    log.parent.mkdir(parents=True, exist_ok=True)
    log.write_text(json.dumps({"cost_usd": 0.5, "usage": {"input_tokens": 10, "output_tokens": 2}}) + "\n")


class Row:
    """A live-looking row: the online checkpoint appears after ``online_after`` polls, the process exits later."""

    def __init__(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, **overrides: Any) -> None:
        self.env = environment(tmp_path)
        monkeypatch.setattr("benchmarks.sregym.eval.fastloop_row.FastloopEnvironment.load", lambda directory: self.env)
        monkeypatch.setattr(FastloopRowRunner, "_fetch", lambda *args: None)  # the lane's fetch needs a real store
        self.tmp_path = tmp_path
        self.time = Time()
        self.imported: list[tuple[str, str, str]] = []
        self.process: FakeRow | None = None
        self.online_after = overrides.pop("online_after", 3)
        self.exit_after = overrides.pop("exit_after", 30)
        self.exit_code = overrides.pop("exit_code", 0)
        self.final = overrides.pop("final", record(reflection_seconds=420.0, fault_recovery_seconds=31.0))
        self.settings_overrides = overrides
        self.head = "headsha"
        self.results: Path | None = None

    def start(self, command: list[str], **kwargs: Any) -> FakeRow:
        row = command[command.index("--run-id") + 1]
        self.results = self.env.run_dir / "results" / row
        self.results.mkdir(parents=True)
        online = self.results / "incidents.online" / "000000.json"

        def on_poll(call: int) -> None:
            if call == self.online_after:
                online.parent.mkdir(parents=True)
                online.write_text(record(online_ended_at=at(270)).model_dump_json() + "\n")
            if call == self.exit_after:
                (self.results / "incidents.jsonl").write_text(self.final.model_dump_json() + "\n")

        self.process = FakeRow(returncode=self.exit_code, polls=self.exit_after, on_poll=on_poll)
        return self.process

    def runner(self, **kwargs: Any) -> FastloopRowRunner:
        return FastloopRowRunner(
            settings(self.tmp_path, **self.settings_overrides),
            "small",
            start=self.start,
            now=self.time.now,
            sleep=self.time.sleep,
            import_memory=lambda app, problem, workspace, commit: self.imported.append((app, problem, commit)),
            head=lambda workspace: self.head,
            **kwargs,
        )

    def context(self) -> ArmContext:
        return ArmContext(
            config=make_config(self.tmp_path), page_hook=NoPageHook(), now=self.time.now, sleep=self.time.sleep
        )


def test_every_arm_pays_the_end_of_row_recovery_inside_its_row(tmp_path: Path) -> None:
    env = environment(tmp_path)
    for arm in ("sdo", "vanilla"):
        command = row_command(settings(tmp_path), env, cell(arm), "rid", tmp_path / "s.json", None)
        assert "--end-of-row-recovery" in command
    off = row_command(settings(tmp_path, end_of_row_recovery=False), env, cell("sdo"), "rid", tmp_path / "s.json", None)
    assert "--end-of-row-recovery" not in off


def test_execute_returns_at_the_online_checkpoint_while_the_row_process_keeps_running(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = Row(tmp_path, monkeypatch)
    runner = fake.runner()
    lane = runner.claim(cell(), "h1", None)
    result = runner.execute(lane, cell(), Injection(at=0), tmp_path, fake.context())
    assert fake.process is not None
    assert fake.process.returncode is None  # still running: the closing turn has not finished
    assert result.memory_commit_after is None  # the delta is the closing's, not the online row's
    assert result.recovered_at is not None
    assert fake.imported == []
    assert runner.grade(lane, cell()).success is True  # graded from the online record
    assert not (tmp_path / "row-exit.json").exists()


def test_the_drain_waits_for_the_row_to_exit_and_reports_the_closing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, base, head = memory_repo(tmp_path)
    fake = Row(tmp_path, monkeypatch)
    fake.head = head
    fake.env.workspace = repo
    runner = fake.runner()
    lane = runner.claim(cell(), "h1", base)
    receipts = tmp_path / "art"
    write_receipt(
        receipts,
        reflection_decision={"learning_decision": "updated"},
        source_repair="none",
        source_repair_uncaptured=None,
        cleanup_turns_used=1,
        closure_phase_timestamps={
            "reflection_committed": "2026-11-18T12:10:00Z",
            "source_repair_none": "2026-11-18T12:11:00Z",
        },
    )
    fake.final = record(
        reflection_seconds=420.0,
        fault_recovery_seconds=31.0,
        full_cleanup_verified=True,
        cleanup_passes_verified=1,
        incident_id="incident-9",
        artifacts_dir=str(receipts),
    )
    runner.execute(lane, cell(), Injection(at=0), tmp_path, fake.context())
    before = fake.time.now()
    closing = runner.drain(lane, cell(), tmp_path, fake.context())
    assert fake.time.now() > before  # it waited
    assert closing.status == "drained"
    assert closing.incident_id == "incident-9"
    assert closing.closing_s == 420.0
    assert closing.recovery_s == 31.0
    assert closing.memory_commit_after == head
    assert fake.imported == [("hotel", "prob_a", head)]
    assert closing.decision == "updated"
    assert closing.source_repair == "none"
    assert closing.cleanup_turns_used == 1
    assert closing.phases["source_repair_none"] == "2026-11-18T12:11:00Z"
    assert closing.recovery["full_cleanup_verified"] is True
    assert closing.learning["commit"] == head
    assert closing.learning["base"] == base
    assert closing.learning["playbooks"] == {"added": ["new"], "modified": [], "deleted": []}
    assert closing.learning["detectors"] == {"added": ["d1"], "modified": [], "deleted": []}
    assert closing.learning["lib"] == {"added": [], "modified": ["helpers"], "deleted": []}
    assert closing.learning["corpus"] == {"added": ["case1"], "modified": [], "deleted": []}
    assert json.loads((tmp_path / "row-exit.json").read_text())["returncode"] == 0
    assert runner.records["hotel/pass1/prob_a"].reflection_seconds == 420.0  # the final record replaced the online one


def test_the_drain_stops_a_row_that_outlives_its_deadline_and_excludes_its_delta(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = Row(tmp_path, monkeypatch, exit_after=10_000, drain_timeout_seconds=60)
    runner = fake.runner()
    lane = runner.claim(cell(), "h1", None)
    runner.execute(lane, cell(), Injection(at=0), tmp_path, fake.context())
    closing = runner.drain(lane, cell(), tmp_path, fake.context())
    assert fake.process is not None
    assert fake.process.terminated
    assert closing.status == "not_drained"
    assert closing.decision == "not_drained"
    assert closing.memory_commit_after is None
    assert fake.imported == []


def test_a_row_that_exits_non_zero_after_its_online_end_is_a_closing_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = Row(tmp_path, monkeypatch, exit_code=2, exit_after=8)
    runner = fake.runner()
    lane = runner.claim(cell(), "h1", None)
    runner.execute(lane, cell(), Injection(at=0), tmp_path, fake.context())
    closing = runner.drain(lane, cell(), tmp_path, fake.context())
    assert closing.status == "error"
    assert "exited 2" in (closing.error or "")
    assert closing.memory_commit_after is None
    assert fake.imported == []


def test_a_vanilla_row_drains_its_recovery_with_no_closing_turn_or_memory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = Row(tmp_path, monkeypatch, final=record(agent="claudecode", fault_recovery_seconds=40.0))
    runner = fake.runner()
    vanilla = cell("vanilla", "vanilla")
    lane = runner.claim(vanilla, "h1", None)
    runner.execute(lane, vanilla, Injection(at=0), tmp_path, fake.context())
    closing = runner.drain(lane, vanilla, tmp_path, fake.context())
    assert closing.status == "drained"
    assert closing.recovery_s == 40.0
    assert closing.memory_commit_after is None
    assert closing.learning == {}
    assert fake.imported == []


def test_the_drain_of_a_row_that_never_started_is_not_applicable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = Row(tmp_path, monkeypatch)
    runner = fake.runner()
    lane = runner.claim(cell(), "h1", None)
    assert runner.drain(lane, cell(), tmp_path, fake.context()).status == "not_applicable"


def rejected_record(message: str, *, stage: str = "pre_injection") -> IncidentRecord:
    return record(
        injection={"state": "not_injected", "stage": stage},
        detected_at=None,
        dispatched_at=None,
        responder_started_at=None,
        responder_finished_at=None,
        resolved_at=None,
        reflection_started_at=None,
        oracle=None,
        resolution_path=None,
        error=message,
    )


class Attempts:
    """``fastloop run`` attempts that each finish in one step with the next scripted record."""

    def __init__(self, env: Any, records: list[IncidentRecord]) -> None:
        self.env, self.records, self.run_ids = env, records, []

    def start(self, command: list[str], **kwargs: Any) -> FakeRow:
        row = command[command.index("--run-id") + 1]
        self.run_ids.append(row)
        results = self.env.run_dir / "results" / row
        results.mkdir(parents=True)
        (results / "incidents.jsonl").write_text(
            self.records[min(len(self.run_ids), len(self.records)) - 1].model_dump_json() + "\n"
        )
        return FakeRow(returncode=0)


def execute_attempts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, arm: Arm, records: list[IncidentRecord]) -> Any:
    env = environment(tmp_path)
    monkeypatch.setattr("benchmarks.sregym.eval.fastloop_row.FastloopEnvironment.load", lambda directory: env)
    monkeypatch.setattr(FastloopRowRunner, "_fetch", lambda *args: None)
    attempts = Attempts(env, records)
    runner = FastloopRowRunner(
        settings(tmp_path), "small", start=attempts.start, sleep=lambda s: None, head=lambda workspace: "h"
    )
    context = ArmContext(config=make_config(tmp_path), page_hook=NoPageHook(), now=lambda: 0.0, sleep=lambda s: None)
    lane = runner.claim(cell(arm), "h1", "snap" if arm == "sdo" else None)
    result = runner.execute(lane, cell(arm), Injection(at=0), tmp_path, context)
    return attempts, result


@pytest.mark.parametrize("arm", ["sdo", "vanilla"])
def test_a_row_rejected_before_injection_is_retried_on_a_fresh_attempt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, arm: Arm
) -> None:
    flake = rejected_record("no-fault rehearsal has unhealthy samples")
    attempts, result = execute_attempts(tmp_path, monkeypatch, arm, [flake, record()])
    assert len(attempts.run_ids) == 2
    assert attempts.run_ids[1].endswith("-a1")
    assert result.status != "error"


def test_the_pre_injection_retry_is_bounded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from benchmarks.sregym.eval.fastloop_row import ADMISSION_RETRIES

    flake = rejected_record("SDO rejected onboarding: the first install was not healthy")
    attempts, _ = execute_attempts(tmp_path, monkeypatch, "sdo", [flake])
    assert len(attempts.run_ids) == 1 + ADMISSION_RETRIES


def test_a_failure_after_injection_is_never_retried(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    broken = rejected_record("no-fault rehearsal has unhealthy samples", stage="injection")
    attempts, _ = execute_attempts(tmp_path, monkeypatch, "sdo", [broken, record()])
    assert len(attempts.run_ids) == 1


def test_an_unrelated_pre_injection_error_is_not_retried(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    attempts, _ = execute_attempts(tmp_path, monkeypatch, "sdo", [rejected_record("some other failure"), record()])
    assert len(attempts.run_ids) == 1


def test_every_claim_reapplies_the_lane_cpu_cap_to_that_lanes_cluster(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        "benchmarks.sregym.eval.fastloop_row.FastloopEnvironment.load", lambda directory: environment(tmp_path)
    )
    monkeypatch.setattr("benchmarks.sregym.eval.fastloop_row.subprocess.run", lambda command, **kw: None)
    monkeypatch.setattr("benchmarks.sregym.eval.fastloop_row.resolve_cgroup_parent", lambda setting: setting)
    applied: list[tuple[str, str, str | None]] = []

    def apply_cpu(cluster: str, lane_cpus: str, *, parent: str | None = None) -> None:
        applied.append((cluster, lane_cpus, parent))

    runner = FastloopRowRunner(
        settings(tmp_path, lane_cpus=4, lane_cgroup_parent="x.slice"), "small", apply_cpu=apply_cpu
    )
    runner.claim(cell("vanilla", "vanilla"), "h1", None)
    runner.claim(cell("vanilla", "vanilla"), "h1", None)
    assert applied == [("c", "4", "x.slice")] * 2  # idempotent: the same cap, every claim


def test_a_claim_records_each_nodes_cpuset_and_its_shared_cores(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from benchmarks.sregym.fastloop.lane_cgroup import CpuPlacement

    monkeypatch.setattr(
        "benchmarks.sregym.eval.fastloop_row.FastloopEnvironment.load", lambda directory: environment(tmp_path)
    )
    monkeypatch.setattr("benchmarks.sregym.eval.fastloop_row.resolve_cgroup_parent", lambda setting: setting)
    placement = CpuPlacement("c-worker", frozenset({4, 5, 6}), frozenset({6}))
    runner = FastloopRowRunner(settings(tmp_path), "small", apply_cpu=lambda *a, **kw: [placement])
    lane = runner.claim(cell("vanilla", "vanilla"), "h1", None)
    assert json.loads(lane.info["cpu_placement"]) == [{"node": "c-worker", "cpuset": "4-6", "shared": "6"}]


def test_row_settings_default_to_six_cpus_per_lane_and_an_auto_parent(tmp_path: Path) -> None:
    default = settings(tmp_path)
    assert (default.lane_cpus, default.lane_cgroup_parent) == (6.0, "auto")
    with pytest.raises(ValueError, match="lane_cpus"):
        settings(tmp_path, lane_cpus=0)
