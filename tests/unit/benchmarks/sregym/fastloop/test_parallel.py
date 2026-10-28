"""Parallel orchestration preserves lane order and joins every failure."""

import ast
import json
import subprocess
import threading
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from benchmarks.sregym.fastloop import shared_memory
from benchmarks.sregym.fastloop.parallel import Encounter, ParallelPlan, run_parallel
from benchmarks.sregym.fastloop.records import IncidentRecord
from benchmarks.sregym.fastloop.stream import load_stream_config


def record(problem, lane, index=0):
    return IncidentRecord(
        run_id="synthetic",
        index=index,
        agent="sdo",
        problem_id=problem,
        model="scripted",
        injection={
            "state": "injected",
            "started_at": datetime.now(timezone.utc),
            "finished_at": datetime.now(timezone.utc),
        },
        incident_id=f"{lane}-{index}",
    )


def plan(tmp_path):
    return ParallelPlan(
        run_id="proof",
        accepted_repository=tmp_path / "accepted",
        lanes={"left": tmp_path / "left", "right": tmp_path / "right"},
        encounters=[
            Encounter(problem="alpha", lane="left"),
            Encounter(problem="beta", lane="right"),
            Encounter(problem="alpha-repeat", lane="left"),
        ],
    )


def test_lanes_overlap_but_each_lane_preserves_order_and_all_rows_are_joined(tmp_path):
    selected = plan(tmp_path)
    barrier = threading.Barrier(2, timeout=5)
    checked = []

    def execute(name, directory, encounters, _plan):
        assert checked == ["left", "right"], "every preflight must finish before either lane starts"
        barrier.wait()
        return [record(e.problem, name, i) for i, e in enumerate(encounters)]

    rows = run_parallel(selected, tmp_path / "output", preflight=lambda name, *_: checked.append(name), execute=execute)
    assert [(r.index, r.problem_id, r.lane_id) for r in rows] == [
        (0, "alpha", "left"),
        (1, "beta", "right"),
        (2, "alpha-repeat", "left"),
    ]
    assert len((tmp_path / "output" / "incidents.jsonl").read_text().splitlines()) == 3
    assert json.loads((tmp_path / "output" / "parallel_result.json").read_text())["passed"] is True


def test_preflight_failure_starts_no_lane(tmp_path):
    def reject(*_):
        raise ValueError("unshared repository")

    with pytest.raises(ValueError, match="unshared repository"):
        run_parallel(
            plan(tmp_path),
            tmp_path / "output",
            preflight=reject,
            execute=lambda *_: pytest.fail("launched before all preflights passed"),
        )


def test_lane_failure_keeps_peer_evidence_and_reports_missing_encounters(tmp_path):
    barrier = threading.Barrier(2, timeout=5)

    def execute(name, directory, encounters, _plan):
        barrier.wait()
        if name == "left":
            raise RuntimeError("lane failed before closure")
        return [record(e.problem, name, i) for i, e in enumerate(encounters)]

    with pytest.raises(RuntimeError, match="left"):
        run_parallel(plan(tmp_path), tmp_path / "output", preflight=lambda *_: None, execute=execute)
    result = json.loads((tmp_path / "output" / "parallel_result.json").read_text())
    assert result["passed"] is False
    assert result["completed"] == 1
    assert len((tmp_path / "output" / "incidents.jsonl").read_text().splitlines()) == 1


def test_duplicate_incident_identity_cannot_be_reported_as_valid(tmp_path):
    def execute(name, directory, encounters, _plan):
        rows = [record(e.problem, name, i) for i, e in enumerate(encounters)]
        rows[0].incident_id = "collision"
        return rows

    with pytest.raises(RuntimeError, match="incident"):
        run_parallel(plan(tmp_path), tmp_path / "output", preflight=lambda *_: None, execute=execute)


@pytest.mark.parametrize(
    "change",
    [
        {"lanes": {"left": Path("/same"), "right": Path("/same")}},
        {"encounters": [Encounter(problem="alpha", lane="absent")]},
        {"lanes": {"bad/name": Path("/lane")}},
    ],
)
def test_plan_rejects_ambiguous_lane_ownership(tmp_path, change):
    data = plan(tmp_path).model_dump()
    data.update(change)
    with pytest.raises(ValueError, match="lane"):
        ParallelPlan.model_validate(data)


def test_memory_provenance_preserves_every_peer_commit_and_rejects_rewind(tmp_path):
    from benchmarks.sregym.fastloop.parallel_memory import memory_ancestry, memory_head

    root = tmp_path / "accepted"
    root.mkdir()

    def git(*args):
        return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()

    git("init", "-q")
    git("config", "user.name", "synthetic")
    git("config", "user.email", "synthetic@invalid")
    git("commit", "--allow-empty", "-qm", "seed")
    first = memory_head(root)
    git("commit", "--allow-empty", "-qm", "left learning")
    left = memory_head(root)
    git("commit", "--allow-empty", "-qm", "right learning")
    right = memory_head(root)
    assert memory_ancestry(root, first, right) == [left, right]
    with pytest.raises(ValueError, match="rewound"):
        memory_ancestry(root, right, first)


def test_parallel_prepare_runs_before_any_lane_preflight(monkeypatch, tmp_path):
    from benchmarks.sregym.fastloop import cli, parallel, shared_memory

    selected = plan(tmp_path)
    path = tmp_path / "plan.json"
    path.write_text(selected.model_dump_json())
    calls = []
    monkeypatch.setattr(shared_memory, "prepare_shared_lanes", lambda *_: calls.append("prepare"))
    monkeypatch.setattr(parallel, "run_parallel", lambda *_: calls.append("run"))
    assert cli.main(["parallel", "--plan", str(path), "--output", str(tmp_path / "output"), "--prepare"]) == 0
    assert calls == ["prepare", "run"]


@pytest.mark.parametrize(("provider", "model"), [("codex", "gpt-6-luna"), ("claude", "claude-sonnet-5-5")])
def test_lane_process_overrides_ambient_kubeconfig_and_caps_host_build_jobs(monkeypatch, tmp_path, provider, model):
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment
    from benchmarks.sregym.fastloop.parallel import execute_lane
    from benchmarks.sregym.fastloop.records import write_records

    selected = ParallelPlan(
        **{
            **plan(tmp_path).model_dump(),
            "provider": provider,
            "model": model,
            "capability_mode": "permissive",
            "permissive_responder": True,
        }
    )
    for name, run_dir in selected.lanes.items():
        FastloopEnvironment(
            run_dir=run_dir,
            cluster=name,
            kubeconfig=run_dir / "config",
            namespace="app",
            application=f"app-{name}",
            workspace=selected.accepted_repository,
            sregym_dir=tmp_path / "sregym",
            private_tmp=run_dir / "tmp",
        ).save()
    monkeypatch.setenv("SREGYM_BASE_KUBECONFIG", "/wrong-lane")

    def command(argv, **kwargs):
        from benchmarks.sregym.fastloop.cli import build_parser

        args = build_parser().parse_args(argv[argv.index("benchmarks.sregym.fastloop") + 1 :])
        assert args.cleanup_passes == 1
        assert (args.provider, args.model) == (provider, model)
        stream = load_stream_config(args.stream)
        assert (stream.capability_mode, stream.permissive_responder) == ("permissive", True)
        assert stream.diagnosis_grading == "off"
        assert kwargs["env"]["SREGYM_BASE_KUBECONFIG"] == str(tmp_path / "left" / "config")
        assert kwargs["env"]["KUBECONFIG"] == str(tmp_path / "left" / "config")
        assert kwargs["env"]["GOMAXPROCS"] == "4"
        assert kwargs["env"]["GOFLAGS"].endswith("-p=4")
        path = tmp_path / "left" / "results" / "proof-left" / "incidents.jsonl"
        write_records(path, [record("alpha", "left")])
        return subprocess.CompletedProcess(argv, 0)

    monkeypatch.setattr(subprocess, "run", command)
    assert len(execute_lane("left", tmp_path / "output", [selected.encounters[0]], selected)) == 1


def test_two_scripted_lanes_use_real_loop_grading_and_paused_recovery(tmp_path):
    from benchmarks.sregym.fastloop.loop import AgentOutcome, InjectionWindow, LoopConfig, run_incidents
    from benchmarks.sregym.fastloop.records import OracleVerdict

    selected = plan(tmp_path)
    selected.accepted_repository.mkdir()
    subprocess.run(["git", "init", "-q", str(selected.accepted_repository)], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(selected.accepted_repository),
            "-c",
            "user.name=synthetic",
            "-c",
            "user.email=synthetic@invalid",
            "commit",
            "--allow-empty",
            "-qm",
            "seed",
        ],
        check=True,
    )
    barrier = threading.Barrier(2, timeout=5)
    events = {name: [] for name in selected.lanes}

    def execute(name, directory, encounters, _plan):
        state = {"learned": False, "paused": False, "index": 0}

        class Driver:
            def inject(self, problem):
                state.update(learned=False, paused=False)
                events[name].append("inject")
                now = datetime.now(timezone.utc)
                return InjectionWindow(now, now)

            def oracle(self):
                events[name].append("oracle")
                return OracleVerdict(kind="health-check", success=True)

            def recover(self):
                assert state["learned"]
                assert state["paused"]
                events[name].append("recover")
                return 0.0

        driver = Driver()

        class Agent:
            name = "sdo"
            model = "scripted"

            def resolve(self, index, problem, inject):
                state["index"] = index
                window = inject()
                if index == 0:
                    barrier.wait()
                events[name].append("closure")
                return AgentOutcome(
                    injection=window, resolved_at=datetime.now(timezone.utc), incident_id=f"{name}-{index}"
                )

            def learn(self, outcome):
                checkpoint = directory / "incidents.grades" / f"{state['index']:06d}.json"
                assert json.loads(checkpoint.read_text())["oracle"]["success"] is True
                events[name].append("learn")
                state["learned"] = True
                return outcome

            def prepare_recovery(self, outcome):
                assert state["learned"]
                events[name].append("pause")
                state["paused"] = True

            def close(self):
                pass

        config = LoopConfig(
            run_id=f"scripted-{name}",
            problems=tuple(e.problem for e in encounters),
            incidents=len(encounters),
            results_path=directory / "incidents.jsonl",
            lane_id=name,
            accepted_repository=selected.accepted_repository,
        )
        return run_incidents(Agent(), driver, config)

    rows = run_parallel(selected, tmp_path / "output", preflight=lambda *_: None, execute=execute)
    assert len(rows) == 3
    for name in selected.lanes:
        assert events[name][:6] == ["inject", "closure", "oracle", "learn", "pause", "recover"]
    assert all(row.memory_before_commit == row.memory_after_commit for row in rows)


@pytest.mark.parametrize(("provider", "model"), [("codex", "gpt-6-luna"), ("claude", "claude-sonnet-5-5")])
def test_parallel_prepare_passes_plan_model_to_real_run_parser(monkeypatch, tmp_path, provider, model):
    from contextlib import nullcontext

    from benchmarks.sregym.fastloop import cli, shared_memory
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

    selected = ParallelPlan(**{**plan(tmp_path).model_dump(), "provider": provider, "model": model})
    for name, run_dir in selected.lanes.items():
        FastloopEnvironment(
            run_dir=run_dir,
            cluster=name,
            kubeconfig=run_dir / "config",
            namespace="app",
            application=f"app-{name}",
            workspace=run_dir / "application",
            sregym_dir=tmp_path / "sregym",
            private_tmp=run_dir / "tmp",
            parallel_root=tmp_path,
        ).save()
    monkeypatch.setattr(shared_memory, "lane_nodes", lambda *_: [])
    monkeypatch.setattr(shared_memory, "repository_volume_path", lambda *_: "/synthetic")
    monkeypatch.setattr(cli, "_worker", lambda *_: nullcontext(object()))
    monkeypatch.setattr(cli, "check_host_capacity", lambda *_: None)

    class Prepared(Exception):
        pass

    def agent(args, *_args, **_kwargs):
        assert (args.provider, args.model) == (provider, model)
        raise Prepared

    monkeypatch.setattr(cli, "_sdo_agent", agent)
    with pytest.raises(Prepared):
        shared_memory.prepare_lane(selected, "left")


@pytest.mark.parametrize("fields", [{"provider": "unknown"}, {"model": ""}])
def test_parallel_plan_rejects_invalid_worker_settings(tmp_path, fields):
    with pytest.raises(ValueError, match="provider|model"):
        ParallelPlan(**{**plan(tmp_path).model_dump(), **fields})


def test_parallel_plan_defaults_and_worker_registration_round_trip(tmp_path):
    selected = plan(tmp_path)
    assert (selected.provider, selected.model) == ("codex", "gpt-6-luna")
    registrations = tmp_path / "registrations"
    selected = ParallelPlan(**{**selected.model_dump(), "agent_registrations": registrations})
    restored = ParallelPlan.model_validate_json(selected.model_dump_json())
    from benchmarks.sregym.fastloop.cli import build_parser

    args = build_parser().parse_args(
        ["run", "--agent", "sdo", "--run-dir", str(tmp_path), *restored.worker_arguments()]
    )
    assert (args.provider, args.model, args.agent_registrations) == ("codex", "gpt-6-luna", registrations)


def _lane(tmp_path):
    return SimpleNamespace(parallel_root=tmp_path / "root", images=SimpleNamespace(controller="sdo-controller:frozen"))


def test_lane_tree_is_read_by_a_git_container_because_kind_nodes_ship_no_git(tmp_path):
    command = shared_memory.lane_git_command(
        _lane(tmp_path), "/opt/sdo-parallel/lanes/kind-a", tmp_path / "accepted", "rev-parse", "HEAD^{tree}"
    )
    assert command[:2] == ("docker", "run")
    assert "exec" not in command
    assert f"{tmp_path.resolve()}/root/lanes/kind-a:/lane:ro" in command
    assert command[command.index("--entrypoint") + 1] == "git"
    assert "sdo-controller:frozen" in command
    assert command[-2:] == ("rev-parse", "HEAD^{tree}")


@pytest.mark.parametrize("volume", ["/elsewhere/lanes/kind-a", "/opt/sdo-parallel/../etc"])
def test_lane_tree_rejects_volumes_outside_the_shared_root(tmp_path, volume):
    with pytest.raises(ValueError, match="eval: lane volume"):
        shared_memory.lane_git_command(_lane(tmp_path), volume, tmp_path, "status")


def test_attachment_never_runs_tools_inside_nodes_that_kind_images_lack():
    tree = ast.parse(Path(shared_memory.__file__).read_text())
    node_commands = [
        {part.value for part in call.args if isinstance(part, ast.Constant)}
        for call in ast.walk(tree)
        if isinstance(call, ast.Call)
        and getattr(call.func, "id", "") == "_command"
        and [part.value for part in call.args[:2] if isinstance(part, ast.Constant)] == ["docker", "exec"]
    ]
    assert node_commands, "attachment no longer execs in nodes; drop this ratchet"
    assert not [command for command in node_commands if "git" in command]


def test_accepted_checkout_is_owned_by_the_controller_uid_and_shared_with_the_host_group():
    commands = shared_memory.accepted_ownership_commands("/opt/sdo-parallel/accepted", host_gid=1001)
    assert commands[0] == ("chown", "-R", "65532:1001", "/opt/sdo-parallel/accepted")
    assert ("chmod", "-R", "g+rwX", "/opt/sdo-parallel/accepted") in commands
    assert any(
        command[:3] == ("find", "/opt/sdo-parallel/accepted", "-type") and "g+s" in command for command in commands
    )
