from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

import pytest

from benchmarks.sregym.fastloop.dispatch import DispatchConfig, dispatch, load_dispatch_config
from benchmarks.sregym.fastloop.environment import FastloopEnvironment
from benchmarks.sregym.fastloop.records import IncidentRecord, OracleVerdict, load_records

APPS = (
    ("astronomy_shop",) * 2
    + ("social_network",) * 2
    + ("astronomy_shop",) * 3
    + ("social_network", "astronomy_shop", "social_network")
)


def environment(root: Path, app: str) -> FastloopEnvironment:
    return FastloopEnvironment(
        run_dir=root / app,
        workspace=root / app / "workspace",
        cluster=app,
        kubeconfig=root / app / "kubeconfig",
        namespace=app.replace("_", "-"),
        application=app,
        sregym_dir=root / "harness",
        private_tmp=root / app / "tmp",
        seed_commit=("a" if app == "astronomy_shop" else "b") * 40,
    )


@dataclass
class FakeEnvironment:
    environment: FastloopEnvironment
    events: list[tuple[int, str, str]]
    fail_at: int | None = None
    missing_cleanup_at: int | None = None
    memory: list[int] = field(default_factory=list)

    def accepted_commit(self) -> str:
        return f"{self.environment.application}:{self.memory}"

    def run_slot(self, index: int, problem_id: str, run_id: str) -> IncidentRecord:
        self.events.append((index, self.environment.application, self.accepted_commit()))
        self.memory.append(index)
        return IncidentRecord(
            run_id=run_id,
            index=index,
            problem_id=problem_id,
            agent="sdo",
            model="fake",
            injection_started_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            injection_finished_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            recovery_oracle=OracleVerdict(kind="sregym-mitigation-oracle", success=index != self.fail_at),
            full_cleanup_verified=index != self.missing_cleanup_at,
        )


def config(tmp_path: Path) -> DispatchConfig:
    plan = [{"sequence": i, "problem_id": f"mf__{APPS[i % 10]}__n2__s{i}__reliable__v_"} for i in range(50)]
    (tmp_path / "plan.json").write_text(json.dumps(plan))
    (tmp_path / "stream.json").write_text(json.dumps({"incidents": [p["problem_id"] for p in plan[:10]]}))
    environments = {}
    for app in set(APPS):
        env = environment(tmp_path, app)
        env.save()
        environments[app] = {"run_dir": app, "seed_commit": env.seed_commit}
    path = tmp_path / "dispatch.json"
    path.write_text(json.dumps({"plan": "plan.json", "stream": "stream.json", "environments": environments}))
    return load_dispatch_config(path)


def test_order_is_global_and_memory_persists_only_within_each_app(tmp_path: Path) -> None:
    settings = config(tmp_path)
    events: list[tuple[int, str, str]] = []
    environments = {app: FakeEnvironment(env, events) for app, env in settings.environments.items()}
    output = tmp_path / "result"

    assert dispatch(settings, environments, output, run_id="mixed") == 0

    assert [i for i, _, _ in events] == list(range(10))
    assert [app for _, app, _ in events] == [app.replace("_", "-") for app in APPS]
    assert events[4][2] == "astronomy-shop:[0, 1]"
    assert events[7][2] == "social-network:[2, 3]"
    assert environments["astronomy_shop"].memory == [0, 1, 4, 5, 6, 8]
    assert environments["social_network"].memory == [2, 3, 7, 9]
    assert [row.index for row in load_records(output / "incidents.jsonl")] == list(range(10))
    manifest = json.loads((output / "run_manifest.json").read_text())
    assert manifest["status"] == "completed"
    assert manifest["plan_sha256"] == hashlib.sha256((tmp_path / "plan.json").read_bytes()).hexdigest()
    assert len(manifest["plan"]) == 50
    assert manifest["slots"][4]["accepted_commit_before"] == events[4][2]
    assert manifest["environments"]["social_network"]["accepted_commit"] == "social-network:[2, 3, 7, 9]"


@pytest.mark.parametrize("failure", ["fail_at", "missing_cleanup_at"])
def test_cleanup_failure_halts_before_advancing_even_across_apps(tmp_path: Path, failure: str) -> None:
    settings = config(tmp_path)
    events: list[tuple[int, str, str]] = []
    environments = {app: FakeEnvironment(env, events) for app, env in settings.environments.items()}
    setattr(environments["astronomy_shop"], failure, 1)

    assert dispatch(settings, environments, tmp_path / "result", run_id="mixed") == 1

    assert [i for i, _, _ in events] == [0, 1]
    assert environments["social_network"].memory == []
    manifest = json.loads((tmp_path / "result/run_manifest.json").read_text())
    assert manifest["status"] == "failed"
    assert manifest["slots"][-1]["status"] == "failed"


def test_child_failure_is_durable_and_never_advances(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    settings = config(tmp_path)
    events: list[tuple[int, str, str]] = []
    environments = {app: FakeEnvironment(env, events) for app, env in settings.environments.items()}

    def broken(*_args: object) -> IncidentRecord:
        raise RuntimeError("worker cleanup failed")

    monkeypatch.setattr(environments["astronomy_shop"], "run_slot", broken)
    assert dispatch(settings, environments, tmp_path / "result", run_id="mixed") == 1
    manifest = json.loads((tmp_path / "result/run_manifest.json").read_text())
    assert "worker cleanup failed" in manifest["error"]
    assert len(manifest["slots"]) == 1
    assert not events


@pytest.mark.parametrize("mutation", ["reorder", "short_plan", "shared_workspace", "wrong_app", "seed_mismatch"])
def test_invalid_configuration_fails_before_dispatch(tmp_path: Path, mutation: str) -> None:
    config(tmp_path)
    if mutation in {"reorder", "short_plan"}:
        path = tmp_path / "plan.json"
        plan = json.loads(path.read_text())
        if mutation == "reorder":
            plan[0]["problem_id"], plan[2]["problem_id"] = plan[2]["problem_id"], plan[0]["problem_id"]
        else:
            plan.pop()
        path.write_text(json.dumps(plan))
    else:
        env = FastloopEnvironment.load(tmp_path / "social_network")
        if mutation == "shared_workspace":
            env.workspace = tmp_path / "astronomy_shop/workspace"
        elif mutation == "wrong_app":
            env.application = "hotel-reservation"
        else:
            env.seed_commit = "c" * 40
        env.save()
    with pytest.raises(ValueError, match="stream|50-slot|isolated|application|seed"):
        load_dispatch_config(tmp_path / "dispatch.json")


@pytest.mark.parametrize(
    "run_args",
    [("--stream", "x"), ("--continue-on-error",), ("--problem", "wrong"), ("--agent", "codex"), ("--slot-offset", "8")],
)
def test_child_options_cannot_change_dispatch_routing(run_args: tuple[str, ...]) -> None:
    from benchmarks.sregym.fastloop.dispatch import validate_run_args

    with pytest.raises(ValueError, match="cannot override"):
        validate_run_args(run_args)


def test_subprocess_command_uses_own_environment_and_global_slot(tmp_path: Path) -> None:
    from benchmarks.sregym.fastloop.cli import build_parser
    from benchmarks.sregym.fastloop.dispatch import FastloopSlotEnvironment, validate_run_args
    from benchmarks.sregym.fastloop.stream import StreamConfig

    env = environment(tmp_path, "social_network")
    flags = ("--no-checkpoints",)
    validate_run_args(flags)
    child = FastloopSlotEnvironment(env, StreamConfig((("problem",),)), flags)
    args = build_parser().parse_args(child.command(7, "problem", "mixed-slot-007")[3:])
    assert args.run_dir == env.run_dir
    assert (args.problem, args.incidents, args.slot_offset, args.run_id) == (["problem"], 1, 7, "mixed-slot-007")
    assert args.require_full_cleanup
    assert args.no_checkpoints
    assert args.require_fault_tracking is False
    assert args.continue_on_error is False


def test_plan_hash_pin_cannot_be_changed_silently(tmp_path: Path) -> None:
    config(tmp_path)
    path = tmp_path / "dispatch.json"
    data = json.loads(path.read_text())
    data["plan_sha256"] = "0" * 64
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="SHA-256"):
        load_dispatch_config(path)


def test_output_directory_is_never_reused(tmp_path: Path) -> None:
    settings = config(tmp_path)
    environments = {app: FakeEnvironment(env, []) for app, env in settings.environments.items()}
    with pytest.raises(FileExistsError):
        dispatch(settings, environments, tmp_path, run_id="mixed")


def test_failed_child_still_records_its_latest_accepted_commit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    settings = config(tmp_path)
    environments = {app: FakeEnvironment(env, []) for app, env in settings.environments.items()}
    astronomy = environments["astronomy_shop"]

    def broken(*_args: object) -> IncidentRecord:
        astronomy.memory.append(0)
        raise RuntimeError("child failed after accepting memory")

    monkeypatch.setattr(astronomy, "run_slot", broken)
    assert dispatch(settings, environments, tmp_path / "result", run_id="mixed") == 1
    manifest = json.loads((tmp_path / "result/run_manifest.json").read_text())
    assert manifest["environments"]["astronomy_shop"]["accepted_commit"] == "astronomy-shop:[0]"
    assert manifest["slots"][0]["accepted_commit_after"] == "astronomy-shop:[0]"
