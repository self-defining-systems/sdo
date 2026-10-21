from __future__ import annotations

import json
import subprocess
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.sregym.fastloop import cli, reset
from benchmarks.sregym.fastloop.baseline_recovery import ApplicationBaseline
from benchmarks.sregym.fastloop.environment import FastloopEnvironment
from benchmarks.sregym.fastloop.loop import AgentOutcome, InjectionWindow, LoopConfig, run_incidents
from benchmarks.sregym.fastloop.records import OracleVerdict
from benchmarks.sregym.fastloop.reset import KubectlResetCluster, reset_environment

if TYPE_CHECKING:
    from pathlib import Path


class FakeCluster:
    def __init__(self) -> None:
        self.objects = [
            {"kind": "Deployment", "metadata": {"name": "app", "namespace": "hotel"}, "spec": {"replicas": 0}}
        ]
        self.accepted_commit = "changed"
        self.sessions = ["old-session"]
        self.state = {"incident_open": True}
        self.jobs = ["sdo-responder-old"]

    def list_objects(self, namespace: str) -> list[dict[str, Any]]:
        assert namespace == "hotel"
        return self.objects

    def apply(self, namespace: str, value: dict[str, Any]) -> None:
        assert namespace == "hotel"
        self.objects = [value]

    def delete(self, namespace: str, kind: str, name: str) -> None:
        self.objects = [item for item in self.objects if item["metadata"]["name"] != name]

    def reset_controller(self, seed_commit: str, *, preserve_repository: bool = False) -> None:
        if not preserve_repository:
            self.accepted_commit = seed_commit
        self.sessions.clear()
        self.state.clear()
        self.jobs.clear()


class FakeWorker:
    calls: list[str]

    def __init__(self) -> None:
        self.calls = []

    def request(self, op: str, **_kwargs: object) -> dict[str, object]:
        self.calls.append(op)
        return {"healthy": True} if op == "health" else {"success": True}


def test_reset_restores_application_and_clears_controller_on_existing_environment(tmp_path: Path) -> None:
    environment = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="kind-test",
        kubeconfig=tmp_path / "kubeconfig",
        namespace="hotel",
        application="hotel",
        workspace=tmp_path / "workspace",
        sregym_dir=tmp_path / "sregym",
        private_tmp=None,
        seed_commit="a" * 40,
    )
    environment.save()
    baseline = ApplicationBaseline(
        "hotel", [{"kind": "Deployment", "metadata": {"name": "app", "namespace": "hotel"}, "spec": {"replicas": 1}}]
    )
    environment.baseline_path.write_text(json.dumps({"fastloop_application": {"namespaces": [baseline.__dict__]}}))
    cluster = FakeCluster()

    worker = FakeWorker()
    reset_environment(environment, cluster, worker)

    assert cluster.objects == baseline.objects
    assert cluster.accepted_commit == environment.seed_commit
    assert cluster.sessions == cluster.jobs == []
    assert cluster.state == {}
    assert worker.calls == ["health", "baseline_oracle"]


def test_reset_requires_a_captured_baseline_and_seed_commit(tmp_path: Path) -> None:
    environment = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="kind-test",
        kubeconfig=tmp_path / "kubeconfig",
        namespace="hotel",
        application="hotel",
        workspace=tmp_path / "workspace",
        sregym_dir=tmp_path / "sregym",
        private_tmp=None,
    )
    cluster = FakeCluster()

    with pytest.raises(ValueError, match="seed commit"):
        reset_environment(environment, cluster, FakeWorker())
    assert cluster.accepted_commit == "changed"


def test_reset_accepts_up_cluster_before_controller_install(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []

    def fake_kubectl(args: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(args)
        return subprocess.CompletedProcess(args, 1, "", 'Error from server (NotFound): namespaces "sdo" not found')

    monkeypatch.setattr("benchmarks.sregym.fastloop.reset.kubectl", fake_kubectl)
    KubectlResetCluster(tmp_path / "kubeconfig", "sdo").reset_controller("a" * 40)

    assert calls == [["get", "namespace", "sdo"]]


def test_fail_soft_controller_reset_preserves_accepted_repository(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[list[str]] = []

    def fake_kubectl(args: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(args)
        if args[:2] == ["get", "pods"]:
            return subprocess.CompletedProcess(
                args, 0, '{"items":[{"metadata":{"name":"controller"},"status":{"phase":"Running"}}]}', ""
            )
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr("benchmarks.sregym.fastloop.reset.kubectl", fake_kubectl)
    KubectlResetCluster(tmp_path / "kubeconfig", "sdo").reset_controller("a" * 40, preserve_repository=True)

    command = next(args for args in calls if args[:1] == ["exec"])
    assert "rm -rf /workspace/.sdo-runtime/sessions /workspace/worktrees" in command
    assert all("git -C /workspace/application" not in arg for arg in command)


def test_failed_learning_keeps_program_for_third_incident(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from datetime import datetime, timezone

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=workspace, check=True)
    (workspace / "README.md").write_text("seed\n")
    subprocess.run(["git", "add", "README.md"], cwd=workspace, check=True)
    commit = ["git", "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm"]
    subprocess.run([*commit, "seed"], cwd=workspace, check=True)
    seed_commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=workspace, text=True).strip()
    program = workspace / ".sdo/playbooks/service-targetport-mismatch/diagnose.go"
    environment = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="kind-test",
        kubeconfig=tmp_path / "kubeconfig",
        namespace="hotel",
        application="hotel",
        workspace=workspace,
        sregym_dir=tmp_path / "sregym",
        private_tmp=None,
        seed_commit=seed_commit,
    )
    baseline = ApplicationBaseline(
        "hotel",
        [{"kind": "Deployment", "metadata": {"name": "app", "namespace": "hotel"}, "spec": {"replicas": 1}}],
    )
    environment.baseline_path.write_text(json.dumps({"fastloop_application": {"namespaces": [baseline.__dict__]}}))
    cluster = FakeCluster()
    monkeypatch.setattr(reset, "KubectlResetCluster", lambda *_args: cluster)
    now = datetime(2026, 10, 21, tzinfo=timezone.utc)
    accepted_commit = ""

    class Agent:
        name = "sdo"
        model = "test"

        def resolve(self, index: int, _problem: str, inject: Any) -> AgentOutcome:
            window = inject()
            if index == 2:
                assert program.is_file()
                assert (
                    subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=workspace, text=True).strip()
                    == accepted_commit
                )
                assert cluster.accepted_commit == accepted_commit
            return AgentOutcome(injection=window, incident_id=f"incident-{index}")

        def learn(self, outcome: AgentOutcome) -> AgentOutcome:
            nonlocal accepted_commit
            if outcome.incident_id == "incident-0":
                program.parent.mkdir(parents=True)
                program.write_text("package targetport\n")
                subprocess.run(["git", "add", ".sdo"], cwd=workspace, check=True)
                subprocess.run([*commit, "accepted program"], cwd=workspace, check=True)
                accepted_commit = subprocess.check_output(
                    ["git", "rev-parse", "HEAD"], cwd=workspace, text=True
                ).strip()
                cluster.accepted_commit = accepted_commit
            if outcome.incident_id == "incident-1":
                raise RuntimeError("learning failed")
            return outcome

        def close(self) -> None:
            pass

    class Driver:
        def inject(self, _problem: str) -> InjectionWindow:
            return InjectionWindow(started_at=now, finished_at=now)

        def oracle(self) -> OracleVerdict:
            return OracleVerdict(kind="sregym-mitigation-oracle", success=True)

        def recover(self) -> float:
            return 0.0

    config = LoopConfig(
        run_id="test",
        problems=("fault",),
        incidents=3,
        results_path=tmp_path / "incidents.jsonl",
        continue_on_error=True,
    )
    records = run_incidents(
        Agent(),
        Driver(),
        config,
        on_failed_incident=lambda: cli._reset_failed_incident(environment, FakeWorker()),
    )
    assert len(records) == 3
    assert "learning failed" in (records[1].error or "")
    assert records[2].error is None
    assert cluster.accepted_commit == accepted_commit
