from __future__ import annotations

import json
import subprocess
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.sregym.fastloop.baseline_recovery import ApplicationBaseline
from benchmarks.sregym.fastloop.environment import FastloopEnvironment
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

    def reset_controller(self, seed_commit: str) -> None:
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
