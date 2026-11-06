"""The kubectl-backed scenario reads, against a fake ``kubectl`` that serves synthetic JSON."""

from __future__ import annotations

import json
import stat
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.fastloop.toy_live import KubectlScenarioOps
from benchmarks.sregym.fastloop.toy_upstream import UpstreamLayout

if TYPE_CHECKING:
    from pathlib import Path

FAKE = """#!/usr/bin/env python3
import json, sys
args = sys.argv[1:]
ns = args[args.index("-n") + 1]
rest = args[args.index("-n") + 2:]
docs = json.load(open(__import__("os").environ["FAKE_KUBECTL_DOCS"]))
key = ns + " " + " ".join(a for a in rest if a != "-o" and a != "json")
if key.startswith(ns + " exec"):
    print("abc123"); sys.exit(0)
if key not in docs:
    sys.stderr.write("Error from server (NotFound): " + key); sys.exit(1)
print(json.dumps(docs[key]))
"""


def _pod(name: str, phase: str = "Running", restarts: int = 0, **labels: str) -> dict:
    return {
        "metadata": {"name": name, "labels": labels},
        "status": {"phase": phase, "containerStatuses": [{"restartCount": restarts, "imageID": f"sha256:{name}"}]},
    }


@pytest.fixture
def ops(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> KubectlScenarioOps:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    kubectl = bin_dir / "kubectl"
    kubectl.write_text(FAKE)
    kubectl.chmod(kubectl.stat().st_mode | stat.S_IEXEC)
    docs = {
        "toy-sdo get configmap": {
            "items": [
                {"metadata": {"name": "a-request"}, "data": {"agent-task.json": "{}"}},
                {"metadata": {"name": "b-result"}, "data": {"result.json": "{}"}},
            ]
        },
        "toy-sdo get pods -l job-name=sdo-controller-run": {
            "items": [_pod("old", "Failed", 0), _pod("new", restarts=2)]
        },
        "toy-sdo get configmap sdo-controller-state": {
            "data": {"runtime-state.json": json.dumps({"incident_open": True})}
        },
        "toy get deployments": {
            "items": [{"metadata": {"name": "web"}, "spec": {"template": {"spec": {"containers": [{"image": "i:1"}]}}}}]
        },
        "toy get pods": {"items": [_pod("web-1", restarts=1, app="web")]},
    }
    path = tmp_path / "docs.json"
    path.write_text(json.dumps(docs))
    monkeypatch.setenv("PATH", f"{bin_dir}:/usr/bin:/bin")
    monkeypatch.setenv("FAKE_KUBECTL_DOCS", str(path))
    environment = SimpleNamespace(namespace="toy", kubeconfig=tmp_path / "kubeconfig", run_dir=tmp_path)
    return KubectlScenarioOps(environment, UpstreamLayout("toy"))  # type: ignore[arg-type]


def test_snapshot_reads_workloads_tasks_and_controller_pods(ops: KubectlScenarioOps) -> None:
    snapshot = ops.snapshot()
    assert snapshot.images == {"web": "i:1"}
    assert snapshot.digests == {"web": "sha256:web-1"}
    assert snapshot.agent_tasks == 1  # only request ConfigMaps count
    assert snapshot.pod_failures == 1
    assert snapshot.controller_relaunches == 2 + 2  # two controller pods plus two container restarts
    assert snapshot.head == "abc123"


def test_incident_flag_comes_from_the_controller_state(ops: KubectlScenarioOps) -> None:
    assert ops.incident_open() is True


def test_absent_holds_configmap_means_no_holds(ops: KubectlScenarioOps) -> None:
    assert ops.holds() == []
