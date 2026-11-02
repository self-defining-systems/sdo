"""Every worker op that restarts workloads must survive an unschedulable surge (lane 50, 2026-10-27).

RST3 fixed ``refresh_baseline_clients``. The worker's own ``recover`` and
``refresh`` ops restart workloads too (a fault's recover_fault, its
refresh_after_recovery) and waited 300 s on the same surge pod that could not
schedule next to its old pod on the 1-worker cluster. The bridge now runs those
ops under one guard.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from benchmarks.sregym.fastloop import surge_recovery
from benchmarks.sregym.fastloop.worker_bridge import SURGE_GUARDED_OPS, handle_with_surge_guard

NAMESPACE = "social-network"
STUCK = "social-graph-service"


@dataclass
class OneWorkerCluster:
    """One Deployment whose restarted pod cannot schedule while the old pod exists."""

    lock: threading.Lock = field(default_factory=threading.Lock)
    old_pod: bool = True
    new_pods: int = 0
    replicas: int = 1
    scale_calls: list[int] = field(default_factory=list)

    def restart(self) -> None:
        with self.lock:
            self.new_pods = 1

    def settled(self) -> bool:
        with self.lock:
            return self.new_pods == 1 and not self.old_pod and self.replicas == 1

    def snapshot(self) -> list[dict[str, Any]]:
        with self.lock:
            rows: list[dict[str, Any]] = [
                {
                    "kind": "Deployment",
                    "metadata": {"name": STUCK},
                    "spec": {"replicas": self.replicas, "selector": {"matchLabels": {"service": STUCK}}},
                }
            ]
            if self.old_pod:
                rows.append(
                    {
                        "kind": "Pod",
                        "metadata": {"name": "old", "labels": {"service": STUCK, "pod-template-hash": "a"}},
                        "spec": {"nodeName": "worker"},
                        "status": {"phase": "Running"},
                    }
                )
            if self.new_pods:
                running = not self.old_pod
                rows.append(
                    {
                        "kind": "Pod",
                        "metadata": {"name": "new", "labels": {"service": STUCK, "pod-template-hash": "b"}},
                        "spec": {"nodeName": "worker" if running else None},
                        "status": {
                            "phase": "Running" if running else "Pending",
                            "conditions": []
                            if running
                            else [{"type": "PodScheduled", "status": "False", "reason": "Unschedulable"}],
                        },
                    }
                )
            return rows

    def scale(self, deployment: str, replicas: int) -> None:
        assert deployment == STUCK
        with self.lock:
            self.scale_calls.append(replicas)
            self.replicas = replicas
            if replicas == 0:
                self.old_pod = False
                self.new_pods = 0
            else:
                self.new_pods = 1


@pytest.fixture(autouse=True)
def fast_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(surge_recovery, "GUARD_GRACE_S", 0.0)
    monkeypatch.setattr(surge_recovery, "GUARD_INTERVAL_S", 0.01)
    monkeypatch.setattr(surge_recovery, "POLL_INTERVAL_S", 0.0)


def worker_op(cluster: OneWorkerCluster, *, patience_s: float = 5.0) -> Any:
    """A worker op that restarts the workload and, like ``rollout status``, waits for it to settle."""

    def handle(_worker: Any, request: dict[str, Any]) -> dict[str, Any]:
        cluster.restart()
        deadline = time.monotonic() + patience_s
        while not cluster.settled():
            if time.monotonic() > deadline:
                return {"success": False, "errors": [f"{request['op']}: rollout never settled"]}
            time.sleep(0.005)
        return {"success": True, "pending": [], "errors": []}

    return handle


@pytest.mark.parametrize("op", sorted(SURGE_GUARDED_OPS))
def test_restarting_worker_op_finishes_when_the_surge_pod_cannot_schedule(op: str) -> None:
    cluster = OneWorkerCluster()

    reply = handle_with_surge_guard(
        worker_op(cluster),
        object(),
        {"op": op},
        namespaces=[NAMESPACE],
        kubeconfig=Path("/fixture/kubeconfig"),
        cluster_factory=lambda _ns, _config: cluster,
    )

    assert reply["success"] is True, reply
    # Bounded Recreate: scale to zero, then back to the captured replicas.
    assert cluster.scale_calls == [0, 1]


def test_ops_that_do_not_restart_workloads_run_unguarded() -> None:
    cluster = OneWorkerCluster()
    calls: list[str] = []

    def handle(_worker: Any, request: dict[str, Any]) -> dict[str, Any]:
        calls.append(request["op"])
        return {"success": True}

    handle_with_surge_guard(
        handle,
        object(),
        {"op": "health"},
        namespaces=[NAMESPACE],
        kubeconfig=Path("/fixture/kubeconfig"),
        cluster_factory=lambda _ns, _config: pytest.fail("guard must not observe the cluster"),
    )

    assert calls == ["health"]
    assert cluster.scale_calls == []


def test_a_rollout_that_can_surge_is_never_touched() -> None:
    cluster = OneWorkerCluster()

    def handle(_worker: Any, _request: dict[str, Any]) -> dict[str, Any]:
        # A healthy rollout: the new pod schedules at once and the old pod retires.
        with cluster.lock:
            cluster.new_pods, cluster.old_pod = 1, False
        time.sleep(0.1)
        return {"success": True}

    handle_with_surge_guard(
        handle,
        object(),
        {"op": "recover"},
        namespaces=[NAMESPACE],
        kubeconfig=Path("/fixture/kubeconfig"),
        cluster_factory=lambda _ns, _config: cluster,
    )

    assert cluster.scale_calls == []


def test_grace_period_spares_a_surge_pod_that_is_only_briefly_pending(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(surge_recovery, "GUARD_GRACE_S", 60.0)
    cluster = OneWorkerCluster()
    watch = surge_recovery.SurgeWatch(cluster, NAMESPACE, clock=iter([0.0, 1.0, 2.0]).__next__)
    cluster.restart()

    assert watch.poll() == []
    assert watch.poll() == []
    assert cluster.scale_calls == []
