"""Restore must finish when a client rollout cannot surge (lane 53, 2026-10-26).

The old home-timeline-service pod carried required pod anti-affinity that the
restored template no longer has. The surge pod matched that rule, stayed
Pending, and the old pod never left, so the client refresh deadlocked.

The fake cluster below models only what the deadlock needs: a Deployment whose
old pod holds a hard constraint against any sibling pod, a scheduler that
leaves the surge pod Unschedulable while the old pod exists, and graceful
termination that takes a few polls.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest

from benchmarks.sregym.fastloop import seed_recovery, surge_recovery

NAMESPACE = "social-network"
STUCK = "home-timeline-service"
TERMINATION_POLLS = 3


def _pod(name: str, deployment: str, template_hash: str, *, running: bool, terminating: int = 0) -> dict[str, Any]:
    pod: dict[str, Any] = {
        "kind": "Pod",
        "metadata": {
            "name": name,
            "labels": {"service": deployment, "pod-template-hash": template_hash},
            "creationTimestamp": "2026-10-26T22:00:00Z",
        },
        "spec": {"nodeName": "worker" if running else None},
        "status": {"phase": "Running" if running else "Pending", "conditions": []},
    }
    if not running:
        pod["status"]["conditions"] = [
            {
                "type": "PodScheduled",
                "status": "False",
                "reason": "Unschedulable",
                "message": "0/2 nodes are available: 1 node(s) didn't satisfy existing pods anti-affinity rules.",
            }
        ]
    if terminating:
        pod["metadata"]["deletionTimestamp"] = "2026-10-26T22:01:00Z"
    return pod


@dataclass
class FakeCluster:
    """Deployments by name -> replicas; pods are derived from a tiny scheduler."""

    deployments: dict[str, int]
    hard_constraint: set[str]
    pods: dict[str, dict[str, Any]] = field(default_factory=dict)
    terminating_polls: dict[str, int] = field(default_factory=dict)
    scale_calls: list[tuple[str, int]] = field(default_factory=list)
    kubectl_calls: list[list[str]] = field(default_factory=list)
    generation: int = 0

    def __post_init__(self) -> None:
        for name, replicas in self.deployments.items():
            for index in range(replicas):
                self.pods[f"{name}-old-{index}"] = _pod(f"{name}-old-{index}", name, "old", running=True)

    def rollout_restart(self) -> None:
        self.generation += 1
        for name, replicas in self.deployments.items():
            for index in range(replicas):
                self._create(name, f"{name}-new{self.generation}-{index}")
        self._reconcile()

    def _create(self, deployment: str, name: str) -> None:
        self.pods[name] = _pod(name, deployment, f"new{self.generation}", running=False)

    def _siblings(self, deployment: str) -> list[dict[str, Any]]:
        return [pod for pod in self.pods.values() if pod["metadata"]["labels"]["service"] == deployment]

    def _reconcile(self) -> None:
        for name in self.deployments:
            siblings = self._siblings(name)
            for pod in siblings:
                if pod["status"]["phase"] == "Pending":
                    others = [other for other in siblings if other is not pod]
                    blocked = name in self.hard_constraint and others
                    if not blocked:
                        pod["status"] = {"phase": "Running", "conditions": []}
                        pod["spec"]["nodeName"] = "worker"
            # Old pods are only retired once the surge pod is ready.
            ready_new = [
                pod
                for pod in siblings
                if pod["metadata"]["labels"]["pod-template-hash"] != "old" and pod["status"]["phase"] == "Running"
            ]
            if len(ready_new) >= self.deployments[name]:
                for pod in siblings:
                    if pod["metadata"]["labels"]["pod-template-hash"] == "old":
                        self.pods.pop(pod["metadata"]["name"], None)

    def tick(self) -> None:
        for name in list(self.terminating_polls):
            self.terminating_polls[name] -= 1
            if self.terminating_polls[name] <= 0:
                self.pods.pop(name, None)
                del self.terminating_polls[name]
        self._reconcile()

    def scale(self, deployment: str, replicas: int) -> None:
        self.scale_calls.append((deployment, replicas))
        self.deployments[deployment] = replicas
        siblings = self._siblings(deployment)
        if replicas == 0:
            for pod in siblings:
                pod["metadata"]["deletionTimestamp"] = "2026-10-26T22:01:00Z"
                self.terminating_polls[pod["metadata"]["name"]] = TERMINATION_POLLS
            return
        live = [pod for pod in siblings if "deletionTimestamp" not in pod["metadata"]]
        for index in range(len(live), replicas):
            self.generation += 1
            self._create(deployment, f"{deployment}-scaled{self.generation}-{index}")
        self._reconcile()

    def items(self) -> list[dict[str, Any]]:
        self.tick()
        rows: list[dict[str, Any]] = []
        for name, replicas in self.deployments.items():
            rows.append(
                {
                    "kind": "Deployment",
                    "metadata": {"name": name},
                    "spec": {"replicas": replicas, "selector": {"matchLabels": {"service": name}}},
                }
            )
        rows.extend(self.pods.values())
        return rows

    def settled(self) -> SimpleNamespace:
        self.tick()
        stuck = [
            name
            for name in self.deployments
            if any(
                pod["status"]["phase"] != "Running" or "deletionTimestamp" in pod["metadata"]
                for pod in self._siblings(name)
            )
            or len(self._siblings(name)) != self.deployments[name]
        ]
        reason = "" if not stuck else "not settled: " + ", ".join(stuck)
        return SimpleNamespace(settled=not stuck, waited_s=1.0, reason=reason)

    # kubectl surface used by the code under test
    def kubectl(self, args: list[str], **_: Any) -> SimpleNamespace:
        self.kubectl_calls.append(args)
        if args[:1] == ["get"]:
            return SimpleNamespace(stdout=json.dumps({"items": self.items()}), returncode=0)
        if args[:2] == ["scale", f"deployment/{args[1].split('/')[-1]}"]:
            replicas = int(next(arg for arg in args if arg.startswith("--replicas=")).split("=")[1])
            self.scale(args[1].split("/")[-1], replicas)
            return SimpleNamespace(stdout="", returncode=0)
        raise AssertionError(f"unexpected kubectl call: {args}")


@pytest.fixture
def wired(monkeypatch):
    from benchmarks.sregym import adapter

    clients = seed_recovery.RESTORE_CLIENTS[NAMESPACE]

    def build(hard_constraint: set[str]) -> tuple[FakeCluster, SimpleNamespace]:
        cluster = FakeCluster(dict.fromkeys(clients, 1), hard_constraint)
        runner = SimpleNamespace(wait_for_rollouts=lambda _ns, timeout_s: cluster.settled())
        monkeypatch.setattr(adapter, "kubectl", cluster.kubectl)
        monkeypatch.setattr(
            seed_recovery.importlib, "import_module", lambda _name: SimpleNamespace(KubeClusterIO=lambda: runner)
        )
        monkeypatch.setattr(surge_recovery.time, "sleep", lambda _s: None)
        monkeypatch.setenv("KUBECONFIG", "/fixture/kubeconfig")
        exec_command = Mock(side_effect=lambda _command: cluster.rollout_restart())
        return cluster, SimpleNamespace(namespace=NAMESPACE, kubectl=SimpleNamespace(exec_command=exec_command))

    return build


def test_refresh_finishes_when_old_pod_blocks_the_surge_pod(wired):
    cluster, problem = wired({STUCK})
    result = seed_recovery.refresh_baseline_clients(problem)
    assert result["success"] is True, result
    assert [row["deployment"] for row in result["recreated"]] == [STUCK]
    assert result["recreated"][0]["reason"].startswith("Unschedulable")
    # Bounded Recreate: scale to 0, wait for termination, scale back to the captured replicas.
    assert cluster.scale_calls == [(STUCK, 0), (STUCK, 1)]
    assert len(cluster._siblings(STUCK)) == 1


def test_healthy_rollouts_are_left_alone(wired):
    cluster, problem = wired(set())
    result = seed_recovery.refresh_baseline_clients(problem)
    assert result["success"] is True
    assert result["recreated"] == []
    assert cluster.scale_calls == []


def test_scale_back_happens_even_if_termination_never_finishes(wired, monkeypatch):
    cluster, problem = wired({STUCK})
    monkeypatch.setattr(surge_recovery, "TERMINATION_TIMEOUT_S", 0)
    result = seed_recovery.refresh_baseline_clients(problem)
    assert result["success"] is False
    assert "termination" in result["reason"]
    assert cluster.scale_calls[-1] == (STUCK, 1), "a deployment must never be left scaled to zero"


def test_pending_pod_without_a_live_old_pod_is_not_a_surge():
    new = _pod("web-new-0", "web", "new1", running=False)
    deployment = {
        "kind": "Deployment",
        "metadata": {"name": "web"},
        "spec": {"replicas": 1, "selector": {"matchLabels": {"service": "web"}}},
    }
    assert surge_recovery.find_blocked_surges([deployment, new], ["web"]) == []
    old = _pod("web-old-0", "web", "old", running=True)
    found = surge_recovery.find_blocked_surges([deployment, new, old], ["web"])
    assert [(row.deployment, row.pod) for row in found] == [("web", "web-new-0")]
    old["metadata"]["deletionTimestamp"] = "2026-10-26T22:01:00Z"
    assert surge_recovery.find_blocked_surges([deployment, new, old], ["web"]) == []
