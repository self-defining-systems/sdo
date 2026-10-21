"""Restore one warm fastloop lane to its captured application and seed repository."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Protocol

from benchmarks.sregym.adapter import kubectl
from benchmarks.sregym.fastloop.baseline_recovery import (
    KubectlApplicationCluster,
    load_application_baseline,
    reconcile_application,
)

if TYPE_CHECKING:
    from pathlib import Path

    from benchmarks.sregym.fastloop.environment import FastloopEnvironment


class ResetCluster(Protocol):
    def list_objects(self, namespace: str) -> list[dict[str, Any]]: ...

    def delete(self, namespace: str, kind: str, name: str) -> None: ...

    def apply(self, namespace: str, value: dict[str, Any]) -> None: ...

    def reset_controller(self, seed_commit: str, *, preserve_repository: bool = False) -> None: ...


class HealthWorker(Protocol):
    def request(self, op: str, **kwargs: Any) -> dict[str, Any]: ...


class KubectlResetCluster(KubectlApplicationCluster):
    def __init__(self, kubeconfig: Path, control_namespace: str) -> None:
        super().__init__(kubeconfig)
        self.control_namespace = control_namespace

    def reset_controller(self, seed_commit: str, *, preserve_repository: bool = False) -> None:
        namespace = self.control_namespace
        control_namespace = kubectl(
            ["get", "namespace", namespace],
            namespace=None,
            kubeconfig=self.kubeconfig,
            check=False,
        )
        if control_namespace.returncode:
            if "(NotFound)" in control_namespace.stderr:
                return
            raise RuntimeError(f"could not inspect controller namespace {namespace}: {control_namespace.stderr}")
        pods = json.loads(
            kubectl(
                ["get", "pods", "-l", "job-name=sdo-controller-run", "-o", "json"],
                namespace=namespace,
                kubeconfig=self.kubeconfig,
            ).stdout
        )["items"]
        running = [pod for pod in pods if pod.get("status", {}).get("phase") == "Running"]
        job = kubectl(
            ["get", "job", "sdo-controller-run", "-o", "json"],
            namespace=namespace,
            kubeconfig=self.kubeconfig,
            check=False,
        )
        if job.returncode == 0 and len(running) != 1:
            raise RuntimeError(f"reset needs one running controller Pod in {namespace}, found {len(running)}")
        if running:
            kubectl(
                ["apply", "-f", "-"],
                namespace=namespace,
                kubeconfig=self.kubeconfig,
                input_text='{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"sdo-controller-maintenance"},"data":{"state":"paused"}}',
            )
            cleanup = "rm -rf /workspace/.sdo-runtime/sessions /workspace/worktrees"
            if not preserve_repository:
                cleanup = (
                    'git -C /workspace/application reset --hard "$1" && '
                    "git -C /workspace/application clean -fd && " + cleanup
                )
            kubectl(
                [
                    "exec",
                    running[0]["metadata"]["name"],
                    "-c",
                    "controller",
                    "--",
                    "sh",
                    "-c",
                    cleanup,
                    "reset",
                    seed_commit,
                ],
                namespace=namespace,
                kubeconfig=self.kubeconfig,
            )
        kubectl(
            ["delete", "jobs", "--all", "--ignore-not-found=true", "--wait=true"],
            namespace=namespace,
            kubeconfig=self.kubeconfig,
        )
        kubectl(
            ["delete", "configmap", "sdo-controller-state", "--ignore-not-found=true"],
            namespace=namespace,
            kubeconfig=self.kubeconfig,
        )


def reset_environment(
    environment: FastloopEnvironment, cluster: ResetCluster, worker: HealthWorker, *, preserve_repository: bool = False
) -> None:
    if not environment.seed_commit:
        raise ValueError("reset requires a seed commit captured by fastloop up")
    baselines = load_application_baseline(environment.baseline_path, environment.namespace)
    if baselines is None:
        raise ValueError("reset requires an application baseline captured by fastloop up")
    cluster.reset_controller(environment.seed_commit, preserve_repository=preserve_repository)
    for baseline in baselines:
        reconcile_application(cluster, baseline, baseline.namespace)
    environment.state_path.unlink(missing_ok=True)
    health = worker.request("health", namespace=environment.namespace, timeout=60)
    if not health.get("healthy"):
        raise RuntimeError(f"reset left {environment.namespace} unhealthy: {health.get('unready')}")
    oracle = worker.request("baseline_oracle", problem_id=environment.deploy_problem, timeout=60)
    if not oracle.get("success"):
        raise RuntimeError(f"reset left the fault oracle failing for {environment.deploy_problem}: {oracle}")
