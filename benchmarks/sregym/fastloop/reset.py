"""Restore one warm fastloop lane to its captured application and seed repository."""

from __future__ import annotations

import json
import logging
import time
from typing import TYPE_CHECKING, Any, Protocol

from benchmarks.sregym.adapter import PersistentState, kubectl
from benchmarks.sregym.fastloop.baseline_recovery import (
    ApplicationCluster,
    KubectlApplicationCluster,
    accepted_restore_baselines,
    load_application_baseline,
    reconcile_application,
)

if TYPE_CHECKING:
    from pathlib import Path

    from benchmarks.sregym.fastloop.environment import FastloopEnvironment


logger = logging.getLogger(__name__)


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
        running = [pod for pod in pods if pod.get("status", {}).get("phase") not in {"Succeeded", "Failed"}]
        job = kubectl(
            ["get", "job", "sdo-controller-run", "-o", "json"],
            namespace=namespace,
            kubeconfig=self.kubeconfig,
            check=False,
        )
        if job.returncode == 0 and len(running) > 1:
            raise RuntimeError(f"reset found multiple running controller Pods in {namespace}")
        cleanup_spec = None
        if job.returncode == 0:
            cleanup_spec = json.loads(job.stdout)["spec"]["template"]["spec"]
        elif running:
            cleanup_spec = running[0]["spec"]
        # Stop the controller too: maintenance still finishes in-flight work,
        # including reflection. Foreground deletion awaits all Job-owned Pods.
        kubectl(
            [
                "delete",
                "jobs",
                "--all",
                "--ignore-not-found=true",
                "--cascade=foreground",
                "--wait=true",
                "--timeout=120s",
            ],
            namespace=namespace,
            kubeconfig=self.kubeconfig,
        )
        # A controller Pod can outlive its Job. Await those observed writers
        # too before deleting the state that their next write could recreate.
        for pod in running:
            kubectl(
                ["delete", "pod", pod["metadata"]["name"], "--ignore-not-found=true", "--wait=true", "--timeout=120s"],
                namespace=namespace,
                kubeconfig=self.kubeconfig,
            )
        # Retire only transient controller state after all writers stop. The
        # next incident must not resume an expired request/proposal checkout.
        kubectl(
            [
                "delete",
                "configmap",
                "sdo-controller-state",
                "sdo-controller-state-drift-verdicts",
                "--ignore-not-found=true",
                "--wait=true",
            ],
            namespace=namespace,
            kubeconfig=self.kubeconfig,
        )
        # Without a trusted controller template there is no safe image/mount
        # for filesystem cleanup. State retirement still applies to this path.
        if cleanup_spec is None:
            logger.info("Reset retired controller state without a controller template; filesystem scratch retained")
            return
        controller = next(container for container in cleanup_spec["containers"] if container["name"] == "controller")
        mount = next(mount for mount in controller["volumeMounts"] if mount["mountPath"] == "/workspace")
        volume = next(volume for volume in cleanup_spec["volumes"] if volume["name"] == mount["name"])
        # Native ledgers are durable broker history, unlike sibling proposal
        # checkouts. Keep them across warm resets; remove only transient scratch.
        cleanup = "rm -rf /workspace/.sdo-runtime/sessions && "
        if preserve_repository:
            cleanup += (
                "if [ -d /workspace/worktrees ]; then "
                "find /workspace/worktrees -mindepth 1 -maxdepth 1 ! -name native-ledger "
                "-exec rm -rf -- {} +; fi"
            )
        else:
            cleanup = (
                'git -C /workspace/application reset --hard "$1" && '
                "git -C /workspace/application clean -fd && " + cleanup + "rm -rf /workspace/worktrees"
            )
        pod = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "sdo-reset-cleanup", "namespace": namespace},
            "spec": {
                "restartPolicy": "Never",
                "automountServiceAccountToken": False,
                "imagePullSecrets": cleanup_spec.get("imagePullSecrets", []),
                "securityContext": cleanup_spec.get("securityContext", {}),
                "containers": [
                    {
                        "name": "cleanup",
                        "image": controller["image"],
                        "command": ["sh", "-c", cleanup, "reset", seed_commit],
                        "volumeMounts": [mount],
                        "securityContext": controller.get("securityContext", {}),
                    }
                ],
                "volumes": [volume],
            },
        }
        kubectl(
            ["delete", "pod", "sdo-reset-cleanup", "--ignore-not-found=true", "--wait=true", "--timeout=120s"],
            namespace=namespace,
            kubeconfig=self.kubeconfig,
        )
        kubectl(["apply", "-f", "-"], namespace=namespace, kubeconfig=self.kubeconfig, input_text=json.dumps(pod))
        try:
            kubectl(
                ["wait", "--for=jsonpath={.status.phase}=Succeeded", "pod/sdo-reset-cleanup", "--timeout=120s"],
                namespace=namespace,
                kubeconfig=self.kubeconfig,
            )
        finally:
            kubectl(
                ["delete", "pod", "sdo-reset-cleanup", "--ignore-not-found=true", "--wait=true", "--timeout=120s"],
                namespace=namespace,
                kubeconfig=self.kubeconfig,
            )


def reset_environment(
    environment: FastloopEnvironment, cluster: ResetCluster, worker: HealthWorker, *, preserve_repository: bool = False
) -> None:
    if not environment.seed_commit:
        raise ValueError("reset requires a seed commit captured by fastloop up")
    cluster.reset_controller(environment.seed_commit, preserve_repository=preserve_repository)
    restore_application(environment, cluster, worker)
    if preserve_repository:
        state = PersistentState.load(environment.state_path)
        record = state.controllers.get(environment.namespace)
        if record is not None:
            record.controller_pod_uid = ""
            record.controller_pod_name = ""
            record.pending = None
            state.save(environment.state_path)
    else:
        environment.state_path.unlink(missing_ok=True)


def restore_application(environment: FastloopEnvironment, cluster: ApplicationCluster, worker: HealthWorker) -> None:
    """Restore only the app, from one captured installed manifest set, once.

    The controller, accepted memory and incident clocks are untouched. This is
    also the one allowed stabilization retry before any fault is injected.
    """
    baselines = load_application_baseline(environment.baseline_path, environment.namespace)
    if baselines is None:
        raise ValueError("reset requires an application baseline captured by fastloop up")
    if isinstance(cluster, KubectlApplicationCluster):
        from benchmarks.sregym.fastloop.clean_start import restore_admission_baseline

        restore_admission_baseline(environment.kubeconfig, environment.baseline_path, environment.workspace)
    worker.request("clean_start", problem_id=environment.deploy_problem, timeout=200)
    baselines = accepted_restore_baselines(environment.workspace, baselines)
    for baseline in baselines:
        reconcile_application(cluster, baseline, baseline.namespace)
    deadline = time.monotonic() + 60
    if isinstance(cluster, KubectlApplicationCluster):
        from benchmarks.sregym.fastloop.baseline_recovery import retire_stalled_replicas

        retire_stalled_replicas(environment.kubeconfig, environment.namespace)
    while True:
        health = worker.request("health", namespace=environment.namespace, timeout=60)
        if health.get("healthy") or time.monotonic() >= deadline:
            break
        time.sleep(min(1, max(0, deadline - time.monotonic())))
    if not health.get("healthy"):
        raise RuntimeError(f"reset left {environment.namespace} unhealthy: {health.get('unready')}")
    from benchmarks.sregym.fastloop.admission import workload_identity

    identity = None
    if isinstance(cluster, KubectlApplicationCluster):
        from benchmarks.sregym.fastloop.admission import workload_identity

        identity = workload_identity(environment.kubeconfig, environment.namespace)
    oracle = worker.request("baseline_oracle", problem_id=environment.deploy_problem, verify_traffic=True, timeout=200)
    if not oracle.get("success"):
        refreshed = worker.request("refresh_baseline_clients", problem_id=environment.deploy_problem, timeout=120)
        if refreshed.get("success") is not True:
            raise RuntimeError(
                f"reset client refresh failed for {environment.namespace}: {refreshed}; traffic: {oracle}"
            )
        oracle = worker.request(
            "baseline_oracle", problem_id=environment.deploy_problem, verify_traffic=True, timeout=200
        )
        if not oracle.get("success"):
            raise RuntimeError(f"reset left independent traffic failing for {environment.deploy_problem}: {oracle}")

    if identity is not None:
        from benchmarks.sregym.adapter.eval_guards import assert_stable_workloads

        assert_stable_workloads(identity, workload_identity(environment.kubeconfig, environment.namespace))
