"""Independent seed traffic verification, owned solely by the benchmark adapter."""

from __future__ import annotations

import importlib
import json
import os
import secrets
import shlex
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from benchmarks.sregym.fastloop.surge_recovery import (
    BlockedSurge,
    KubectlRolloutCluster,
    SurgeRecoveryError,
    recreate_blocked_surges,
)

#: Time for a plain client rollout to settle before checking for a rollout that cannot surge.
REFRESH_SETTLE_S = 45
#: Time for the Recreate-restored workloads to settle.
REFRESH_RETRY_S = 60


def verify_baseline_traffic(problem: Any, baseline: dict[str, Any]) -> dict[str, Any]:
    """Require seed health and fresh real HTTP, without regrading injected faults."""
    if baseline.get("success") is not True:
        return baseline
    grading = importlib.import_module("sregym.conductor.problems.composition.grading")
    clusters = importlib.import_module("sregym.conductor.problems.colocated_composite")
    entry = grading.ENTRY_POINTS.get(problem.app.name)
    details: dict[str, Any] = {"scope": "seed-baseline", "baseline_oracle": baseline}
    if entry is None:
        return {
            "kind": "health-check",
            "success": False,
            "details": {**details, "traffic_error": "application has no independent traffic entry points"},
        }
    manifest = grading.probe_pod_manifest(
        f"seed-recovery-probe-{secrets.token_hex(4)}",
        problem.namespace,
        entry,
    )
    from benchmarks.sregym.adapter import kubectl
    from benchmarks.sregym.fastloop.clean_start import HARNESS_NAMESPACE, isolate_benchmark_probe

    kubectl(
        ["apply", "-f", "-"],
        namespace=None,
        kubeconfig=Path(os.environ["KUBECONFIG"]),
        input_text=json.dumps({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": HARNESS_NAMESPACE}}),
    )

    manifest = isolate_benchmark_probe(manifest)
    phase, logs = clusters.KubeClusterIO().run_pod(manifest, timeout_s=grading.probe_deadline_s(entry) + 30)
    result = grading.parse_probe_log(logs)
    details.update(
        traffic_phase=phase, traffic_ok=result.ok, traffic_complete=result.complete, traffic_detail=result.detail()
    )
    return {
        "kind": "health-check",
        "success": phase == "Succeeded" and result.ok and result.complete,
        "details": details,
    }


@dataclass(frozen=True)
class StartupDependency:
    registry: str
    clients: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.registry or not self.clients or self.registry in self.clients:
            raise ValueError("startup dependency requires distinct registry and registering clients")


# Source-declared stateless services. Storage workloads keep their data and
# lifecycle; only processes holding connections/registrations are refreshed.
RESTORE_CLIENTS = {
    "hotel-reservation": ("frontend", "geo", "profile", "rate", "recommendation", "reservation", "search", "user"),
    "astronomy-shop": (
        "ad",
        "cart",
        "checkout",
        "currency",
        "email",
        "fraud-detection",
        "frontend",
        "frontend-proxy",
        "image-provider",
        "llm",
        "payment",
        "product-catalog",
        "product-reviews",
        "quote",
        "recommendation",
        "shipping",
    ),
    "social-network": (
        "compose-post-service",
        "home-timeline-service",
        "media-frontend",
        "media-service",
        "nginx-thrift",
        "post-storage-service",
        "social-graph-service",
        "text-service",
        "unique-id-service",
        "url-shorten-service",
        "user-mention-service",
        "user-service",
        "user-timeline-service",
    ),
}
STARTUP_DEPENDENCIES = {
    "hotel-reservation": (StartupDependency("consul", RESTORE_CLIENTS["hotel-reservation"]),),
    "astronomy-shop": (),
    "social-network": (),
}


def require_dependency_order(dependency: StartupDependency, pod_times: dict[str, list[str]]) -> None:
    registry = pod_times.get(dependency.registry) or []
    if not registry or any(not pod_times.get(client) for client in dependency.clients):
        raise ValueError("restore dependency evidence missing")
    if any(min(pod_times[client]) <= max(registry) for client in dependency.clients):
        raise ValueError(f"restore clients must register after dependency {dependency.registry}")


def refresh_baseline_clients(problem: Any, *, dependencies_only: bool = False) -> dict[str, Any]:
    """Complete restoration after dependencies settle; never an agent repair.

    Every supported app declares its source-owned client set. Unknown topology
    fails rather than guessing storage safety or suppressing traffic evidence.
    """
    from benchmarks.sregym.adapter import kubectl

    clients = RESTORE_CLIENTS.get(problem.namespace)
    if clients is None:
        return {"success": False, "reason": "application has no baseline client refresh contract"}
    config = Path(os.environ["KUBECONFIG"])
    listing = json.loads(
        kubectl(["get", "deployments,pods", "-o", "json"], namespace=problem.namespace, kubeconfig=config).stdout
    )["items"]
    deployed = {row["metadata"]["name"] for row in listing if row["kind"] == "Deployment"}
    if not set(clients).issubset(deployed):
        raise ValueError(
            f"restore client profile differs from installed source topology: {sorted(set(clients) - deployed)}"
        )
    if dependencies_only and not STARTUP_DEPENDENCIES[problem.namespace]:
        return {"success": True, "scope": "application-dependencies", "deployments": [], "waited_s": 0}
    resources = " ".join(f"deployment/{name}" for name in clients)
    problem.kubectl.exec_command(f"kubectl rollout restart {resources} -n {shlex.quote(problem.namespace)}")
    clusters = importlib.import_module("sregym.conductor.problems.colocated_composite")
    rollouts = clusters.KubeClusterIO()
    settled = rollouts.wait_for_rollouts(problem.namespace, timeout_s=REFRESH_SETTLE_S)
    recreated: list[BlockedSurge] = []
    if not settled.settled:
        # A rollout that cannot surge never settles on its own; restore those workloads with a bounded Recreate.
        try:
            recreated = recreate_blocked_surges(
                KubectlRolloutCluster(problem.namespace, config), clients, namespace=problem.namespace
            )
        except SurgeRecoveryError as error:
            return _refresh_result(clients, settled.waited_s, str(error), recreated=[], settled=False)
        if recreated:
            settled = rollouts.wait_for_rollouts(problem.namespace, timeout_s=REFRESH_RETRY_S)
    if settled.settled:
        listing = json.loads(
            kubectl(["get", "deployments,pods", "-o", "json"], namespace=problem.namespace, kubeconfig=config).stdout
        )["items"]
        pod_times = {}
        for deployment in (row for row in listing if row["kind"] == "Deployment"):
            labels = deployment["spec"]["selector"]["matchLabels"]
            pod_times[deployment["metadata"]["name"]] = [
                row["metadata"]["creationTimestamp"]
                for row in listing
                if row["kind"] == "Pod"
                and not row["metadata"].get("deletionTimestamp")
                and all(row["metadata"].get("labels", {}).get(key) == value for key, value in labels.items())
            ]
        for dependency in STARTUP_DEPENDENCIES[problem.namespace]:
            require_dependency_order(dependency, pod_times)
    return _refresh_result(clients, settled.waited_s, settled.reason, recreated=recreated, settled=settled.settled)


def _refresh_result(
    clients: tuple[str, ...], waited_s: float, reason: str, *, recreated: list[BlockedSurge], settled: bool
) -> dict[str, Any]:
    return {
        "success": settled,
        "scope": "application-clients",
        "deployments": list(clients),
        "waited_s": waited_s,
        "reason": reason,
        "recreated": [surge.as_receipt() for surge in recreated],
    }
