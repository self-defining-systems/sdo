"""Independent seed traffic verification, owned solely by the benchmark adapter."""

from __future__ import annotations

import importlib
import json
import os
import secrets
import shlex
from pathlib import Path
from typing import Any


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


def refresh_baseline_clients(problem: Any) -> dict[str, Any]:
    """Re-register hotel clients after a restored Consul loses its in-memory catalog.

    This is a benchmark restoration action, never an agent repair or health verdict.
    The caller must independently re-check HTTP after the bounded rollout.
    """
    if problem.app.name != "Hotel Reservation":
        return {"success": False, "reason": "application has no baseline client refresh contract"}
    clients = ("frontend", "geo", "profile", "rate", "recommendation", "reservation", "search", "user")
    resources = " ".join(f"deployment/{name}" for name in clients)
    problem.kubectl.exec_command(f"kubectl rollout restart {resources} -n {shlex.quote(problem.namespace)}")
    clusters = importlib.import_module("sregym.conductor.problems.colocated_composite")
    settled = clusters.KubeClusterIO().wait_for_rollouts(problem.namespace, timeout_s=90)
    return {
        "success": settled.settled,
        "scope": "application-clients",
        "deployments": list(clients),
        "waited_s": settled.waited_s,
        "reason": settled.reason,
    }
