"""Independent seed traffic verification, owned solely by the benchmark adapter."""

from __future__ import annotations

import importlib
import secrets
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
