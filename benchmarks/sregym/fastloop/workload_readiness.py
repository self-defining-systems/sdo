"""Workload rollout/readiness identity, importable inside SREGym's own worker venv.

The worker bridge runs without SDO's dependencies, so this module may only use the
lazy adapter facade (kubectl, budgets) and the standard library.
"""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.adapter import Deadline, WaitKind, kubectl, wait_for

if TYPE_CHECKING:
    from pathlib import Path


class WorkloadNotReadyError(ValueError):
    """The installed workload has not finished rolling out or becoming Ready."""


def workload_identity(kubeconfig: Path, namespace: str) -> dict[str, Any]:
    listing = json.loads(
        kubectl(
            ["get", "deployments,statefulsets,daemonsets,pods", "-o", "json"],
            namespace=namespace,
            kubeconfig=kubeconfig,
        ).stdout
    )["items"]
    workloads = {}
    pods = [row for row in listing if row["kind"] == "Pod" and not row["metadata"].get("deletionTimestamp")]
    for row in listing:
        if row["kind"] == "Pod":
            continue
        metadata, spec = row["metadata"], row["spec"]
        selector = spec.get("selector", {}).get("matchLabels", {})
        selected = [
            pod
            for pod in pods
            if selector and all(pod["metadata"].get("labels", {}).get(key) == val for key, val in selector.items())
        ]
        status = row.get("status", {})
        desired = spec.get("replicas", 1)
        if row["kind"] == "DaemonSet":
            desired = status.get("desiredNumberScheduled", 0)
            updated, current, ready = (
                status.get("updatedNumberScheduled"),
                status.get("currentNumberScheduled"),
                status.get("numberReady", 0),
            )
        else:
            updated, current, ready = (
                status.get("updatedReplicas"),
                status.get("replicas"),
                status.get("readyReplicas", 0),
            )
        observed, generation = status.get("observedGeneration", -1), metadata.get("generation", 0)
        label = f"{row['kind']}/{metadata['name']}"
        if desired == 0:
            raise ValueError(f"restore invariant: installed workload scaled to zero: {row['kind']}/{metadata['name']}")
        # A rollout is a template change still in progress; an unchanged
        # template whose pods lost readiness (crash, eviction, responder
        # restart) is a different cause and is named as such.
        if (
            observed < generation
            or updated != desired
            or current != desired
            or (row["kind"] == "StatefulSet" and status.get("currentRevision") != status.get("updateRevision"))
        ):
            raise WorkloadNotReadyError(
                f"restore invariant: rollout not complete: {label} "
                f"(generation {generation} observed {observed}; updated {updated}/{desired}; current {current})"
            )
        if ready != desired or len(selected) != desired:
            pods = ", ".join(
                f"{pod['metadata'].get('name', pod['metadata'].get('uid'))} {pod.get('status', {}).get('phase')}"
                for pod in selected
            )
            raise WorkloadNotReadyError(
                f"restore invariant: workload not Ready: {label} (ready {ready}/{desired}; pods: {pods})"
            )
        if not selected or any(
            not any(
                condition.get("type") == "Ready" and condition.get("status") == "True"
                for condition in pod.get("status", {}).get("conditions", [])
            )
            for pod in selected
        ):
            raise WorkloadNotReadyError(f"restore invariant: workload not Ready: {row['kind']}/{metadata['name']}")
        # A roll is a pod replaced or restarted. The object's generation is no roll signal: SDO's own
        # acceptance apply bumps it on an unchanged workload (a recreated Deployment's first adoption).
        workloads[f"{row['kind']}/{metadata['name']}"] = {
            "pods": sorted(
                (
                    pod["metadata"]["uid"],
                    tuple(
                        status.get("restartCount", 0) for status in pod.get("status", {}).get("containerStatuses", [])
                    ),
                )
                for pod in selected
            ),
        }
    if not workloads:
        raise ValueError("restore invariant: no installed application workloads")
    return workloads


def await_workload_identity(kubeconfig: Path, namespace: str) -> dict[str, Any]:
    """Wait for rollout and pod readiness before taking the restore identity.

    Only a not-yet-Ready workload is retryable. Malformed inventory, zero
    replicas and an empty application remain immediate restore failures.
    """
    budget = Deadline.after(wait_for(WaitKind.ROLLOUT), "restore workloads", time.monotonic)
    while True:
        try:
            identity = workload_identity(kubeconfig, namespace)
            budget.remaining()
            return identity
        except WorkloadNotReadyError as exc:
            time.sleep(min(wait_for(WaitKind.SAMPLE), budget.left()))
            if budget.left() <= 0:
                raise WorkloadNotReadyError(f"{exc}; restore readiness deadline exhausted; no injection") from exc
