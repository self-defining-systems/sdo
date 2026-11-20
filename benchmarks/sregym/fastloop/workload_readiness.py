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
    from collections.abc import Callable
    from pathlib import Path


class WorkloadNotReadyError(ValueError):
    """The installed workload has not finished rolling out or becoming Ready."""


def _container_trouble(pod: dict[str, Any]) -> str:
    """Why a pod's containers are not running cleanly: restarts, the waiting reason and the last exit."""
    notes = []
    for status in pod.get("status", {}).get("containerStatuses", []):
        restarts = status.get("restartCount", 0)
        waiting = status.get("state", {}).get("waiting", {}).get("reason")
        last = status.get("lastState", {}).get("terminated")
        if not restarts and not waiting and not last:
            continue
        parts = [f"{restarts} restarts"]
        if waiting:
            parts.append(waiting)
        if last:
            parts.append(f"last exit {last.get('exitCode')} ({last.get('reason')})")
        notes.append(f"container {status.get('name')}: {', '.join(parts)}")
    return f" [{'; '.join(notes)}]" if notes else ""


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
                + _container_trouble(pod)
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
    """Wait for rollout and pod readiness before taking the restore identity."""
    return await_identity(lambda: workload_identity(kubeconfig, namespace))


def await_identity(read: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    """Retry ``read`` while workloads are still rolling out or becoming Ready.

    Only a not-yet-Ready workload is retryable. Malformed inventory, zero
    replicas and an empty application remain immediate restore failures.
    """
    budget = Deadline.after(wait_for(WaitKind.ROLLOUT), "restore workloads", time.monotonic)
    while True:
        try:
            identity = read()
            budget.remaining()
            return identity
        except WorkloadNotReadyError as exc:
            time.sleep(min(wait_for(WaitKind.SAMPLE), budget.left()))
            if budget.left() <= 0:
                raise WorkloadNotReadyError(f"{exc}; restore readiness deadline exhausted; no injection") from exc


def pod_health_problems(pods: list[dict[str, Any]], restarts: dict[tuple[str, str], int]) -> list[str]:
    """One sample of pod-level health: phase, every container Ready, no waiting reason, no restart growth.

    ``restarts`` holds the first count seen per (pod uid, container) and is updated in place; a later
    sample above it is a restart during the window, which a Ready instant between backoffs would hide.
    """
    problems = []
    for pod in pods:
        metadata, status = pod["metadata"], pod.get("status", {})
        if metadata.get("deletionTimestamp") or status.get("phase") == "Succeeded":
            continue
        name = metadata.get("name", metadata.get("uid"))
        if status.get("phase") != "Running":
            problems.append(f"pod {name} is {status.get('phase')}")
            continue
        for container in status.get("containerStatuses", []):
            label = f"pod {name} container {container.get('name')}"
            first = restarts.setdefault((metadata["uid"], container["name"]), container.get("restartCount", 0))
            if container.get("restartCount", 0) > first:
                problems.append(f"{label} restarted ({first} -> {container['restartCount']})")
            waiting = container.get("state", {}).get("waiting", {}).get("reason")
            if waiting:
                problems.append(f"{label} is waiting: {waiting}")
            elif container.get("ready") is not True:
                problems.append(f"{label} is not Ready")
    return problems


def sustained_pod_health(
    kubeconfig: Path,
    namespace: str,
    *,
    window_seconds: float | None = None,
    poll_seconds: float | None = None,
    read_pods: Callable[[], list[dict[str, Any]]] | None = None,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> list[str]:
    """Sample pods across at least ``window_seconds`` (default one minute); return what failed, empty when clean.

    A crash-looping container is Ready for short windows between backoffs, so one sample proves nothing:
    every sample must show every container Ready, and no restart count may grow over the window.
    """
    window = wait_for(WaitKind.MINUTE) if window_seconds is None else window_seconds
    poll = wait_for(WaitKind.POLL) if poll_seconds is None else poll_seconds

    def read() -> list[dict[str, Any]]:
        listing = kubectl(["get", "pods", "-o", "json"], namespace=namespace, kubeconfig=kubeconfig)
        return json.loads(listing.stdout)["items"]

    read_pods = read_pods or read
    restarts: dict[tuple[str, str], int] = {}
    end = clock() + window
    while True:
        problems = pod_health_problems(read_pods(), restarts)
        if problems:
            return problems
        if clock() >= end:
            return []
        sleep(poll)
