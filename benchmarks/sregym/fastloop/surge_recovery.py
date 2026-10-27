"""Recovery-only Recreate restore for rollouts that cannot surge.

A restored Deployment rolls out by creating a new pod before retiring the old
one. When the old pod holds a hard constraint the new pod cannot satisfy
(required pod anti-affinity, a host port, an RWO volume, a node selector), the
new pod stays Unschedulable and the old pod never leaves: the rollout
deadlocks. Recovery then scales that one workload to zero, waits for its pods
to terminate, and scales it back. It is an eval action; production SDO never
calls it.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

from benchmarks.sregym.adapter import WaitKind, wait_for

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence
    from pathlib import Path

logger = logging.getLogger(__name__)

#: Upper bound on one workload's graceful termination while it is scaled to zero.
TERMINATION_TIMEOUT_S = wait_for(WaitKind.SLOW_COMMAND)
POLL_INTERVAL_S = wait_for(WaitKind.POLL_FAST)
UNSCHEDULABLE = "Unschedulable"
#: How long a surge pod may stay Unschedulable behind a bound old pod before a restart path recreates it.
GUARD_GRACE_S = wait_for(WaitKind.REFRESH_SETTLE)
GUARD_INTERVAL_S = wait_for(WaitKind.POLL_FAST) * 5


class SurgeRecoveryError(RuntimeError):
    """A blocked workload did not terminate in time; it has been scaled back up."""


@dataclass(frozen=True)
class BlockedSurge:
    deployment: str
    pod: str
    replicas: int
    reason: str

    def as_receipt(self) -> dict[str, Any]:
        return {"deployment": self.deployment, "pod": self.pod, "replicas": self.replicas, "reason": self.reason}


class RolloutCluster(Protocol):
    def snapshot(self) -> list[dict[str, Any]]: ...

    def scale(self, deployment: str, replicas: int) -> None: ...


def _terminating(pod: dict[str, Any]) -> bool:
    return bool(pod["metadata"].get("deletionTimestamp"))


def _selects(deployment: dict[str, Any], pod: dict[str, Any]) -> bool:
    labels = pod["metadata"].get("labels", {})
    selector = deployment["spec"].get("selector", {}).get("matchLabels", {})
    return bool(selector) and all(labels.get(key) == value for key, value in selector.items())


def _unschedulable_reason(pod: dict[str, Any]) -> str | None:
    if pod.get("status", {}).get("phase") != "Pending":
        return None
    for condition in pod["status"].get("conditions") or []:
        if (
            condition.get("type") == "PodScheduled"
            and condition.get("status") == "False"
            and condition.get("reason") == UNSCHEDULABLE
        ):
            message = condition.get("message") or ""
            return f"{UNSCHEDULABLE}: {message}".rstrip(": ")
    return None


def find_blocked_surges(items: list[dict[str, Any]], deployments: list[str] | tuple[str, ...]) -> list[BlockedSurge]:
    """Deployments whose new-template pod is Unschedulable while an old-template pod is still bound."""

    pods = [item for item in items if item["kind"] == "Pod" and not _terminating(item)]
    found: list[BlockedSurge] = []
    for deployment in (item for item in items if item["kind"] == "Deployment"):
        name = deployment["metadata"]["name"]
        if name not in deployments:
            continue
        siblings = [pod for pod in pods if _selects(deployment, pod)]
        for pod in siblings:
            reason = _unschedulable_reason(pod)
            if reason is None:
                continue
            generation = pod["metadata"].get("labels", {}).get("pod-template-hash")
            holder = any(
                other["spec"].get("nodeName")
                and other["metadata"].get("labels", {}).get("pod-template-hash") != generation
                for other in siblings
            )
            if holder:
                found.append(BlockedSurge(name, pod["metadata"]["name"], deployment["spec"].get("replicas", 1), reason))
                break
    return found


def recreate_blocked_surges(
    cluster: RolloutCluster, deployments: list[str] | tuple[str, ...], *, namespace: str
) -> list[BlockedSurge]:
    """Scale each blocked workload to 0, wait for its pods to go, then scale back to its replicas."""

    blocked = find_blocked_surges(cluster.snapshot(), deployments)
    for surge in blocked:
        logger.warning(
            "eval restore recreate_blocked_surge: %s/Deployment/%s pod %s (%s)",
            namespace,
            surge.deployment,
            surge.pod,
            surge.reason,
        )
        cluster.scale(surge.deployment, 0)
        try:
            _wait_until_gone(cluster, surge)
        finally:
            cluster.scale(surge.deployment, surge.replicas)
    return blocked


def _wait_until_gone(cluster: RolloutCluster, surge: BlockedSurge) -> None:
    deadline = time.monotonic() + TERMINATION_TIMEOUT_S
    while True:
        items = cluster.snapshot()
        deployment = next(
            (item for item in items if item["kind"] == "Deployment" and item["metadata"]["name"] == surge.deployment),
            None,
        )
        remaining = [
            pod["metadata"]["name"]
            for pod in items
            if pod["kind"] == "Pod" and deployment and _selects(deployment, pod)
        ]
        if not remaining:
            return
        if time.monotonic() >= deadline:
            raise SurgeRecoveryError(
                f"eval: deployment/{surge.deployment} pods did not finish termination within "
                f"{TERMINATION_TIMEOUT_S:g}s: {remaining}; it was scaled back to {surge.replicas}"
            )
        time.sleep(POLL_INTERVAL_S)


@dataclass(frozen=True)
class KubectlRolloutCluster:
    namespace: str
    kubeconfig: Path

    def snapshot(self) -> list[dict[str, Any]]:
        from benchmarks.sregym.adapter import kubectl

        return json.loads(
            kubectl(
                ["get", "deployments,pods", "-o", "json"], namespace=self.namespace, kubeconfig=self.kubeconfig
            ).stdout
        )["items"]

    def scale(self, deployment: str, replicas: int) -> None:
        from benchmarks.sregym.adapter import kubectl

        kubectl(
            ["scale", f"deployment/{deployment}", f"--replicas={replicas}"],
            namespace=self.namespace,
            kubeconfig=self.kubeconfig,
        )


@dataclass
class SurgeWatch:
    """Observes one namespace during a harness operation that restarts workloads.

    Any path that restarts a Deployment (a fault's recovery, its client refresh,
    a fault-driver rollout) can deadlock on a surge pod the 1-worker cluster
    cannot schedule. ``poll`` recreates a workload only after its surge pod has
    stayed blocked for ``grace_s``, so a normal rollout is never touched.
    """

    cluster: RolloutCluster
    namespace: str
    grace_s: float | None = None
    clock: Callable[[], float] = time.monotonic
    first_seen: dict[tuple[str, str], float] = field(default_factory=dict)
    recreated: list[BlockedSurge] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def poll(self) -> list[BlockedSurge]:
        items = self.cluster.snapshot()
        names = [item["metadata"]["name"] for item in items if item["kind"] == "Deployment"]
        now = self.clock()
        grace_s = GUARD_GRACE_S if self.grace_s is None else self.grace_s
        blocked = {(surge.deployment, surge.pod) for surge in find_blocked_surges(items, names)}
        self.first_seen = {key: self.first_seen.get(key, now) for key in blocked}
        due = sorted({deployment for (deployment, pod), seen in self.first_seen.items() if now - seen >= grace_s})
        if not due:
            return []
        try:
            done = recreate_blocked_surges(self.cluster, due, namespace=self.namespace)
        except SurgeRecoveryError as error:
            self.errors.append(str(error))
            logger.error("%s", error)
            return []
        self.first_seen = {}
        self.recreated.extend(done)
        return done


@contextmanager
def guard_blocked_surges(watches: Sequence[SurgeWatch], *, interval_s: float | None = None) -> Iterator[None]:
    """Poll each watch on a background thread while the wrapped operation runs."""

    stop = threading.Event()

    def run() -> None:
        while not stop.wait(GUARD_INTERVAL_S if interval_s is None else interval_s):
            for watch in watches:
                try:
                    watch.poll()
                except Exception as error:  # the guarded operation, not the guard, owns failure
                    watch.errors.append(f"{type(error).__name__}: {error}")
                    logger.warning("eval surge guard poll failed in %s: %s", watch.namespace, error)

    thread = threading.Thread(target=run, name="surge-guard", daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join()
