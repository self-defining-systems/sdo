"""Lease-fenced retirement of exactly one application's namespaces on a warm lane."""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

from benchmarks.sregym.adapter import control_namespace_for, kubectl

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

APPLICATION_NAMESPACES = frozenset({"hotel-reservation", "social-network", "astronomy-shop"})


def quiesce_native_controller(kubeconfig: Path, namespace: str) -> None:
    """Use the native pause/closure fence before replacing an idle installation."""
    from benchmarks.sregym.adapter import KubectlClusterOps, closure_pending, pause_for_recovery

    if namespace not in {control_namespace_for(app) for app in APPLICATION_NAMESPACES}:
        raise ValueError("eval: quiesce requires an application control namespace")
    ops = KubectlClusterOps()
    if closure_pending(ops.runtime_state(namespace)):
        raise RuntimeError("eval: app retirement refuses unfinished native closure or reflection")
    jobs = json.loads(kubectl(["get", "jobs", "-o", "json"], namespace=namespace, kubeconfig=kubeconfig).stdout)[
        "items"
    ]
    if any(row.get("status", {}).get("active") and row["metadata"]["name"] != "sdo-controller-run" for row in jobs):
        raise RuntimeError("eval: app retirement refuses active native workers; finish their closure first")
    if any(row["metadata"]["name"] == "sdo-controller-run" and row.get("status", {}).get("active") for row in jobs):
        pause_for_recovery(ops, namespace)


@dataclass(frozen=True)
class RetirementRequest:
    cluster: str
    namespace: str
    next_namespace: str
    lease: str
    coord: str = "/mnt/data/shli/coord/coord"

    def __post_init__(self) -> None:
        if self.namespace not in APPLICATION_NAMESPACES or self.next_namespace not in APPLICATION_NAMESPACES:
            raise ValueError("retirement requires a known application namespace")
        if self.namespace == self.next_namespace:
            raise ValueError("retirement requires a different next application namespace")
        if not self.cluster or not self.lease:
            raise ValueError("retirement requires cluster and lease identities")

    @property
    def namespaces(self) -> frozenset[str]:
        return frozenset({self.namespace, control_namespace_for(self.namespace)})


class RetirementOps(Protocol):
    def check_lease(self) -> None: ...
    def namespace_names(self) -> set[str]: ...
    def quiesce(self, namespace: str) -> None: ...
    def delete_namespace(self, namespace: str) -> None: ...
    def wait_absent(self, namespaces: frozenset[str]) -> None: ...
    def verify_next(self, namespace: str) -> dict[str, Any]: ...


def retire_application(
    request: RetirementRequest, ops: RetirementOps, *, deploy_next: Callable[[], None]
) -> dict[str, Any]:
    """Retire, await deletion, attach/deploy the next app, then verify the lane.

    The next deployment must use attach-only cluster reuse. No cluster creation
    or deletion capability is part of this interface. Retrying after a partial
    retirement deletes only still-present members of the same fixed app set.
    """
    ops.check_lease()
    existing = ops.namespace_names()
    control = control_namespace_for(request.namespace)
    if control in existing:
        ops.check_lease()
        ops.quiesce(control)
    deleted = []
    for namespace in sorted(request.namespaces & existing):
        ops.check_lease()
        ops.delete_namespace(namespace)
        deleted.append(namespace)
    ops.wait_absent(request.namespaces)
    ops.check_lease()
    deploy_next()
    ops.check_lease()
    resources = ops.verify_next(request.next_namespace)
    if resources.get("passed") is not True:
        raise ValueError("eval: next application lane_resources did not pass")
    return {
        "schema_version": "sdo.fastloop-app-retirement/v1",
        "cluster": request.cluster,
        "lease": request.lease,
        "retired_app": request.namespace,
        "next_app": request.next_namespace,
        "retired_namespaces": sorted(request.namespaces),
        "deleted_namespaces": deleted,
        "cluster_recreated": False,
        "lane_resources": resources,
    }


@dataclass(frozen=True)
class KubectlRetirementOps:
    request: RetirementRequest
    kubeconfig: Path

    def check_lease(self) -> None:
        subprocess.run([self.request.coord, "check", self.request.cluster, "--lease", self.request.lease], check=True)
        self.assert_target_cluster()

    def assert_target_cluster(self) -> None:
        expected = set(
            subprocess.check_output(
                ["kind", "get", "nodes", "--name", self.request.cluster],
                text=True,
                timeout=10,
            ).split()
        )
        observed = set(
            kubectl(
                ["get", "nodes", "-o", "jsonpath={.items[*].metadata.name}"], namespace=None, kubeconfig=self.kubeconfig
            ).stdout.split()
        )
        if not expected or observed != expected:
            raise ValueError("eval: app retirement kubeconfig does not target the leased kind cluster")

    def namespace_names(self) -> set[str]:
        result = kubectl(["get", "namespaces", "-o", "json"], namespace=None, kubeconfig=self.kubeconfig)
        return {row["metadata"]["name"] for row in json.loads(result.stdout)["items"]}

    def quiesce(self, namespace: str) -> None:
        quiesce_native_controller(self.kubeconfig, namespace)

    def delete_namespace(self, namespace: str) -> None:
        if namespace not in self.request.namespaces:
            raise ValueError("eval: refusing to retire a namespace outside the application set")
        self.check_lease()
        kubectl(
            ["delete", "namespace", namespace, "--ignore-not-found=true", "--wait=true", "--timeout=180s"],
            namespace=None,
            kubeconfig=self.kubeconfig,
        )

    def wait_absent(self, namespaces: frozenset[str]) -> None:
        if remaining := self.namespace_names() & namespaces:
            raise RuntimeError(f"eval: retired application namespaces remain: {sorted(remaining)}")

    def verify_next(self, namespace: str) -> dict[str, Any]:
        from benchmarks.sregym.adapter import assert_lane_resources

        return assert_lane_resources(self.kubeconfig, app_namespaces={namespace})
