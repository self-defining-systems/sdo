"""Restore when a captured namespace no longer exists (fault cleanup deleted it).

Fixture shape from lane 53 (h9.3): the baseline captured a fault-infrastructure
namespace (``policy-system``) that held only the API-generated default
ServiceAccount; the admission fault's recovery deleted the namespace, and the
restore then applied into it and failed with ``namespaces not found``.
"""

from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from benchmarks.sregym.fastloop.baseline_recovery import (
    ApplicationBaseline,
    ApplicationNamespaceAbsent,
    KubectlApplicationCluster,
    reconcile_application,
    save_application_baseline,
    seal_existing_install,
)
from benchmarks.sregym.fastloop.fault_driver import SregymFaultDriver


def obj(kind: str, name: str, namespace: str, **fields: Any) -> dict[str, Any]:
    return {"apiVersion": "v1", "kind": kind, "metadata": {"name": name, "namespace": namespace}, **fields}


@dataclass
class ApiServer:
    """Namespaced writes into a missing namespace fail, as on a real API server."""

    namespaces: set[str]
    objects: dict[tuple[str, str, str], dict[str, Any]] = field(default_factory=dict)
    events: list[tuple[str, ...]] = field(default_factory=list)

    def list_objects(self, namespace: str) -> list[dict[str, Any]]:
        if namespace not in self.namespaces:
            raise ApplicationNamespaceAbsent(namespace)
        return [deepcopy(value) for key, value in self.objects.items() if key[0] == namespace]

    def delete(self, namespace: str, kind: str, name: str) -> None:
        self.events.append(("delete", namespace, kind, name))
        self.objects.pop((namespace, kind, name), None)

    def apply(self, namespace: str, value: dict[str, Any]) -> None:
        if namespace not in self.namespaces:
            raise RuntimeError(f'Error from server (NotFound): namespaces "{namespace}" not found')
        self.events.append(("apply", namespace, value["kind"], value["metadata"]["name"]))
        self.objects[(namespace, value["kind"], value["metadata"]["name"])] = deepcopy(value)

    def create_namespace(self, namespace: str) -> None:
        self.events.append(("create_namespace", namespace))
        self.namespaces.add(namespace)


def test_absent_namespace_holding_only_generated_defaults_is_left_absent() -> None:
    cluster = ApiServer({"social-network"})
    baseline = ApplicationBaseline("policy-system", [obj("ServiceAccount", "default", "policy-system")])

    cleanup = reconcile_application(cluster, baseline, "policy-system")

    assert cluster.events == []
    assert cleanup.absent_namespace == "left_absent"
    assert cleanup.restored == []


def test_absent_namespace_with_application_objects_is_recreated_before_any_write() -> None:
    cluster = ApiServer({"social-network"})
    baseline = ApplicationBaseline(
        "media",
        [
            obj("ServiceAccount", "default", "media"),
            obj("ConfigMap", "media-config", "media", data={"k": "v"}),
            obj("Service", "media", "media", spec={"ports": [{"port": 80}]}),
        ],
    )

    cleanup = reconcile_application(cluster, baseline, "media")

    assert cluster.events[0] == ("create_namespace", "media")
    assert {event[2:] for event in cluster.events[1:]} == {
        ("ServiceAccount", "default"),
        ("ConfigMap", "media-config"),
        ("Service", "media"),
    }
    assert cleanup.absent_namespace == "recreated"
    assert all(item.recreated for item in cleanup.reapplied)


def test_fault_driver_recovery_survives_a_fault_deleted_infrastructure_namespace() -> None:
    class Worker:
        def request(self, op: str, **_args: Any) -> dict[str, Any]:
            if op == "recover":
                # The admission fault's recovery deletes its backend namespace.
                cluster.namespaces.discard("policy-system")
                return {
                    "seconds": 1.0,
                    "protocol_version": 2,
                    "recovery_contract": "seed-compatible/v1",
                    "success": True,
                }
            if op == "refresh":
                return {"protocol_version": 2, "recovery_contract": "seed-compatible/v1", "success": True}
            if op == "health":
                return {"healthy": True, "unready": []}
            raise AssertionError(op)

    cluster = ApiServer({"social-network", "policy-system"})
    app = obj("ConfigMap", "nginx", "social-network", data={"a": "b"})
    cluster.objects[("social-network", "ConfigMap", "nginx")] = deepcopy(app)
    driver = SregymFaultDriver(
        Worker(),
        namespace="social-network",
        baseline=(
            ApplicationBaseline("policy-system", [obj("ServiceAccount", "default", "policy-system")]),
            ApplicationBaseline("social-network", [app]),
        ),
        cluster=cluster,
    )

    driver.recover()

    assert driver.last_full_cleanup_verified
    assert "policy-system" not in cluster.namespaces


def test_kubectl_listing_of_a_missing_namespace_raises_typed_absence(monkeypatch: Any, tmp_path: Path) -> None:
    def api(args: list[str], **_kwargs: Any) -> Any:
        if args[:2] == ["get", "namespace"]:
            return SimpleNamespace(stdout="")
        return SimpleNamespace(stdout=json.dumps({"items": []}))

    monkeypatch.setattr("benchmarks.sregym.fastloop.baseline_recovery.kubectl", api)

    with pytest.raises(ApplicationNamespaceAbsent) as raised:
        KubectlApplicationCluster(tmp_path / "kubeconfig").list_objects("policy-system")
    assert raised.value.namespace == "policy-system"
    assert "policy-system" in str(raised.value)


def test_kubectl_listing_of_an_empty_existing_namespace_returns_no_objects(monkeypatch: Any, tmp_path: Path) -> None:
    def api(args: list[str], **_kwargs: Any) -> Any:
        if args[:2] == ["get", "namespace"]:
            return SimpleNamespace(stdout="namespace/hotel\n")
        return SimpleNamespace(stdout=json.dumps({"items": []}))

    monkeypatch.setattr("benchmarks.sregym.fastloop.baseline_recovery.kubectl", api)

    assert KubectlApplicationCluster(tmp_path / "kubeconfig").list_objects("hotel") == []


def test_seal_accepts_a_captured_defaults_only_namespace_that_is_gone(tmp_path: Path) -> None:
    app = obj("ConfigMap", "nginx", "social-network", data={"a": "b"})
    cluster = ApiServer({"social-network"})
    cluster.objects[("social-network", "ConfigMap", "nginx")] = deepcopy(app)
    path = tmp_path / "cluster_baseline_state.json"
    path.write_text(json.dumps({"namespaces": []}), encoding="utf-8")
    save_application_baseline(
        path,
        (
            ApplicationBaseline("policy-system", [obj("ServiceAccount", "default", "policy-system")]),
            ApplicationBaseline("social-network", [app]),
        ),
        installed=True,
    )

    seal_existing_install(path, cluster, "social-network")
