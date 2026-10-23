from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from benchmarks.sregym.fastloop.baseline_recovery import (
    KubectlApplicationCluster,
    application_namespaces,
    capture_application_baseline,
    load_application_baseline,
    reconcile_application,
    save_application_baseline,
)


def test_recovery_recreates_missing_objects_with_sdo_field_ownership(monkeypatch: Any, tmp_path: Path) -> None:
    calls: list[list[str]] = []

    def record(args: list[str], **_kwargs: Any) -> Any:
        calls.append(args)
        return SimpleNamespace(stdout="")

    monkeypatch.setattr("benchmarks.sregym.fastloop.baseline_recovery.kubectl", record)
    KubectlApplicationCluster(tmp_path / "kubeconfig").apply("hotel", obj("Service", "reservation", spec={"ports": []}))
    assert calls == [
        ["get", "service/reservation", "-o", "json", "--ignore-not-found=true"],
        ["apply", "--server-side", "--field-manager=sdo-deployer", "--force-conflicts", "-f", "-"],
    ]


@pytest.mark.parametrize("literal_baseline", [True, False])
def test_restore_replaces_environment_value_source_after_another_manager_changed_it(
    monkeypatch: Any, tmp_path: Path, literal_baseline: bool
) -> None:
    literal = {"name": "DATABASE_URL", "value": "synthetic-database"}
    reference = {"name": "DATABASE_URL", "valueFrom": {"secretKeyRef": {"name": "synthetic-db", "key": "url"}}}
    expected, injected = (literal, reference) if literal_baseline else (reference, literal)
    baseline = obj(
        "Deployment", "catalog", spec={"template": {"spec": {"containers": [{"name": "catalog", "env": [expected]}]}}}
    )
    live = deepcopy(baseline)
    live["metadata"]["resourceVersion"] = "17"
    live["spec"]["template"]["spec"]["containers"][0]["env"] = [injected]

    def api(args: list[str], **_kwargs: Any) -> Any:
        if args[0] == "get":
            return SimpleNamespace(stdout=json.dumps(live))
        if args[0] == "apply":
            # SSA preserves fields owned by the injector, creating an invalid EnvVar union.
            raise RuntimeError("valueFrom may not be specified when value is not empty")
        assert args[:3] == ["patch", "deployment/catalog", "--type=json"]
        for operation in json.loads(Path(args[4]).read_text(encoding="utf-8")):
            if operation["op"] == "test":
                assert operation["path"] == "/metadata/resourceVersion"
                assert operation["value"] == live["metadata"]["resourceVersion"]
            else:
                assert operation["path"] == "/spec"
                live["spec"] = operation["value"]
        return SimpleNamespace(stdout="patched")

    monkeypatch.setattr("benchmarks.sregym.fastloop.baseline_recovery.kubectl", api)

    KubectlApplicationCluster(tmp_path / "kubeconfig").apply("hotel", baseline)

    assert live["spec"] == baseline["spec"]


def obj(kind: str, name: str, namespace: str = "hotel", **fields: Any) -> dict[str, Any]:
    return {"apiVersion": "apps/v1", "kind": kind, "metadata": {"name": name, "namespace": namespace}, **fields}


@dataclass
class FakeCluster:
    objects: dict[tuple[str, str, str], dict[str, Any]] = field(default_factory=dict)
    touched: list[tuple[str, str, str]] = field(default_factory=list)

    def list_objects(self, namespace: str) -> list[dict[str, Any]]:
        return [deepcopy(value) for key, value in self.objects.items() if key[0] == namespace]

    def delete(self, namespace: str, kind: str, name: str) -> None:
        self.touched.append((namespace, kind, name))
        self.objects.pop((namespace, kind, name))

    def apply(self, namespace: str, value: dict[str, Any]) -> None:
        kind, name = value["kind"], value["metadata"]["name"]
        self.touched.append((namespace, kind, name))
        self.objects[(namespace, kind, name)] = deepcopy(value)

    def add(self, value: dict[str, Any]) -> None:
        self.objects[(value["metadata"]["namespace"], value["kind"], value["metadata"]["name"])] = value


def test_extra_job_is_deleted_and_counted() -> None:
    cluster = FakeCluster()
    cluster.add(obj("Deployment", "mongo", spec={"replicas": 1}))
    baseline = capture_application_baseline(cluster, "hotel")
    cluster.add(obj("Job", "grant-rate-fix", spec={"template": {}}))

    cleanup = reconcile_application(cluster, baseline, "hotel")

    assert ("hotel", "Job", "grant-rate-fix") not in cluster.objects
    assert cleanup.deleted_counts == {"Job": 1}


def test_modified_deployment_is_restored_and_named() -> None:
    cluster = FakeCluster()
    cluster.add(obj("Deployment", "mongodb-geo", spec={"replicas": 1}))
    baseline = capture_application_baseline(cluster, "hotel")
    cluster.objects[("hotel", "Deployment", "mongodb-geo")]["spec"]["replicas"] = 2

    cleanup = reconcile_application(cluster, baseline, "hotel")

    assert cluster.objects[("hotel", "Deployment", "mongodb-geo")]["spec"] == {"replicas": 1}
    assert cleanup.restored == ["Deployment/mongodb-geo"]


def test_deleted_application_daemonset_is_restored() -> None:
    cluster = FakeCluster()
    original = obj("DaemonSet", "collector", spec={"selector": {"matchLabels": {"app": "collector"}}})
    cluster.add(original)
    baseline = capture_application_baseline(cluster, "hotel")
    cluster.objects.clear()

    cleanup = reconcile_application(cluster, baseline, "hotel")

    assert cluster.objects[("hotel", "DaemonSet", "collector")] == original
    assert cleanup.restored == ["DaemonSet/collector"]


def test_control_namespace_objects_are_untouched() -> None:
    cluster = FakeCluster()
    baseline = capture_application_baseline(cluster, "hotel")
    cluster.add(obj("Job", "sdo-controller-run", "hotel-sdo", spec={}))

    cleanup = reconcile_application(cluster, baseline, "hotel")

    assert cluster.touched == []
    assert cleanup.deleted_counts == {}
    assert ("hotel-sdo", "Job", "sdo-controller-run") in cluster.objects


def test_harness_workload_is_untouched() -> None:
    cluster = FakeCluster()
    baseline = capture_application_baseline(cluster, "hotel")
    cluster.add(obj("Job", "wrk2-job", spec={"template": {}}))
    cluster.add(obj("ConfigMap", "wrk2-payload-script", data={"payload": "script"}))
    pod = obj("Pod", "wrk2-job-abc", spec={})
    pod["metadata"]["ownerReferences"] = [{"kind": "Job", "name": "wrk2-job"}]
    cluster.add(pod)

    cleanup = reconcile_application(cluster, baseline, "hotel")

    assert cluster.touched == []
    assert cleanup.deleted_counts == {}
    assert len(cluster.objects) == 3


def test_controller_pods_are_recreated_by_their_controller_not_the_snapshot() -> None:
    cluster = FakeCluster()
    pod = obj("Pod", "mongo-old", spec={})
    pod["metadata"]["ownerReferences"] = [{"kind": "ReplicaSet", "name": "mongo-rs"}]
    cluster.add(pod)
    baseline = capture_application_baseline(cluster, "hotel")
    cluster.objects.clear()
    replacement = obj("Pod", "mongo-new", spec={})
    replacement["metadata"]["ownerReferences"] = [{"kind": "ReplicaSet", "name": "mongo-rs"}]
    cluster.add(replacement)

    cleanup = reconcile_application(cluster, baseline, "hotel")

    assert cleanup.restored == []
    assert cluster.touched == []


def test_baseline_file_preserves_infrastructure_and_older_files_skip_reconciliation(tmp_path: Path) -> None:
    path = tmp_path / "cluster_baseline_state.json"
    path.write_text('{"namespaces": ["observe"]}', encoding="utf-8")
    assert load_application_baseline(path, "hotel") is None

    save_application_baseline(path, (capture_application_baseline(FakeCluster(), "hotel"),))

    assert load_application_baseline(path, "hotel") is not None
    assert load_application_baseline(path, "other") is None
    assert json.loads(path.read_text(encoding="utf-8"))["namespaces"] == ["observe"]


def test_capture_discovers_secondary_app_namespaces_without_control_or_observability(
    tmp_path: Path, monkeypatch: Any
) -> None:
    from benchmarks.sregym.fastloop import baseline_recovery

    path = tmp_path / "cluster_baseline_state.json"
    path.write_text('{"namespaces": ["default", "observe"]}', encoding="utf-8")
    names = ["default", "observe", "hotel", "hotel-db", "hotel-sdo"]

    def fake_kubectl(*_args: Any, **_kwargs: Any) -> Any:
        return type("Result", (), {"stdout": json.dumps({"items": [{"metadata": {"name": name}} for name in names]})})()

    monkeypatch.setattr(baseline_recovery, "kubectl", fake_kubectl)

    assert application_namespaces(path, tmp_path / "kubeconfig", "hotel") == ("hotel", "hotel-db")
