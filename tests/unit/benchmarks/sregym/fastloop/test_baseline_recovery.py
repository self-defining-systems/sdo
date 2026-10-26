from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from benchmarks.sregym.fastloop.baseline_recovery import (
    ApplicationBaseline,
    KubectlApplicationCluster,
    accepted_restore_baselines,
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


@pytest.mark.parametrize("legacy_binding", [False, True])
def test_restore_bound_claim_preserves_new_volume_and_restores_requested_storage(
    monkeypatch: Any, tmp_path: Path, legacy_binding: bool
) -> None:
    cluster = FakeCluster()
    original = obj(
        "PersistentVolumeClaim",
        "geo-pvc",
        spec={
            "accessModes": ["ReadWriteOnce"],
            "storageClassName": "synthetic-storage",
            "volumeName": "old-volume",
            "resources": {"requests": {"storage": "2Gi"}},
        },
        status={"phase": "Bound"},
    )
    cluster.add(original)
    baseline = capture_application_baseline(cluster, "hotel")
    if legacy_binding:
        # Existing saved baselines also need to work without recapturing the app.
        baseline.objects[0]["spec"]["volumeName"] = "old-volume"
    else:
        assert "volumeName" not in baseline.objects[0]["spec"]
    live = deepcopy(original)
    live["metadata"]["resourceVersion"] = "17"
    live["spec"]["volumeName"] = "new-volume"
    live["spec"]["resources"]["requests"]["storage"] = "1Gi"
    volume_data = {"new-volume": "existing application data"}
    calls: list[str] = []

    def api(args: list[str], **_kwargs: Any) -> Any:
        calls.append(args[0])
        if args[0] == "get":
            return SimpleNamespace(stdout=json.dumps(live))
        assert args[:3] == ["patch", "persistentvolumeclaim/geo-pvc", "--type=json"]
        for operation in json.loads(Path(args[4]).read_text(encoding="utf-8")):
            if operation["path"] == "/spec":
                assert operation["value"]["volumeName"] == "new-volume", "PVC binding is immutable"
                live["spec"] = operation["value"]
        return SimpleNamespace(stdout="patched")

    monkeypatch.setattr("benchmarks.sregym.fastloop.baseline_recovery.kubectl", api)
    cluster.objects[("hotel", "PersistentVolumeClaim", "geo-pvc")] = live
    monkeypatch.setattr(cluster, "apply", KubectlApplicationCluster(tmp_path / "kubeconfig").apply)

    cleanup = reconcile_application(cluster, baseline, "hotel")

    assert calls == ["get", "patch"]
    assert cleanup.restored == ["PersistentVolumeClaim/geo-pvc"]
    assert cluster.touched == []  # No deletion or recreation of a data claim.
    assert live["spec"] == {**original["spec"], "volumeName": "new-volume"}
    assert volume_data[live["spec"]["volumeName"]] == "existing application data"


def test_recreated_claim_binding_alone_does_not_require_restoration() -> None:
    cluster = FakeCluster()
    cluster.add(obj("PersistentVolumeClaim", "geo-pvc", spec={"volumeName": "old-volume"}))
    baseline = capture_application_baseline(cluster, "hotel")
    cluster.objects[("hotel", "PersistentVolumeClaim", "geo-pvc")]["spec"]["volumeName"] = "new-volume"

    cleanup = reconcile_application(cluster, baseline, "hotel")

    assert cleanup.restored == []
    assert cluster.touched == []


@pytest.mark.parametrize("legacy_binding", [False, True])
def test_restore_service_preserves_new_cluster_ips_and_repairs_target_port(
    monkeypatch: Any, tmp_path: Path, legacy_binding: bool
) -> None:
    cluster = FakeCluster()
    original = obj(
        "Service",
        "consul",
        spec={"clusterIP": "10.0.0.1", "clusterIPs": ["10.0.0.1"], "ports": [{"port": 80, "targetPort": 8080}]},
    )
    cluster.add(original)
    baseline = capture_application_baseline(cluster, "hotel")
    if legacy_binding:
        baseline.objects[0]["spec"].update(clusterIP="10.0.0.1", clusterIPs=["10.0.0.1"])
    else:
        assert "clusterIP" not in baseline.objects[0]["spec"]
        assert "clusterIPs" not in baseline.objects[0]["spec"]
    live = deepcopy(original)
    live["metadata"]["resourceVersion"] = "17"
    live["spec"].update(clusterIP="10.0.0.2", clusterIPs=["10.0.0.2"])
    live["spec"]["ports"][0]["targetPort"] = 8888

    def api(args: list[str], **_kwargs: Any) -> Any:
        if args[0] == "get":
            return SimpleNamespace(stdout=json.dumps(live))
        assert args[:3] == ["patch", "service/consul", "--type=json"]
        for operation in json.loads(Path(args[4]).read_text(encoding="utf-8")):
            if operation["path"] == "/spec":
                assert operation["value"]["clusterIP"] == "10.0.0.2"
                assert operation["value"]["clusterIPs"] == ["10.0.0.2"]
                live["spec"] = operation["value"]
        return SimpleNamespace(stdout="patched")

    monkeypatch.setattr("benchmarks.sregym.fastloop.baseline_recovery.kubectl", api)
    cluster.objects[("hotel", "Service", "consul")] = live
    monkeypatch.setattr(cluster, "apply", KubectlApplicationCluster(tmp_path / "kubeconfig").apply)

    reconcile_application(cluster, baseline, "hotel")

    assert live["spec"]["ports"] == original["spec"]["ports"]
    assert cluster.touched == []


def test_headless_service_intent_is_preserved_in_a_captured_baseline() -> None:
    cluster = FakeCluster()
    service = obj("Service", "mongo", spec={"clusterIP": "None", "clusterIPs": ["None"]})
    cluster.add(service)

    assert capture_application_baseline(cluster, "hotel").objects == [service]


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


ALLOCATED_CASES = [
    pytest.param("ConfigMap", ("metadata", "uid"), "old-uid", "live-uid", id="uid"),
    pytest.param("ConfigMap", ("metadata", "resourceVersion"), "1", "17", id="resource-version"),
    pytest.param(
        "ConfigMap",
        ("metadata", "creationTimestamp"),
        "2020-01-01T00:00:00Z",
        "2026-01-01T00:00:00Z",
        id="creation-time",
    ),
    pytest.param("ConfigMap", ("metadata", "generation"), 1, 2, id="generation"),
    pytest.param(
        "ConfigMap", ("metadata", "managedFields"), [{"manager": "old"}], [{"manager": "live"}], id="managed-fields"
    ),
    pytest.param("Service", ("spec", "clusterIP"), "10.0.0.1", "10.0.0.2", id="service-ip"),
    pytest.param("Service", ("spec", "clusterIPs"), ["10.0.0.1"], ["10.0.0.2"], id="service-ips"),
    pytest.param("Service", ("spec", "ipFamilies"), ["IPv4"], ["IPv6"], id="service-families"),
    pytest.param("Service", ("spec", "ipFamilyPolicy"), "SingleStack", "PreferDualStack", id="service-family-policy"),
    pytest.param("Service", ("spec", "healthCheckNodePort"), 32001, 32002, id="service-health-node-port"),
    pytest.param("Service", ("spec", "ports", 0, "nodePort"), 32003, 32004, id="service-node-port"),
    pytest.param("PersistentVolumeClaim", ("spec", "volumeName"), "old-volume", "live-volume", id="pvc-volume"),
    pytest.param("Pod", ("spec", "nodeName"), "old-node", "live-node", id="pod-node"),
    pytest.param(
        "Job",
        ("spec", "selector"),
        {"matchLabels": {"controller-uid": "old"}},
        {"matchLabels": {"controller-uid": "live"}},
        id="job-selector",
    ),
    *[
        pytest.param("Job", (*prefix, label), "old-controller", "live-controller", id=f"job-{location}-{label}")
        for location, prefix in [
            ("labels", ("metadata", "labels")),
            ("template", ("spec", "template", "metadata", "labels")),
            ("selector", ("spec", "selector", "matchLabels")),
        ]
        for label in ["controller-uid", "batch.kubernetes.io/controller-uid"]
    ],
]


def set_path(value: Any, path: tuple[Any, ...], content: Any) -> None:
    for index, key in enumerate(path[:-1]):
        if isinstance(value, dict) and key not in value:
            value[key] = [] if isinstance(path[index + 1], int) else {}
        if isinstance(value, list):
            while len(value) <= key:
                value.append({})
        value = value[key]
    value[path[-1]] = deepcopy(content)


def get_path(value: Any, path: tuple[Any, ...]) -> Any:
    for key in path:
        if isinstance(value, dict) and key not in value:
            return None
        value = value[key]
    return value


def allocated_fixture(kind: str) -> dict[str, Any]:
    if kind == "ConfigMap":
        return obj(kind, "synthetic", data={"desired": "healthy"})
    if kind == "Service":
        return obj(
            kind,
            "synthetic",
            spec={"ports": [{"name": "http", "port": 80, "targetPort": 8080}], "selector": {"app": "healthy"}},
        )
    if kind == "PersistentVolumeClaim":
        return obj(kind, "synthetic", spec={"resources": {"requests": {"storage": "2Gi"}}})
    if kind == "Job":
        return obj(
            kind,
            "synthetic",
            spec={
                "backoffLimit": 2,
                "template": {
                    "metadata": {"labels": {"app": "healthy"}},
                    "spec": {"containers": [{"name": "app", "image": "synthetic:healthy"}]},
                },
            },
        )
    return obj(kind, "synthetic", spec={"containers": [{"name": "app", "image": "synthetic:healthy"}]})


@pytest.mark.parametrize(("kind", "path", "old", "new"), ALLOCATED_CASES)
@pytest.mark.parametrize("legacy", [False, True])
def test_allocated_fields_keep_live_identity_while_desired_content_is_restored(
    monkeypatch: Any, tmp_path: Path, kind: str, path: tuple[Any, ...], old: Any, new: Any, legacy: bool
) -> None:
    original = allocated_fixture(kind)
    set_path(original, path, old)
    cluster = FakeCluster()
    cluster.add(original)
    captured = capture_application_baseline(cluster, "hotel").objects[0]
    assert get_path(captured, path) is None
    restore = deepcopy(original if legacy else captured)
    live = deepcopy(original)
    live["metadata"]["resourceVersion"] = "17"
    set_path(live, path, new)
    # Every case also restores ordinary desired content, rather than merely skipping the object.
    desired = (
        ("data", "desired")
        if kind == "ConfigMap"
        else ("spec", "selector", "app")
        if kind == "Service"
        else ("spec", "resources", "requests", "storage")
        if kind == "PersistentVolumeClaim"
        else ("spec", "backoffLimit")
        if kind == "Job"
        else ("spec", "containers", 0, "image")
    )
    expected = get_path(original, desired)
    set_path(live, desired, 9 if kind == "Job" else "faulty")

    def api(args: list[str], **_kwargs: Any) -> Any:
        if args[0] == "get":
            return SimpleNamespace(stdout=json.dumps(live))
        assert args[0] == "patch"
        for operation in json.loads(Path(args[4]).read_text()):
            if operation["op"] == "test":
                assert operation["value"] == live["metadata"]["resourceVersion"]
            elif operation["op"] == "add":
                live[operation["path"].removeprefix("/")] = operation["value"]
            elif operation["op"] == "remove":
                live.pop(operation["path"].removeprefix("/"), None)
        return SimpleNamespace(stdout="patched")

    monkeypatch.setattr("benchmarks.sregym.fastloop.baseline_recovery.kubectl", api)
    KubectlApplicationCluster(tmp_path / "kubeconfig").apply("hotel", restore)

    assert get_path(live, path) == new
    assert get_path(live, desired) == expected
    assert get_path(original, path) == old


@pytest.mark.parametrize(("kind", "path", "old", "new"), ALLOCATED_CASES)
def test_missing_object_recreation_omits_allocated_fields(
    monkeypatch: Any, tmp_path: Path, kind: str, path: tuple[Any, ...], old: Any, new: Any
) -> None:
    original = allocated_fixture(kind)
    set_path(original, path, old)
    created = []

    def api(args: list[str], **kwargs: Any) -> Any:
        if args[0] == "get":
            return SimpleNamespace(stdout="")
        assert args[0] == "apply"
        created.append(json.loads(kwargs["input_text"]))
        return SimpleNamespace(stdout="created")

    monkeypatch.setattr("benchmarks.sregym.fastloop.baseline_recovery.kubectl", api)
    KubectlApplicationCluster(tmp_path / "kubeconfig").apply("hotel", original)

    assert len(created) == 1
    assert get_path(created[0], path) is None


@pytest.mark.parametrize(("kind", "path", "old", "new"), ALLOCATED_CASES)
def test_allocated_field_changes_alone_never_restore_the_object(
    kind: str, path: tuple[Any, ...], old: Any, new: Any
) -> None:
    cluster = FakeCluster()
    original = allocated_fixture(kind)
    set_path(original, path, old)
    cluster.add(original)
    baseline = capture_application_baseline(cluster, "hotel")
    set_path(original, path, new)

    assert reconcile_application(cluster, baseline, "hotel").restored == []
    assert cluster.touched == []


@pytest.mark.parametrize("phase", ["Pending", "Bound"])
def test_reconciliation_never_deletes_an_extra_pvc(phase: str) -> None:
    cluster = FakeCluster()
    baseline = capture_application_baseline(cluster, "hotel")
    cluster.add(obj("PersistentVolumeClaim", "data", status={"phase": phase}))

    assert reconcile_application(cluster, baseline, "hotel").deleted_counts == {}
    assert ("hotel", "PersistentVolumeClaim", "data") in cluster.objects
    assert cluster.touched == []


@pytest.mark.parametrize(("source_class", "expected"), [(None, "openebs-hostpath"), ("explicit", "explicit"), ("", "")])
def test_source_claim_retains_installed_default_only_when_source_omits_class(
    tmp_path: Path, source_class: str | None, expected: str
) -> None:
    value: dict[str, Any] = {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": "data", "namespace": "hotel-reservation"},
        "spec": {"accessModes": ["ReadWriteOnce"]},
    }
    captured = deepcopy(value)
    captured["spec"]["storageClassName"] = "openebs-hostpath"
    if source_class is not None:
        value["spec"]["storageClassName"] = source_class
    root = tmp_path / ".sdo/rendered"
    root.mkdir(parents=True)
    payload = json.dumps(value).encode()
    (root / "claim.json").write_bytes(payload)
    (root / "lock.json").write_text(
        json.dumps(
            {
                "objects": [
                    {
                        "ref": {
                            "api_version": "v1",
                            "kind": "PersistentVolumeClaim",
                            "namespace": "hotel-reservation",
                            "name": "data",
                        },
                        "file": "claim.json",
                        "sha256": hashlib.sha256(payload).hexdigest(),
                    }
                ]
            }
        )
    )
    result = accepted_restore_baselines(tmp_path, (ApplicationBaseline("hotel-reservation", [captured]),))
    assert result[0].objects[0]["spec"]["storageClassName"] == expected


def test_deployed_faulty_application_service_remains_captured_and_reported() -> None:
    service = obj("Service", "app", spec={"selector": {"app": "healthy"}, "ports": [{"port": 80}]})
    service["metadata"]["managedFields"] = [{"manager": "sdo-deployer", "operation": "Apply"}]
    cluster = FakeCluster()
    cluster.add(service)
    baseline = capture_application_baseline(cluster, "hotel")
    faulty = deepcopy(service)
    faulty["spec"]["selector"] = {"app": "wrong"}
    cluster.add(faulty)
    assert capture_application_baseline(cluster, "hotel").objects[0]["spec"]["selector"] == {"app": "wrong"}
    reconcile_application(cluster, baseline, "hotel")
    assert cluster.touched == [("hotel", "Service", "app")]


@pytest.mark.parametrize("ownership", ["label", "ownership-label", "manager", "owner"])
def test_capture_excludes_sdo_ownership_but_keeps_plain_app_rbac(ownership: str) -> None:
    # Hand-written fixture: names intentionally do not identify the installer.
    application = {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": {"name": "sdo-looking-app-binding", "namespace": "hotel"},
        "subjects": [],
        "roleRef": {"kind": "Role", "name": "app"},
    }
    installed = {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": {"name": "ordinary-binding", "namespace": "hotel"},
        "subjects": [],
        "roleRef": {"kind": "Role", "name": "control"},
    }
    controller = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "control-index",
            "namespace": "hotel",
            "uid": "owner-uid",
            "labels": {"app.kubernetes.io/managed-by": "sdo"},
        },
    }
    if ownership == "label":
        installed["metadata"]["labels"] = {"app.kubernetes.io/managed-by": "sdo"}
    elif ownership == "ownership-label":
        installed["metadata"]["labels"] = {"sdo.dev/responder-read-owner": "control"}
    elif ownership == "manager":
        installed["metadata"]["managedFields"] = [{"manager": "sdo-operator", "operation": "Update"}]
    else:
        installed["metadata"]["ownerReferences"] = [{"uid": "owner-uid", "kind": "ConfigMap", "name": "control-index"}]
    cluster = FakeCluster()
    for value in (application, installed, controller):
        cluster.add(value)
    assert capture_application_baseline(cluster, "hotel").objects == [application]


def test_capture_keeps_app_rbac_with_repair_field_ownership() -> None:
    cluster = FakeCluster()
    app = obj("RoleBinding", "application")
    app["metadata"]["managedFields"] = [{"manager": "sdo-deployer", "operation": "Apply"}]
    cluster.add(app)
    expected = deepcopy(app)
    expected["metadata"].pop("managedFields")
    assert capture_application_baseline(cluster, "hotel").objects == [expected]
