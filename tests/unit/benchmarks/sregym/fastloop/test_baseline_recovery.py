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
    KubectlApplicationCluster,
    accepted_restore_baselines,
    application_namespaces,
    capture_application_baseline,
    load_application_baseline,
    reconcile_application,
    save_application_baseline,
    seal_existing_install,
)
from tests.unit.benchmarks.sregym.fastloop.accepted_source_fixture import commit_accepted_source


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
    source = commit_accepted_source(tmp_path, [value])
    result = accepted_restore_baselines(source, (ApplicationBaseline("hotel-reservation", [captured]),))
    assert result[0].objects[0]["spec"]["storageClassName"] == expected


def _sealed(tmp_path: Path, objects: list[dict[str, Any]]) -> tuple[ApplicationBaseline, ...]:
    path = tmp_path / "baseline.json"
    path.write_text("{}")
    save_application_baseline(path, (ApplicationBaseline("app", objects),), installed=True)
    baselines = load_application_baseline(path, "app")
    assert baselines is not None
    return baselines


def test_sealed_restore_never_reverts_passive_objects_to_the_pre_adoption_capture(tmp_path: Path) -> None:
    """The capture predates SDO's adoption of accepted source; restoring it flips adopted fields back."""
    config = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "settings", "namespace": "app"}}
    redirect = {"apiVersion": "v1", "kind": "Service", "metadata": {"name": "redirect", "namespace": "app"}}
    routes = {"apiVersion": "v1", "kind": "Service", "metadata": {"name": "routes", "namespace": "app"}}
    api = {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": "api", "namespace": "app"}}
    captured = [
        {**config, "data": {"app.yml": "pre-adoption", "extra": "kept"}},
        {**redirect, "spec": {"type": "ExternalName", "externalName": "sink.other.svc", "selector": {"app": "x"}}},
        {**routes, "spec": {"ports": [{"name": "http", "port": 80, "protocol": "TCP"}, {"name": "m", "port": 90}]}},
        {**api, "spec": {"template": {"spec": {"containers": [{"name": "api", "image": "installed"}]}}}},
    ]
    accepted = [
        {**config, "data": {"app.yml": "adopted"}},
        {
            **redirect,
            "metadata": {**redirect["metadata"], "labels": {"app.kubernetes.io/name": "redirect"}},
            "spec": {"type": "ExternalName", "externalName": "sink.other.svc"},
        },
        {**routes, "spec": {"ports": [{"name": "http", "port": 80}]}},
        {
            **api,
            "spec": {
                "template": {"spec": {"containers": [{"name": "api", "image": "installed", "args": ["--declared"]}]}}
            },
        },
    ]
    source = commit_accepted_source(tmp_path, accepted)

    restored = {
        value["metadata"]["name"]: value
        for value in accepted_restore_baselines(source, _sealed(tmp_path, captured))[0].objects
    }

    assert restored["settings"]["data"] == {"app.yml": "adopted", "extra": "kept"}
    assert restored["redirect"]["metadata"]["labels"] == {"app.kubernetes.io/name": "redirect"}
    assert restored["redirect"]["spec"]["selector"] == {"app": "x"}
    # Declared fields already present in the capture (plus extras SDO cannot remove) stay as captured.
    assert restored["routes"] == captured[2]
    # Compose the accepted workload policy before restore; the old chart capture
    # must not drop it and open a baseline incident after onboarding.
    assert restored["api"] == accepted[3]


def test_restore_after_adoption_leaves_adopted_config_in_place(tmp_path: Path) -> None:
    config = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "settings", "namespace": "app"}}
    cluster = FakeCluster()
    cluster.add({**config, "data": {"app.yml": "adopted"}})
    source = commit_accepted_source(tmp_path, [{**config, "data": {"app.yml": "adopted"}}])
    (baseline,) = accepted_restore_baselines(
        source, _sealed(tmp_path, [{**config, "data": {"app.yml": "pre-adoption"}}])
    )

    reconcile_application(cluster, baseline, "app")

    assert cluster.objects[("app", "ConfigMap", "settings")]["data"] == {"app.yml": "adopted"}


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


@pytest.mark.parametrize(
    "kind",
    [
        "Deployment",
        "DaemonSet",
        "StatefulSet",
        "Service",
        "ConfigMap",
        "Job",
        "CronJob",
        "Pod",
        "NetworkPolicy",
        "PersistentVolumeClaim",
        "ServiceAccount",
        "Role",
        "RoleBinding",
    ],
)
@pytest.mark.parametrize("ownership", ["label", "ownership-label", "manager"])
@pytest.mark.parametrize("created_after_capture", [False, True])
def test_application_restore_leaves_sdo_objects_and_descendants_untouched(
    kind: str, ownership: str, created_after_capture: bool
) -> None:
    cluster = FakeCluster()
    app = obj("Service", "application", spec={"ports": [{"port": 80}]})
    cluster.add(app)
    baseline = capture_application_baseline(cluster, "hotel")
    root = obj(kind, "controller-state", spec={"synthetic": "owned"})
    root["metadata"]["uid"] = "controller-uid"
    if ownership == "label":
        root["metadata"]["labels"] = {"app.kubernetes.io/managed-by": "sdo"}
    elif ownership == "ownership-label":
        root["metadata"]["labels"] = {"sdo.dev/observation-owner": "controller"}
    else:
        root["metadata"]["managedFields"] = [{"manager": "sdo-operator", "operation": "Update"}]
    child = obj("Job", "worker", spec={"synthetic": "child"})
    child["metadata"].update(uid="worker-uid", ownerReferences=[{"uid": "controller-uid", "kind": kind}])
    descendant = obj("ConfigMap", "worker-state", data={"state": "owned"})
    descendant["metadata"]["ownerReferences"] = [{"uid": "worker-uid", "kind": "Job"}]
    pod = obj("Pod", "worker-pod", spec={})
    pod["metadata"]["ownerReferences"] = [{"uid": "worker-uid", "kind": "Job"}]
    # Reverse graph order ensures ownership propagation reaches a fixed point.
    for value in (descendant, pod, child, root):
        cluster.add(value)
    if not created_after_capture:
        baseline = capture_application_baseline(cluster, "hotel")
    assert baseline.objects == [app]
    before = deepcopy(cluster.objects)

    cleanup = reconcile_application(cluster, baseline, "hotel")

    assert cluster.touched == []
    assert cluster.objects == before
    assert cleanup.deleted_counts == {}
    assert cleanup.restored == []


def test_legacy_baseline_cannot_restore_over_live_sdo_ownership() -> None:
    cluster = FakeCluster()
    owned = obj("ConfigMap", "controller-state", data={"state": "current"})
    owned["metadata"]["managedFields"] = [{"manager": "sdo-operator", "operation": "Update"}]
    cluster.add(owned)
    stale = obj("ConfigMap", "controller-state", data={"state": "stale"})
    baseline = ApplicationBaseline("hotel", [stale])

    cleanup = reconcile_application(cluster, baseline, "hotel")

    assert cluster.touched == []
    assert cleanup.restored == []
    assert cluster.objects[("hotel", "ConfigMap", "controller-state")] == owned


def test_installed_seal_compares_only_application_ownership(tmp_path: Path) -> None:
    cluster = FakeCluster()
    cluster.add(obj("Service", "application", spec={"ports": [{"port": 80}]}))
    baseline = capture_application_baseline(cluster, "hotel")
    path = tmp_path / "baseline.json"
    path.write_text("{}")
    save_application_baseline(path, (baseline,))
    owned = obj("ConfigMap", "controller-state", data={"state": "current"})
    owned["metadata"]["labels"] = {"app.kubernetes.io/managed-by": "sdo"}
    cluster.add(owned)

    seal_existing_install(path, cluster, "hotel")

    loaded = load_application_baseline(path, "hotel")
    assert loaded is not None
    assert loaded[0].installed_digest
    assert loaded[0].objects == baseline.objects
    assert cluster.touched == []


@pytest.mark.parametrize("kind", ["ConfigMap", "Service", "Job", "Deployment", "NetworkPolicy"])
def test_application_extras_are_deleted_beside_sdo_owned_objects(kind: str) -> None:
    cluster = FakeCluster()
    baseline = capture_application_baseline(cluster, "hotel")
    owned = obj(kind, "controller-state")
    owned["metadata"].update(uid="controller-uid", labels={"app.kubernetes.io/managed-by": "sdo"})
    extra = obj(kind, "application-extra")
    # A stale or unrelated owner UID is not proof of SDO ownership.
    extra["metadata"]["ownerReferences"] = [{"kind": kind, "uid": "unrelated-uid"}]
    cluster.add(owned)
    cluster.add(extra)

    cleanup = reconcile_application(cluster, baseline, "hotel")

    assert cluster.touched == [("hotel", kind, "application-extra")]
    assert cleanup.deleted_counts == {kind: 1}
    assert cluster.objects == {("hotel", kind, "controller-state"): owned}


@pytest.mark.parametrize("operation", ["restore", "seal"])
def test_application_helper_migration_leaves_sdo_owned_deployments_untouched(operation: str, tmp_path: Path) -> None:
    namespace = "hotel-reservation"
    cluster = FakeCluster()
    owned = obj(
        "Deployment",
        "mongodb-geo",
        namespace,
        spec={
            "template": {
                "spec": {
                    "volumes": [{"name": "failure-script", "configMap": {"name": "failure-admin-geo"}}],
                    "containers": [
                        {
                            "name": "hotel-reserv-geo-mongo",
                            "volumeMounts": [
                                {"name": "failure-script", "mountPath": "/scripts"},
                            ],
                        }
                    ],
                }
            },
        },
    )
    owned["metadata"]["labels"] = {"app.kubernetes.io/managed-by": "sdo"}
    cluster.add(owned)
    baseline = capture_application_baseline(cluster, namespace)
    assert baseline.objects == []

    if operation == "restore":
        cleanup = reconcile_application(cluster, baseline, namespace)
        assert cleanup.restored == []
        assert cleanup.deleted_counts == {}
    else:
        path = tmp_path / "baseline.json"
        path.write_text("{}")
        save_application_baseline(path, (baseline,))
        seal_existing_install(path, cluster, namespace)

    assert cluster.touched == []
    assert cluster.objects[(namespace, "Deployment", "mongodb-geo")] == owned


def test_sealed_restore_merges_service_ports_by_name_and_keeps_captured_extras(tmp_path: Path) -> None:
    routes = {"apiVersion": "v1", "kind": "Service", "metadata": {"name": "routes", "namespace": "app"}}
    captured = [
        {
            **routes,
            "spec": {
                "ports": [
                    {"name": "http", "port": 80, "targetPort": 8080, "protocol": "TCP"},
                    {"name": "metrics", "port": 90, "protocol": "TCP"},
                    {"port": 100, "protocol": "TCP"},
                ]
            },
        }
    ]
    source = commit_accepted_source(
        tmp_path,
        [
            {
                **routes,
                "spec": {
                    "ports": [{"name": "http", "port": 80, "targetPort": 9090}, {"port": 100, "targetPort": 1000}]
                },
            }
        ],
    )

    ports = accepted_restore_baselines(source, _sealed(tmp_path, captured))[0].objects[0]["spec"]["ports"]

    assert ports == [
        {"name": "http", "port": 80, "targetPort": 9090, "protocol": "TCP"},
        {"name": "metrics", "port": 90, "protocol": "TCP"},
        {"port": 100, "targetPort": 1000, "protocol": "TCP"},
    ]


def redirect(name: str, namespace: str = "social-network", target: str = "otel-collector.observe") -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": name, "namespace": namespace, "labels": {"app": name}},
        "spec": {"type": "ExternalName", "externalName": f"{target}.svc.cluster.local", "selector": {"app": name}},
    }


def test_harness_redirect_removed_by_sdo_stays_removed_across_restores() -> None:
    """Fixture of the delete-then-restore loop: re-creating would mint a new identity."""
    cluster = FakeCluster()
    cluster.add(obj("Deployment", "compose-post", namespace="social-network", spec={"replicas": 1}))
    for name in ("jaeger", "jaeger-agent", "jaeger-collector"):
        cluster.add(redirect(name))
    baseline = capture_application_baseline(cluster, "social-network")
    cluster.delete("social-network", "Service", "jaeger-agent")
    cluster.touched.clear()

    for _ in range(2):
        cleanup = reconcile_application(cluster, baseline, "social-network")
        assert ("social-network", "Service", "jaeger-agent") not in cluster.objects
        assert cluster.touched == []
        assert cleanup.restored == []
        assert cleanup.harness_absent == ["Service/jaeger-agent"]


def test_source_declared_harness_redirect_is_recreated() -> None:
    cluster = FakeCluster()
    cluster.add(redirect("jaeger", namespace="hotel"))
    baseline = capture_application_baseline(cluster, "hotel")
    cluster.delete("hotel", "Service", "jaeger")

    cleanup = reconcile_application(cluster, baseline, "hotel", declared=frozenset({("Service", "jaeger")}))

    assert cluster.objects[("hotel", "Service", "jaeger")]["spec"]["type"] == "ExternalName"
    assert [entry.to_dict() for entry in cleanup.reapplied] == [
        {"kind": "Service", "namespace": "hotel", "name": "jaeger", "recreated": True, "harness_owned": True}
    ]
    assert cleanup.undeclared_recreated == []


def test_changed_harness_redirect_is_patched_and_extra_redirect_is_kept() -> None:
    cluster = FakeCluster()
    cluster.add(redirect("jaeger", namespace="hotel"))
    baseline = capture_application_baseline(cluster, "hotel")
    cluster.objects[("hotel", "Service", "jaeger")]["spec"]["externalName"] = "elsewhere.example"
    cluster.add(redirect("jaeger-query", namespace="hotel"))

    cleanup = reconcile_application(cluster, baseline, "hotel")

    assert cluster.objects[("hotel", "Service", "jaeger")]["spec"] == baseline.objects[0]["spec"]
    assert ("hotel", "Service", "jaeger-query") in cluster.objects
    assert cleanup.deleted_counts == {}
    assert [(e.name, e.recreated, e.harness_owned) for e in cleanup.reapplied] == [("jaeger", False, True)]


def test_application_redirect_to_an_application_namespace_is_an_ordinary_object() -> None:
    cluster = FakeCluster()
    cluster.add(redirect("search", namespace="hotel", target="search.hotel"))
    baseline = capture_application_baseline(cluster, "hotel")
    cluster.delete("hotel", "Service", "search")

    cleanup = reconcile_application(cluster, baseline, "hotel")

    assert ("hotel", "Service", "search") in cluster.objects
    assert cleanup.harness_absent == []
    assert [(e.name, e.recreated, e.harness_owned) for e in cleanup.reapplied] == [("search", True, False)]


def test_recreated_object_outside_accepted_source_is_reported_as_unexplained() -> None:
    cluster = FakeCluster()
    cluster.add(obj("Deployment", "geo", spec={"replicas": 1}))
    cluster.add(obj("ConfigMap", "runtime-extra", data={"a": "b"}))
    baseline = capture_application_baseline(cluster, "hotel")
    cluster.delete("hotel", "Deployment", "geo")
    cluster.delete("hotel", "ConfigMap", "runtime-extra")

    cleanup = reconcile_application(cluster, baseline, "hotel", declared=frozenset({("Deployment", "geo")}))

    assert cleanup.undeclared_recreated == ["ConfigMap/runtime-extra"]


def test_restore_composition_reaches_cluster_with_source_labels_and_policy(tmp_path: Path) -> None:
    """The restore producer's accepted fields reach the real reconcile consumer."""
    accepted = obj(
        "Deployment",
        "api",
        namespace="app",
        spec={"template": {"spec": {"containers": [{"name": "api", "image": "same", "args": ["--accepted"]}]}}},
    )
    accepted["metadata"]["labels"] = {"app": "api"}
    captured = deepcopy(accepted)
    del captured["metadata"]["labels"]
    captured["spec"]["template"]["spec"]["containers"][0].pop("args")
    source = commit_accepted_source(tmp_path, [accepted])
    baseline = accepted_restore_baselines(source, _sealed(tmp_path, [captured]))[0]
    cluster = FakeCluster()
    cluster.add(captured)
    reconcile_application(cluster, baseline, "app")
    assert cluster.objects["app", "Deployment", "api"] == accepted
    # A missing source label alone is still a real restore write; extra live
    # labels are retained rather than granting source authority over them.
    current = cluster.objects["app", "Deployment", "api"]
    current["metadata"]["labels"] = {"runtime.example/owner": "controller"}
    cluster.touched.clear()
    reconcile_application(cluster, baseline, "app")
    assert cluster.touched == [("app", "Deployment", "api")]


def test_kubectl_restore_writes_labels_without_replacing_other_metadata(monkeypatch: Any, tmp_path: Path) -> None:
    desired = obj("Service", "redirect", spec={"type": "ExternalName", "externalName": "synthetic.example"})
    desired["metadata"]["labels"] = {"app": "redirect"}
    current = deepcopy(desired)
    current["metadata"].update(labels={"runtime.example/owner": "controller"}, resourceVersion="17")
    patches = []

    def api(args: list[str], **_kwargs: Any) -> Any:
        if args[0] == "get":
            return SimpleNamespace(stdout=json.dumps(current))
        patches.extend(json.loads(Path(args[4]).read_text()))
        return SimpleNamespace(stdout="patched")

    monkeypatch.setattr("benchmarks.sregym.fastloop.baseline_recovery.kubectl", api)
    KubectlApplicationCluster(tmp_path / "kube").apply("hotel", desired)
    metadata_patches = [patch for patch in patches if patch["path"].startswith("/metadata/")]
    assert metadata_patches == [
        {"op": "test", "path": "/metadata/resourceVersion", "value": "17"},
        {"op": "add", "path": "/metadata/labels", "value": {"runtime.example/owner": "controller", "app": "redirect"}},
    ]
