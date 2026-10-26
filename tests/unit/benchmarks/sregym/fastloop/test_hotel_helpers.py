import json
from copy import deepcopy
from enum import StrEnum
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from benchmarks.sregym.fastloop import hotel_helpers as helpers
from benchmarks.sregym.fastloop.baseline_recovery import (
    ApplicationBaseline,
    load_application_baseline,
    reconcile_application,
    save_application_baseline,
)

NS = "hotel-reservation"


def legacy() -> list[dict[str, Any]]:
    return [
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"namespace": NS, "name": "failure-admin-geo"},
            "data": {"remove-mitigate-admin-geo-mongo.sh": "synthetic"},
        },
        {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"namespace": NS, "name": "mongodb-geo"},
            "spec": {
                "template": {
                    "spec": {
                        "volumes": [
                            {"name": "failure-script", "configMap": {"name": "failure-admin-geo"}},
                            {"name": "custom", "configMap": {"name": "custom-app"}},
                        ],
                        "containers": [
                            {
                                "name": "hotel-reserv-geo-mongo",
                                "volumeMounts": [
                                    {"name": "failure-script", "mountPath": "/scripts"},
                                    {"name": "custom", "mountPath": "/custom"},
                                ],
                            }
                        ],
                    }
                }
            },
        },
    ]


class Cluster:
    def __init__(self, objects: list[dict[str, Any]]) -> None:
        self.objects = deepcopy(objects)
        self.writes: list[str] = []

    def list_objects(self, namespace: str) -> list[dict[str, Any]]:
        return deepcopy(self.objects)

    def apply(self, namespace: str, value: dict[str, Any]) -> None:
        self.writes.append("apply")
        self.objects = [v for v in self.objects if v["metadata"]["name"] != value["metadata"]["name"]] + [
            deepcopy(value)
        ]

    def delete(self, namespace: str, kind: str, name: str) -> None:
        self.writes.append("delete")
        self.objects = [v for v in self.objects if v["metadata"]["name"] != name]


def test_legacy_cleanup_preserves_custom_mount_and_is_idempotent() -> None:
    cluster = Cluster(legacy())
    helpers.retire_helpers(cluster, NS)
    helpers.assert_script_free(cluster.list_objects(NS), NS)
    pod = cluster.objects[0]["spec"]["template"]["spec"]
    assert pod["volumes"] == [{"name": "custom", "configMap": {"name": "custom-app"}}]
    assert pod["containers"][0]["volumeMounts"] == [{"name": "custom", "mountPath": "/custom"}]
    writes = list(cluster.writes)
    helpers.retire_helpers(cluster, NS)
    assert cluster.writes == writes


@pytest.mark.parametrize(
    "edge",
    ["wrong-deployment", "wrong-path", "custom-key", "owner", "wrong-namespace", "projected", "duplicate", "finalizer"],
)
def test_unknown_identity_or_mount_fails_before_any_write(edge: str) -> None:
    objects = legacy()
    cm, deployment = objects
    pod = deployment["spec"]["template"]["spec"]
    if edge == "wrong-deployment":
        deployment["metadata"]["name"] = "custom-mongo"
    elif edge == "wrong-path":
        pod["containers"][0]["volumeMounts"][0]["mountPath"] = "/custom"
    elif edge == "custom-key":
        cm["data"]["custom"] = "preserve"
    elif edge == "owner":
        cm["metadata"]["ownerReferences"] = [{"kind": "Custom", "name": "owner"}]
    elif edge == "wrong-namespace":
        deployment["metadata"]["namespace"] = "other"
    elif edge == "projected":
        pod["volumes"][0] = {
            "name": "failure-script",
            "projected": {"sources": [{"configMap": {"name": "failure-admin-geo"}}]},
        }
    elif edge == "finalizer":
        cm["metadata"]["finalizers"] = ["custom/retain"]
    elif edge == "duplicate":
        pod["volumes"].append(deepcopy(pod["volumes"][0]))
    cluster = Cluster(objects)
    with pytest.raises(ValueError, match="helper"):
        helpers.retire_helpers(cluster, NS)
    assert cluster.writes == []


def test_sealed_legacy_baseline_migrates_with_source_hash_and_restore_stays_clean(tmp_path: Path) -> None:
    path = tmp_path / "baseline.json"
    path.write_text("{}")
    objects = legacy()[1:]  # helper CMs were omitted by the old capture filter
    save_application_baseline(path, (ApplicationBaseline(NS, objects),), installed=True)
    original = path.read_bytes()
    helpers.migrate_baseline(path, NS)
    baseline = load_application_baseline(path, NS)
    assert baseline is not None
    assert baseline[0].installed_digest
    evidence = json.loads(path.read_text())["hotel_helper_migrations"][0]
    import hashlib

    assert evidence["source_sha256"] == hashlib.sha256(original).hexdigest()
    assert evidence["before_digest"] != evidence["after_digest"]
    assert evidence["removed"] == ["Deployment/mongodb-geo:failure-script"]
    cluster = Cluster(legacy())
    helpers.retire_helpers(cluster, NS)
    reconcile_application(cluster, baseline[0], NS)
    helpers.assert_script_free(cluster.objects, NS)
    migrated = path.read_bytes()
    helpers.migrate_baseline(path, NS)
    assert path.read_bytes() == migrated
    cluster.objects = legacy()
    with pytest.raises(ValueError, match="helper"):
        helpers.assert_script_free(cluster.objects, NS)


def test_clean_absent_profile_and_custom_scripts_are_preserved() -> None:
    class Profile(StrEnum):
        MOUNTED = "mounted"
        ABSENT = "absent"

    app = SimpleNamespace(namespace=NS, failure_scripts=Profile.MOUNTED)
    helpers.configure_profile(app)
    assert app.failure_scripts is Profile.ABSENT
    cluster = Cluster(
        [{"kind": "ConfigMap", "metadata": {"name": "custom", "namespace": NS}, "data": {"app.sh": "custom"}}]
    )
    helpers.retire_helpers(cluster, NS)
    helpers.assert_script_free(cluster.objects, NS)
    assert not cluster.writes


def test_admission_is_read_only_and_catches_scripts_under_another_name() -> None:
    objects = legacy()
    objects[0]["metadata"]["name"] = "renamed-helper"
    with pytest.raises(ValueError, match="helper"):
        helpers.assert_script_free(objects, NS)
    helpers.assert_script_free([], "astronomy-shop")


@pytest.mark.parametrize("default_mode", [None, 420])
def test_api_defaulted_known_mount_and_legacy_restore_are_script_free(default_mode: int | None) -> None:
    objects = legacy()
    if default_mode is not None:
        objects[1]["spec"]["template"]["spec"]["volumes"][0]["configMap"]["defaultMode"] = default_mode
    cluster = Cluster(objects)
    # Reconcile itself must prevent old captures (including sealed captures)
    # from resurrecting helper mounts, without needing a prior file migration.
    reconcile_application(cluster, ApplicationBaseline(NS, objects[1:]), NS)
    helpers.assert_script_free(cluster.objects, NS)


def test_ownership_failure_propagates() -> None:
    class RefusingCluster(Cluster):
        def apply(self, namespace: str, value: dict[str, Any]) -> None:
            raise PermissionError("synthetic ownership rejection")

    cluster = RefusingCluster(legacy())
    with pytest.raises(PermissionError, match="ownership rejection"):
        helpers.retire_helpers(cluster, NS)
    assert len(cluster.objects) == 2


@pytest.mark.parametrize("compose", [False, True])
def test_worker_injection_guard_is_unconditional_and_precedes_inject(monkeypatch: Any, compose: bool) -> None:
    from benchmarks.sregym.fastloop import worker_bridge

    events: list[str] = []

    class Worker:
        def _problem_factory(self, problem_id: str) -> Any:
            return SimpleNamespace(namespace=NS)

        def handle(self, request: dict[str, Any]) -> dict[str, Any]:
            events.append("injected")
            return {}

    worker = Worker()
    module = SimpleNamespace(
        FastloopWorker=Worker,
        main=lambda: worker.handle({"op": "inject", "problem_id": "same-seed", "compose": compose}),
    )
    monkeypatch.setenv("KUBECONFIG", "synthetic-kubeconfig")
    monkeypatch.setattr(
        worker_bridge.importlib,
        "import_module",
        lambda name: module if name.endswith(".worker") else SimpleNamespace(probe_pod_manifest=lambda *args: {}),
    )

    def guard(*args: Any) -> None:
        events.append("guard")
        helpers.assert_script_free(legacy(), NS)

    monkeypatch.setattr(helpers, "admit_install", guard)
    with pytest.raises(ValueError, match="helper remains"):
        worker_bridge.main()
    assert events == ["guard"]


def test_duplicate_objects_and_tampered_seal_fail_without_changes(tmp_path: Path) -> None:
    cluster = Cluster([legacy()[0], legacy()[0]])
    with pytest.raises(ValueError, match="duplicate object identities"):
        helpers.retire_helpers(cluster, NS)
    assert cluster.writes == []
    path = tmp_path / "baseline.json"
    path.write_text("{}")
    save_application_baseline(path, (ApplicationBaseline(NS, legacy()[1:]),), installed=True)
    document = json.loads(path.read_text())
    document["fastloop_application"]["namespaces"][0]["objects"][0]["spec"]["tampered"] = True
    path.write_text(json.dumps(document))
    before = path.read_bytes()
    with pytest.raises(ValueError, match="digest differs"):
        helpers.migrate_baseline(path, NS)
    assert path.read_bytes() == before


@pytest.mark.parametrize("redeploy", [False, True])
def test_worker_deploy_uses_absent_profile_without_changing_problem_identity(monkeypatch: Any, redeploy: bool) -> None:
    from benchmarks.sregym.fastloop import clean_start, worker_bridge

    class Profile(StrEnum):
        MOUNTED = "mounted"
        ABSENT = "absent"

    app = SimpleNamespace(namespace=NS, failure_scripts=Profile.MOUNTED)
    problem = SimpleNamespace(namespace=NS, app=app)
    events: list[str] = []
    request = {
        "op": "deploy",
        "problem_id": "unchanged-seeded-problem",
        "baseline_path": "synthetic.json",
        "redeploy": redeploy,
    }

    class Worker:
        def _problem_factory(self, problem_id: str) -> Any:
            assert problem_id == request["problem_id"]
            return problem

        def handle(self, actual: dict[str, Any]) -> dict[str, Any]:
            assert actual == request
            assert self._problem_factory(actual["problem_id"]) is problem
            assert app.failure_scripts is Profile.ABSENT
            events.append("deploy")
            return {"deployed": True}

    worker = Worker()
    module = SimpleNamespace(FastloopWorker=Worker, main=lambda: worker.handle(request))
    monkeypatch.setenv("KUBECONFIG", "synthetic-kubeconfig")
    monkeypatch.setattr(
        worker_bridge.importlib,
        "import_module",
        lambda name: module if name.endswith(".worker") else SimpleNamespace(probe_pod_manifest=lambda *args: {}),
    )
    monkeypatch.setattr(clean_start, "remove_injected_objects", lambda *args: None)
    monkeypatch.setattr(clean_start, "prepare_harness_traffic", lambda *args: False)
    monkeypatch.setattr(clean_start, "assert_clean_baseline", lambda *args, **kwargs: None)
    monkeypatch.setattr(helpers, "prepare_install", lambda *args: events.append("prepare"))
    assert worker_bridge.main() == {"deployed": True}
    assert events == ["prepare", "deploy"]
