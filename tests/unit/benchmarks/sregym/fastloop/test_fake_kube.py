from __future__ import annotations

import json
from typing import TYPE_CHECKING
from urllib.request import urlopen

import pytest

from benchmarks.sregym.fastloop.fake_kube import FakeKubernetesAPIServer

if TYPE_CHECKING:
    from pathlib import Path


def test_http_reads_captured_snapshot_without_a_cluster(tmp_path: Path) -> None:
    snapshot = {
        "namespace": "hotel",
        "services": [
            {"metadata": {"name": "front", "namespace": "hotel"}, "spec": {"ports": [{"port": 80, "targetPort": 9999}]}}
        ],
    }
    with FakeKubernetesAPIServer(snapshot) as server:
        kubeconfig = server.write_kubeconfig(tmp_path / "kubeconfig")
        config = json.loads(kubeconfig.read_text())
        endpoint = config["clusters"][0]["cluster"]["server"]
        assert config["contexts"][0]["context"]["namespace"] == "hotel"
        for path in ("/api", "/apis", "/api/v1"):
            with urlopen(endpoint + path, timeout=5) as response:
                assert json.load(response)["kind"] in {"APIVersions", "APIGroupList", "APIResourceList"}
        with urlopen(endpoint + "/api/v1/namespaces/hotel/services/front", timeout=5) as response:
            service = json.load(response)

    assert service["kind"] == "Service"
    assert service["spec"]["ports"][0]["targetPort"] == 9999


def test_fake_api_accepts_go_snapshot_null_collections() -> None:
    with FakeKubernetesAPIServer({"namespaceName": "demo", "pods": None, "services": None}) as server:
        status, response = server.read("/api/v1/namespaces/demo/pods")
    assert status == 200
    assert response["items"] == []


<<<<<<< HEAD
def test_new_inputs_and_secret_redaction() -> None:
    snapshot = {
        "namespace": "app",
        "statefulSets": [{"metadata": {"name": "db", "namespace": "app"}}],
        "nodes": [{"metadata": {"name": "node"}}],
        "secrets": [
            {
                "metadata": {"name": "auth", "namespace": "app", "annotations": {"unsafe": "CANARY"}},
                "data": {"password": "CANARY"},
                "stringData": {"token": "CANARY"},
            }
        ],
    }
    with FakeKubernetesAPIServer(snapshot) as server:
        assert server.read("/apis/apps/v1/namespaces/app/statefulsets")[0] == 200
        assert server.read("/api/v1/nodes")[1]["items"][0]["metadata"]["name"] == "node"
        assert "CANARY" not in json.dumps(list(server.objects.values()))
        server.set_kubelet("node", {"pods": []}, "# synthetic\n")
        assert server.read("/api/v1/nodes/node/proxy/stats/summary") == (200, {"pods": []})
        assert server.read("/api/v1/nodes/node/proxy/metrics/cadvisor") == (200, "# synthetic\n")


_NEW_KINDS = [
    ("apps/v1", "statefulsets", "StatefulSet", "statefulSets"),
    ("apps/v1", "daemonsets", "DaemonSet", "daemonSets"),
    ("batch/v1", "jobs", "Job", "jobs"),
    ("batch/v1", "cronjobs", "CronJob", "cronJobs"),
    ("v1", "persistentvolumeclaims", "PersistentVolumeClaim", "persistentVolumeClaims"),
    ("v1", "serviceaccounts", "ServiceAccount", "serviceAccounts"),
    ("autoscaling/v2", "horizontalpodautoscalers", "HorizontalPodAutoscaler", "horizontalPodAutoscalers"),
    ("policy/v1", "poddisruptionbudgets", "PodDisruptionBudget", "podDisruptionBudgets"),
    ("networking.k8s.io/v1", "ingresses", "Ingress", "ingresses"),
    ("v1", "nodes", "Node", "nodes"),
    ("v1", "secrets", "Secret", "secrets"),
]


@pytest.mark.parametrize(("version", "plural", "kind", "collection"), _NEW_KINDS)
def test_new_kinds_list_watch_and_patch(version: str, plural: str, kind: str, collection: str) -> None:
    item = {"metadata": {"name": "test", "labels": {"phase": "initial"}}}
    if kind != "Node":
        item["metadata"]["namespace"] = "app"
    with FakeKubernetesAPIServer({"namespace": "app", collection: [item]}) as server:
        prefix = "/api/v1" if version == "v1" else f"/apis/{version}"
        path = prefix + ("" if kind == "Node" else "/namespaces/app") + f"/{plural}"
        status, listing = server.read(path)
        assert status == 200
        assert listing["kind"] == kind + "List"
        assert len(listing["items"]) == 1
        revision = listing["metadata"]["resourceVersion"]
        status, updated = server.patch(path + "/test", {"metadata": {"labels": {"phase": "recovered"}}})
        assert status == 200
        assert updated["metadata"]["labels"]["phase"] == "recovered"
        host, port = server._http.server_address[:2]
        with urlopen(f"http://{host}:{port}{path}?watch=true&resourceVersion={revision}", timeout=2) as response:
            event = json.loads(response.readline())
        assert event["type"] == "MODIFIED"
        assert event["object"]["kind"] == kind
        assert event["object"]["metadata"]["labels"]["phase"] == "recovered"
        assert int(event["object"]["metadata"]["resourceVersion"]) > int(revision)
        discovered = {resource["name"]: resource for resource in server.read(prefix)[1]["resources"]}
        assert discovered[plural]["namespaced"] == (kind != "Node")


def test_secret_updates_cannot_reintroduce_values() -> None:
    with FakeKubernetesAPIServer({"namespace": "app", "secrets": [{"metadata": {"name": "auth"}}]}) as server:
        status, secret = server.patch(
            "/api/v1/namespaces/app/secrets/auth",
            {
                "data": {"password": "CANARY"},
                "stringData": {"token": "CANARY"},
                "metadata": {"annotations": {"last-applied": "CANARY"}},
            },
        )
        assert status == 200
        assert set(secret["data"]) == {"password", "token"}
        assert "CANARY" not in json.dumps(secret)
        assert "CANARY" not in json.dumps(server._events)


def test_kubelet_endpoints_are_node_scoped_and_unconfigured_is_unavailable() -> None:
    with FakeKubernetesAPIServer({"namespace": "app"}) as server:
        server.set_kubelet("one", {"pods": []}, "# one\n")
        server.set_kubelet("two", {"pods": [{"podRef": {"name": "two"}}]}, "# two\n")
        assert server.read("/api/v1/nodes/missing/proxy/stats/summary")[0] == 404
        host, port = server._http.server_address[:2]
        for node in ("one", "two"):
            with urlopen(f"http://{host}:{port}/api/v1/nodes/{node}/proxy/metrics/cadvisor", timeout=2) as response:
                assert response.read().decode() == f"# {node}\n"


def test_go_secret_metadata_snapshot_is_replayed_without_values() -> None:
    with FakeKubernetesAPIServer(
        {
            "namespace": "app",
            "secrets": [{"name": "auth", "namespace": "app", "type": "Opaque", "dataKeys": ["password"]}],
        }
    ) as server:
        status, secret = server.read("/api/v1/namespaces/app/secrets/auth")
        assert status == 200
        assert secret["data"] == {"password": ""}
=======
def test_fake_api_serves_objects_only_capture() -> None:
    snapshot = {
        "object_snapshot_version": 2,
        "namespace": "demo",
        "objects": [
            {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "web", "namespace": "demo"}},
            {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "other", "namespace": "elsewhere"}},
            {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": "web", "namespace": "demo"}},
        ],
    }
    with FakeKubernetesAPIServer(snapshot) as server:
        status, pods = server.read("/api/v1/namespaces/demo/pods")
        _, deployment = server.read("/apis/apps/v1/namespaces/demo/deployments/web")
    assert status == 200
    assert [item["metadata"]["name"] for item in pods["items"]] == ["web"]
    assert deployment["kind"] == "Deployment"
>>>>>>> origin/vic-lsh/dev/sdo-improve
