from __future__ import annotations

import json
from typing import TYPE_CHECKING
from urllib.request import urlopen

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
