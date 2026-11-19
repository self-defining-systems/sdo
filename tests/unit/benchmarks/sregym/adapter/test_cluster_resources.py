"""Benchmark cluster resources stay outside the production installer."""

from __future__ import annotations

import json
import subprocess
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path

from benchmarks.sregym.adapter.cluster_resources import (
    BenchmarkClusterResourcesConfig,
    delete_submission_bridge,
    prepare_benchmark_resources,
    wait_submission_bridge,
)


def _config(tmp_path: Path) -> BenchmarkClusterResourcesConfig:
    return BenchmarkClusterResourcesConfig(
        namespace="hotel",
        control_namespace="hotel-sdo",
        responder_image="responder:benchmark",
        credentials_secret="agent-credentials",
        registry_push_secret="registry-push",
        registry_insecure=True,
        kind_cluster_name="sregym-w0",
    )


def test_preparation_isolates_registry_secret_and_maps_every_kind_node(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("FASTLOOP_DOCKERHUB_AUTH_FILE", raising=False)  # hermetic: never read the host's credentials
    config = _config(tmp_path)
    calls: list[tuple[list[str], str | None]] = []

    def run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        payload = kwargs.get("input")
        calls.append((command, payload if isinstance(payload, str) else None))
        if command[:3] == ["kind", "get", "nodes"]:
            return subprocess.CompletedProcess(command, 0, "sregym-w0-control-plane\nsregym-w0-worker\n", "")
        if "get" in command and "nodes" in command:
            return subprocess.CompletedProcess(
                command,
                0,
                json.dumps(
                    {
                        "items": [
                            {"metadata": {"name": "sregym-w0-control-plane"}},
                            {"metadata": {"name": "sregym-w0-worker"}},
                        ]
                    }
                ),
                "",
            )
        if "get" in command and "service/sdo-benchmark-registry" in command:
            return subprocess.CompletedProcess(command, 0, json.dumps({"spec": {"clusterIP": "10.96.12.3"}}), "")
        if "get" in command and "deployment/sdo-benchmark-registry" in command:
            return subprocess.CompletedProcess(command, 0, json.dumps({"status": {"readyReplicas": 1}}), "")
        if command[:2] == ["docker", "exec"] and "config" in command:
            return subprocess.CompletedProcess(command, 0, "config_path = '/etc/containerd/certs.d'", "")
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(subprocess, "run", run)
    prepare_benchmark_resources(
        config, relay_target_base="http://host.docker.internal:8123", kubeconfig=tmp_path / "lane.kubeconfig"
    )

    applied = [json.loads(payload) for command, payload in calls if "apply" in command and payload is not None]
    assert applied
    resources = [item for document in applied for item in document["items"]]
    secret = next(item for item in resources if item["kind"] == "Secret")
    assert secret["metadata"]["name"] == "registry-push"
    assert secret["type"] == "kubernetes.io/dockerconfigjson"
    assert secret["stringData"][".dockerconfigjson"] == '{"auths":{}}'
    assert "agent-credentials" not in json.dumps(resources)
    registry = next(
        item
        for item in resources
        if item["kind"] == "Deployment" and item["metadata"]["name"] == "sdo-benchmark-registry"
    )
    assert registry["spec"]["template"]["spec"]["automountServiceAccountToken"] is False
    bridge = next(
        item for item in resources if item["kind"] == "Deployment" and item["metadata"]["name"] == "sdo-sregym-bridge"
    )
    assert bridge["spec"]["template"]["spec"]["hostNetwork"] is True
    assert bridge["spec"]["template"]["spec"]["automountServiceAccountToken"] is False
    assert bridge["spec"]["template"]["spec"]["securityContext"]["runAsNonRoot"] is True
    assert bridge["spec"]["template"]["spec"]["containers"][0]["securityContext"]["readOnlyRootFilesystem"] is True
    mappings = [(command, payload) for command, payload in calls if command[:2] == ["docker", "exec"] and payload]
    assert len(mappings) == 2
    assert all("10.96.12.3:5000" in payload for _, payload in mappings)
    assert all(config.registry_host in payload for _, payload in mappings)


def test_preparation_refuses_foreign_nodes_before_node_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        if command[:3] == ["kind", "get", "nodes"]:
            return subprocess.CompletedProcess(command, 0, "sregym-w0-control-plane\n", "")
        if "get" in command and "nodes" in command:
            return subprocess.CompletedProcess(
                command, 0, json.dumps({"items": [{"metadata": {"name": "another-worker"}}]}), ""
            )
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(subprocess, "run", run)
    with pytest.raises(RuntimeError, match="kind node mismatch"):
        prepare_benchmark_resources(_config(tmp_path), relay_target_base=None, kubeconfig=tmp_path / "lane.kubeconfig")
    assert not any(command[:2] == ["docker", "exec"] for command in calls)


def test_registry_requires_separate_secret_and_explicit_kind_http_opt_in(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="separate"):
        BenchmarkClusterResourcesConfig(
            namespace="hotel",
            control_namespace="hotel-sdo",
            responder_image="responder:benchmark",
            credentials_secret="same",
            registry_push_secret="same",
            registry_insecure=True,
            kind_cluster_name="sregym-w0",
        )
    with pytest.raises(ValueError, match="registry_insecure"):
        BenchmarkClusterResourcesConfig(
            namespace="hotel",
            control_namespace="hotel-sdo",
            responder_image="responder:benchmark",
            credentials_secret="agents",
            registry_push_secret="registry",
            registry_insecure=False,
            kind_cluster_name="sregym-w0",
        )


def test_bridge_wait_and_cleanup_are_namespace_scoped(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, json.dumps({"status": {"readyReplicas": 1}}), "")

    monkeypatch.setattr(subprocess, "run", run)
    kubeconfig = tmp_path / "lane.kubeconfig"
    wait_submission_bridge("hotel-sdo", kubeconfig=kubeconfig)
    delete_submission_bridge("hotel-sdo", kubeconfig=kubeconfig)
    assert all(
        command[:5] == ["kubectl", "--kubeconfig", str(kubeconfig), "--namespace", "hotel-sdo"] for command in calls
    )
    assert any("deployment/sdo-sregym-bridge" in command for command in calls)


def _push_secret_payload(tmp_path: Path) -> dict[str, object]:
    from benchmarks.sregym.adapter.cluster_resources import _registry_resources

    secret = next(item for item in _registry_resources(_config(tmp_path)) if item["kind"] == "Secret")
    return json.loads(secret["stringData"][".dockerconfigjson"])


def test_registry_push_secret_carries_only_docker_hub_auth_from_the_named_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """In-cluster BuildKit pulls base images anonymously otherwise and is rate-limited per host IP."""
    docker_config = tmp_path / "config.json"
    docker_config.write_text(
        json.dumps(
            {
                "auths": {
                    "https://index.docker.io/v1/": {"auth": "c3ludGhldGljOnN5bnRoZXRpYw=="},
                    "ghcr.io": {"auth": "b3RoZXI6b3RoZXI="},
                },
                "credsStore": "desktop",
            }
        )
    )
    monkeypatch.setenv("FASTLOOP_DOCKERHUB_AUTH_FILE", str(docker_config))
    payload = _push_secret_payload(tmp_path)
    assert payload == {"auths": {"https://index.docker.io/v1/": {"auth": "c3ludGhldGljOnN5bnRoZXRpYw=="}}}


def test_registry_push_secret_stays_empty_without_the_auth_file_setting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("FASTLOOP_DOCKERHUB_AUTH_FILE", raising=False)
    assert _push_secret_payload(tmp_path) == {"auths": {}}


@pytest.mark.parametrize("contents", [None, "not json", "[]", '{"auths": []}'])
def test_registry_push_secret_fails_closed_on_an_unusable_auth_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, contents: str | None
) -> None:
    docker_config = tmp_path / "config.json"
    if contents is not None:
        docker_config.write_text(contents)
    monkeypatch.setenv("FASTLOOP_DOCKERHUB_AUTH_FILE", str(docker_config))
    with pytest.raises(ValueError, match="FASTLOOP_DOCKERHUB_AUTH_FILE"):
        _push_secret_payload(tmp_path)
