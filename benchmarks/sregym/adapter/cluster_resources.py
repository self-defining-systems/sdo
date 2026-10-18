"""Disposable SREGym kind resources for the native Go operator.

The registry and submission relay are benchmark transport. Production SDO
receives only their typed endpoint and Secret names.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_DNS_LABEL = re.compile(r"^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$")
_KIND_NODE = re.compile(r"^[a-z0-9][a-z0-9-]*$")
_REGISTRY_NAME = "sdo-benchmark-registry"
_BRIDGE_NAME = "sdo-sregym-bridge"


@dataclass(frozen=True)
class BenchmarkRegistryTarget:
    host: str
    insecure: bool

    @property
    def image_prefix(self) -> str:
        return self.host + "/"


@dataclass(frozen=True)
class BenchmarkClusterResourcesConfig:
    namespace: str
    control_namespace: str
    responder_image: str
    credentials_secret: str
    registry_push_secret: str
    registry_insecure: bool
    kind_cluster_name: str

    def __post_init__(self) -> None:
        for label, value in (
            ("namespace", self.namespace),
            ("control_namespace", self.control_namespace),
            ("credentials_secret", self.credentials_secret),
            ("registry_push_secret", self.registry_push_secret),
        ):
            if not _DNS_LABEL.fullmatch(value) or len(value) > 63:
                raise ValueError(f"{label} must be a Kubernetes DNS label")
        if not self.responder_image.strip():
            raise ValueError("responder_image must be non-empty")
        if self.registry_push_secret == self.credentials_secret:
            raise ValueError("registry push Secret must be separate from agent credentials")
        if not self.registry_insecure:
            raise ValueError("kind HTTP registry requires explicit registry_insecure opt-in")
        if not _KIND_NODE.fullmatch(self.kind_cluster_name):
            raise ValueError("kind_cluster_name is invalid")

    @property
    def registry_host(self) -> str:
        return f"{_REGISTRY_NAME}.{self.control_namespace}.svc.cluster.local:5000"

    @property
    def registry_target(self) -> BenchmarkRegistryTarget:
        return BenchmarkRegistryTarget(host=self.registry_host, insecure=self.registry_insecure)


def _trusted_kubeconfig(kubeconfig: Path | None) -> Path:
    chosen = kubeconfig or Path(os.environ.get("SREGYM_BASE_KUBECONFIG") or os.environ.get("KUBECONFIG", ""))
    if not chosen.is_absolute() or str(chosen) == ".":
        raise ValueError("benchmark cluster mutation requires an absolute trusted kubeconfig")
    return chosen


def _run(command: list[str], *, input_text: str | None = None) -> str:
    try:
        completed = subprocess.run(
            command,
            input=input_text,
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError(f"benchmark cluster command could not run: {exc}") from exc
    if completed.returncode:
        raise RuntimeError(f"benchmark cluster command failed: {(completed.stderr or completed.stdout)[-2048:]}")
    return completed.stdout


def kubectl(
    args: list[str],
    *,
    namespace: str | None,
    kubeconfig: Path | None = None,
    input_text: str | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Run benchmark-scoped kubectl against the trusted lane kubeconfig."""

    trusted = _trusted_kubeconfig(kubeconfig)
    command = ["kubectl", "--kubeconfig", str(trusted)]
    if namespace is not None:
        if not _DNS_LABEL.fullmatch(namespace):
            raise ValueError("namespace must be a Kubernetes DNS label")
        command.extend(("--namespace", namespace))
    try:
        completed = subprocess.run(
            [*command, *args],
            input=input_text,
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError(f"benchmark kubectl could not run: {exc}") from exc
    if check and completed.returncode:
        detail = (completed.stderr or completed.stdout)[-2048:]
        raise RuntimeError(f"benchmark kubectl {' '.join(args)} failed: {detail}")
    return completed


def _kubectl(kubeconfig: Path, namespace: str | None, *args: str, input_text: str | None = None) -> str:
    return kubectl(list(args), namespace=namespace, kubeconfig=kubeconfig, input_text=input_text).stdout


def _json_result(value: str, subject: str) -> dict[str, Any]:
    try:
        document = json.loads(value)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{subject} returned invalid JSON") from exc
    if not isinstance(document, dict):
        raise RuntimeError(f"{subject} returned no JSON object")
    return document


def _kind_nodes(config: BenchmarkClusterResourcesConfig, kubeconfig: Path) -> list[str]:
    kind_nodes = _run(["kind", "get", "nodes", "--name", config.kind_cluster_name]).splitlines()
    if not kind_nodes or any(not _KIND_NODE.fullmatch(node) for node in kind_nodes):
        raise RuntimeError("kind returned invalid node identities")
    cluster = _json_result(_kubectl(kubeconfig, None, "get", "nodes", "-o", "json"), "Kubernetes node query")
    items = cluster.get("items")
    if not isinstance(items, list):
        raise RuntimeError("Kubernetes node query has no items")
    observed = {
        item.get("metadata", {}).get("name")
        for item in items
        if isinstance(item, dict) and isinstance(item.get("metadata"), dict)
    }
    if observed != set(kind_nodes) or any(not node.startswith(config.kind_cluster_name + "-") for node in kind_nodes):
        raise RuntimeError("kind node mismatch: trusted kubeconfig does not reach the selected benchmark cluster")
    return kind_nodes


def _registry_resources(config: BenchmarkClusterResourcesConfig) -> list[dict[str, Any]]:
    labels = {"app.kubernetes.io/name": _REGISTRY_NAME}
    namespace = config.control_namespace
    return [
        {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": namespace}},
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": config.registry_push_secret, "namespace": namespace},
            "type": "kubernetes.io/dockerconfigjson",
            "stringData": {".dockerconfigjson": '{"auths":{}}'},
        },
        {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {"name": _REGISTRY_NAME, "namespace": namespace},
            "spec": {"accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": "5Gi"}}},
        },
        {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": _REGISTRY_NAME, "namespace": namespace},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": labels},
                "template": {
                    "metadata": {"labels": labels},
                    "spec": {
                        "automountServiceAccountToken": False,
                        "containers": [
                            {
                                "name": "registry",
                                "image": "registry:2",
                                "ports": [{"containerPort": 5000, "name": "registry"}],
                                "readinessProbe": {"httpGet": {"path": "/v2/", "port": 5000}, "periodSeconds": 2},
                                "volumeMounts": [{"name": "data", "mountPath": "/var/lib/registry"}],
                            }
                        ],
                        "volumes": [{"name": "data", "persistentVolumeClaim": {"claimName": _REGISTRY_NAME}}],
                    },
                },
            },
        },
        {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": _REGISTRY_NAME, "namespace": namespace},
            "spec": {"selector": labels, "ports": [{"name": "registry", "port": 5000, "targetPort": 5000}]},
        },
    ]


def _bridge_resources(config: BenchmarkClusterResourcesConfig, relay_target_base: str) -> list[dict[str, Any]]:
    if not relay_target_base.startswith(("http://", "https://")):
        raise ValueError("relay_target_base must be an HTTP URL")
    labels = {"app.kubernetes.io/name": _BRIDGE_NAME}
    namespace = config.control_namespace
    return [
        {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": _BRIDGE_NAME, "namespace": namespace},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": labels},
                "template": {
                    "metadata": {"labels": labels},
                    "spec": {
                        "hostNetwork": True,
                        "dnsPolicy": "ClusterFirstWithHostNet",
                        "automountServiceAccountToken": False,
                        "securityContext": {
                            "runAsNonRoot": True,
                            "runAsUser": 65532,
                            "fsGroup": 65532,
                            "seccompProfile": {"type": "RuntimeDefault"},
                        },
                        "containers": [
                            {
                                "name": "relay",
                                "image": config.responder_image,
                                "imagePullPolicy": "IfNotPresent",
                                "command": ["python3", "-m", "benchmarks.sregym.adapter.submission_relay"],
                                "args": ["--listen-port", "18000", "--target-base", relay_target_base],
                                "ports": [{"name": "http", "containerPort": 18000, "protocol": "TCP"}],
                                "readinessProbe": {"httpGet": {"path": "/healthz", "port": 18000}, "periodSeconds": 2},
                                "securityContext": {
                                    "allowPrivilegeEscalation": False,
                                    "readOnlyRootFilesystem": True,
                                    "capabilities": {"drop": ["ALL"]},
                                },
                                "resources": {
                                    "requests": {"cpu": "10m", "memory": "32Mi"},
                                    "limits": {"cpu": "100m", "memory": "128Mi"},
                                },
                            }
                        ],
                    },
                },
            },
        },
        {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": _BRIDGE_NAME, "namespace": namespace},
            "spec": {"selector": labels, "ports": [{"name": "http", "port": 8000, "targetPort": 18000}]},
        },
    ]


def _wait_ready(name: str, namespace: str, kubeconfig: Path, *, timeout_seconds: float = 120) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_status = "not observed"
    while time.monotonic() < deadline:
        deployment = _json_result(
            _kubectl(kubeconfig, namespace, "get", f"deployment/{name}", "-o", "json"),
            name,
        )
        status = deployment.get("status")
        if isinstance(status, dict) and status.get("readyReplicas") == 1:
            return
        last_status = json.dumps(status)
        time.sleep(1)
    raise RuntimeError(f"{name} did not become ready: {last_status}")


def _configure_kind_nodes(config: BenchmarkClusterResourcesConfig, nodes: list[str], cluster_ip: str) -> None:
    if not re.fullmatch(r"(?:\d{1,3}\.){3}\d{1,3}", cluster_ip):
        raise RuntimeError("registry Service lacks a valid IPv4 ClusterIP")
    host = config.registry_host
    file_path = f"/etc/containerd/certs.d/{host}/hosts.toml"
    hosts = f'server = "http://{host}"\n[host."http://{cluster_ip}:5000"]\n  capabilities = ["pull", "resolve"]\n'
    for node in nodes:
        runtime_config = _run(["docker", "exec", node, "containerd", "config", "dump"])
        if "/etc/containerd/certs.d" not in runtime_config:
            raise RuntimeError(f"kind node {node} does not load containerd registry hosts")
        _run(
            ["docker", "exec", "-i", node, "sh", "-ec", f"mkdir -p /etc/containerd/certs.d/{host}; cat > {file_path}"],
            input_text=hosts,
        )


def prepare_benchmark_resources(
    config: BenchmarkClusterResourcesConfig,
    *,
    relay_target_base: str | None,
    kubeconfig: Path | None,
) -> None:
    """Provision transport in the selected disposable kind lane, then wire node pulls."""

    trusted = _trusted_kubeconfig(kubeconfig)
    nodes = _kind_nodes(config, trusted)
    resources = _registry_resources(config)
    if relay_target_base is not None:
        resources.extend(_bridge_resources(config, relay_target_base))
    _kubectl(
        trusted,
        None,
        "apply",
        "-f",
        "-",
        input_text=json.dumps({"apiVersion": "v1", "kind": "List", "items": resources}),
    )
    _wait_ready(_REGISTRY_NAME, config.control_namespace, trusted)
    service = _json_result(
        _kubectl(trusted, config.control_namespace, "get", f"service/{_REGISTRY_NAME}", "-o", "json"),
        "benchmark registry Service",
    )
    spec = service.get("spec")
    cluster_ip = spec.get("clusterIP") if isinstance(spec, dict) else None
    if not isinstance(cluster_ip, str):
        raise RuntimeError("benchmark registry Service has no ClusterIP")
    _configure_kind_nodes(config, nodes, cluster_ip)


def wait_submission_bridge(control_namespace: str, kubeconfig: Path | None = None) -> None:
    if not _DNS_LABEL.fullmatch(control_namespace):
        raise ValueError("control namespace must be a Kubernetes DNS label")
    _wait_ready(_BRIDGE_NAME, control_namespace, _trusted_kubeconfig(kubeconfig))


def delete_submission_bridge(control_namespace: str, kubeconfig: Path | None = None) -> None:
    if not _DNS_LABEL.fullmatch(control_namespace):
        raise ValueError("control namespace must be a Kubernetes DNS label")
    _kubectl(
        _trusted_kubeconfig(kubeconfig),
        control_namespace,
        "delete",
        f"deployment/{_BRIDGE_NAME}",
        f"service/{_BRIDGE_NAME}",
        "--ignore-not-found=true",
        "--wait=false",
    )
