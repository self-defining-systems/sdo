"""Restore only the application namespace between fastloop incidents."""

from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

from benchmarks.sregym.adapter import kubectl

APPLICATION_BASELINE_KEY = "fastloop_application"
NON_APPLICATION_NAMESPACES = frozenset(
    {
        "observe",
        "observability",
        "monitoring",
        "prometheus",
        "jaeger",
        "loki",
        "sregym",
        "kube-system",
        "kube-public",
        "kube-node-lease",
    }
)
WORKLOAD_KINDS = frozenset(
    {
        "Deployment",
        "StatefulSet",
        "Service",
        "ConfigMap",
        "Job",
        "CronJob",
        "Pod",
        "NetworkPolicy",
        "PersistentVolumeClaim",
    }
)
_RESOURCE_NAMES = (
    "deployments,statefulsets,services,configmaps,jobs,cronjobs,pods,networkpolicies,persistentvolumeclaims"
)

if TYPE_CHECKING:
    from pathlib import Path


class ApplicationCluster(Protocol):
    def list_objects(self, namespace: str) -> list[dict[str, Any]]: ...

    def delete(self, namespace: str, kind: str, name: str) -> None: ...

    def apply(self, namespace: str, value: dict[str, Any]) -> None: ...


class KubectlApplicationCluster:
    def __init__(self, kubeconfig: Path) -> None:
        self.kubeconfig = kubeconfig

    def list_objects(self, namespace: str) -> list[dict[str, Any]]:
        output = kubectl(["get", _RESOURCE_NAMES, "-o", "json"], namespace=namespace, kubeconfig=self.kubeconfig).stdout
        return json.loads(output)["items"]

    def delete(self, namespace: str, kind: str, name: str) -> None:
        kubectl(
            ["delete", f"{kind.lower()}/{name}", "--ignore-not-found=true", "--wait=true"],
            namespace=namespace,
            kubeconfig=self.kubeconfig,
        )

    def apply(self, namespace: str, value: dict[str, Any]) -> None:
        kubectl(["apply", "-f", "-"], namespace=namespace, kubeconfig=self.kubeconfig, input_text=json.dumps(value))


@dataclass(frozen=True)
class ApplicationBaseline:
    namespace: str
    objects: list[dict[str, Any]]


@dataclass
class RecoveryCleanup:
    deleted_counts: dict[str, int] = field(default_factory=dict)
    restored: list[str] = field(default_factory=list)
    skipped_reason: str | None = None


def _harness_owned(value: dict[str, Any]) -> bool:
    metadata = value.get("metadata", {})
    name = metadata.get("name", "")
    if name.startswith(("wrk2-", "failure-admin-")) or name == "kube-root-ca.crt":
        return True
    if metadata.get("labels", {}).get("job") == "workload":
        return True
    return any(owner.get("name", "").startswith("wrk2-") for owner in metadata.get("ownerReferences", []))


def _managed(value: dict[str, Any], namespace: str) -> bool:
    metadata = value.get("metadata", {})
    return (
        value.get("kind") in WORKLOAD_KINDS
        and metadata.get("namespace") == namespace
        and isinstance(metadata.get("name"), str)
        and (value.get("kind") != "Pod" or not metadata.get("ownerReferences"))
        and not _harness_owned(value)
    )


def _manifest(value: dict[str, Any]) -> dict[str, Any]:
    result = deepcopy(value)
    result.pop("status", None)
    metadata = result["metadata"]
    for key in ("uid", "resourceVersion", "generation", "creationTimestamp", "managedFields", "selfLink"):
        metadata.pop(key, None)
    return result


def _identity(value: dict[str, Any]) -> tuple[str, str]:
    return value["kind"], value["metadata"]["name"]


def _content(value: dict[str, Any]) -> tuple[Any, Any, Any]:
    return value.get("spec"), value.get("data"), value.get("binaryData")


def capture_application_baseline(cluster: ApplicationCluster, namespace: str) -> ApplicationBaseline:
    if not namespace or namespace.endswith("-sdo") or namespace in NON_APPLICATION_NAMESPACES:
        raise ValueError(f"refusing to capture non-application namespace {namespace!r}")
    objects = [_manifest(value) for value in cluster.list_objects(namespace) if _managed(value, namespace)]
    return ApplicationBaseline(namespace=namespace, objects=objects)


def application_namespaces(path: Path, kubeconfig: Path, primary: str) -> tuple[str, ...]:
    """Find app namespaces created after SREGym's infrastructure snapshot."""

    infrastructure = set(json.loads(path.read_text(encoding="utf-8")).get("namespaces", []))
    listing = json.loads(kubectl(["get", "namespaces", "-o", "json"], namespace=None, kubeconfig=kubeconfig).stdout)
    created = {
        value["metadata"]["name"] for value in listing["items"] if value["metadata"]["name"] not in infrastructure
    }
    return tuple(
        sorted(
            {
                primary,
                *(name for name in created if not name.endswith("-sdo") and name not in NON_APPLICATION_NAMESPACES),
            }
        )
    )


def save_application_baseline(path: Path, baselines: tuple[ApplicationBaseline, ...]) -> None:
    document = json.loads(path.read_text(encoding="utf-8"))
    document[APPLICATION_BASELINE_KEY] = {
        "namespaces": [{"namespace": baseline.namespace, "objects": baseline.objects} for baseline in baselines]
    }
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def load_application_baseline(path: Path, namespace: str) -> tuple[ApplicationBaseline, ...] | None:
    if not path.is_file():
        return None
    value = json.loads(path.read_text(encoding="utf-8")).get(APPLICATION_BASELINE_KEY)
    if not isinstance(value, dict) or not isinstance(value.get("namespaces"), list):
        return None
    namespaces = value["namespaces"]
    if not any(item.get("namespace") == namespace for item in namespaces if isinstance(item, dict)):
        return None
    if not all(isinstance(item, dict) and isinstance(item.get("objects"), list) for item in namespaces):
        return None
    return tuple(ApplicationBaseline(item["namespace"], item["objects"]) for item in namespaces)


def reconcile_application(
    cluster: ApplicationCluster, baseline: ApplicationBaseline, namespace: str
) -> RecoveryCleanup:
    if baseline.namespace != namespace or namespace.endswith("-sdo") or namespace in NON_APPLICATION_NAMESPACES:
        raise ValueError(f"refusing to reconcile non-application namespace {namespace!r}")
    cleanup = RecoveryCleanup()
    expected = {_identity(value): value for value in baseline.objects if _managed(value, namespace)}
    current = {_identity(value): value for value in cluster.list_objects(namespace) if _managed(value, namespace)}
    for (kind, name), value in sorted(current.items()):
        if (kind, name) in expected:
            continue
        if kind == "PersistentVolumeClaim" and value.get("status", {}).get("phase") != "Pending":
            continue
        cluster.delete(namespace, kind, name)
        cleanup.deleted_counts[kind] = cleanup.deleted_counts.get(kind, 0) + 1
    for (kind, name), value in sorted(expected.items()):
        present = current.get((kind, name))
        if present is None or _content(present) != _content(value):
            cluster.apply(namespace, value)
            cleanup.restored.append(f"{kind}/{name}")
    return cleanup
