"""Restore only the application namespace between fastloop incidents."""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Protocol

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
    }
)
_RESOURCE_NAMES = (
    "deployments,daemonsets,statefulsets,services,configmaps,jobs,cronjobs,pods,networkpolicies,"
    "persistentvolumeclaims,serviceaccounts,roles,rolebindings"
)


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
        allocated_paths = _allocated_paths(value)
        value = _manifest(value)
        resource = f"{value['kind'].lower()}/{value['metadata']['name']}"
        output = kubectl(
            ["get", resource, "-o", "json", "--ignore-not-found=true"],
            namespace=namespace,
            kubeconfig=self.kubeconfig,
        ).stdout
        if output.strip():
            current = json.loads(output)
            for path in allocated_paths:
                _replace_allocated(value, current, path)
            # SSA merges fields owned by other managers. In particular, restoring
            # an EnvVar literal can retain the injector's valueFrom and fail API
            # validation. Replace complete captured content, guarded against races.
            patch = [
                {"op": "test", "path": "/metadata/resourceVersion", "value": current["metadata"]["resourceVersion"]}
            ]
            for key in RESTORED_FIELDS:
                if key in value:
                    patch.append({"op": "add", "path": f"/{key}", "value": value[key]})
                elif key in current:
                    patch.append({"op": "remove", "path": f"/{key}"})
            # kubectl patch reads --patch-file as a filename, including a literal
            # "-". Keep baseline values out of command arguments in a private file.
            with TemporaryDirectory(prefix="sdo-baseline-") as directory:
                patch_path = Path(directory) / "patch.json"
                patch_path.write_text(json.dumps(patch), encoding="utf-8")
                kubectl(
                    ["patch", resource, "--type=json", "--patch-file", str(patch_path)],
                    namespace=namespace,
                    kubeconfig=self.kubeconfig,
                )
            return
        kubectl(
            ["apply", "--server-side", "--field-manager=sdo-deployer", "--force-conflicts", "-f", "-"],
            namespace=namespace,
            kubeconfig=self.kubeconfig,
            input_text=json.dumps(value),
        )


def _baseline_digest(objects: list[dict[str, Any]]) -> str:
    return hashlib.sha256(json.dumps(objects, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


@dataclass(frozen=True)
class ApplicationBaseline:
    namespace: str
    objects: list[dict[str, Any]]
    installed_digest: str | None = None

    def __post_init__(self) -> None:
        if self.installed_digest is not None and self.installed_digest != _baseline_digest(self.objects):
            raise ValueError("installed baseline digest differs from its sealed manifest set")


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


# Paths shared by capture, comparison, and restore. A wildcard selects list items.
ALLOCATED_FIELDS = (
    ("*", ("status",)),
    *(
        ("*", ("metadata", name))
        for name in ("uid", "resourceVersion", "creationTimestamp", "generation", "managedFields", "selfLink")
    ),
    *(
        ("Service", ("spec", name))
        for name in ("clusterIP", "clusterIPs", "ipFamilies", "ipFamilyPolicy", "healthCheckNodePort")
    ),
    ("Service", ("spec", "ports", "*", "nodePort")),
    ("PersistentVolumeClaim", ("spec", "volumeName")),
    ("Pod", ("spec", "nodeName")),
    ("Job", ("spec", "selector")),
    *(
        ("Job", (*prefix, label))
        for prefix in (("metadata", "labels"), ("spec", "template", "metadata", "labels"))
        for label in ("controller-uid", "batch.kubernetes.io/controller-uid")
    ),
)


def _allocated_paths(value: dict[str, Any]) -> list[tuple[str, ...]]:
    headless = value["kind"] == "Service" and value.get("spec", {}).get("clusterIP") == "None"
    return [
        path
        for kind, path in ALLOCATED_FIELDS
        if kind in ("*", value["kind"]) and not (headless and path in (("spec", "clusterIP"), ("spec", "clusterIPs")))
    ]


def _replace_allocated(target: Any, live: Any, path: tuple[str, ...]) -> None:
    key, *remaining = path
    if key == "*":
        if not isinstance(target, list):
            return
        current = live if isinstance(live, list) else []
        for index, item in enumerate(target):
            # Service ports retain allocations by name when the desired list moves.
            matches = [
                v
                for v in current
                if isinstance(v, dict) and isinstance(item, dict) and item.get("name") and v.get("name") == item["name"]
            ]
            prior = matches[0] if matches else current[index] if index < len(current) else None
            _replace_allocated(item, prior, tuple(remaining))
        return
    if not isinstance(target, dict):
        return
    current = live if isinstance(live, dict) else {}
    if not remaining:
        if key in current:
            target[key] = deepcopy(current[key])
        else:
            target.pop(key, None)
        return
    if key not in target and key in current and isinstance(current[key], dict):
        target[key] = {}
    if key in target:
        _replace_allocated(target[key], current.get(key), tuple(remaining))


def _manifest(value: dict[str, Any]) -> dict[str, Any]:
    result = deepcopy(value)
    for path in _allocated_paths(result):
        _replace_allocated(result, None, path)
    return result


def _identity(value: dict[str, Any]) -> tuple[str, str]:
    return value["kind"], value["metadata"]["name"]


RESTORED_FIELDS = (
    "spec",
    "data",
    "binaryData",
    "immutable",
    "rules",
    "roleRef",
    "subjects",
    "automountServiceAccountToken",
    "imagePullSecrets",
)


def _content(value: dict[str, Any]) -> tuple[Any, ...]:
    value = _manifest(value)
    return tuple(value.get(key) for key in RESTORED_FIELDS)


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


def save_application_baseline(
    path: Path,
    baselines: tuple[ApplicationBaseline, ...],
    *,
    installed: bool = False,
) -> None:
    document = json.loads(path.read_text(encoding="utf-8"))
    document[APPLICATION_BASELINE_KEY] = {
        "namespaces": [
            {
                "namespace": baseline.namespace,
                "objects": baseline.objects,
                **(
                    {"installed_digest": _baseline_digest(baseline.objects)}
                    if installed or baseline.installed_digest
                    else {}
                ),
            }
            for baseline in baselines
        ]
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
    return tuple(
        ApplicationBaseline(item["namespace"], item["objects"], item.get("installed_digest")) for item in namespaces
    )


def reconcile_application(
    cluster: ApplicationCluster, baseline: ApplicationBaseline, namespace: str
) -> RecoveryCleanup:
    if baseline.namespace != namespace or namespace.endswith("-sdo") or namespace in NON_APPLICATION_NAMESPACES:
        raise ValueError(f"refusing to reconcile non-application namespace {namespace!r}")
    from benchmarks.sregym.fastloop.hotel_helpers import retire_helpers, without_helpers

    desired, _ = without_helpers(baseline.objects, namespace)
    listed = cluster.list_objects(namespace)
    retire_helpers(cluster, namespace, objects=listed)
    current_objects, _ = without_helpers(
        [v for v in listed if v.get("kind") != "Pod" or not v.get("metadata", {}).get("ownerReferences")], namespace
    )
    cleanup = RecoveryCleanup()
    expected = {_identity(value): value for value in desired if _managed(value, namespace)}
    current = {_identity(value): value for value in current_objects if _managed(value, namespace)}
    for kind, name in sorted(current):
        if (kind, name) in expected:
            continue
        # A legacy snapshot did not record namespace identity/RBAC. Its absence
        # cannot authorize deleting default or externally provisioned grants.
        if kind in {"PersistentVolumeClaim", "ServiceAccount", "Role", "RoleBinding"}:
            continue
        cluster.delete(namespace, kind, name)
        cleanup.deleted_counts[kind] = cleanup.deleted_counts.get(kind, 0) + 1
    # Namespace prerequisites precede controllers so their first Pods can start.
    priorities = {
        "ServiceAccount": 0,
        "ConfigMap": 0,
        "Role": 0,
        "RoleBinding": 1,
        "Service": 1,
        "PersistentVolumeClaim": 1,
        "NetworkPolicy": 1,
    }
    for (kind, name), value in sorted(expected.items(), key=lambda item: (priorities.get(item[0][0], 2), item[0])):
        present = current.get((kind, name))
        if present is None or _content(present) != _content(value):
            cluster.apply(namespace, value)
            cleanup.restored.append(f"{kind}/{name}")
    return cleanup


def accepted_restore_baselines(
    workspace: Path,
    baselines: tuple[ApplicationBaseline, ...],
    *,
    seed_commit: str | None = None,
) -> tuple[ApplicationBaseline, ...]:
    """Use the sealed installed manifests as the sole source for selected objects.

    Captured runtime-only objects and API defaults remain available. Never apply
    a captured spec followed by a conflicting source spec (a second rollout).
    """
    import yaml

    installed_source = True
    if seed_commit and any(baseline.installed_digest for baseline in baselines):
        changes = subprocess.run(
            ["git", "-C", str(workspace), "diff", "--name-only", seed_commit, "HEAD", "--", ".", ":(exclude).sdo"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout.strip()
        installed_source = not changes
    if installed_source and all(baseline.installed_digest for baseline in baselines):
        return baselines
    root = workspace / ".sdo/rendered"
    lock_path = root / "lock.json"
    if not lock_path.is_file():
        return baselines
    selected = {}
    captured = {
        (value.get("apiVersion"), value["kind"], baseline.namespace, value["metadata"]["name"]): value
        for baseline in baselines
        for value in baseline.objects
    }
    seen = set()
    for row in json.loads(lock_path.read_text())["objects"]:
        reference = row.get("ref", {})
        if reference.get("kind") == "Secret":
            continue
        identity = tuple(reference.get(field) for field in ("api_version", "kind", "namespace", "name"))
        if identity in seen:
            raise ValueError(f"duplicate accepted restore manifest: {identity}")
        seen.add(identity)
        if row.get("error") or not row.get("file") or not row.get("sha256"):
            name = reference.get("name", "")
            known_gap = re.fullmatch(
                rf"(?:Deployment/{re.escape(name)}: )?trusted deployment image evidence missing for container "
                r"\S+ \(init=(?:true|false)\)",
                str(row.get("error", "")),
            )
            if (
                reference.get("kind") != "Deployment"
                or not known_gap
                or "file" in row
                or "sha256" in row
                or identity not in captured
            ):
                raise ValueError(f"unbacked or malformed rendered restore gap: {reference}")
            # A typed evidence gap supplies no replacement spec. Retain the
            # exact installed capture; it is never reconstructed from runtime.
            continue
        path = (root / row["file"]).resolve()
        if not path.is_relative_to(root.resolve()):
            raise ValueError("restore manifest escapes accepted rendered directory")
        payload = path.read_bytes()
        if hashlib.sha256(payload).hexdigest() != row["sha256"]:
            raise ValueError(f"restore manifest digest differs from accepted lock: {path.name}")
        value = yaml.safe_load(payload)
        if value.get("kind") == "Secret":
            continue
        ref = value.get("metadata", {})
        actual = {
            "api_version": value.get("apiVersion"),
            "kind": value.get("kind"),
            "namespace": ref.get("namespace", ""),
            "name": ref.get("name"),
        }
        if any(actual.get(field) != expected for field, expected in reference.items()):
            raise ValueError(f"restore manifest identity differs from accepted lock: {path.name}")
        key = (value["kind"], ref.get("namespace", ""), ref["name"])
        if key in selected:
            raise ValueError(f"duplicate accepted restore manifest: {key}")
        # A source PVC may omit its admission-defaulted immutable class.
        # Retain that installed allocation; an explicit source class still wins.
        installed = captured.get(identity)
        if value["kind"] == "PersistentVolumeClaim" and "storageClassName" not in value.get("spec", {}):
            installed_spec = installed.get("spec", {}) if installed else {}
            if "storageClassName" in installed_spec:
                value = deepcopy(value)
                value.setdefault("spec", {})["storageClassName"] = installed_spec["storageClassName"]
        selected[key] = value
    return tuple(
        baseline
        if baseline.installed_digest and installed_source
        else ApplicationBaseline(
            baseline.namespace,
            [
                deepcopy(selected.get((value["kind"], baseline.namespace, value["metadata"]["name"]), value))
                for value in baseline.objects
            ],
        )
        for baseline in baselines
    )


def stalled_replica_sets(objects: list[dict[str, Any]]) -> list[str]:
    """Generated FailedCreate state can outlive a removed admission fault."""
    unready = {
        row["metadata"]["uid"]
        for row in objects
        if row["kind"] == "Deployment"
        and row.get("status", {}).get("readyReplicas", 0) < row.get("spec", {}).get("replicas", 1)
    }
    return sorted(
        row["metadata"]["name"]
        for row in objects
        if row["kind"] == "ReplicaSet"
        and any(
            owner.get("kind") == "Deployment" and owner.get("controller") is True and owner.get("uid") in unready
            for owner in row["metadata"].get("ownerReferences", [])
        )
        and any(
            condition.get("type") == "ReplicaFailure" and condition.get("status") == "True"
            for condition in row.get("status", {}).get("conditions", [])
        )
    )


def retire_stalled_replicas(kubeconfig: Path, namespace: str) -> None:
    listing = json.loads(
        kubectl(["get", "deployments,replicasets", "-o", "json"], namespace=namespace, kubeconfig=kubeconfig).stdout
    )["items"]
    for name in stalled_replica_sets(listing):
        kubectl(
            ["delete", f"replicaset/{name}", "--cascade=foreground", "--wait=true", "--timeout=20s"],
            namespace=namespace,
            kubeconfig=kubeconfig,
        )


def seal_existing_install(path: Path, cluster: ApplicationCluster, namespace: str) -> None:
    """Upgrade a clean pre-controller installation, never recapture fault state."""
    from benchmarks.sregym.fastloop.hotel_helpers import migrate_baseline, retire_helpers

    migrate_baseline(path, namespace)
    retire_helpers(cluster, namespace)
    baselines = load_application_baseline(path, namespace)
    if not baselines:
        raise ValueError("installed source seal requires the original up capture")
    for baseline in baselines:
        expected = {_identity(v): _content(v) for v in baseline.objects if _managed(v, baseline.namespace)}
        actual = {
            _identity(v): _content(v)
            for v in cluster.list_objects(baseline.namespace)
            if _managed(v, baseline.namespace)
        }
        if actual != expected:
            raise ValueError("installed source seal differs from the original clean up capture")
    save_application_baseline(path, baselines, installed=True)
