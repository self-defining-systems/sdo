"""Benchmark-only enforcement of HotelReservation's script-free app profile.

Only the harness's two helper ConfigMaps and exact appended Deployment mounts
are migratable. Unknown identities fail closed before the first write.
"""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

    from benchmarks.sregym.fastloop.baseline_recovery import ApplicationCluster

NAMESPACE = "hotel-reservation"
HELPERS = {"failure-admin-geo", "failure-admin-rate"}
SCRIPT_KEYS = {
    "remove-admin-mongo.sh",
    *(
        f"{action}-admin-{service}-mongo.sh"
        for service in ("geo", "rate")
        for action in ("revoke", "revoke-mitigate", "remove-mitigate")
    ),
}


def configure_profile(app: Any) -> None:
    """Use the harness's supported ABSENT profile for generic hotel deployment."""
    if app.namespace == NAMESPACE:
        app.failure_scripts = type(app.failure_scripts)("absent")


def _references(value: Any) -> bool:
    if isinstance(value, list):
        return any(_references(item) for item in value)
    if not isinstance(value, dict):
        return False
    if value.get("name") == "failure-script":
        return True
    if any(
        isinstance(value.get(key), dict) and value[key].get("name") in HELPERS
        for key in ("configMap", "configMapRef", "configMapKeyRef")
    ):
        return True
    return any(_references(item) for item in value.values())


def assert_script_free(objects: list[dict[str, Any]], namespace: str) -> None:
    """Read-only admission: reject helpers, copied script keys and references."""
    if namespace != NAMESPACE:
        return
    for value in objects:
        metadata = value.get("metadata", {})
        if metadata.get("namespace") != namespace:
            raise ValueError("eval: helper inspection returned an unexpected namespace")
        if (
            value.get("kind") == "ConfigMap"
            and (
                metadata.get("name") in HELPERS
                or SCRIPT_KEYS.intersection(set(value.get("data", {})) | set(value.get("binaryData", {})))
            )
        ) or _references(value.get("spec", {})):
            raise ValueError(f"eval: hotel helper remains: {value.get('kind')}/{metadata.get('name')}")


def without_helpers(objects: list[dict[str, Any]], namespace: str) -> tuple[list[dict[str, Any]], list[str]]:
    """Pure migration plan; arbitrary application mounts are retained verbatim."""
    if namespace != NAMESPACE:
        return deepcopy(objects), []
    identities = [(value.get("kind"), value.get("metadata", {}).get("name")) for value in objects]
    if len(set(identities)) != len(identities):
        raise ValueError("eval: helper migration encountered duplicate object identities")
    result, removed = [], []
    for original in objects:
        value = deepcopy(original)
        metadata = value.get("metadata", {})
        kind, name = value.get("kind"), metadata.get("name")
        if metadata.get("namespace") != namespace:
            raise ValueError("eval: helper migration encountered an unexpected namespace")
        if kind == "ConfigMap" and name in HELPERS:
            if (
                metadata.get("ownerReferences")
                or metadata.get("finalizers")
                or value.get("binaryData")
                or not set(value.get("data", {})) <= SCRIPT_KEYS
            ):
                raise ValueError(f"eval: helper ConfigMap has unexpected ownership/content: {name}")
            removed.append(f"ConfigMap/{name}")
            continue
        if kind == "Deployment" and name in {"mongodb-geo", "mongodb-rate"}:
            pod = value.get("spec", {}).get("template", {}).get("spec", {})
            volumes = pod.get("volumes", [])
            matching = [v for v in volumes if v.get("name") == "failure-script"]
            if matching:
                expected = {
                    "name": "failure-script",
                    "configMap": {"name": f"failure-admin-{name.removeprefix('mongodb-')}"},
                }
                containers = pod.get("containers", [])
                mount = {"name": "failure-script", "mountPath": "/scripts"}
                mounts = containers[0].get("volumeMounts", []) if containers else []
                if (
                    metadata.get("ownerReferences")
                    or len(matching) != 1
                    or {
                        **matching[0],
                        "configMap": {k: v for k, v in matching[0].get("configMap", {}).items() if k != "defaultMode"},
                    }
                    != expected
                    or matching[0].get("configMap", {}).get("defaultMode", 420) != 420
                    or not containers
                    or containers[0].get("name") != f"hotel-reserv-{name.removeprefix('mongodb-')}-mongo"
                    or [m for m in mounts if m.get("name") == "failure-script"] != [mount]
                ):
                    raise ValueError(f"eval: helper mount has unexpected identity/shape: {name}")
                pod["volumes"] = [v for v in volumes if v.get("name") != "failure-script"]
                containers[0]["volumeMounts"] = [m for m in mounts if m != mount]
                removed.append(f"Deployment/{name}:failure-script")
        result.append(value)
    assert_script_free(result, namespace)
    return result, sorted(removed)


def retire_helpers(
    cluster: ApplicationCluster, namespace: str, *, objects: list[dict[str, Any]] | None = None
) -> list[str]:
    if namespace != NAMESPACE:
        return []
    originals = cluster.list_objects(namespace) if objects is None else objects
    # Generated Pods are observed by admission after rollout, never rewritten.
    candidates = [v for v in originals if v.get("kind") != "Pod" or not v.get("metadata", {}).get("ownerReferences")]
    desired, removed = without_helpers(candidates, namespace)
    expected = {(v["kind"], v["metadata"]["name"]): v for v in desired}
    for value in candidates:
        identity = (value["kind"], value["metadata"]["name"])
        replacement = expected.get(identity)
        if replacement is not None and replacement != value:
            cluster.apply(namespace, replacement)
    for value in candidates:
        if (value["kind"], value["metadata"]["name"]) not in expected:
            cluster.delete(namespace, value["kind"], value["metadata"]["name"])
    return removed


def migrate_baseline(path: Path, namespace: str) -> None:
    """Transform sealed captures without recapturing live state or touching source."""
    if namespace != NAMESPACE or not path.is_file():
        return
    from benchmarks.sregym.fastloop.baseline_recovery import _baseline_digest, load_application_baseline

    original = path.read_bytes()
    baselines = load_application_baseline(path, namespace)  # verifies original seals
    if not baselines:
        return
    document = json.loads(original)
    rows = document["fastloop_application"]["namespaces"]
    for baseline, row in zip(baselines, rows, strict=True):
        desired, removed = without_helpers(baseline.objects, baseline.namespace)
        if not removed:
            continue
        evidence = {
            "contract": "hotel-script-free/v1",
            "source_path": str(path.resolve()),
            "source_sha256": hashlib.sha256(original).hexdigest(),
            "namespace": baseline.namespace,
            "before_digest": _baseline_digest(baseline.objects),
            "after_digest": _baseline_digest(desired),
            "removed": removed,
        }
        document.setdefault("hotel_helper_migrations", []).append(evidence)
        row["objects"] = desired
        if baseline.installed_digest:
            row["installed_digest"] = _baseline_digest(desired)
    if document != json.loads(original):
        temporary = path.with_suffix(".helper.tmp")
        temporary.write_text(json.dumps(document, indent=2) + "\n")
        temporary.replace(path)


def prepare_install(kubeconfig: Path, namespace: str, baseline_path: Path) -> None:
    """Reused install migration via existing first-party ownership-aware API."""
    if namespace != NAMESPACE:
        return
    from benchmarks.sregym.adapter import kubectl
    from benchmarks.sregym.fastloop.baseline_recovery import KubectlApplicationCluster

    migrate_baseline(baseline_path, namespace)
    exists = kubectl(
        ["get", "namespace", namespace, "-o", "name", "--ignore-not-found=true"],
        namespace=None,
        kubeconfig=kubeconfig,
    ).stdout.strip()
    if not exists:
        return  # fresh deployment; API/auth errors still propagate
    removed = retire_helpers(KubectlApplicationCluster(kubeconfig), namespace)
    for identity in removed:
        if identity.startswith("Deployment/"):
            kubectl(
                ["rollout", "status", identity.split(":")[0], "--timeout=60s"],
                namespace=namespace,
                kubeconfig=kubeconfig,
            )
    admit_install(kubeconfig, namespace)


def admit_install(kubeconfig: Path, namespace: str) -> dict[str, Any]:
    if namespace != NAMESPACE:
        return {"profile": "not-hotel", "passed": True}
    from benchmarks.sregym.fastloop.baseline_recovery import KubectlApplicationCluster

    objects = KubectlApplicationCluster(kubeconfig).list_objects(namespace)
    assert_script_free(objects, namespace)
    return {"profile": "hotel-script-free/v1", "passed": True, "objects_checked": len(objects)}
