"""Read-only object observations for composite faults; no harness verdicts."""

from __future__ import annotations

import json
from collections.abc import Callable
from copy import deepcopy
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from benchmarks.sregym.faults.description import FaultSpec

ObjectReader = Callable[[str, str], dict[str, Any] | None]


def deployment_available(read: ObjectReader, name: str) -> bool:
    deployment = read("deployment", name)
    if not deployment or (deployment.get("metadata") or {}).get("deletionTimestamp"):
        return False
    spec = deployment.get("spec") or {}
    status = deployment.get("status") or {}
    desired = spec.get("replicas")
    desired = 1 if desired is None else int(desired)
    generation = (deployment.get("metadata") or {}).get("generation") or 0
    return (
        desired >= 1
        and int(status.get("observedGeneration") or 0) >= int(generation)
        and int(status.get("readyReplicas") or 0) == desired
        and int(status.get("updatedReplicas") or 0) == desired
        and int(status.get("replicas") or 0) == desired
    )


def hpa_recovered(read: ObjectReader, fault: FaultSpec, component: str) -> bool:
    hpa = read("hpa", fault.parameters["hpa_name"])
    deployment = read("deployment", component)
    if not hpa or not deployment or not deployment_available(lambda *_: deployment, component):
        return False
    target = (hpa.get("spec") or {}).get("scaleTargetRef") or {}
    status = hpa.get("status") or {}
    generation = (hpa.get("metadata") or {}).get("generation")
    if target.get("kind") != "Deployment" or target.get("name") != component or generation is None:
        return False
    if int(status.get("observedGeneration") or 0) < int(generation):
        return False
    containers = deployment["spec"]["template"]["spec"].get("containers") or []
    if not containers:
        return False
    for container in containers:
        value = str(((container.get("resources") or {}).get("requests") or {}).get("cpu", "0"))
        try:
            if not float(value.removesuffix("m")) > 0:
                return False
        except ValueError:
            return False
    active = any(c.get("type") == "ScalingActive" and c.get("status") == "True" for c in status.get("conditions") or [])
    metric = any(
        m.get("type") == "Resource"
        and (m.get("resource") or {}).get("name") == "cpu"
        and isinstance(((m["resource"].get("current") or {}).get("averageUtilization")), (int, float))
        for m in status.get("currentMetrics") or []
    )
    return active and metric


def _quantity_bytes(value: Any) -> Decimal | None:
    text = str(value)
    units = {
        **{f"{u}i": Decimal(1024) ** i for i, u in enumerate("KMGTPE", 1)},
        **{u: Decimal(1000) ** i for i, u in enumerate("kMGTPE", 1)},
        "m": Decimal(".001"),
    }
    try:
        for suffix, multiple in units.items():
            if text.endswith(suffix):
                return Decimal(text[: -len(suffix)]) * multiple
        return Decimal(text)
    except InvalidOperation:
        return None


def shared_memory_recovered(read: ObjectReader, fault: FaultSpec, component: str) -> bool:
    obj = read("deployment", component)
    if not obj or not deployment_available(lambda *_: obj, component):
        return False
    spec = obj["spec"]["template"]["spec"]
    volumes = {v["name"]: v for v in spec.get("volumes") or []}
    required = Decimal(fault.parameters["scratch_mib"]) * 1024 * 1024
    for container in spec.get("containers") or []:
        if container.get("name") != "worker":
            continue
        for mount in container.get("volumeMounts") or []:
            if mount.get("mountPath") != "/dev/shm" or mount.get("readOnly") or mount.get("subPath"):
                continue
            empty_dir = volumes.get(mount.get("name"), {}).get("emptyDir") or {}
            if empty_dir.get("medium") != "Memory":
                continue
            capacity = _quantity_bytes(empty_dir["sizeLimit"]) if "sizeLimit" in empty_dir else Decimal("Infinity")
            if capacity is not None and not capacity.is_nan() and capacity >= required:
                return True
    return False


def finalizer_recovered(read: ObjectReader, fault: FaultSpec, component: str) -> bool:
    params = fault.parameters
    role = read("clusterrole", params["clusterrole_name"])
    binding = read("clusterrolebinding", params["clusterrolebinding_name"])
    controller = read("deployment", params["controller_name"])
    if (
        not role
        or not binding
        or not controller
        or not deployment_available(lambda *_: controller, params["controller_name"])
    ):
        return False
    pod_spec = (((controller.get("spec") or {}).get("template") or {}).get("spec")) or {}
    if pod_spec.get("serviceAccountName", "default") != params["sa_name"]:
        return False
    if not read("serviceaccount", params["sa_name"]):
        return False
    if read("configmap", component) is not None:
        return False
    role_ref = binding.get("roleRef") or {}
    if role_ref.get("kind") != "ClusterRole" or role_ref.get("name") != params["clusterrole_name"]:
        return False
    if not any(
        s.get("kind") == "ServiceAccount"
        and s.get("name") == params["sa_name"]
        and s.get("namespace") == params["namespace"]
        for s in binding.get("subjects") or []
    ):
        return False
    for resource, required in [
        ("configmaps", {"get", "patch", "update"}),
        ("configmaps/finalizers", {"patch", "update"}),
    ]:
        granted: set[str] = set()
        for rule in role.get("rules") or []:
            if not ({"", "*"} & set(rule.get("apiGroups") or [])):
                continue
            if not ({resource, "*"} & set(rule.get("resources") or [])):
                continue
            if rule.get("resourceNames") and component not in rule["resourceNames"]:
                continue
            granted.update(rule.get("verbs") or [])
        if "*" not in granted and not required <= granted:
            return False
    return True


def _native_sidecar(spec: dict[str, Any], primary: str, sidecar: str) -> bool:
    return (
        any(c.get("name") == primary for c in spec.get("containers") or [])
        and not any(c.get("name") == sidecar for c in spec.get("containers") or [])
        and any(
            c.get("name") == sidecar and c.get("restartPolicy") == "Always" for c in spec.get("initContainers") or []
        )
    )


def cronjob_recovered(read: ObjectReader, fault: FaultSpec) -> bool:
    params = fault.parameters
    cronjob = read("cronjob", params["cronjob_name"])
    listing = read("jobs", "")
    if not cronjob or not listing or (cronjob.get("spec") or {}).get("suspend"):
        return False
    uid = (cronjob.get("metadata") or {}).get("uid")
    if not uid or (cronjob.get("metadata") or {}).get("deletionTimestamp"):
        return False
    template = (((cronjob.get("spec") or {}).get("jobTemplate") or {}).get("spec") or {}).get("template") or {}
    if not _native_sidecar(template.get("spec") or {}, params["primary_container"], params["sidecar_container"]):
        return False
    completed = False
    for job in listing.get("items") or []:
        owners = (job.get("metadata") or {}).get("ownerReferences") or []
        if not any(
            o.get("kind") == "CronJob" and o.get("uid") == uid and o.get("name") == params["cronjob_name"]
            for o in owners
        ):
            continue
        status = job.get("status") or {}
        terminal = {c.get("type") for c in status.get("conditions") or [] if c.get("status") == "True"}
        spec = (((job.get("spec") or {}).get("template") or {}).get("spec")) or {}
        native = _native_sidecar(spec, params["primary_container"], params["sidecar_container"])
        if not terminal & {"Complete", "Failed"} and not native:
            return False
        completed |= native and "Complete" in terminal
    return completed


def _selector_matches(labels: dict[str, str], selector: dict[str, Any]) -> bool:
    if not selector.get("matchLabels") and not selector.get("matchExpressions"):
        return False
    if any(labels.get(key) != value for key, value in (selector.get("matchLabels") or {}).items()):
        return False
    for expression in selector.get("matchExpressions") or []:
        key, op, values = expression.get("key"), expression.get("operator"), expression.get("values") or []
        if op == "In" and labels.get(key) in values:
            continue
        if op == "NotIn" and labels.get(key) not in values:
            continue
        if op == "Exists" and key in labels:
            continue
        if op == "DoesNotExist" and key not in labels:
            continue
        return False
    return True


def _pod_memory_profiles(read: ObjectReader, selector: dict[str, Any]) -> list[dict[str, Any]]:
    listing = read("pods", "")
    if listing is None or "items" not in listing:
        raise ValueError("probe needs the admitted Pod inventory")
    profiles = []
    for pod in listing["items"]:
        metadata = pod.get("metadata") or {}
        if metadata.get("deletionTimestamp") or not _selector_matches(metadata.get("labels") or {}, selector):
            continue
        if not any(
            c.get("type") == "Ready" and c.get("status") == "True"
            for c in (pod.get("status") or {}).get("conditions") or []
        ):
            return []
        containers = (pod.get("spec") or {}).get("containers") or []
        profile = {
            c["name"]: tuple(
                ((c.get("resources") or {}).get(field) or {}).get("memory") for field in ("requests", "limits")
            )
            for c in containers
        }
        if not profile or len(profile) != len(containers):
            return []
        profiles.append(profile)
    return profiles


def admitted_memory_probe(read: ObjectReader, component: str) -> Callable[[], bool]:
    baseline = read("deployment", component)
    if not baseline or not deployment_available(lambda *_: baseline, component):
        raise ValueError(f"probe needs an available pre-injection baseline for {component}")
    selector = deepcopy((baseline.get("spec") or {}).get("selector") or {})
    baseline_profiles = _pod_memory_profiles(read, selector)
    if not baseline_profiles or any(profile != baseline_profiles[0] for profile in baseline_profiles):
        raise ValueError(f"probe needs consistent pre-injection admitted Pod resources for {component}")
    expected = deepcopy(baseline_profiles[0])

    def restored() -> bool:
        profiles = _pod_memory_profiles(read, selector)
        return (
            bool(profiles)
            and all(profile == expected for profile in profiles)
            and deployment_available(read, component)
        )

    return restored


def flag_disabled(read: Callable[[str, str], dict[str, Any] | None], flag_name: str) -> bool:
    configmap = read("configmap", "flagd-config")
    if not configmap:
        return False
    raw = (configmap.get("data") or {}).get("demo.flagd.json")
    if not isinstance(raw, str):
        return False
    flag = (json.loads(raw).get("flags") or {}).get(flag_name) or {}
    # Missing/unknown variants are not evidence of recovery.
    return flag.get("defaultVariant") == "off"
