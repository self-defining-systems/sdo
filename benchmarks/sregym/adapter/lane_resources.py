"""Read-only admission for long-lived lanes: bounded memory and one app.

The decision is a pure function of namespace/pod inventory, kubelet summaries,
host MemAvailable and a clock. Memory collection uses Kubernetes GET only.
Disk collection reads DockerRootDir and its host filesystem; neither check
uses exec, Secrets, or an agent/model. Missing measurements reject admission.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Set

_GIB = 1024**3
_DNS = re.compile(r"[a-z0-9](?:[-a-z0-9.]*[a-z0-9])?\Z")
_INFRASTRUCTURE = frozenset(
    {
        "default",
        "kube-system",
        "kube-public",
        "kube-node-lease",
        "local-path-storage",
        "openebs",
        "calico-system",
        "calico-apiserver",
        "tigera-operator",
        "observe",
        "sregym",
        # Pinned harness admission-webhook and chaos infrastructure.
        "policy-system",
        "khaos",
        "chaos-mesh",
    }
)


@dataclass(frozen=True)
class LaneResourcePolicy:
    """Byte budgets measured before injection, independent of fault/oracle data."""

    host_floor_bytes: int = 16 * _GIB
    default_container_budget_bytes: int = 8 * _GIB
    jaeger_budget_bytes: int = 2 * _GIB
    host_disk_floor_bytes: int = 100 * _GIB
    host_disk_min_free_percent: int = 10
    max_sample_age_seconds: int = 60
    # Exact namespace/pod/container or namespace/container overrides.
    component_budgets: Mapping[str, int] = field(default_factory=dict)
    infrastructure_namespaces: frozenset[str] = _INFRASTRUCTURE

    def __post_init__(self) -> None:
        for name in (
            "host_floor_bytes",
            "default_container_budget_bytes",
            "jaeger_budget_bytes",
            "host_disk_floor_bytes",
            "host_disk_min_free_percent",
            "max_sample_age_seconds",
        ):
            _positive(getattr(self, name), name)
        if self.host_disk_min_free_percent >= 100:
            raise ValueError("host_disk_min_free_percent must be less than 100")
        if not isinstance(self.component_budgets, Mapping):
            raise TypeError("component_budgets must be a mapping")
        if not isinstance(self.infrastructure_namespaces, (set, frozenset)):
            raise TypeError("infrastructure_namespaces must be a set of namespace names")
        for name, budget in self.component_budgets.items():
            if not isinstance(name, str) or not name:
                raise ValueError("component budget requires a nonempty component identity")
            _positive(budget, f"budget for {name}")
        for namespace in self.infrastructure_namespaces:
            _namespace(namespace)
        object.__setattr__(self, "component_budgets", MappingProxyType(dict(self.component_budgets)))
        object.__setattr__(self, "infrastructure_namespaces", frozenset(self.infrastructure_namespaces))


def _positive(value: Any, subject: str) -> None:
    if type(value) is not int:
        raise TypeError(f"{subject} must be an integer")
    if value <= 0:
        raise ValueError(f"{subject} must be positive")


def _namespace(value: str) -> None:
    if not isinstance(value, str) or not _DNS.fullmatch(value) or len(value) > 63 or "." in value:
        raise ValueError(f"eval: invalid namespace {value!r}")


def _timestamp(value: Any, subject: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.utcoffset() is None:
            raise ValueError("timezone absent")
        return parsed
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError(f"eval: {subject} timestamp unavailable") from exc


def _memory_pod_uid(pod: dict[str, Any]) -> str:
    metadata, spec = pod["metadata"], pod["spec"]
    annotations = metadata.get("annotations", {})
    mirror = annotations.get("kubernetes.io/config.mirror")
    if not mirror:
        return metadata["uid"]
    # Kubelet summaries name the static Pod UID, while the API lists its mirror
    # object's distinct UID. Bind the alternate identity to its file/hash/Node.
    if (
        annotations.get("kubernetes.io/config.source") != "file"
        or annotations.get("kubernetes.io/config.hash") != mirror
        or not any(
            owner.get("kind") == "Node" and owner.get("name") == spec.get("nodeName") and owner.get("uid")
            for owner in metadata.get("ownerReferences", [])
        )
    ):
        raise ValueError(f"eval: {metadata['namespace']}/{metadata['name']} memory mirror identity unavailable")
    return mirror


def evaluate_lane_resources(
    *,
    namespaces: dict[str, Any],
    pods: dict[str, Any],
    summaries: list[dict[str, Any]],
    app_namespaces: Set[str],
    host_available_bytes: int,
    now: datetime,
    policy: LaneResourcePolicy | None = None,
) -> dict[str, Any]:
    """Reject a foreign namespace, low host headroom or unmeasured/over-budget container."""
    policy = policy or LaneResourcePolicy()
    if not app_namespaces:
        raise ValueError("eval: configured app namespaces must not be empty")
    for namespace in app_namespaces:
        _namespace(namespace)
    if now.utcoffset() is None:
        raise ValueError("eval: admission clock must include timezone")
    allowed = policy.infrastructure_namespaces | set(app_namespaces) | {f"{name}-sdo" for name in app_namespaces}
    observed = set()
    for row in namespaces["items"]:
        metadata = row["metadata"]
        name = metadata["name"]
        observed.add(name)
        if name not in allowed:
            age = (now - _timestamp(metadata.get("creationTimestamp"), f"namespace {name} age")).total_seconds()
            raise ValueError(
                f"eval: foreign app namespace {name} age={age / 3600:.1f}h; expected {sorted(app_namespaces)}"
            )
        if name in app_namespaces and metadata.get("deletionTimestamp"):
            raise ValueError(f"eval: configured app namespace {name} is terminating")
    if missing := set(app_namespaces) - observed:
        raise ValueError(f"eval: missing configured app namespaces {sorted(missing)}")
    host = _host_headroom(host_available_bytes, policy)

    measured: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for summary in summaries:
        for pod in summary["pods"]:
            ref = pod["podRef"]
            for container in pod.get("containers", []):
                key = (ref["namespace"], ref["name"], ref["uid"], container["name"])
                if key in measured:
                    raise ValueError(f"eval: duplicate memory measurement for {'/'.join(key)}")
                memory = container.get("memory")
                measured[key] = memory if isinstance(memory, dict) else {}
    containers = []
    for pod in pods["items"]:
        if pod.get("status", {}).get("phase") in {"Succeeded", "Failed"}:
            continue
        metadata, spec = pod["metadata"], pod["spec"]
        namespace, name, uid = metadata["namespace"], metadata["name"], _memory_pod_uid(pod)
        running = {
            row["name"]
            for field_name in ("containerStatuses", "initContainerStatuses", "ephemeralContainerStatuses")
            for row in pod.get("status", {}).get(field_name, [])
            if "running" in row.get("state", {})
        }
        for container in (
            spec.get("containers", []) + spec.get("initContainers", []) + spec.get("ephemeralContainers", [])
        ):
            container_name = container["name"]
            if container_name not in running:
                continue
            component = f"{namespace}/{name}/{container_name}"
            memory = measured.get((namespace, name, uid, container_name), {})
            used = memory.get("workingSetBytes")
            if type(used) is not int or used < 0:
                raise ValueError(f"eval: {component} memory measurement unavailable")
            age = (now - _timestamp(memory.get("time"), f"{component} memory")).total_seconds()
            if age < -5 or age > policy.max_sample_age_seconds:
                raise ValueError(f"eval: {component} memory sample stale or future: age={age:.1f}s")
            image = container.get("image", "").split("@", 1)[0]
            jaeger = "jaegertracing/all-in-one" in image
            budget = policy.component_budgets.get(
                component,
                policy.component_budgets.get(
                    f"{namespace}/{container_name}",
                    policy.jaeger_budget_bytes if jaeger else policy.default_container_budget_bytes,
                ),
            )
            if used > budget:
                raise ValueError(f"eval: {component} memory {used} bytes exceeds budget {budget} bytes")
            containers.append({"component": component, "working_set_bytes": used, "budget_bytes": budget})
    if not containers:
        raise ValueError("eval: lane has no running container memory measurements")
    return {
        "passed": True,
        "observed_at": now.isoformat(),
        **host,
        "app_namespaces": sorted(app_namespaces),
        "containers": sorted(containers, key=lambda row: row["component"]),
    }


def _host_headroom(available_bytes: int, policy: LaneResourcePolicy) -> dict[str, Any]:
    if type(available_bytes) is not int or available_bytes < 0:
        raise ValueError("eval: host available memory unavailable")
    if available_bytes < policy.host_floor_bytes:
        raise ValueError(
            f"eval: host available memory {available_bytes} bytes below floor {policy.host_floor_bytes} bytes"
        )
    return {"host_available_bytes": available_bytes, "host_floor_bytes": policy.host_floor_bytes}


def assert_host_headroom(*, policy: LaneResourcePolicy | None = None) -> dict[str, Any]:
    """Reject a low-memory host before lane creation, without any cluster query."""
    return {"passed": True, **_host_headroom(host_available_memory(), policy or LaneResourcePolicy())}


def evaluate_host_disk(
    *, mount: str, total_bytes: int, free_bytes: int, policy: LaneResourcePolicy | None = None
) -> dict[str, Any]:
    """Reject full filesystems before OpenSearch high/flood watermarks interfere."""
    policy = policy or LaneResourcePolicy()
    _positive(total_bytes, "disk total_bytes")
    if type(free_bytes) is not int or not 0 <= free_bytes <= total_bytes:
        raise ValueError(f"eval: host disk mount {mount} free-byte measurement unavailable")
    used_percent = 100 * (total_bytes - free_bytes) / total_bytes
    if free_bytes * 100 <= total_bytes * policy.host_disk_min_free_percent or free_bytes < policy.host_disk_floor_bytes:
        raise ValueError(
            f"eval: host disk mount {mount} {used_percent:.1f}% used, {free_bytes} bytes free; "
            f"require usage <{100 - policy.host_disk_min_free_percent}% and "
            f">={policy.host_disk_floor_bytes} bytes free; free space on this mount before lane setup/injection"
        )
    return {
        "passed": True,
        "mount": mount,
        "total_bytes": total_bytes,
        "free_bytes": free_bytes,
        "used_percent": used_percent,
        "min_free_percent": policy.host_disk_min_free_percent,
        "floor_bytes": policy.host_disk_floor_bytes,
    }


def assert_host_disk_headroom(path: Path | None = None, *, policy: LaneResourcePolicy | None = None) -> dict[str, Any]:
    """Inspect the local Docker kind-node storage filesystem; never change it."""
    try:
        if path is None:
            inspected = subprocess.run(
                ["docker", "info", "--format", "{{.DockerRootDir}}"],
                check=True,
                capture_output=True,
                text=True,
                timeout=5,
            )
            location = inspected.stdout.strip()
            if not location or not Path(location).is_absolute():
                raise ValueError("DockerRootDir absent or not absolute")
            path = Path(location)
        storage = path.resolve(strict=True)
        # Resolve the actual mount (including bind mounts), not the current
        # worktree filesystem. mountinfo encodes whitespace/backslash as octal.
        mounts = [
            Path(re.sub(r"\\([0-7]{3})", lambda match: chr(int(match[1], 8)), line.split()[4]))
            for line in Path("/proc/self/mountinfo").read_text().splitlines()
        ]
        mount = max((row for row in mounts if storage.is_relative_to(row)), key=lambda row: len(row.parts))
        usage = shutil.disk_usage(storage)
    except (OSError, subprocess.SubprocessError, ValueError, IndexError) as exc:
        raise ValueError(f"eval: kind host storage filesystem unavailable ({path}): {exc}") from exc
    return {
        **evaluate_host_disk(mount=str(mount), total_bytes=usage.total, free_bytes=usage.free, policy=policy),
        "storage_path": str(storage),
    }


def host_available_memory(path: Path = Path("/proc/meminfo")) -> int:
    """Linux host floor; MemFree omits reclaimable cache and must not substitute."""
    try:
        for line in path.read_text().splitlines():
            fields = line.split()
            if fields[:1] == ["MemAvailable:"] and len(fields) == 3 and fields[2] == "kB":
                value = int(fields[1]) * 1024
                if value >= 0:
                    return value
    except (OSError, ValueError) as exc:
        raise ValueError("eval: host MemAvailable unavailable") from exc
    raise ValueError("eval: host MemAvailable unavailable")


def assert_lane_resources(
    kubeconfig: Path,
    *,
    app_namespaces: Set[str],
    policy: LaneResourcePolicy | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Bounded GET-only collection for HU's shared admission/dry-run path.

    Runs on the local kind host, as fastloop does. The floor measures that host;
    a remote cluster must supply its host measurement to evaluate_lane_resources.
    All Kubernetes reads together are capped at fifteen seconds.
    """
    if not kubeconfig.is_absolute():
        raise ValueError("eval: lane resource admission requires an absolute trusted kubeconfig")
    deadline = time.monotonic() + 15

    def get(args: list[str]) -> dict[str, Any]:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ValueError("eval: lane memory inventory unavailable: read budget exhausted")
        try:
            result = subprocess.run(
                ["kubectl", "--kubeconfig", str(kubeconfig), "get", *args, "--request-timeout=4s"],
                capture_output=True,
                text=True,
                check=True,
                timeout=min(5, remaining),
            )
            document = json.loads(result.stdout)
            if not isinstance(document, dict):
                raise ValueError("expected JSON object")
            return document
        except (OSError, subprocess.SubprocessError, ValueError) as exc:
            raise ValueError(f"eval: lane memory inventory unavailable ({' '.join(args)})") from exc

    try:
        namespaces = get(["namespaces", "-o", "json"])
        nodes = get(["nodes", "-o", "json"])
        summaries = []
        if not nodes["items"]:
            raise ValueError("eval: node memory inventory unavailable")
        for node in nodes["items"]:
            name = node["metadata"]["name"]
            if not isinstance(name, str) or not _DNS.fullmatch(name):
                raise ValueError("eval: invalid node identity in memory inventory")
            summary = get(["--raw", f"/api/v1/nodes/{name}/proxy/stats/summary"])
            if summary["node"]["nodeName"] != name:
                raise ValueError(f"eval: node memory inventory identity mismatch: {name}")
            summaries.append(summary)
        # A short-lived startup container may finish before the kubelet sample
        # is collected. Judge the current running set, rather than requiring a
        # measurement for an earlier lifetime that has already ended. New or
        # still-running containers retain exact UID/sample requirements.
        pods = get(["pods", "--all-namespaces", "-o", "json"])
        return evaluate_lane_resources(
            namespaces=namespaces,
            pods=pods,
            summaries=summaries,
            app_namespaces=app_namespaces,
            host_available_bytes=host_available_memory(),
            now=now or datetime.now(timezone.utc),
            policy=policy,
        )
    except (KeyError, TypeError) as exc:
        raise ValueError("eval: malformed lane memory/namespace inventory unavailable") from exc
