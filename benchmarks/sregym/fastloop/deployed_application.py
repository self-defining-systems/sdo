"""Which application a lane is running, judged from the live objects and images, not from what fastloop last ran.

``fastloop up`` deploys from its seed and a row's setup skips the deploy while the app is healthy, so a lane
rebuilt from a seed keeps serving the seed's application to rows that plan another accepted commit. A row may
also leave the app changed (a deployer patch, a verified source repair, an agent write). The record binds the
accepted-source identity a deploy used to a fingerprint of what that deploy produced on one cluster instance;
the row-start decision recreates the app when the planned source differs, when the live objects or running
images no longer match that fingerprint, or when anything is unknown.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.adapter import kubectl
from benchmarks.sregym.fastloop.wipe import Tier2Reason, Tier2Trigger, WipeError
from benchmarks.sregym.repository_git import host_git_command

if TYPE_CHECKING:
    from pathlib import Path

# Top-level repository entries that never change what the application deploys, except the deployment
# artifact class, which does (plan, authored files, telemetry overlay, source patches). Every other file is
# in the source images' build context, whose digest decides the built image, so it counts.
_UNDEPLOYED_TOP_LEVEL = frozenset({".sdo", ".sdo-runtime"})
_DEPLOYED_MEMORY = ".sdo/deployment"
# A rollout restart re-creates pods from the same template; it does not change what is deployed.
RESTART_ANNOTATION = "kubectl.kubernetes.io/restartedAt"
_WORKLOAD_POD_OWNERS = frozenset({"ReplicaSet", "StatefulSet", "DaemonSet"})


def application_source_identity(workspace: Path, revision: str = "HEAD") -> str:
    """Content identity of what a revision deploys: its tree outside SDO memory, plus ``.sdo/deployment``."""
    entries = []
    for argv in (["ls-tree", revision], ["ls-tree", revision, "--", _DEPLOYED_MEMORY]):
        completed = subprocess.run(host_git_command(workspace, *argv), capture_output=True, text=True, check=False)
        if completed.returncode:
            raise WipeError(f"eval: cannot read {revision} in {workspace}: {completed.stderr.strip()}")
        entries.extend(
            line
            for line in completed.stdout.splitlines()
            if line.partition("\t")[2] == _DEPLOYED_MEMORY or line.partition("\t")[2] not in _UNDEPLOYED_TOP_LEVEL
        )
    return hashlib.sha256("\n".join(sorted(set(entries))).encode()).hexdigest()


def workspace_revision(workspace: Path) -> str | None:
    completed = subprocess.run(
        host_git_command(workspace, "rev-parse", "--verify", "HEAD^{commit}"),
        capture_output=True,
        text=True,
        check=False,
    )
    return completed.stdout.strip() or None if completed.returncode == 0 else None


def application_fingerprint(objects: list[dict[str, Any]], namespace: str) -> str:
    """Digest of the application objects' restored content, partitioned exactly as baseline capture is."""
    from benchmarks.sregym.fastloop.baseline_recovery import _ApplicationOwnership, _content, _identity

    rows = []
    for value in _ApplicationOwnership.from_objects(objects).application_objects(objects, namespace):
        content = json.loads(json.dumps(_content(value), default=str))
        template = content[0].get("template", {}) if isinstance(content[0], dict) else {}
        annotations = template.get("metadata", {}).get("annotations") if isinstance(template, dict) else None
        if isinstance(annotations, dict):
            annotations.pop(RESTART_ANNOTATION, None)
            if not annotations:
                del template["metadata"]["annotations"]
        rows.append([list(_identity(value)), content])
    rows.sort(key=lambda row: row[0])
    return hashlib.sha256(json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


@dataclass(frozen=True)
class LiveApplication:
    """The cluster instance, the application objects and the images its workload pods actually run."""

    cluster: str
    objects: str
    images: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.cluster or not self.objects:
            raise ValueError("a live application needs a cluster instance and an object fingerprint")


def observe(kubeconfig: Path, namespace: str) -> LiveApplication:
    from benchmarks.sregym.fastloop.baseline_recovery import KubectlApplicationCluster

    cluster = kubectl(
        ["get", "namespace", "kube-system", "-o", "jsonpath={.metadata.uid}"], namespace=None, kubeconfig=kubeconfig
    ).stdout.strip()
    objects = application_fingerprint(KubectlApplicationCluster(kubeconfig).list_objects(namespace), namespace)
    pods = json.loads(kubectl(["get", "pods", "-o", "json"], namespace=namespace, kubeconfig=kubeconfig).stdout)
    images = {
        f"{status.get('image')}@{status.get('imageID')}"
        for pod in pods["items"]
        if not pod["metadata"].get("deletionTimestamp")
        and any(owner.get("kind") in _WORKLOAD_POD_OWNERS for owner in pod["metadata"].get("ownerReferences", []))
        for status in pod.get("status", {}).get("containerStatuses", [])
        if status.get("imageID")
    }
    return LiveApplication(cluster, objects, tuple(sorted(images)))


class DeployedApplication:
    """The source a deploy used, bound to the live application it produced. Missing or malformed is unknown."""

    def __init__(self, kubeconfig: Path, namespace: str) -> None:
        if not namespace or "/" in namespace or "\\" in namespace:
            raise ValueError("deployed application requires an application namespace")
        self.path = kubeconfig.with_name(f"{kubeconfig.name}.{namespace}.deployed-application.json")

    def record(self, source: str, live: LiveApplication) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(self.path.name + ".tmp")
        document = {"version": 2, "source": source, "live": {**asdict(live), "images": list(live.images)}}
        temporary.write_text(json.dumps(document) + "\n", encoding="utf-8")
        os.replace(temporary, self.path)

    def clear(self) -> None:
        self.path.unlink(missing_ok=True)

    def reason(self, planned: str | None, live: LiveApplication | None) -> Tier2Reason | None:
        if live is None:
            return Tier2Reason(Tier2Trigger.DEPLOYED_APPLICATION_CHANGED, "the live application cannot be observed")
        try:
            document = json.loads(self.path.read_text(encoding="utf-8"))
            source = document["source"] if document.get("version") == 2 else None
            recorded = LiveApplication(
                document["live"]["cluster"], document["live"]["objects"], tuple(document["live"]["images"])
            )
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            source, recorded = None, None
        if not isinstance(source, str) or not source or recorded is None:
            return Tier2Reason(
                Tier2Trigger.ACCEPTED_SOURCE_CHANGED, "the lane does not record which source its application runs"
            )
        if recorded != live:
            changed = [
                name for name in ("cluster", "objects", "images") if getattr(recorded, name) != getattr(live, name)
            ]
            return Tier2Reason(
                Tier2Trigger.DEPLOYED_APPLICATION_CHANGED,
                "the live application differs from what its deploy produced: " + ", ".join(changed),
            )
        if planned is None:
            return Tier2Reason(Tier2Trigger.ACCEPTED_SOURCE_CHANGED, "the planned application source is unreadable")
        if source != planned:
            return Tier2Reason(
                Tier2Trigger.ACCEPTED_SOURCE_CHANGED,
                f"the application runs source {source[:12]}; the row plans {planned[:12]}",
            )
        return None


def record_deployed_application(environment: Any) -> None:
    """After a completed deploy: the workspace's source is what now runs. Anything unreadable records nothing."""
    store = DeployedApplication(environment.kubeconfig, environment.namespace)
    store.clear()
    try:
        source = application_source_identity(environment.workspace)
        live = observe(environment.kubeconfig, environment.namespace)
    except (WipeError, OSError, ValueError, RuntimeError, subprocess.SubprocessError):
        return
    store.record(source, live)


def deployed_application_change(environment: Any, revision: str = "HEAD") -> Tier2Reason | None:
    """Why the lane's running app is not the deploy of ``revision``; unknown facts recreate, never abort."""
    try:
        planned: str | None = application_source_identity(environment.workspace, revision)
    except (WipeError, OSError):
        planned = None
    try:
        live: LiveApplication | None = observe(environment.kubeconfig, environment.namespace)
    except (OSError, ValueError, RuntimeError, KeyError, subprocess.SubprocessError):
        live = None
    return DeployedApplication(environment.kubeconfig, environment.namespace).reason(planned, live)


def forget_deployed_application(environment: Any) -> None:
    DeployedApplication(environment.kubeconfig, environment.namespace).clear()
