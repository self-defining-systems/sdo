"""Whether a lane's running application is the deploy of a row's application revision.

The invariant: every harness deploy of the application (``fastloop up``, a row-start recreation, a setup-proof
deploy that actually deployed) captures the application baseline and stamps it with the source it deployed: the
revision fixed before the deploy, that revision's source identity, the cluster instance and the digest of the
captured baseline. The baseline is the state every fault restore returns the app to. A row keeps the app only
when the stamp names the row's application source, belongs to this cluster and this baseline, and the live app
still matches the baseline; anything unknown recreates.

Comparison uses the fields the restore machinery restores (``RESTORED_FIELDS`` over the kinds it lists), so
"the app" means exactly what a restore can put back. Labels and object annotations are outside it, as for
restore: SDO's install annotates objects (``sdo.dev/accepted-paths``) without changing the application. Images
are compared by the refs in the workload specs: a source build publishes a lane-local tag only during a deploy,
which re-stamps, so a tag cannot change underneath a stamp. Secrets, Ingress and HPA are not restored and so not
compared; a fault there is handled by the fault's own recovery.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.adapter import kubectl
from benchmarks.sregym.fastloop.wipe import Tier2Reason, Tier2Trigger, WipeError
from benchmarks.sregym.repository_git import host_git_command

if TYPE_CHECKING:
    from pathlib import Path

SOURCE_KEY = "application_source"
# Top-level repository entries that never change what the application deploys, except the deployment
# artifact class, which does (plan, authored files, telemetry overlay, source patches). Every other file is
# in the source images' build context, whose digest decides the built image, so it counts.
_UNDEPLOYED_TOP_LEVEL = frozenset({".sdo", ".sdo-runtime"})
_DEPLOYED_MEMORY = ".sdo/deployment"
# A rollout restart re-creates pods from the same template; it does not change what is deployed.
RESTART_ANNOTATION = "kubectl.kubernetes.io/restartedAt"
# Everything a stamp cannot vouch for; each recreates.
_UNKNOWN = (OSError, ValueError, LookupError, TypeError, AttributeError, RuntimeError, subprocess.SubprocessError)


_PLAN = ".sdo/deployment/plan.yaml"


def application_source_identity(workspace: Path, revision: str, *, ignore_plan: bool = False) -> str:
    """Content identity of what a revision deploys: its tree outside SDO memory, plus ``.sdo/deployment``.

    ``ignore_plan`` leaves the plan file out of the deployment entries (see ``_plan_only_names_the_deployed_source``).
    """
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
    if ignore_plan:
        entries = _entries_without_plan(workspace, revision, entries)
    return hashlib.sha256("\n".join(sorted(set(entries))).encode()).hexdigest()


def _entries_without_plan(workspace: Path, revision: str, entries: list[str]) -> list[str]:
    """``.sdo/deployment`` is one tree entry; replace it by its entries other than the plan file."""
    kept = [line for line in entries if line.partition("\t")[2] != _DEPLOYED_MEMORY]
    completed = subprocess.run(
        host_git_command(workspace, "ls-tree", "-r", revision, "--", _DEPLOYED_MEMORY),
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode:
        raise WipeError(f"eval: cannot read {revision} in {workspace}: {completed.stderr.strip()}")
    return kept + [line for line in completed.stdout.splitlines() if line.partition("\t")[2] != _PLAN]


def _canonical_keys(value: Any) -> Any:
    """The plan's ProtoJSON spells keys camelCase, the harness's source snake_case: compare in one spelling."""
    if isinstance(value, dict):
        return {re.sub(r"(?<!^)(?=[A-Z])", "_", str(key)).lower(): _canonical_keys(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_canonical_keys(item) for item in value]
    return value


def _plan_at(workspace: Path, revision: str) -> dict[str, Any] | None:
    completed = subprocess.run(
        host_git_command(workspace, "show", f"{revision}:{_PLAN}"), capture_output=True, text=True, check=False
    )
    if completed.returncode:
        return None
    try:
        plan = json.loads(completed.stdout)
    except ValueError:
        return None
    return plan if isinstance(plan, dict) else None


def _plan_only_names_the_deployed_source(environment: Any, deployed: str, revision: str) -> bool:
    """Whether the two revisions differ in ``.sdo/deployment`` only by a plan that names the source the app came from.

    The harness deploys the application from its own source (``deployment_source_for_environment``) whatever the
    plan says, and an install replaces a seeded memory's authored plan with one that records exactly that source.
    A plan that names it, and equals the deployed revision's plan in everything else (images, expected workloads),
    describes the live app: moving its pointer changes nothing the harness deployed. Any other change to the plan or
    to the files beside it is a change to what is deployed.
    """
    from benchmarks.sregym.fastloop.accepted_source import deployment_source_for_environment

    workspace = environment.workspace
    if application_source_identity(workspace, deployed, ignore_plan=True) != application_source_identity(
        workspace, revision, ignore_plan=True
    ):
        return False
    before, after = _plan_at(workspace, deployed), _plan_at(workspace, revision)
    if before is None or after is None:
        return False
    harness = _canonical_keys(deployment_source_for_environment(environment))
    rest = lambda plan: _canonical_keys({key: value for key, value in plan.items() if key != "source"})  # noqa: E731
    return _canonical_keys(after.get("source")) == harness and rest(before) == rest(after)


def workspace_revision(workspace: Path) -> str | None:
    completed = subprocess.run(
        host_git_command(workspace, "rev-parse", "--verify", "HEAD^{commit}"),
        capture_output=True,
        text=True,
        check=False,
    )
    return (completed.stdout.strip() or None) if completed.returncode == 0 else None


def cluster_instance(kubeconfig: Path) -> str:
    """The cluster incarnation: a rebuilt kind cluster gets a new kube-system UID even at the same paths."""
    uid = kubectl(
        ["get", "namespace", "kube-system", "-o", "jsonpath={.metadata.uid}"], namespace=None, kubeconfig=kubeconfig
    ).stdout.strip()
    if not uid:
        raise ValueError("cluster has no kube-system UID")
    return uid


def live_objects(kubeconfig: Path, namespace: str) -> list[dict[str, Any]]:
    from benchmarks.sregym.fastloop.baseline_recovery import KubectlApplicationCluster

    return KubectlApplicationCluster(kubeconfig).list_objects(namespace)


def _transient(value: dict[str, Any]) -> bool:
    """Jobs a schedule or a TTL creates and removes on their own are not part of the deployed application."""
    if value.get("kind") != "Job":
        return False
    owners = value.get("metadata", {}).get("ownerReferences") or []
    return any(owner.get("kind") == "CronJob" for owner in owners) or (
        value.get("spec", {}).get("ttlSecondsAfterFinished") is not None
    )


def application_content(objects: list[dict[str, Any]], namespace: str) -> dict[tuple[str, str], Any]:
    """Restorable content per application object, partitioned exactly as baseline capture and restore are."""
    from benchmarks.sregym.fastloop.baseline_recovery import _ApplicationOwnership, _content, _identity

    content = {}
    for value in _ApplicationOwnership.from_objects(objects).application_objects(objects, namespace):
        if _transient(value):
            continue
        fields = json.loads(json.dumps(_content(value), default=str))
        template = fields[0].get("template") if isinstance(fields[0], dict) else None
        metadata = template.get("metadata") if isinstance(template, dict) else None
        annotations = metadata.get("annotations") if isinstance(metadata, dict) else None
        if isinstance(metadata, dict) and isinstance(annotations, dict):
            annotations.pop(RESTART_ANNOTATION, None)
            if not annotations:
                del metadata["annotations"]
        content[_identity(value)] = fields
    return content


def _baseline_digest(environment: Any) -> str:
    """Digest of the captured baseline's application content, every namespace it holds."""
    from benchmarks.sregym.fastloop.baseline_recovery import APPLICATION_BASELINE_KEY

    document = json.loads(environment.baseline_path.read_text(encoding="utf-8"))
    namespaces = document[APPLICATION_BASELINE_KEY]["namespaces"]
    rows = sorted(
        [item["namespace"], [list(key), value]]
        for item in namespaces
        for key, value in application_content(item["objects"], item["namespace"]).items()
    )
    if not rows:
        raise ValueError("the application baseline holds no application objects")
    return hashlib.sha256(json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _write_stamp(environment: Any, stamp: dict[str, Any] | None) -> None:
    path = environment.baseline_path
    document = json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}
    if stamp is None:
        if SOURCE_KEY not in document:
            return
        document.pop(SOURCE_KEY)
    else:
        document[SOURCE_KEY] = stamp
    temporary = path.with_suffix(".source.tmp")
    temporary.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def forget_deployed_source(environment: Any) -> None:
    """Before a deploy replaces the app, no stamp may vouch for it."""
    _write_stamp(environment, None)


def record_deployed_source(environment: Any, revision: str) -> None:
    """After a completed deploy of ``revision`` and its baseline capture: stamp the baseline with that source.

    Unreadable facts leave no stamp, so the next row recreates rather than trusting an unknown app.
    """
    try:
        stamp = {
            "version": 1,
            "revision": revision,
            "source": application_source_identity(environment.workspace, revision),
            "cluster": cluster_instance(environment.kubeconfig),
            "baseline": _baseline_digest(environment),
        }
    except _UNKNOWN:
        forget_deployed_source(environment)
        return
    _write_stamp(environment, stamp)


def deployed_application_change(environment: Any, revision: str) -> Tier2Reason | None:
    """Why the running app is not the deploy of ``revision``; ``None`` keeps it. Never raises for unknowns."""
    from benchmarks.sregym.fastloop.baseline_recovery import load_application_baseline

    try:
        stamp = json.loads(environment.baseline_path.read_text(encoding="utf-8")).get(SOURCE_KEY)
        recorded = stamp["source"] if isinstance(stamp, dict) and stamp.get("version") == 1 else None
        planned = application_source_identity(environment.workspace, revision)
        baseline = _baseline_digest(environment)
    except _UNKNOWN as exc:
        return Tier2Reason(Tier2Trigger.ACCEPTED_SOURCE_CHANGED, f"the deployed application source is unknown: {exc}")
    if not isinstance(recorded, str) or stamp.get("baseline") != baseline:
        return Tier2Reason(
            Tier2Trigger.ACCEPTED_SOURCE_CHANGED, "no deploy stamp vouches for the lane's application baseline"
        )
    if recorded != planned and not _plan_only_names_the_deployed_source(
        environment, str(stamp.get("revision", "")), revision
    ):
        return Tier2Reason(
            Tier2Trigger.ACCEPTED_SOURCE_CHANGED,
            f"the application is the deploy of {stamp.get('revision', '?')[:12]}; the row runs {revision[:12]}",
        )
    try:
        if stamp.get("cluster") != cluster_instance(environment.kubeconfig):
            return Tier2Reason(Tier2Trigger.DEPLOYED_APPLICATION_CHANGED, "the stamp belongs to another cluster")
        baselines = load_application_baseline(environment.baseline_path, environment.namespace) or ()
        changed = [
            item.namespace
            for item in baselines
            if application_content(live_objects(environment.kubeconfig, item.namespace), item.namespace)
            != application_content(item.objects, item.namespace)
        ]
    except _UNKNOWN as exc:
        return Tier2Reason(Tier2Trigger.DEPLOYED_APPLICATION_CHANGED, f"the live application is unknown: {exc}")
    if not baselines or changed:
        return Tier2Reason(
            Tier2Trigger.DEPLOYED_APPLICATION_CHANGED,
            "the live application differs from its deploy's baseline in " + (", ".join(changed) or "every namespace"),
        )
    return None
