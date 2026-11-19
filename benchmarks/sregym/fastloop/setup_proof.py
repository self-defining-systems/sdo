"""Content-bound setup receipts. Health/admission evidence is always read afresh."""

from __future__ import annotations

import hashlib
import json
import subprocess
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.adapter import Deadline, WaitKind, wait_for
from benchmarks.sregym.fastloop.accepted_source import manifest_paths_for_environment
from benchmarks.sregym.repository_git import host_git_command

if TYPE_CHECKING:
    from collections.abc import Callable

    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

SCHEMA = "sdo.setup-proof/v1"
RECEIPTS = "setup-proof"
INVARIANTS = (
    "lane_resources",
    "restore",
    "onboarding",
    "setup_memory",
    "rbac_equality",
    "stabilization",
    "probe_placement",
    "baseline",
    "closure_policy",
    "observation_conformance",
)
# Inventory configuration, not observations such as Events/EndpointSlices.
RESOURCES = (
    "deployments,statefulsets,daemonsets,replicasets,pods,services,configmaps,"
    "roles,rolebindings,networkpolicies,persistentvolumeclaims,serviceaccounts,jobs,cronjobs"
)
CLUSTER_RESOURCES = (
    "nodes,namespaces,clusterroles,clusterrolebindings,validatingwebhookconfigurations,mutatingwebhookconfigurations,"
    "apiservices,ingressclasses,customresourcedefinitions,priorityclasses,storageclasses,persistentvolumes"
)
VOLATILE_CONFIGMAPS = {"sdo-controller-state", "sdo-controller-maintenance"}


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


@dataclass(frozen=True)
class SetupFingerprint:
    lane: str
    code_revision: str
    images: dict[str, Any]
    accepted_memory_commit: str
    installed_memory_commit: str
    app_manifests: str
    live_objects: dict[str, str]
    tools: dict[str, str]

    def __post_init__(self) -> None:
        for key, value in asdict(self).items():
            if not value:
                raise ValueError(f"eval: setup fingerprint has no {key}")


def object_identities(objects: list[dict[str, Any]]) -> dict[str, str]:
    result = {}
    for obj in objects:
        metadata = obj["metadata"]
        if obj["kind"] == "Secret":
            raise ValueError("eval: setup inventory must exclude Secrets")
        key = f"{metadata.get('namespace', '<cluster>')}/{obj['kind']}/{metadata['name']}"
        if not metadata.get("uid") or key in result:
            raise ValueError(f"eval: incomplete or duplicate live identity: {key}")
        stable = {key: value for key, value in obj.items() if key not in {"metadata", "status"}}
        stable["metadata"] = {
            key: json.loads(json.dumps(metadata[key]))
            for key in (
                "name",
                "namespace",
                "uid",
                "generation",
                "labels",
                "annotations",
                "ownerReferences",
                "deletionTimestamp",
            )
            if key in metadata
        }
        stable["metadata"].get("annotations", {}).pop("kubectl.kubernetes.io/last-applied-configuration", None)
        if obj["kind"] == "ConfigMap" and metadata["name"] in VOLATILE_CONFIGMAPS:
            stable.pop("data", None)
        if obj["kind"] == "Pod":
            # A restart changes the process that established the proof.
            stable["containers"] = [
                {key: row.get(key) for key in ("name", "imageID", "containerID", "restartCount")}
                for group in ("containerStatuses", "initContainerStatuses", "ephemeralContainerStatuses")
                for row in obj.get("status", {}).get(group, [])
            ]
        result[key] = digest(stable)
    return dict(sorted(result.items()))


def image_identity(nodes: list[dict[str, Any]], references: dict[str, str]) -> dict[str, Any]:
    def normalized(name: str) -> str:
        return name.removeprefix("docker.io/").removeprefix("library/")

    images: dict[str, Any] = {}
    for role, reference in references.items():
        images[role] = {}
        for node in nodes:
            matches = [
                image
                for image in node.get("status", {}).get("images", [])
                if normalized(reference) in {normalized(name) for name in image.get("names", [])}
            ]
            digests = sorted({name for image in matches for name in image["names"] if "@sha256:" in name})
            if not digests:
                raise ValueError(f"eval: no installed {role} digest on {node['metadata']['name']}")
            images[role][node["metadata"]["name"]] = digests
    if not nodes:
        raise ValueError("eval: lane has no nodes")
    return images


class _Reader:
    """Local files, GETs and one read-only PVC HEAD read share one snapshot-read budget."""

    def __init__(self, run: Callable[..., subprocess.CompletedProcess[str]], monotonic: Callable[[], float]) -> None:
        self.budget = Deadline.after(wait_for(WaitKind.SNAPSHOT_READ), "setup fingerprint", monotonic)
        self._run = run

    def __call__(self, command: list[str]) -> str:
        timeout = self.budget.bounded_timeout(command[0], minimum=WaitKind.POLL_FAST)
        return self._run(command, check=True, text=True, capture_output=True, timeout=timeout).stdout.strip()

    def finish(self) -> None:
        if self.budget.left() <= 0:
            raise TimeoutError("eval: setup fingerprint exceeded 15 seconds")


@dataclass(frozen=True)
class _Runtime:
    """What every setup proof binds besides live objects: committed code, the accepted workspace and tools."""

    code_revision: str
    accepted: str
    app_manifests: str
    tools: dict[str, str]


def _runtime(environment: FastloopEnvironment, execute: _Reader) -> _Runtime:
    root = Path(__file__).resolve().parents[3]
    revision = execute(
        host_git_command(root, "describe", "--always", "--abbrev=40", "--dirty", "--match=__eval_no_tag__")
    )
    if revision.endswith("-dirty"):
        raise ValueError("eval: setup reuse requires committed code")
    if execute(host_git_command(root, "status", "--porcelain", "--untracked-files=normal")):
        raise ValueError("eval: setup reuse requires committed code")
    accepted = execute(host_git_command(environment.workspace, "rev-parse", "HEAD"))
    if execute(host_git_command(environment.workspace, "status", "--porcelain", "--untracked-files=all")):
        raise ValueError("eval: accepted workspace has uncommitted changes")
    paths = [environment.baseline_path]
    for selection in manifest_paths_for_environment(environment):
        path = (environment.workspace / selection).resolve()
        if not path.is_relative_to(environment.workspace.resolve()):
            raise ValueError("eval: manifest selection escapes accepted workspace")
        paths.extend(sorted(path.rglob("*")) if path.is_dir() else [path])
    manifests = {
        str(path.relative_to(environment.workspace))
        if path.is_relative_to(environment.workspace)
        else "installed-baseline": hashlib.sha256(path.read_bytes()).hexdigest()
        for path in paths
        if path.is_file()
    }
    if not manifests:
        raise ValueError("eval: application manifests are unavailable")
    tools = {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for directory in (root / "benchmarks/sregym/fastloop", root / "benchmarks/sregym/adapter")
        for path in sorted(directory.rglob("*"))
        if path.suffix in {".py", ".go"}
    }
    tools["lane-configuration"] = digest(
        {
            "namespace": environment.namespace,
            "application": environment.application,
            "app": environment.app,
            "deploy_problem": environment.deploy_problem,
            "seed_commit": environment.seed_commit,
            "worker_env": environment.worker_env,
            "dev_mount_snapshot": environment.dev_mount_snapshot,
        }
    )
    if environment.dev_mount:
        mounted_revision = execute(
            host_git_command(
                environment.dev_mount,
                "describe",
                "--always",
                "--abbrev=40",
                "--dirty",
                "--match=__eval_no_tag__",
            )
        )
        if mounted_revision.endswith("-dirty") or execute(
            host_git_command(
                environment.dev_mount,
                "status",
                "--porcelain",
                "--untracked-files=normal",
            )
        ):
            raise ValueError("eval: mounted code must be committed before setup reuse")
        tools["mounted-code-revision"] = mounted_revision
        binary = environment.dev_mount / ".fastloop/sdo-operator"
        tools["installed-operator"] = hashlib.sha256(binary.read_bytes()).hexdigest()
    return _Runtime(revision, accepted, digest(manifests), tools)


def _live(environment: FastloopEnvironment, execute: _Reader) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    base = ["kubectl", "--kubeconfig", str(environment.kubeconfig), "--request-timeout=4s", "get"]
    # Retained infrastructure (for example CoreDNS) must not escape warm reuse
    # checks merely because it lives outside the application/control namespaces.
    # The explicit resource list excludes Secrets and observation-only objects.
    objects = json.loads(execute([*base, RESOURCES, "--all-namespaces", "-o", "json"]))["items"]
    cluster = json.loads(execute([*base, CLUSTER_RESOURCES, "-o", "json"]))["items"]
    return objects, cluster


def collect_fingerprint(
    environment: FastloopEnvironment,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    monotonic: Callable[[], float] = time.monotonic,
) -> SetupFingerprint:
    """Local files, GETs and one read-only PVC HEAD read share one snapshot-read budget."""
    execute = _Reader(run, monotonic)
    runtime = _runtime(environment, execute)
    objects, cluster = _live(environment, execute)
    nodes = [obj for obj in cluster if obj["kind"] == "Node"]
    states = [
        obj
        for obj in objects
        if obj["kind"] == "ConfigMap"
        and obj["metadata"]["name"] == "sdo-controller-state"
        and obj["metadata"].get("namespace") == environment.namespace + "-sdo"
    ]
    if len(states) != 1:
        raise ValueError("eval: installed controller state is unavailable")
    # The persisted drift revision is an inventory content hash, not Git HEAD.
    # Read the actual accepted repository through its current owning Pod.
    controllers = [
        obj
        for obj in objects
        if obj["kind"] == "Pod"
        and obj["metadata"].get("namespace") == environment.namespace + "-sdo"
        and not obj["metadata"].get("deletionTimestamp")
        and any(
            owner.get("kind") == "Job" and owner.get("name") == "sdo-controller-run"
            for owner in obj["metadata"].get("ownerReferences", [])
        )
    ]
    if len(controllers) != 1:
        raise ValueError("eval: installed controller Pod identity is unavailable")
    controller = controllers[0]
    containers = [row for row in controller["spec"]["containers"] if row["name"] == "controller"]
    if len(containers) != 1:
        raise ValueError("eval: installed controller container is unavailable")
    arguments = containers[0].get("args", [])
    if "--app" not in arguments or arguments.index("--app") + 1 >= len(arguments):
        raise ValueError("eval: installed accepted repository path is unavailable")
    repository = arguments[arguments.index("--app") + 1]
    if not repository.startswith("/"):
        raise ValueError("eval: installed accepted repository path must be absolute")
    installed = execute(
        [
            "kubectl",
            "--kubeconfig",
            str(environment.kubeconfig),
            "--request-timeout=4s",
            "exec",
            "pod/" + controller["metadata"]["name"],
            "-n",
            environment.namespace + "-sdo",
            "-c",
            "controller",
            "--",
            "git",
            "-C",
            repository,
            "rev-parse",
            "HEAD",
        ]
    )
    if installed != runtime.accepted:
        raise ValueError("eval: installed accepted-memory commit differs from accepted workspace")
    execute.finish()
    return SetupFingerprint(
        environment.cluster,
        runtime.code_revision,
        image_identity(nodes, environment.images.model_dump()),
        runtime.accepted,
        installed,
        runtime.app_manifests,
        object_identities(objects + cluster),
        runtime.tools,
    )


def validate_report(report: dict[str, Any], fingerprint: SetupFingerprint) -> None:
    if fingerprint.code_revision.endswith("-dirty"):
        raise ValueError("eval: setup receipt requires committed code")
    expected = {"passed": True, "fault_injected": False, "live_model_calls": False}
    if any(report.get(key) is not value for key, value in expected.items()):
        raise ValueError("eval: only a passing no-fault proof can be saved")
    if report.get("code_revision") != fingerprint.code_revision or report.get("lane") != fingerprint.lane:
        raise ValueError("eval: setup report identity differs from fingerprint")
    invariants = report.get("invariants", {})
    if (
        not isinstance(invariants, dict)
        or any(
            not isinstance(invariants.get(name), dict) or invariants[name].get("passed") is not True
            for name in INVARIANTS
        )
        or any(not isinstance(row, dict) or row.get("passed") is not True for row in invariants.values())
    ):
        raise ValueError("eval: setup report lacks passing invariants")


def save_receipt(directory: Path, fingerprint: SetupFingerprint, report: dict[str, Any]) -> Path:
    validate_report(report, fingerprint)
    document = {"schema_version": SCHEMA, "fingerprint": asdict(fingerprint), "report": report}
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / (digest(document) + ".json")
    path.write_text(json.dumps(document, indent=2) + "\n")
    temporary = directory / "latest.tmp"
    temporary.write_text(json.dumps({"receipt": path.name}) + "\n")
    temporary.replace(directory / "latest.json")
    return path


def reuse_decision(directory: Path, fingerprint: SetupFingerprint) -> dict[str, Any]:
    try:
        name = json.loads((directory / "latest.json").read_text())["receipt"]
        if Path(name).name != name:
            raise ValueError("invalid receipt path")
        path = directory / name
        document = json.loads(path.read_text())
        if path.stem != digest(document) or document["schema_version"] != SCHEMA:
            raise ValueError("invalid receipt content")
        validate_report(document["report"], SetupFingerprint(**document["fingerprint"]))
        expected = document["fingerprint"]
        changed = sorted(key for key, value in asdict(fingerprint).items() if expected.get(key) != value)
        if fingerprint.code_revision.endswith("-dirty"):
            changed.append("uncommitted_code")
        return {"reused": not changed, "receipt": str(path), "changed": changed}
    except (OSError, ValueError, KeyError, TypeError) as exc:
        return {"reused": False, "receipt": None, "changed": [f"passing_receipt_unavailable: {exc}"]}


APPLICATION_SCHEMA = "sdo.application-setup-proof/v1"
APPLICATION_RECEIPTS = "application-setup-proof"
# Kinds whose objects are the running state of a configuration, not configuration the deploy establishes.
RUNTIME_KINDS = frozenset({"Pod", "ReplicaSet"})
# Annotations a rollout or kubectl rewrites while the configuration stays the same.
VOLATILE_ANNOTATIONS = frozenset(
    {"kubectl.kubernetes.io/last-applied-configuration", "deployment.kubernetes.io/revision"}
)
RESTARTED_AT = "kubectl.kubernetes.io/restartedAt"
# Objects the SDO install puts in the application namespace carry this label; a Tier-1 wipe removes them.
INSTALL_LABEL = "sdo.dev/application-namespace"


def application_identities(objects: list[dict[str, Any]], control_namespace: str) -> dict[str, str]:
    """Configuration identity of what the application deploy establishes, outside the SDO install.

    The install (its namespace, cluster RBAC named after it, the volumes bound to its claims and labelled
    helpers) is what a row-start wipe deletes and the row reinstalls, so it never takes part. Runtime (Pods,
    ReplicaSets, status, resource versions, rollout revisions and restarts) does not either:
    the admission after the deploy decision re-proves health afresh. An object that was
    restored to its proven configuration keeps its identity; an added, removed or edited object, or a rebuilt lane
    (new Node or Namespace UID), changes it.
    """
    if not control_namespace or "/" in control_namespace:
        raise ValueError("eval: application setup identity requires the control namespace")
    result = {}
    for obj in objects:
        metadata = obj["metadata"]
        if obj["kind"] == "Secret":
            raise ValueError("eval: setup inventory must exclude Secrets")
        namespace = metadata.get("namespace")
        if (
            obj["kind"] in RUNTIME_KINDS
            or namespace == control_namespace
            or (namespace is None and control_namespace in metadata["name"])
            or INSTALL_LABEL in (metadata.get("labels") or {})
            or (obj.get("spec", {}).get("claimRef") or {}).get("namespace") == control_namespace
        ):
            continue
        key = f"{namespace or '<cluster>'}/{obj['kind']}/{metadata['name']}"
        if key in result:
            raise ValueError(f"eval: duplicate live identity: {key}")
        stable = json.loads(
            json.dumps({name: value for name, value in obj.items() if name not in {"metadata", "status"}})
        )
        template = stable.get("spec", {}).get("template", {}).get("metadata", {})
        # A rollout restart (row start restarts Jaeger) restarts Pods; the configuration stays the same.
        (template.get("annotations") or {}).pop(RESTARTED_AT, None)
        if template.get("annotations") == {}:
            del template["annotations"]
        if stable.get("spec", {}).get("template", {}).get("metadata") == {}:
            del stable["spec"]["template"]["metadata"]
        kept = {
            name: json.loads(json.dumps(metadata[name]))
            for name in ("name", "namespace", "labels", "annotations", "deletionTimestamp", "finalizers")
            if name in metadata
        }
        for name in VOLATILE_ANNOTATIONS:
            kept.get("annotations", {}).pop(name, None)
        kept["owners"] = sorted(f"{row.get('kind')}/{row.get('name')}" for row in metadata.get("ownerReferences", []))
        if obj["kind"] in {"Node", "Namespace"}:
            # A recreated lane or application namespace is a different setup even with equal configuration.
            kept["uid"] = metadata.get("uid")
        stable["metadata"] = kept
        result[key] = digest(stable)
    return dict(sorted(result.items()))


@dataclass(frozen=True)
class ApplicationFingerprint:
    """What a row's application deploy establishes and depends on; never the SDO install a row-start wipe removes."""

    lane: str
    code_revision: str
    images: dict[str, Any]
    app_manifests: str
    live_objects: dict[str, str]
    tools: dict[str, str]

    def __post_init__(self) -> None:
        for key, value in asdict(self).items():
            if not value:
                raise ValueError(f"eval: application setup fingerprint has no {key}")


def collect_application_fingerprint(
    environment: FastloopEnvironment,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    monotonic: Callable[[], float] = time.monotonic,
) -> ApplicationFingerprint:
    execute = _Reader(run, monotonic)
    runtime = _runtime(environment, execute)
    objects, cluster = _live(environment, execute)
    execute.finish()
    return ApplicationFingerprint(
        environment.cluster,
        runtime.code_revision,
        image_identity([obj for obj in cluster if obj["kind"] == "Node"], environment.images.model_dump()),
        runtime.app_manifests,
        application_identities(objects + cluster, environment.namespace + "-sdo"),
        runtime.tools,
    )


def _application_proof(admission: dict[str, Any]) -> dict[str, Any]:
    if admission.get("passed") is not True:
        raise ValueError("eval: only a passing pre-injection admission can seal an application setup")
    return {"fault_injected": False, "admission": admission}


def seal_application_receipt(directory: Path, fingerprint: ApplicationFingerprint, admission: dict[str, Any]) -> Path:
    """Seal the application this lane proved healthy before injection; the latest seal replaces earlier ones."""
    document = {
        "schema_version": APPLICATION_SCHEMA,
        "fingerprint": asdict(fingerprint),
        "proof": _application_proof(admission),
    }
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / (digest(document) + ".json")
    path.write_text(json.dumps(document, indent=2) + "\n")
    temporary = directory / "latest.tmp"
    temporary.write_text(json.dumps({"receipt": path.name}) + "\n")
    temporary.replace(directory / "latest.json")
    return path


def application_reuse_decision(directory: Path, fingerprint: ApplicationFingerprint) -> dict[str, Any]:
    try:
        name = json.loads((directory / "latest.json").read_text())["receipt"]
        if Path(name).name != name:
            raise ValueError("invalid receipt path")
        path = directory / name
        document = json.loads(path.read_text())
        if path.stem != digest(document) or document["schema_version"] != APPLICATION_SCHEMA:
            raise ValueError("invalid receipt content")
        _application_proof(document["proof"]["admission"])
        if document["proof"].get("fault_injected") is not False:
            raise ValueError("receipt does not prove a no-fault setup")
        expected = ApplicationFingerprint(**document["fingerprint"])
        changed = sorted(key for key, value in asdict(fingerprint).items() if getattr(expected, key) != value)
        return {"reused": not changed, "receipt": str(path), "changed": changed}
    except (OSError, ValueError, KeyError, TypeError) as exc:
        return {"reused": False, "receipt": None, "changed": [f"passing_receipt_unavailable: {exc}"]}


def check_application_reuse(environment: FastloopEnvironment) -> dict[str, Any]:
    """Row start, after the SDO wipe: reuse the deployed application only while it matches its last passing seal.

    Unavailable evidence runs the deploy; it never grants a skip.
    """
    directory = environment.run_dir / APPLICATION_RECEIPTS
    if not (directory / "latest.json").is_file():
        return {"reused": False, "receipt": None, "changed": ["no_passing_receipt"]}
    try:
        return application_reuse_decision(directory, collect_application_fingerprint(environment))
    except (OSError, ValueError, KeyError, TypeError, TimeoutError, subprocess.SubprocessError) as exc:
        return {"reused": False, "receipt": None, "changed": [f"fingerprint_unavailable: {exc}"]}


def seal_after_admission(environment: FastloopEnvironment, admission: dict[str, Any]) -> Path:
    return seal_application_receipt(
        environment.run_dir / APPLICATION_RECEIPTS, collect_application_fingerprint(environment), admission
    )


def check_reuse(environment: FastloopEnvironment) -> tuple[SetupFingerprint | None, dict[str, Any]]:
    """Unavailable evidence runs the full setup; it never grants a skip."""
    directory = environment.run_dir / RECEIPTS
    if not (directory / "latest.json").is_file():
        return None, {"reused": False, "receipt": None, "changed": ["no_passing_receipt"]}
    try:
        fingerprint = collect_fingerprint(environment)
        decision = reuse_decision(directory, fingerprint)
        decision["message"] = (
            f"reused {decision['receipt']}"
            if decision["reused"]
            else "full setup required: " + ", ".join(decision["changed"])
        )
        return fingerprint, decision
    except (OSError, ValueError, KeyError, TypeError, TimeoutError, subprocess.SubprocessError) as exc:
        return None, {"reused": False, "receipt": None, "changed": [f"fingerprint_unavailable: {exc}"]}
