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
    "nodes,clusterroles,clusterrolebindings,validatingwebhookconfigurations,mutatingwebhookconfigurations"
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


def collect_fingerprint(
    environment: FastloopEnvironment,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    monotonic: Callable[[], float] = time.monotonic,
) -> SetupFingerprint:
    """Local files, GETs and one read-only PVC HEAD read share one snapshot-read budget."""
    budget = Deadline.after(wait_for(WaitKind.SNAPSHOT_READ), "setup fingerprint", monotonic)

    def execute(command: list[str]) -> str:
        timeout = budget.bounded_timeout(command[0], minimum=WaitKind.POLL_FAST)
        return run(command, check=True, text=True, capture_output=True, timeout=timeout).stdout.strip()

    root = Path(__file__).resolve().parents[3]
    revision = execute(
        ["git", "-C", str(root), "describe", "--always", "--abbrev=40", "--dirty", "--match=__eval_no_tag__"]
    )
    if revision.endswith("-dirty"):
        raise ValueError("eval: setup reuse requires committed code")
    if execute(["git", "-C", str(root), "status", "--porcelain", "--untracked-files=normal"]):
        raise ValueError("eval: setup reuse requires committed code")
    accepted = execute(["git", "-C", str(environment.workspace), "rev-parse", "HEAD"])
    if execute(["git", "-C", str(environment.workspace), "status", "--porcelain", "--untracked-files=all"]):
        raise ValueError("eval: accepted workspace has uncommitted changes")
    base = ["kubectl", "--kubeconfig", str(environment.kubeconfig), "--request-timeout=4s", "get"]
    objects = []
    for namespace in (environment.namespace, environment.namespace + "-sdo"):
        objects.extend(json.loads(execute([*base, RESOURCES, "-n", namespace, "-o", "json"]))["items"])
    cluster = json.loads(execute([*base, CLUSTER_RESOURCES, "-o", "json"]))["items"]
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
            *base[:-1],
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
    if installed != accepted:
        raise ValueError("eval: installed accepted-memory commit differs from accepted workspace")
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
            [
                "git",
                "-C",
                str(environment.dev_mount),
                "describe",
                "--always",
                "--abbrev=40",
                "--dirty",
                "--match=__eval_no_tag__",
            ]
        )
        if mounted_revision.endswith("-dirty") or execute(
            [
                "git",
                "-C",
                str(environment.dev_mount),
                "status",
                "--porcelain",
                "--untracked-files=normal",
            ]
        ):
            raise ValueError("eval: mounted code must be committed before setup reuse")
        tools["mounted-code-revision"] = mounted_revision
        binary = environment.dev_mount / ".fastloop/sdo-operator"
        tools["installed-operator"] = hashlib.sha256(binary.read_bytes()).hexdigest()
    if budget.left() <= 0:
        raise TimeoutError("eval: setup fingerprint exceeded 15 seconds")
    return SetupFingerprint(
        environment.cluster,
        revision,
        image_identity(nodes, environment.images.model_dump()),
        accepted,
        installed,
        digest(manifests),
        object_identities(objects + cluster),
        tools,
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
