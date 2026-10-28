"""Explicit shared-checkout attachment for idle, prepared kind lanes.

Only the accepted application directory is rebound. Proposal, capture, session,
ledger and build directories retain their lane's existing private PVC.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any, TypeVar

from benchmarks.sregym.adapter import WaitKind, control_namespace_for, kubectl, wait_for
from benchmarks.sregym.fastloop.environment import FastloopEnvironment
from benchmarks.sregym.fastloop.parallel_memory import memory_ancestry, memory_head

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from benchmarks.sregym.fastloop.parallel import ParallelPlan

NODE_ROOT = "/opt/sdo-parallel"
PVC_NAME = "sdo-application-repository"
JOB_NAME = "sdo-controller-run"
SHARED_ID = "sdo-shared-id"
T = TypeVar("T")


def shared_controller_job(job: dict[str, Any], application: str) -> dict[str, Any]:
    result = deepcopy(job)
    result.pop("status", None)
    metadata = result["metadata"]
    for key in ("uid", "resourceVersion", "creationTimestamp", "generation", "managedFields", "ownerReferences"):
        metadata.pop(key, None)
    spec = result["spec"]
    spec.pop("selector", None)
    spec.pop("manualSelector", None)
    labels = spec["template"].get("metadata", {}).get("labels", {})
    for key in ("controller-uid", "batch.kubernetes.io/controller-uid", "job-name", "batch.kubernetes.io/job-name"):
        labels.pop(key, None)
    containers = spec["template"]["spec"]["containers"]
    if len(containers) != 1 or containers[0]["name"] != "controller":
        raise ValueError("eval: shared attachment requires the native controller Job")
    args = containers[0]["args"]
    if args.count("--application") != 1:
        raise ValueError("eval: controller Job lacks one application identity")
    args[args.index("--application") + 1] = application
    environment = {row["name"]: row for row in containers[0].get("env", [])}
    containers[0]["env"] = list(environment.values())
    return result


def accepted_ownership_commands(path: str, *, host_gid: int) -> list[tuple[str, ...]]:
    """Node-side commands giving the controller uid ownership and the host group write access.

    Git refuses to read a repository owned by another uid (also through the operator's sync Pod),
    so the controller uid owns the checkout; the host keeps access through its group.
    """
    return [
        ("chown", "-R", f"65532:{host_gid}", path),
        ("chmod", "-R", "g+rwX", path),
        ("find", path, "-type", "d", "-exec", "chmod", "g+s", "{}", "+"),
    ]


def _command(*args: str) -> str:
    return subprocess.check_output(args, text=True, stderr=subprocess.PIPE, timeout=wait_for(WaitKind.COMMAND)).strip()


def _json(environment: FastloopEnvironment, *args: str, namespace: str | None = None) -> dict[str, Any]:
    return json.loads(kubectl(list(args), namespace=namespace, kubeconfig=environment.kubeconfig).stdout)


def lane_nodes(environment: FastloopEnvironment) -> list[str]:
    nodes = _command("kind", "get", "nodes", "--name", environment.cluster).splitlines()
    if not nodes or environment.parallel_root is None:
        raise ValueError("eval: shared lanes require --parallel-root at kind creation")
    for node in nodes:
        mounts = json.loads(_command("docker", "inspect", "--format", "{{json .Mounts}}", node))
        if not any(
            row.get("Type") == "bind"
            and row.get("Destination") == NODE_ROOT
            and Path(row["Source"]).resolve() == environment.parallel_root.resolve()
            for row in mounts
        ):
            raise ValueError(f"eval: {node} lacks the selected host shared bind mount")
    return nodes


def repository_volume_path(environment: FastloopEnvironment) -> str:
    control = control_namespace_for(environment.namespace)
    claim = _json(environment, "get", "pvc", PVC_NAME, "-o", "json", namespace=control)
    volume = _json(environment, "get", "pv", claim["spec"]["volumeName"], "-o", "json")
    ref = volume["spec"].get("claimRef", {})
    if ref.get("namespace") != control or ref.get("name") != PVC_NAME:
        raise ValueError("eval: repository PV is not owned by this lane claim")
    # HostPath retains nested mounts through the container runtime's recursive
    # bind. Do not use the local-volume plugin's intermediate plain bind.
    source = volume["spec"].get("hostPath") or {}
    path = source.get("path", "")
    if not path or not Path(path).is_absolute() or ".." in Path(path).parts or Path(path) == Path("/"):
        raise ValueError("eval: shared lanes require an existing local hostPath repository PV")
    if path != f"{NODE_ROOT}/lanes/{environment.cluster}":
        raise ValueError("eval: shared PVC must be the lane-private preprovisioned hostPath")
    return path


def lane_git_command(environment: FastloopEnvironment, volume_path: str, accepted: Path, *git: str) -> tuple[str, ...]:
    """Command that reads a lane's mounted accepted repository outside the kind node.

    Kind node images ship no git and the lane repository is owned by the controller
    uid, so a throwaway root container built from the lane's own controller image
    (which ships git) reads the host side of the node bind mount. The accepted
    checkout's objects are visible read-only for tree comparison.
    """
    if environment.parallel_root is None:
        raise ValueError("eval: shared lanes require --parallel-root at kind creation")
    try:
        relative = PurePosixPath(volume_path).relative_to(NODE_ROOT)
    except ValueError:
        raise ValueError(f"eval: lane volume {volume_path} is outside the shared node root {NODE_ROOT}") from None
    if ".." in relative.parts:
        raise ValueError(f"eval: lane volume {volume_path} escapes the shared node root")
    host = environment.parallel_root.resolve() / relative
    return (
        *("docker", "run", "--rm", "--network", "none", "--user", "0", "--entrypoint", "git"),
        *("-e", "GIT_ALTERNATE_OBJECT_DIRECTORIES=/accepted/.git/objects"),
        *("-v", f"{host}:/lane:ro", "-v", f"{accepted.resolve()}:/accepted:ro", environment.images.controller),
        *("-c", "safe.directory=/lane/application", "-C", "/lane/application", *git),
    )


def _tree_entries(listing: str) -> dict[str, str]:
    """``git ls-tree -r`` rows keyed by path; values carry mode, type and object id."""
    entries = {}
    for line in listing.splitlines():
        meta, _, path = line.partition("\t")
        entries[path] = meta
    return entries


def lane_match(environment: FastloopEnvironment, volume_path: str, accepted: Path, head: str) -> str:
    """Newest accepted commit (first-parent history of ``head``) whose tree the lane's tree equals.

    A lane may have been prepared before peers published, so any accepted commit proves the
    lane holds no private memory; its controller adopts the current head when it resumes.
    Cluster-local captures are controller state outside memory, so no path is exempt: raises
    naming the paths that differ from the closest accepted commit.
    """
    history = [
        line.split()
        for line in _command("git", "-C", str(accepted), "log", "--first-parent", "--format=%H %T", head).splitlines()
    ]
    observed = _command(*lane_git_command(environment, volume_path, accepted, "rev-parse", "HEAD^{tree}"))
    for commit, tree in history:
        if tree == observed:
            return commit
    lane = _tree_entries(_command(*lane_git_command(environment, volume_path, accepted, "ls-tree", "-r", "HEAD")))
    closest: list[str] | None = None
    for commit, _tree in history:
        entries = _tree_entries(_command("git", "-C", str(accepted), "ls-tree", "-r", commit))
        paths = sorted(path for path in lane.keys() | entries.keys() if lane.get(path) != entries.get(path))
        if closest is None or len(paths) < len(closest):
            closest = paths
    raise ValueError(
        "eval: lane accepted memory differs; merge through the broker before sharing: "
        + ", ".join(closest or [observed])
    )


class LaneStepsFailed(RuntimeError):
    """Some lanes failed a concurrent step; ``completed`` keeps the lanes that finished."""

    def __init__(self, errors: dict[str, str], completed: dict[str, Any]) -> None:
        super().__init__(f"eval: lanes failed: {errors}")
        self.errors = errors
        self.completed = completed


def concurrently(work: dict[str, Callable[[], T]]) -> dict[str, T]:
    """Run independent per-lane steps together, join every lane, then raise naming each failed lane."""
    results: dict[str, T] = {}
    errors: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=max(1, len(work))) as pool:
        futures = {pool.submit(step): name for name, step in work.items()}
        for future in as_completed(futures):
            name = futures[future]
            try:
                results[name] = future.result()
            except Exception as exc:
                errors[name] = f"{type(exc).__name__}: {exc}"
    if errors:
        raise LaneStepsFailed(errors, results)
    return results


def assert_lane_isolation(environments: list[FastloopEnvironment]) -> None:
    for attribute in ("cluster", "kubeconfig", "run_dir", "private_tmp", "application"):
        values = [getattr(e, attribute) for e in environments]
        if any(value is None for value in values) or len(set(values)) != len(values):
            raise ValueError(f"eval: lanes must have private {attribute}")
    identities = {e.shared_accepted_id for e in environments}
    repositories = {e.workspace.resolve() for e in environments}
    if len(identities) != 1 or "" in identities or len(repositories) != 1:
        raise ValueError("eval: lanes must share exactly one accepted checkout identity")


def assert_shared_lane(environment: FastloopEnvironment) -> None:
    from benchmarks.sregym.adapter import RuntimeConfig, assert_shared_repository, native_install_config

    if not environment.shared_accepted_id or not environment.parallel_lane or environment.parallel_root is None:
        raise ValueError("eval: attach shared memory before running parallel lanes")
    if environment.workspace.resolve() != (environment.parallel_root / "accepted").resolve():
        raise ValueError("eval: shared accepted checkout differs from mounted root")
    expected_inode = str((environment.workspace / ".git").stat().st_ino)
    path = repository_volume_path(environment) + "/application/.git"
    matching = [
        node
        for node in lane_nodes(environment)
        if subprocess.run(
            ["docker", "exec", node, "stat", "-c", "%i", path],
            text=True,
            capture_output=True,
            check=False,
            timeout=wait_for(WaitKind.PROBE),
        ).stdout.strip()
        == expected_inode
    ]
    if len(matching) != 1:
        raise ValueError("eval: repository PVC does not resolve to the host accepted Git directory")
    config = RuntimeConfig(
        repository=environment.workspace,
        namespace=environment.namespace,
        application=environment.application,
        controller_image=environment.images.controller,
        responder_image=environment.images.responder,
        validator_image=environment.images.validator,
        repository_pvc=PVC_NAME,
        credentials_secret="sdo-agent-credentials",
        model="gpt-6-luna",
        timeout_seconds=int(wait_for(WaitKind.RESPONSE, 2)),
        shared_accepted_id=environment.shared_accepted_id,
        controller_namespace=control_namespace_for(environment.namespace),
    )
    with lane_environment(environment):
        assert_shared_repository(native_install_config(config))


@contextmanager
def lane_environment(environment: FastloopEnvironment) -> Iterator[None]:
    # Attachment/preflight is serial. Incident runs always get separate processes.
    names = ("KUBECONFIG", "SREGYM_BASE_KUBECONFIG", "SDO_DEV_MOUNT", "SDO_OPERATOR_BIN")
    previous = {name: os.environ.get(name) for name in names}
    try:
        for name in names[:2]:
            os.environ[name] = str(environment.kubeconfig)
        if environment.dev_mount is not None:
            os.environ["SDO_DEV_MOUNT"] = "1"
            os.environ["SDO_OPERATOR_BIN"] = str(environment.dev_mount / ".fastloop/sdo-operator")
        yield
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


@dataclass(frozen=True)
class Attachment:
    environment: FastloopEnvironment
    job: dict[str, Any]
    node: str
    volume_path: str
    application: str
    matched: list[str] = field(default_factory=list)


def attach_shared_memory(plan: ParallelPlan, report: Path) -> None:
    """Pause the joining lanes, stop their writers, bind, then recreate their existing Jobs.

    The first cohort creates the shared memory behind one quiescent barrier over all its lanes.
    Once the memory is live (its identity exists), unattached plan lanes join as a later cohort:
    only their own writers pause and stop, running lanes keep publishing, and the accepted
    history is only read. Prerequisite: prepared idle native controllers, local PVCs and the
    shared host root mounted during kind creation. No installer or seed-copy path runs here.
    Failures leave the joining lanes' writers stopped and a durable phase report for recovery.
    """
    from benchmarks.sregym import adapter

    accepted = plan.accepted_repository.resolve()
    root = accepted.parent
    if accepted.name != "accepted" or not (accepted / ".git").is_dir():
        raise ValueError("eval: use a full accepted Git checkout at PARALLEL_ROOT/accepted")
    filesystem = _command("stat", "-f", "-c", "%T", str(root))
    if filesystem not in {"ext2/ext3", "xfs", "btrfs", "tmpfs", "zfs"}:
        raise ValueError(f"eval: shared lock requires a local filesystem, observed {filesystem}")
    identity = accepted / ".git" / SHARED_ID
    live = identity.read_text().strip() if identity.exists() else ""
    environments = {name: FastloopEnvironment.load(path.resolve()) for name, path in plan.lanes.items()}
    joining = {name: e for name, e in environments.items() if not e.shared_accepted_id}
    if any(e.shared_accepted_id and e.shared_accepted_id != live for e in environments.values()):
        raise ValueError("eval: partially attached lanes require explicit recovery from the attachment report")
    if not joining:
        assert_lane_isolation(list(environments.values()))
        for environment in environments.values():
            assert_shared_lane(environment)
        return
    # Running writers of a live memory may be mid-commit; only the first cohort needs a clean tree.
    if not live and _command("git", "-C", str(accepted), "status", "--porcelain"):
        raise ValueError("eval: accepted memory must be clean before attachment")
    head = memory_head(accepted)
    if len({e.cluster for e in environments.values()}) != len(environments):
        raise ValueError("eval: each lane requires its own kind cluster")
    attachments = []
    for name, environment in joining.items():
        if environment.parallel_root is None or environment.parallel_root.resolve() != root:
            raise ValueError("eval: every lane must mount the same --parallel-root")
        nodes = lane_nodes(environment)
        path = repository_volume_path(environment)
        control = control_namespace_for(environment.namespace)
        pods = _json(environment, "get", "pods", "--selector", f"job-name={JOB_NAME}", "-o", "json", namespace=control)[
            "items"
        ]
        owners = [pod["spec"]["nodeName"] for pod in pods if pod.get("status", {}).get("phase") == "Running"]
        if len(owners) != 1 or owners[0] not in nodes:
            raise ValueError("eval: repository requires one running controller on a kind node")
        application = f"{environment.application}-{name}"[:30].rstrip("-") + "-" + uuid.uuid4().hex
        job = _json(environment, "get", "job", JOB_NAME, "-o", "json", namespace=control)
        shared_controller_job(job, application)
        attachments.append(Attachment(environment, job, owners[0], path, application))
    report.parent.mkdir(parents=True, exist_ok=True)

    def save(phase: str) -> None:
        report.write_text(
            json.dumps(
                {
                    "phase": phase,
                    "cohort": "late" if live else "initial",
                    "accepted_commit": head,
                    "lanes": [
                        {
                            "run_dir": str(a.environment.run_dir),
                            "cluster": a.environment.cluster,
                            "node": a.node,
                            "volume_path": a.volume_path,
                            "application": a.application,
                            "matched_commit": a.matched[0] if a.matched else None,
                            "original_job": a.job,
                        }
                        for a in attachments
                    ],
                },
                indent=2,
            )
            + "\n"
        )

    save("preflight")
    # Pause every joining controller before stopping any; the existing fence rejects pending
    # closure/reflection. Pausing uses this process's KUBECONFIG, so it stays one lane at a time.
    for item in attachments:
        with lane_environment(item.environment):
            adapter.pause_for_recovery(adapter.KubectlClusterOps(), control_namespace_for(item.environment.namespace))

    def compare(item: Attachment) -> None:
        item.matched.append(lane_match(item.environment, item.volume_path, accepted, head))

    concurrently({item.environment.cluster: lambda item=item: compare(item) for item in attachments})
    save("all-paused")
    concurrently(
        {
            item.environment.cluster: lambda item=item: kubectl(
                ["delete", "job", JOB_NAME, "--cascade=foreground", "--wait=true", "--timeout=120s"],
                namespace=control_namespace_for(item.environment.namespace),
                kubeconfig=item.environment.kubeconfig,
            )
            for item in attachments
        }
    )
    save("all-stopped")
    if not live:
        identity.write_text(uuid.uuid4().hex + "\n")
        for command in accepted_ownership_commands(NODE_ROOT + "/accepted", host_gid=os.getgid()):
            _command("docker", "exec", attachments[0].node, *command)
    for item in attachments:
        _command(
            "docker",
            "exec",
            "--privileged",
            item.node,
            "mount",
            "--bind",
            NODE_ROOT + "/accepted",
            item.volume_path + "/application",
        )
    save("all-bound")
    names = {e.cluster: name for name, e in joining.items()}
    for item in attachments:
        environment = item.environment
        kubectl(
            ["create", "-f", "-"],
            namespace=control_namespace_for(environment.namespace),
            kubeconfig=environment.kubeconfig,
            input_text=json.dumps(shared_controller_job(item.job, item.application)),
        )
        environment.workspace = accepted
        environment.parallel_lane, environment.shared_accepted_id = (
            names[environment.cluster],
            identity.read_text().strip(),
        )
        environment.application = item.application
        # Restart retains Go controller state on its private PVC; refresh the host adapter registry.
        environment.state_path.unlink(missing_ok=True)
        environment.save()
    for item in attachments:
        kubectl(
            ["wait", "--for=condition=Ready", "pod", "--selector", f"job-name={JOB_NAME}", "--timeout=120s"],
            namespace=control_namespace_for(item.environment.namespace),
            kubeconfig=item.environment.kubeconfig,
        )
        assert_shared_lane(item.environment)
    # Peers may have published meanwhile; attachment itself never rewinds the one accepted history.
    memory_ancestry(accepted, head, memory_head(accepted))
    save("attached")


PARALLEL_VOLUME = "sdo-parallel-repository"


def parallel_volume_objects(environment: FastloopEnvironment, node: str) -> list[dict[str, Any]]:
    control = control_namespace_for(environment.namespace)
    volume = PARALLEL_VOLUME
    return [
        {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": control}},
        {
            "apiVersion": "v1",
            "kind": "PersistentVolume",
            "metadata": {"name": volume},
            "spec": {
                "capacity": {"storage": "20Gi"},
                "accessModes": ["ReadWriteOnce"],
                "storageClassName": "",
                "persistentVolumeReclaimPolicy": "Retain",
                "claimRef": {"namespace": control, "name": PVC_NAME},
                "hostPath": {"path": f"{NODE_ROOT}/lanes/{environment.cluster}", "type": "Directory"},
                "nodeAffinity": {
                    "required": {
                        "nodeSelectorTerms": [
                            {
                                "matchExpressions": [
                                    {"key": "kubernetes.io/hostname", "operator": "In", "values": [node]}
                                ]
                            }
                        ]
                    }
                },
            },
        },
        {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {"name": PVC_NAME, "namespace": control},
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "storageClassName": "",
                "volumeName": volume,
                "resources": {"requests": {"storage": "20Gi"}},
            },
        },
    ]


def provision_parallel_volume(environment: FastloopEnvironment) -> None:
    """Prebind a private hostPath before the first native SDO installation."""
    if environment.parallel_root is None:
        raise ValueError("eval: parallel volume requires its kind host mount")
    nodes = lane_nodes(environment)
    inventory = _json(environment, "get", "nodes", "-o", "json")["items"]
    eligible = sorted(
        row["metadata"]["name"]
        for row in inventory
        if row["metadata"]["name"] in nodes
        and not row.get("spec", {}).get("unschedulable")
        and not any(
            taint.get("effect") in {"NoSchedule", "NoExecute"} for taint in row.get("spec", {}).get("taints", [])
        )
    )
    if not eligible:
        raise ValueError("eval: no schedulable kind node for the private repository PVC")
    node = eligible[0]
    private = environment.parallel_root / "lanes" / environment.cluster
    private.mkdir(parents=True, exist_ok=True)
    node_path = f"{NODE_ROOT}/lanes/{environment.cluster}"
    _command("docker", "exec", node, "chgrp", "65532", node_path)
    _command("docker", "exec", node, "chmod", "2770", node_path)
    control = control_namespace_for(environment.namespace)
    prior = kubectl(
        ["get", "pvc", PVC_NAME, "--ignore-not-found=true", "-o", "json"],
        namespace=control,
        kubeconfig=environment.kubeconfig,
        check=False,
    )
    claim = json.loads(prior.stdout) if prior.stdout.strip() else None
    if claim is not None and claim["spec"].get("volumeName") != PARALLEL_VOLUME:
        raise ValueError("eval: refusing to replace an existing lane repository PVC")
    existing = kubectl(
        ["get", "pv", PARALLEL_VOLUME, "--ignore-not-found=true", "-o", "json"],
        namespace=None,
        kubeconfig=environment.kubeconfig,
        check=False,
    )
    volume = json.loads(existing.stdout) if existing.stdout.strip() else None
    bound = (volume or {}).get("spec", {}).get("claimRef", {}).get("uid")
    if bound and bound != (claim or {}).get("metadata", {}).get("uid"):
        # The claim and its PV form one pair: a PV still bound to a deleted claim
        # never binds a new one. Retain plus hostPath keep the repository on disk,
        # so recreate both objects together, never one without the other.
        if claim is not None:
            kubectl(
                ["delete", "pvc", PVC_NAME, "--ignore-not-found=true", "--wait=true"],
                namespace=control,
                kubeconfig=environment.kubeconfig,
            )
        kubectl(
            ["delete", "pv", PARALLEL_VOLUME, "--ignore-not-found=true", "--wait=true"],
            namespace=None,
            kubeconfig=environment.kubeconfig,
        )
    for resource in parallel_volume_objects(environment, node):
        kubectl(
            ["apply", "-f", "-"], namespace=None, kubeconfig=environment.kubeconfig, input_text=json.dumps(resource)
        )


def prepare_lane(plan: ParallelPlan, name: str) -> None:
    """Reuse fastloop's real pre-injection path for one lane, stopping at its no-fault callback.

    Sets this process's KUBECONFIG, so each lane runs in its own process (``prepare_lane_process``).
    """
    from benchmarks.sregym.adapter import (
        PersistentState,
        native_install_config,
        runtime_config_from_payload,
        sync_existing,
    )
    from benchmarks.sregym.fastloop import cli
    from benchmarks.sregym.fastloop.dry_run import NoFaultReady

    environment = FastloopEnvironment.load(plan.lanes[name].resolve())
    args = cli.build_parser().parse_args(
        [
            "run",
            "--run-dir",
            str(environment.run_dir),
            "--agent",
            "sdo",
            "--healthy-baseline",
            "--no-checkpoints",
            "--timeout",
            str(plan.timeout_seconds),
            *plan.worker_arguments(),
        ]
    )
    directory = environment.run_dir / "parallel-setup" / plan.run_id
    directory.mkdir(parents=True, exist_ok=True)
    with lane_environment(environment), cli._worker(environment) as worker:
        cli.check_host_capacity(args)
        agent = cli._sdo_agent(args, environment, directory, worker=worker)

        def no_fault() -> Any:
            raise NoFaultReady

        try:
            try:
                agent.resolve(0, "parallel-prepare", no_fault)
            except NoFaultReady:
                pass
            else:
                raise ValueError("eval: shared preparation never reached the pre-injection gate")
        finally:
            agent.close()
        state = PersistentState.load(environment.state_path)
        record = state.controllers.get(environment.namespace)
        if record is None or record.pending is not None:
            raise ValueError("eval: shared preparation requires an idle native controller registry")
        sync_existing(native_install_config(runtime_config_from_payload(record.runtime_config)))


def prepare_lane_process(plan: ParallelPlan, name: str) -> None:
    """Prepare one lane in a child process with the lane's own kubeconfig and the shared CPU budget.

    Image delivery and app redeploys inside the child take the host-wide flock admissions,
    so concurrent lanes keep serial delivery and the redeploy semaphore.
    """
    from benchmarks.sregym.fastloop.parallel import lane_process_environment

    environment = FastloopEnvironment.load(plan.lanes[name].resolve())
    directory = environment.run_dir / "parallel-setup" / plan.run_id
    directory.mkdir(parents=True, exist_ok=True)
    plan_path = directory / "plan.json"
    plan_path.write_text(plan.model_dump_json(indent=2) + "\n")
    command = [
        sys.executable,
        "-m",
        "benchmarks.sregym.fastloop",
        "parallel-prepare-lane",
        "--plan",
        str(plan_path),
        "--lane",
        name,
    ]
    log_path = directory / "prepare.log"
    with log_path.open("a") as log:
        completed = subprocess.run(
            command,
            stdout=log,
            stderr=subprocess.STDOUT,
            check=False,
            env=lane_process_environment(environment, len(plan.lanes)),
        )
    if completed.returncode:
        raise RuntimeError(f"eval: lane {name} preparation exited {completed.returncode}; see {log_path}")


def _clone_accepted(source: Path, accepted: Path) -> None:
    subprocess.run(
        ["git", "clone", "--no-local", str(source), str(accepted)],
        check=True,
        timeout=wait_for(WaitKind.SLOW_COMMAND),
    )


def prepare_shared_lanes(
    plan: ParallelPlan,
    report: Path,
    *,
    prepare: Callable[[ParallelPlan, str], None] = prepare_lane_process,
    attach: Callable[[ParallelPlan, Path], None] = attach_shared_memory,
) -> None:
    """Prepare every unattached lane at once, then attach behind one barrier.

    Already attached lanes are skipped, so the same command grows a running shared memory
    with a later cohort. If a lane fails, the prepared lanes still attach and the error names
    the failed lanes; rerunning prepares and attaches only those.
    """
    environments = {name: FastloopEnvironment.load(path.resolve()) for name, path in plan.lanes.items()}
    for environment in environments.values():
        if (
            environment.parallel_root is None
            or environment.parallel_root.resolve() != plan.accepted_repository.resolve().parent
        ):
            raise ValueError("eval: prepare requires every lane created with the selected --parallel-root")
        if not environment.shared_accepted_id:
            lane_nodes(environment)
            repository_volume_path(environment)
    pending = [name for name, environment in environments.items() if not environment.shared_accepted_id]
    failure: LaneStepsFailed | None = None
    try:
        concurrently({name: lambda name=name: prepare(plan, name) for name in pending})
    except LaneStepsFailed as exc:
        failure = exc
    ready = [name for name in plan.lanes if failure is None or name not in failure.errors]
    if pending and not set(ready) & set(pending):
        raise failure or RuntimeError("eval: no lane prepared")
    accepted = plan.accepted_repository.resolve()
    if not accepted.exists():
        _clone_accepted(environments[next(name for name in ready if name in pending)].workspace, accepted)
    attach(plan.model_copy(update={"lanes": {name: plan.lanes[name] for name in ready}}), report)
    if failure is not None:
        raise failure
