"""Explicit shared-checkout attachment for idle, prepared kind lanes.

Only the accepted application directory is rebound. Proposal, capture, session,
ledger and build directories retain their lane's existing private PVC.
"""

from __future__ import annotations

import json
import os
import subprocess
import uuid
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.adapter import WaitKind, control_namespace_for, kubectl, wait_for
from benchmarks.sregym.fastloop.environment import FastloopEnvironment
from benchmarks.sregym.fastloop.parallel_memory import memory_head

if TYPE_CHECKING:
    from collections.abc import Iterator

    from benchmarks.sregym.fastloop.parallel import ParallelPlan

NODE_ROOT = "/opt/sdo-parallel"
PVC_NAME = "sdo-application-repository"
JOB_NAME = "sdo-controller-run"


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
    for name, value in {
        "GIT_CONFIG_COUNT": "1",
        "GIT_CONFIG_KEY_0": "safe.directory",
        "GIT_CONFIG_VALUE_0": "/workspace/application",
    }.items():
        environment[name] = {"name": name, "value": value}
    containers[0]["env"] = list(environment.values())
    return result


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
    names = ("KUBECONFIG", "SREGYM_BASE_KUBECONFIG", "SDO_TIME_SCALE", "SDO_DEV_MOUNT", "SDO_OPERATOR_BIN")
    previous = {name: os.environ.get(name) for name in names}
    try:
        for name in names[:2]:
            os.environ[name] = str(environment.kubeconfig)
        os.environ["SDO_TIME_SCALE"] = "1"
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


def attach_shared_memory(plan: ParallelPlan, report: Path) -> None:
    """Pause all lanes, stop all writers, bind, then recreate their existing Jobs.

    Prerequisite: prepared idle native controllers, local PVCs and the shared host
    root mounted during kind creation. No installer or seed-copy path runs here.
    Failures leave all writers stopped and a durable phase report for recovery.
    """
    from benchmarks.sregym.adapter import KubectlClusterOps, pause_for_recovery

    accepted = plan.accepted_repository.resolve()
    root = accepted.parent
    if accepted.name != "accepted" or not (accepted / ".git").is_dir():
        raise ValueError("eval: use a full accepted Git checkout at PARALLEL_ROOT/accepted")
    filesystem = _command("stat", "-f", "-c", "%T", str(root))
    if filesystem not in {"ext2/ext3", "xfs", "btrfs", "tmpfs", "zfs"}:
        raise ValueError(f"eval: shared lock requires a local filesystem, observed {filesystem}")
    if _command("git", "-C", str(accepted), "status", "--porcelain"):
        raise ValueError("eval: accepted memory must be clean before attachment")
    head = memory_head(accepted)
    tree = _command("git", "-C", str(accepted), "rev-parse", "HEAD^{tree}")
    environments = {name: FastloopEnvironment.load(path.resolve()) for name, path in plan.lanes.items()}
    if all(e.shared_accepted_id for e in environments.values()):
        assert_lane_isolation(list(environments.values()))
        for environment in environments.values():
            assert_shared_lane(environment)
        return
    if any(e.shared_accepted_id for e in environments.values()):
        raise ValueError("eval: partially attached lanes require explicit recovery from the attachment report")
    if len({e.cluster for e in environments.values()}) != len(environments):
        raise ValueError("eval: each lane requires its own kind cluster")
    attachments = []
    for name, environment in environments.items():
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
        control = control_namespace_for(environment.namespace)
        job = _json(environment, "get", "job", JOB_NAME, "-o", "json", namespace=control)
        shared_controller_job(job, application)
        attachments.append(Attachment(environment, job, owners[0], path, application))
    report.parent.mkdir(parents=True, exist_ok=True)

    def save(phase: str) -> None:
        report.write_text(
            json.dumps(
                {
                    "phase": phase,
                    "accepted_commit": head,
                    "lanes": [
                        {
                            "run_dir": str(a.environment.run_dir),
                            "cluster": a.environment.cluster,
                            "node": a.node,
                            "volume_path": a.volume_path,
                            "application": a.application,
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
    # Pause every controller before stopping any; the existing fence rejects pending closure/reflection.
    for item in attachments:
        with lane_environment(item.environment):
            pause_for_recovery(KubectlClusterOps(), control_namespace_for(item.environment.namespace))
        observed = _command(
            "docker", "exec", item.node, "git", "-C", item.volume_path + "/application", "rev-parse", "HEAD^{tree}"
        )
        if observed != tree:
            raise ValueError("eval: lane accepted memory differs; merge through the broker before sharing")
    save("all-paused")
    for item in attachments:
        kubectl(
            ["delete", "job", JOB_NAME, "--cascade=foreground", "--wait=true", "--timeout=120s"],
            namespace=control_namespace_for(item.environment.namespace),
            kubeconfig=item.environment.kubeconfig,
        )
    save("all-stopped")
    identity = accepted / ".git" / "sdo-shared-id"
    if not identity.exists():
        identity.write_text(uuid.uuid4().hex + "\n")
    # Retain host ownership; restricted controller FSGroup65532 may write this tree.
    _command("docker", "exec", attachments[0].node, "chgrp", "-R", "65532", NODE_ROOT + "/accepted")
    _command("docker", "exec", attachments[0].node, "chmod", "-R", "g+rwX", NODE_ROOT + "/accepted")
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
    for (name, environment), item in zip(environments.items(), attachments, strict=True):
        kubectl(
            ["create", "-f", "-"],
            namespace=control_namespace_for(environment.namespace),
            kubeconfig=environment.kubeconfig,
            input_text=json.dumps(shared_controller_job(item.job, item.application)),
        )
        environment.workspace = accepted
        environment.parallel_lane, environment.shared_accepted_id = name, identity.read_text().strip()
        environment.application = item.application
        # Restart retains Go controller state on its private PVC; refresh the host adapter registry.
        environment.state_path.unlink(missing_ok=True)
        environment.save()
    for environment in environments.values():
        kubectl(
            ["wait", "--for=condition=Ready", "pod", "--selector", f"job-name={JOB_NAME}", "--timeout=120s"],
            namespace=control_namespace_for(environment.namespace),
            kubeconfig=environment.kubeconfig,
        )
        assert_shared_lane(environment)
    save("attached")


def parallel_volume_objects(environment: FastloopEnvironment, node: str) -> list[dict[str, Any]]:
    control = control_namespace_for(environment.namespace)
    volume = "sdo-parallel-repository"
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
    if prior.stdout.strip() and json.loads(prior.stdout)["spec"].get("volumeName") != "sdo-parallel-repository":
        raise ValueError("eval: refusing to replace an existing lane repository PVC")
    for resource in parallel_volume_objects(environment, node):
        kubectl(
            ["apply", "-f", "-"], namespace=None, kubeconfig=environment.kubeconfig, input_text=json.dumps(resource)
        )


def prepare_shared_lanes(plan: ParallelPlan, report: Path) -> None:
    """Reuse fastloop's real pre-injection path, stopping at its no-fault callback."""
    from benchmarks.sregym.adapter import (
        PersistentState,
        native_install_config,
        runtime_config_from_payload,
        sync_existing,
    )
    from benchmarks.sregym.fastloop import cli
    from benchmarks.sregym.fastloop.dry_run import NoFaultReady

    environments = [FastloopEnvironment.load(path.resolve()) for path in plan.lanes.values()]
    if any(environment.shared_accepted_id for environment in environments):
        attach_shared_memory(plan, report)
        return
    for environment in environments:
        if (
            environment.parallel_root is None
            or environment.parallel_root.resolve() != plan.accepted_repository.resolve().parent
        ):
            raise ValueError("eval: prepare requires every lane created with the selected --parallel-root")
        lane_nodes(environment)
        repository_volume_path(environment)
    for environment in environments:
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
            ]
        )
        if plan.agent_registrations:
            args.agent_registrations = plan.agent_registrations.resolve()
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
    accepted = plan.accepted_repository.resolve()
    if not accepted.exists():
        subprocess.run(
            ["git", "clone", "--no-local", str(environments[0].workspace), str(accepted)],
            check=True,
            timeout=wait_for(WaitKind.SLOW_COMMAND),
        )
    attach_shared_memory(plan, report)
