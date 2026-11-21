"""SDO's workload steps on an EKS lane, on top of the lane provider (``benchmarks.lanes.eks``).

The provider knows clusters, registries and nodes; this module knows what SDO needs from them. Today that is the
Opus session pool: SDO admits an Opus session only through a filesystem pool (``libs.agent_cli.provider_capacity``).
A kind lane shares one pool per host through its node mounts; an EKS lane keeps one per node in the lane's node
directory, provisioned at every row start of an Opus model and never for another model (a node group scaled to zero
and back has new nodes).

The pool's capacity is per node: a lane admits at most ``SDO_OPUS_MAX_SESSIONS`` Opus sessions on each of its nodes,
so a lane of N nodes admits N times that, and lanes that share nodes share the pool. Size the node group and the
number of lanes against the account's Opus session budget; a pool never resizes (see :func:`opus_pool_script`).
"""

from __future__ import annotations

import json
import os
import subprocess
from typing import TYPE_CHECKING

from benchmarks.lanes.eks import AGENT_LANE, NODE_LANE_ROOT, EksProvider
from benchmarks.sregym.adapter import WaitKind, wait_for
from libs.agent_cli.provider_capacity import DEFAULT_OPUS_HOST_SHARE

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, MutableMapping

    from benchmarks.lanes.eks import EksLane
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

OPUS_POOL = "opus-pool"
#: Seconds a new node may take to run its agent before the pool is provisioned on it.
MIRROR_CHECK_TIMEOUT = "120s"
#: The uid SDO's controller and workers run as; the pool's files must be writable for its flocks.
WORKER_UID = 65532


def opus_capacity(environ: Mapping[str, str] | None = None) -> int:
    value = (os.environ if environ is None else environ).get("SDO_OPUS_MAX_SESSIONS", "").strip()
    capacity = int(value) if value else DEFAULT_OPUS_HOST_SHARE
    if capacity <= 0:
        raise ValueError("SDO_OPUS_MAX_SESSIONS must be a positive integer")
    return capacity


def opus_pool_script(capacity: int) -> str:
    """Create the node's pool once; a pool provisioned with another capacity fails closed (never replaced)."""

    root = f"{AGENT_LANE}/{OPUS_POOL}"
    slots = " ".join(f"{root}/slot-{slot}.lock" for slot in range(capacity))
    return "\n".join(
        [
            "set -eu",
            f"mkdir -p {root}",
            f'if [ -s {root}/capacity ] && [ "$(cat {root}/capacity)" != "{capacity}" ]; then',
            f'  echo "node Opus pool has capacity $(cat {root}/capacity), not {capacity}" >&2; exit 1',
            "fi",
            f"[ -s {root}/capacity ] || printf '{capacity}\\n' > {root}/capacity",
            f"touch {slots}",
            f"chown -R {WORKER_UID}:{WORKER_UID} {root}",
            f"chmod 0775 {root} && chmod 0664 {root}/capacity {slots}",
        ]
    )


def provision_opus_pool(environment: FastloopEnvironment, environ: Mapping[str, str] | None = None) -> list[str]:
    """Provision the per-node Opus pool on every node of the lane; returns the nodes."""

    assert environment.eks is not None
    provider = EksProvider(environment.eks, str(environment.kubeconfig))
    return provider.run_on_nodes(opus_pool_script(opus_capacity(environ)), wait=wait_for(WaitKind.ROLLOUT))


def ensure_opus_pool(
    environment: FastloopEnvironment, model: str, environ: Mapping[str, str] | None = None
) -> list[str]:
    """Provision the node pool for a row of ``model``; only an Opus model is admitted through it, so no other model
    provisions it. Returns the nodes provisioned."""

    if "opus" not in model.lower():
        return []
    return provision_opus_pool(environment, environ)


class MirrorCheckError(RuntimeError):
    """A node could not pull an image through the lane's mirror."""


def verify_mirror_serves(
    environment: FastloopEnvironment, *, run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run
) -> str:
    """Prove the mirror serves: a short-lived pod pulls ``sink.PULL_IMAGE`` (the provider pre-publishes it) and exits.

    Bounded by ``MIRROR_CHECK_TIMEOUT``; the pod is always deleted. Raises :class:`MirrorCheckError` naming why the pull
    failed (typically the lane's node agent or ECR mirror, not this adapter). Returns the image."""

    from benchmarks.fuzzops import sink

    name = f"mirror-check-{os.getpid()}"
    kube = ["kubectl", "--kubeconfig", str(environment.kubeconfig)]
    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": "default"},
        "spec": {
            "restartPolicy": "Never",
            "terminationGracePeriodSeconds": 0,
            "securityContext": {
                "runAsNonRoot": True,
                "runAsUser": sink.PULL_UID,
                "seccompProfile": {"type": "RuntimeDefault"},
            },
            "containers": [
                {
                    "name": "check",
                    "image": sink.PULL_IMAGE,
                    "command": ["true"],
                    "securityContext": {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}},
                }
            ],
        },
    }
    try:
        created = run(
            [*kube, "apply", "-f", "-"],
            input=json.dumps(pod),
            capture_output=True,
            text=True,
            check=False,
            timeout=wait_for(WaitKind.SLOW_COMMAND),
        )
        if created.returncode != 0:
            raise MirrorCheckError(f"mirror check pod: {created.stderr.strip()[-300:]}")
        done = run(
            [
                *kube,
                "wait",
                f"pod/{name}",
                "--for=jsonpath={.status.phase}=Succeeded",
                f"--timeout={MIRROR_CHECK_TIMEOUT}",
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=2 * wait_for(WaitKind.SLOW_COMMAND),
        )
        if done.returncode != 0:
            why = run(
                [*kube, "get", "pod", name, "-o", "jsonpath={.status.containerStatuses[0].state.waiting.message}"],
                capture_output=True,
                text=True,
                check=False,
                timeout=wait_for(WaitKind.MINUTE),
            )
            raise MirrorCheckError(
                f"a node could not pull {sink.PULL_IMAGE} through the lane's mirror: "
                f"{(why.stdout or done.stderr).strip()[-300:]}"
            )
    finally:
        run(
            [*kube, "delete", "pod", name, "--ignore-not-found", "--wait=false", "--grace-period=0", "--force"],
            capture_output=True,
            text=True,
            check=False,
            timeout=wait_for(WaitKind.SLOW_COMMAND),
        )
    return sink.PULL_IMAGE


def apply_lane_env(lane: EksLane, environ: MutableMapping[str, str]) -> None:
    """The lane facts a row or ``up`` process reads (native resources, the oracle observability helper): the same ones
    the SREGym worker gets from ``eks_worker_environment``."""

    environ["FASTLOOP_EKS_LANE"] = lane.model_dump_json()
    environ["SREGYM_CLUSTER_PLATFORM"] = "eks"
    if lane.virtual:
        environ["SREGYM_MANAGED_STORAGE"] = "1"
    else:
        environ.pop("SREGYM_MANAGED_STORAGE", None)


def apply_opus_pool_env(environment: FastloopEnvironment, environ: MutableMapping[str, str]) -> None:
    """Point SDO at the node pool: one host share per node, named by the lane's cluster."""

    environ["SDO_OPUS_POOL_MODE"] = "per_host"
    environ["SDO_OPUS_POOL_HOST_ID"] = environment.cluster
    environ["SDO_OPUS_POOL_HOST_PATH"] = f"{NODE_LANE_ROOT}/{OPUS_POOL}"
    environ["SDO_OPUS_MAX_SESSIONS"] = str(opus_capacity(environ))
    for name in ("SDO_OPUS_POOL_NFS_SERVER", "SDO_OPUS_POOL_NFS_PATH", "SDO_OPUS_POOL_DIR"):
        environ.pop(name, None)
