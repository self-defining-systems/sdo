"""SDO's workload steps on an EKS lane, on top of the lane provider (``benchmarks.lanes.eks``).

The provider knows clusters, registries and nodes; this module knows what SDO needs from them. Today that is the
Opus session pool: SDO admits an Opus session only through a filesystem pool (``libs.agent_cli.provider_capacity``).
A kind lane shares one pool per host through its node mounts; an EKS lane keeps one per node in the lane's node
directory, provisioned at ``up`` and again at every row start (a node group scaled to zero and back has new nodes).
"""

from __future__ import annotations

import os
from collections.abc import Mapping, MutableMapping
from typing import TYPE_CHECKING

from benchmarks.lanes.eks import AGENT_LANE, NODE_LANE_ROOT, EksProvider
from libs.agent_cli.provider_capacity import DEFAULT_OPUS_HOST_SHARE

if TYPE_CHECKING:
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

OPUS_POOL = "opus-pool"
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
    return provider.run_on_nodes(opus_pool_script(opus_capacity(environ)))


def apply_opus_pool_env(environment: FastloopEnvironment, environ: MutableMapping[str, str]) -> None:
    """Point SDO at the node pool: one host share per node, named by the lane's cluster."""

    environ["SDO_OPUS_POOL_MODE"] = "per_host"
    environ["SDO_OPUS_POOL_HOST_ID"] = environment.cluster
    environ["SDO_OPUS_POOL_HOST_PATH"] = f"{NODE_LANE_ROOT}/{OPUS_POOL}"
    environ["SDO_OPUS_MAX_SESSIONS"] = str(opus_capacity(environ))
    for name in ("SDO_OPUS_POOL_NFS_SERVER", "SDO_OPUS_POOL_NFS_PATH", "SDO_OPUS_POOL_DIR"):
        environ.pop(name, None)
