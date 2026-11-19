"""The narrow interface a lane provider offers its workload adapters (SREGym fastloop, fuzzops-bench).

A provider owns the cluster side of a lane: attach to (or create) the cluster, give out the kubeconfig, fence a
kubeconfig to that cluster, make images pullable by nodes, write node files, and release the lane. Everything a
benchmark or SDO needs on top (deploying an app, credentials, evidence claims, memory) is the workload adapter's.
Kind lanes still use the fastloop code paths; this interface is where they move when the platform is shared.
"""

from __future__ import annotations

from typing import Protocol


class LaneProvider(Protocol):
    #: ``kind`` or ``eks``.
    platform: str

    def kubeconfig(self) -> str:
        """Path of the kubeconfig that reaches the lane's cluster with cluster-admin rights."""
        ...

    def fence(self) -> None:
        """Raise unless the kubeconfig still reaches this lane's cluster."""
        ...

    def pin(self, local: str) -> str:
        """Make a local image pullable by every node; returns its immutable reference."""
        ...

    def holds(self, reference: str) -> bool:
        """Whether a reference :meth:`pin` returned still resolves to the same image for every node."""
        ...

    def publish(self, local: str, declared: str) -> str:
        """Make every node resolve ``declared`` to the bytes of the local image ``local``; returns its digest."""
        ...

    def write_node_file(self, relative: str, content: str) -> list[str]:
        """Write a containerd ``certs.d`` file on every node; returns the nodes."""
        ...

    def run_on_nodes(self, script: str) -> list[str]:
        """Run a shell script on every node with the lane's node directory at ``/lane``; returns the nodes."""
        ...

    def release(self) -> None:
        """Give the lane back (attach-only providers leave the cluster as it was attached)."""
        ...
