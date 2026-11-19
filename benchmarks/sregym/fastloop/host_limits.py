"""Host-wide admission and fail-closed cgroup caps for fastloop lanes."""

from __future__ import annotations

import fcntl
import logging
import math
import os
import re
import subprocess
import time
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.adapter import WaitKind, parse_memory_size, wait_for
from benchmarks.sregym.fastloop.lane_cgroup import (
    DEFAULT_LANE_CPUS,
    NodeCgroup,
    default_shim_dir,
    describe_mode,
    read_node_cgroup,
    shim_environment,
    split_cpus,
    verify_node,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping

DEFAULT_MEMORY_LIMIT = "16g"  # Astronomy's sampled lane p90 is 9.6 GiB, before node/cache overhead.
DEFAULT_PIDS_LIMIT = 8192
logger = logging.getLogger(__name__)
_HELD: ContextVar[frozenset[tuple[str, str]]] = ContextVar("fastloop_host_locks", default=frozenset())


@dataclass(frozen=True)
class NodeLimits:
    #: Docker ``--cpus`` per node when set explicitly; otherwise ``lane_cpus`` is split across the lane's nodes.
    cpu: str | None = None
    memory: str = DEFAULT_MEMORY_LIMIT
    pids: int = DEFAULT_PIDS_LIMIT
    #: CPU cap of the whole lane (all its kind nodes together).
    lane_cpus: str = DEFAULT_LANE_CPUS

    def __post_init__(self) -> None:
        if self.cpu is not None and (not math.isfinite(float(self.cpu)) or float(self.cpu) <= 0):
            raise ValueError("node CPU limit must be positive and finite")
        if not math.isfinite(float(self.lane_cpus)) or float(self.lane_cpus) <= 0:
            raise ValueError("lane CPU limit must be positive and finite")
        if not re.fullmatch(r"[1-9][0-9]*(?:[bkmgBKMG])?", self.memory):
            raise ValueError("node memory limit must be a positive Docker size, for example 16g")
        if type(self.pids) is not int or self.pids <= 0:
            raise ValueError("node pids limit must be a positive integer")

    def node_cpus(self, nodes: int) -> str:
        """``--cpus`` for each of the lane's ``nodes``: the explicit per-node value, else the lane cap split evenly."""
        return self.cpu if self.cpu is not None else split_cpus(float(self.lane_cpus), nodes)

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> NodeLimits:
        values = os.environ if env is None else {**os.environ, **env}
        return cls(
            values.get("SREGYM_WORKER_CPU_LIMIT") or None,
            values.get("SREGYM_WORKER_MEMORY_LIMIT", DEFAULT_MEMORY_LIMIT),
            int(values.get("SREGYM_WORKER_PIDS_LIMIT", str(DEFAULT_PIDS_LIMIT))),
            values.get("SDO_LANE_CPUS") or DEFAULT_LANE_CPUS,
        )

    @classmethod
    def from_args(cls, args: Any) -> NodeLimits:
        """The limits an ``up`` namespace resolved (``resolve_node_limits``)."""
        return cls(
            args.cpu_limit, args.memory_limit, args.pids_limit, getattr(args, "lane_cpus", None) or DEFAULT_LANE_CPUS
        )


def _lane_nodes(cluster: str) -> list[str]:
    nodes = subprocess.run(
        ["kind", "get", "nodes", "--name", cluster],
        check=True,
        capture_output=True,
        text=True,
        timeout=wait_for(WaitKind.CLUSTER_OP),
    ).stdout.splitlines()
    if not nodes:
        raise ValueError(f"eval: cannot apply node caps: no nodes in {cluster}")
    return nodes


def _update(node: str, *flags: str) -> None:
    subprocess.run(["docker", "update", *flags, node], check=True, timeout=wait_for(WaitKind.CLUSTER_OP))


def apply_node_limits(
    cluster: str,
    limits: NodeLimits | None = None,
    *,
    read: Callable[[str], NodeCgroup | None] = read_node_cgroup,
    parent: str | None = None,
) -> None:
    """Cap every node of ``cluster`` and read the result back from its cgroup (idempotent; safe to repeat)."""
    limits = limits or NodeLimits.from_env()
    parent = parent if parent is not None else configured_parent()
    nodes = _lane_nodes(cluster)
    cpus = limits.node_cpus(len(nodes))
    flags = [
        "--cpus",
        cpus,
        "--memory",
        limits.memory,
        "--memory-swap",
        limits.memory,
        "--pids-limit",
        str(limits.pids),
    ]
    for node in nodes:
        _update(node, *flags)
        verify_node(
            node,
            lambda node=node: _update(node, *flags),
            cpus=float(cpus),
            memory_bytes=parse_memory_size(limits.memory),
            parent=parent,
            read=read,
        )


def apply_lane_cpu(
    cluster: str,
    lane_cpus: str = DEFAULT_LANE_CPUS,
    *,
    read: Callable[[str], NodeCgroup | None] = read_node_cgroup,
    parent: str | None = None,
) -> None:
    """Re-apply only the lane CPU cap (a claim must not touch memory a lane was created with) and verify it."""
    parent = parent if parent is not None else configured_parent()
    nodes = _lane_nodes(cluster)
    cpus = split_cpus(float(lane_cpus), len(nodes))
    for node in nodes:
        _update(node, "--cpus", cpus)
        verify_node(
            node,
            lambda node=node: _update(node, "--cpus", cpus),
            cpus=float(cpus),
            memory_bytes=None,
            parent=parent,
            read=read,
        )


def configured_parent() -> str | None:
    return os.environ.get("SDO_LANE_CGROUP_PARENT") or None


def install_node_limits(worker_infra: object) -> None:
    """Hook the external factory before CNI/image loading and cover reused clusters too.

    The worker process also gets the docker shim first on ``PATH`` when a parent slice is configured, so the
    factory's ``kind create`` puts our nodes under it.
    """
    from typing import cast

    parent, prefix = configured_parent(), os.environ.get("SREGYM_KIND_CLUSTER_PREFIX", "")
    if parent is not None and prefix:
        os.environ.update(shim_environment(parent, prefix, default_shim_dir()))
    logger.info(describe_mode(parent))
    infra = cast("Any", worker_infra)
    infra.apply_worker_cpu_limit = apply_node_limits
    preflight = infra.preflight_cluster_requirements

    def capped_preflight(cluster: str, kubeconfig: str) -> None:
        apply_node_limits(cluster)
        preflight(cluster, kubeconfig)

    infra.preflight_cluster_requirements = capped_preflight


@dataclass(frozen=True)
class HostLimits:
    # /tmp is private in bwrap; /var/tmp is shared by every lane on this host.
    directory: Path = Path("/var/tmp/sdo-fastloop/locks")
    redeploys: int = 2

    def __post_init__(self) -> None:
        if not self.directory.is_absolute():
            raise ValueError("host lock directory must be absolute and shared by all lanes")
        if type(self.redeploys) is not int or self.redeploys < 1:
            raise ValueError("host redeploy limit must be a positive integer")

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> HostLimits:
        values = os.environ if env is None else {**os.environ, **env}
        return cls(
            Path(values.get("FASTLOOP_HOST_LOCK_DIR", str(cls.directory))),
            int(values.get("FASTLOOP_HOST_REDEPLOYS", "2")),
        )

    def worker_env(self) -> dict[str, str]:
        return {"FASTLOOP_HOST_LOCK_DIR": str(self.directory), "FASTLOOP_HOST_REDEPLOYS": str(self.redeploys)}

    @contextmanager
    def admit(self, operation: str) -> Iterator[None]:
        """One image delivery or N redeploys; kernel releases slots on death.

        Reentrant in a caller thread so Tier 2 can span namespace deletion and
        its worker's deploy request. Image delivery uses a separate admission
        domain and can run while a redeploy waits for image loading.
        """
        if operation not in {"image-delivery", "redeploy"}:
            raise ValueError(f"unknown host admission operation: {operation}")
        key = (str(self.directory.resolve()), operation)
        if key in _HELD.get():
            yield
            return
        self.directory.mkdir(parents=True, exist_ok=True)
        count = 1 if operation == "image-delivery" else self.redeploys
        # Fail closed if lanes disagree about the shared semaphore capacity.
        with (self.directory / f"{operation}.capacity").open("a+") as capacity:
            fcntl.flock(capacity, fcntl.LOCK_EX)
            capacity.seek(0)
            recorded = capacity.read().strip()
            if recorded and recorded != str(count):
                raise ValueError(
                    f"eval: host {operation} capacity is {recorded}, requested {count}; configure lanes consistently"
                )
            if not recorded:
                capacity.write(str(count) + "\n")
                capacity.flush()
        while True:
            for slot in range(count):
                name = "image-delivery.lock" if operation == "image-delivery" else f"redeploy-{slot}.lock"
                lock = (self.directory / name).open("a")
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    lock.close()
                    continue
                token = _HELD.set(_HELD.get() | {key})
                try:
                    yield
                finally:
                    _HELD.reset(token)
                    lock.close()
                return
            time.sleep(wait_for(WaitKind.POLL_FAST))
