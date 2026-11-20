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
    CpuPlacement,
    NodeCgroup,
    default_shim_dir,
    describe_mode,
    format_cpu_list,
    in_block,
    parse_cpu_list,
    place_cpus,
    read_host_cores,
    read_node_cgroup,
    shim_environment,
    split_cpus,
    verify_node,
    visible_cpu_count,
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


def host_cores() -> tuple[frozenset[int], ...]:
    """This host's physical cores (SMT sibling sets) limited to the CPUs this process may use."""
    return read_host_cores(Path("/sys/devices/system/cpu"), frozenset(os.sched_getaffinity(0)))


def preferred_cpus(env: Mapping[str, str] | None = None) -> frozenset[int]:
    """``SDO_LANE_PREFERRED_CPUS``: this host's reserved CPU block (a CPU list such as ``88-111,200-223``).

    Lane nodes are pinned inside it while it has free CPUs and spill to the rest of the host only when it is
    full. Unset: no preference. Keep it inside one NUMA node so a lane's CPUs stay local.
    """
    text = (os.environ if env is None else env).get("SDO_LANE_PREFERRED_CPUS", "").strip()
    try:
        return parse_cpu_list(text)
    except ValueError as exc:
        raise ValueError(f"SDO_LANE_PREFERRED_CPUS {text!r} is not a CPU list such as 0-7,16-23") from exc


def _running_cpusets() -> dict[str, frozenset[int]]:
    """Every running container's ``--cpuset-cpus`` by name (empty: not pinned)."""
    ids = subprocess.run(
        ["docker", "ps", "-q"], check=True, capture_output=True, text=True, timeout=wait_for(WaitKind.CLUSTER_OP)
    ).stdout.split()
    if not ids:
        return {}
    rows = subprocess.run(
        ["docker", "inspect", "--format", "{{.Name}}|{{.HostConfig.CpusetCpus}}", *ids],
        check=True,
        capture_output=True,
        text=True,
        timeout=wait_for(WaitKind.CLUSTER_OP),
    ).stdout.splitlines()
    held = {}
    for row in rows:
        name, separator, cpuset = row.partition("|")
        if separator:
            held[name.lstrip("/")] = parse_cpu_list(cpuset)
    return held


@contextmanager
def _cpuset_lock() -> Iterator[None]:
    """Serialize CPU choice across this host's lanes so two lanes never pick the same free CPUs at once."""
    directory = HostLimits.from_env().directory
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / "cpuset.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield


def _pin_and_update(
    nodes: list[str],
    cpus: str,
    flags: list[str],
    *,
    read: Callable[[str], NodeCgroup | None],
    parent: str | None,
    memory_bytes: int | None,
) -> list[CpuPlacement]:
    """``docker update`` each node with ``flags`` plus a cpuset of exactly ``ceil(cpus)`` CPUs, then verify it.

    The size is the fix: in-pod runtimes (JVM, Go) size themselves from the CPUs they see. Placement is best
    effort (``place_cpus``): free whole cores first, else the least-loaded CPUs, which may share cores with other
    experiments; that overlap is logged and returned for the lane's receipt, never refused. A node already
    pinned to the right size on CPUs no one else holds keeps them unless the preferred block
    (``SDO_LANE_PREFERRED_CPUS``) can now hold it and it is not already inside.
    """
    count = visible_cpu_count(float(cpus))
    cores = host_cores()
    block = preferred_cpus()
    placements = []
    with _cpuset_lock():
        pinned = _running_cpusets()
        held = [cpuset for name, cpuset in pinned.items() if name not in nodes and cpuset]
        for node in nodes:
            current = pinned.get(node, frozenset())
            taken = frozenset().union(*held)
            fresh = place_cpus(count, cores, held, node=node, preferred=block)
            if (
                len(current) == count
                and not current & taken
                and in_block(current, block) >= in_block(fresh.cpus, block)
            ):
                placement = CpuPlacement(
                    node, current, frozenset(cpu for cpu in current for core in cores if cpu in core and core & taken)
                )
            else:
                placement = fresh
            if placement.shared:
                logger.warning(
                    "lane cpuset: %s pinned to %s shares cores with other containers' cpusets on %s (not enough "
                    "free whole cores); its CPU count is right, its isolation is partial",
                    node,
                    format_cpu_list(placement.cpus),
                    format_cpu_list(placement.shared),
                )
            held.append(placement.cpus)
            placements.append(placement)
            node_flags = [*flags, "--cpuset-cpus", format_cpu_list(placement.cpus)]
            _update(node, *node_flags)
            verify_node(
                node,
                lambda node=node, node_flags=node_flags: _update(node, *node_flags),
                cpus=float(cpus),
                memory_bytes=memory_bytes,
                visible_cpus=count,
                parent=parent,
                read=read,
            )
    return placements


def apply_node_limits(
    cluster: str,
    limits: NodeLimits | None = None,
    *,
    read: Callable[[str], NodeCgroup | None] = read_node_cgroup,
    parent: str | None = None,
) -> list[CpuPlacement]:
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
    return _pin_and_update(nodes, cpus, flags, read=read, parent=parent, memory_bytes=parse_memory_size(limits.memory))


def apply_lane_cpu(
    cluster: str,
    lane_cpus: str = DEFAULT_LANE_CPUS,
    *,
    read: Callable[[str], NodeCgroup | None] = read_node_cgroup,
    parent: str | None = None,
) -> list[CpuPlacement]:
    """Re-apply only the lane CPU cap and cpuset (a claim must not touch memory a lane was created with); verify."""
    parent = parent if parent is not None else configured_parent()
    nodes = _lane_nodes(cluster)
    cpus = split_cpus(float(lane_cpus), len(nodes))
    return _pin_and_update(nodes, cpus, ["--cpus", cpus], read=read, parent=parent, memory_bytes=None)


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
