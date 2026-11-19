"""Light cgroup isolation for our kind lanes: a shared parent slice, per-node CPU/memory caps and a read-back check.

Two layers, both without root:

* ``--cgroup-parent=<slice>`` on the kind node containers, so every lane we start lives under one systemd slice
  (``sdo-eval.slice``) whose own ``MemoryMax``/``CPUQuota`` cap the whole fleet. kind has no config field for it, so
  a one-file ``docker`` shim (``DOCKER_SHIM``) is put first on ``PATH`` of the processes that run ``kind create``;
  it adds the flag to a ``docker run`` that carries OUR kind-cluster label and passes every other call through
  untouched, so nothing of anyone else's changes.
* per-node ``docker update --cpus/--memory`` (``host_limits``), re-applied on every claim and read back from the
  container's own cgroup files, because a ``systemctl daemon-reload`` can drop properties set through
  ``docker update`` on some runc versions.
* per-node ``--cpuset-cpus`` of ``ceil(cpus)`` host CPUs. A CPU quota is invisible inside the node (the pods'
  cgroups show ``cpu.max max``), so without a cpuset a JVM or Go runtime on a 224-CPU host sizes its thread pools
  and direct buffers for 224 CPUs inside a 6-CPU lane (an OpenSearch with a 400 MiB heap starts 224 netty workers
  and exhausts its 200 MiB direct-memory limit).

Without the slice (a host that has none) only the per-node limits and the admission budget apply.
"""

from __future__ import annotations

import logging
import math
import os
import re
import stat
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from benchmarks.sregym.adapter import WaitKind, wait_for

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

logger = logging.getLogger(__name__)

DEFAULT_PARENT_SLICE = "sdo-eval.slice"
DEFAULT_LANE_CPUS = "6"
#: Settings that mean "do not use a parent slice".
_OFF = frozenset({"none", "off", "no", "false", "0"})
_SLICE_NAME = re.compile(r"[A-Za-z0-9_.@-]+\.slice")
KIND_CLUSTER_LABEL = "io.x-k8s.kind.cluster"
CGROUP_ROOT = Path("/sys/fs/cgroup")
UNIT_DIRECTORIES = (Path("/etc/systemd/system"), Path("/run/systemd/system"), Path("/usr/lib/systemd/system"))

#: Put first on PATH; adds ``--cgroup-parent`` to ``docker run|create`` for our kind clusters only.
DOCKER_SHIM = """#!/bin/sh
# SDO lane docker shim: kind has no cgroup-parent setting, so add it for OUR kind nodes only.
self_dir=$(cd "$(dirname "$0")" && pwd)
real=
old_ifs=$IFS
IFS=:
for dir in $PATH; do
    [ "$dir" = "$self_dir" ] && continue
    if [ -x "$dir/docker" ]; then real="$dir/docker"; break; fi
done
IFS=$old_ifs
[ -n "$real" ] || { echo "sdo docker shim: no docker on PATH" >&2; exit 127; }
if [ -n "$SDO_LANE_CGROUP_PARENT" ] && { [ "$1" = run ] || [ "$1" = create ]; }; then
    prefix=${SDO_LANE_CGROUP_PREFIX:-}
    label=
    match=
    for arg in "$@"; do
        if [ "$label" = 1 ]; then
            case "$arg" in "io.x-k8s.kind.cluster=$prefix"*) [ -n "$prefix" ] && match=1 ;; esac
            label=
            continue
        fi
        case "$arg" in
            --label) label=1 ;;
            --label=io.x-k8s.kind.cluster=*) value=${arg#--label=io.x-k8s.kind.cluster=}
                case "$value" in "$prefix"*) [ -n "$prefix" ] && match=1 ;; esac ;;
        esac
    done
    if [ "$match" = 1 ]; then
        sub=$1
        shift
        set -- "$sub" "--cgroup-parent=$SDO_LANE_CGROUP_PARENT" "$@"
    fi
fi
exec "$real" "$@"
"""


class CgroupMismatch(ValueError):
    """A node container's cgroup does not hold the limit it was given, even after one re-apply."""


def slice_loaded(name: str) -> bool:
    """Whether systemd has a unit file for the slice (read-only; the slice need not be active yet)."""
    return any((directory / name).is_file() for directory in UNIT_DIRECTORIES)


def resolve_cgroup_parent(setting: str | None, *, exists: Callable[[str], bool] = slice_loaded) -> str | None:
    """The parent slice for new kind nodes: unset/``auto`` is ``sdo-eval.slice`` when the host has it, else none."""
    text = (setting or "auto").strip()
    if text.lower() in _OFF:
        return None
    if text.lower() == "auto":
        return DEFAULT_PARENT_SLICE if exists(DEFAULT_PARENT_SLICE) else None
    if _SLICE_NAME.fullmatch(text) is None:
        raise ValueError(
            f"eval: lane_cgroup_parent {text!r} must be a systemd slice name such as {DEFAULT_PARENT_SLICE}"
        )
    if not exists(text):
        raise ValueError(f"eval: lane_cgroup_parent {text!r} has no systemd unit file on this host")
    return text


def describe_mode(parent: str | None) -> str:
    """The one line a host logs about how its lanes are isolated."""
    if parent is None:
        return "lane isolation: per-container limits only (no parent slice) plus the admission budget"
    return (
        f"lane isolation: kind nodes under cgroup parent {parent}, plus per-container limits and the admission budget"
    )


def split_cpus(lane_cpus: float, nodes: int) -> str:
    """Docker ``--cpus`` for each of ``nodes`` nodes of a lane capped at ``lane_cpus``."""
    if nodes < 1:
        raise ValueError("eval: a lane has at least one node")
    return f"{lane_cpus / nodes:.2f}".rstrip("0").rstrip(".")


def kind_run_flags(parent: str | None) -> list[str]:
    """The extra ``docker run`` flags a kind node gets."""
    return [] if parent is None else [f"--cgroup-parent={parent}"]


def install_docker_shim(directory: Path) -> Path:
    """Write the shim as ``<directory>/docker`` (idempotent) and return the directory."""
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "docker"
    if not path.is_file() or path.read_text() != DOCKER_SHIM:
        path.write_text(DOCKER_SHIM)
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return directory


def shim_environment(
    parent: str | None, cluster_prefix: str, shim_dir: Path, base: Mapping[str, str] | None = None
) -> dict[str, str]:
    """Environment for a ``kind create`` so its node containers land in ``parent``; empty without a parent."""
    if parent is None:
        return {}
    path = (os.environ if base is None else base).get("PATH", "")
    install_docker_shim(shim_dir)
    return {
        "PATH": f"{shim_dir}:{path}" if path else str(shim_dir),
        "SDO_LANE_CGROUP_PARENT": parent,
        "SDO_LANE_CGROUP_PREFIX": cluster_prefix,
    }


def default_shim_dir() -> Path:
    return Path(os.environ.get("SDO_LANE_SHIM_DIR", f"/var/tmp/sdo-fastloop/shim-{os.getuid()}"))


def slice_memory_max(parent: str, run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run) -> int | None:
    """The slice's ``MemoryMax`` in bytes through ``systemctl show``; ``None`` for infinity or an unreadable unit."""
    try:
        out = run(
            ["systemctl", "show", parent, "--property=MemoryMax", "--value"],
            capture_output=True,
            text=True,
            check=True,
            timeout=wait_for(WaitKind.PROBE),
        ).stdout.strip()
        value = int(out)
    except (OSError, ValueError, subprocess.SubprocessError):
        return None
    return value if 0 < value < 2**62 else None


@dataclass(frozen=True)
class NodeCgroup:
    """What one node container's own cgroup holds (``cpu.max``/``memory.max``) and where it lives."""

    path: str
    cpus: float | None  # None: unlimited
    memory_bytes: int | None
    #: CPUs the node's workloads can see (``cpuset.cpus.effective``); None when unreadable.
    visible_cpus: int | None = None

    def under(self, parent: str) -> bool:
        # systemd nests "a-b.slice" under "a.slice", so the slice is a path component, not necessarily the first
        return f"/{parent}/" in self.path


def read_node_cgroup(
    container: str,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    root: Path = CGROUP_ROOT,
    proc: Path = Path("/proc"),
) -> NodeCgroup | None:
    """Read the container's cgroup files; ``None`` when they cannot be read (not Linux cgroup v2, not our host)."""
    try:
        pid = int(
            run(
                ["docker", "inspect", "--format", "{{.State.Pid}}", container],
                capture_output=True,
                text=True,
                check=True,
                timeout=wait_for(WaitKind.PROBE),
            ).stdout.strip()
        )
        line = next(row for row in (proc / str(pid) / "cgroup").read_text().splitlines() if row.startswith("0::"))
        full = line.removeprefix("0::")
        # A kind node's init lives one level below the container's own scope, which is where the limits are set.
        match = re.match(r"(.*/docker-[0-9a-f]+\.scope)", full)
        path = match[1] if match else full
        directory = root / path.lstrip("/")
        fields = (directory / "cpu.max").read_text().split()
        memory = (directory / "memory.max").read_text().strip()
    except (OSError, ValueError, StopIteration, subprocess.SubprocessError):
        return None
    try:
        visible: int | None = len(parse_cpu_list((directory / "cpuset.cpus.effective").read_text()))
    except (OSError, ValueError):
        visible = None
    cpus = None if fields[0] == "max" else int(fields[0]) / int(fields[1])
    return NodeCgroup(path, cpus, None if memory == "max" else int(memory), visible)


def parse_cpu_list(text: str) -> frozenset[int]:
    """A cgroup/Docker CPU list such as ``0-2,7`` as a set; empty text is the empty set."""
    cpus: set[int] = set()
    for part in filter(None, text.strip().split(",")):
        first, _, last = part.partition("-")
        cpus.update(range(int(first), int(last or first) + 1))
    return frozenset(cpus)


def format_cpu_list(cpus: Iterable[int]) -> str:
    """The shortest Docker ``--cpuset-cpus`` text for ``cpus``: ranges joined by commas."""
    ordered, parts = sorted(set(cpus)), []
    start = previous = None
    for cpu in [*ordered, None]:
        if cpu is not None and previous is not None and cpu == previous + 1:
            previous = cpu
            continue
        if start is not None:
            parts.append(str(start) if start == previous else f"{start}-{previous}")
        start = previous = cpu
    return ",".join(parts)


def visible_cpu_count(cpus: float) -> int:
    """How many CPUs a node capped at ``cpus`` should show its workloads."""
    return max(1, math.ceil(cpus - 1e-9))


def choose_cpuset(count: int, available: Iterable[int], held: Iterable[frozenset[int]]) -> frozenset[int]:
    """The ``count`` host CPUs other containers' cpusets use least, lowest number first on a tie."""
    use = dict.fromkeys(sorted(set(available)), 0)
    for cpuset in held:
        for cpu in cpuset:
            if cpu in use:
                use[cpu] += 1
    return frozenset(sorted(use, key=lambda cpu: (use[cpu], cpu))[:count])


def cgroup_matches(
    state: NodeCgroup, *, cpus: float | None, memory_bytes: int | None, visible_cpus: int | None = None
) -> bool:
    if cpus is not None and (state.cpus is None or abs(state.cpus - cpus) > 0.01 * cpus):
        return False
    if visible_cpus is not None and state.visible_cpus is not None and state.visible_cpus != visible_cpus:
        return False
    return memory_bytes is None or state.memory_bytes == memory_bytes


def verify_node(
    container: str,
    reapply: Callable[[], None],
    *,
    cpus: float | None,
    memory_bytes: int | None,
    visible_cpus: int | None = None,
    parent: str | None = None,
    read: Callable[[str], NodeCgroup | None] = read_node_cgroup,
) -> NodeCgroup | None:
    """Read the node's cgroup after an update, re-apply once on a mismatch and fail closed if it still differs.

    ``None`` means the files could not be read: the limit is unverifiable here, which is logged, not an error.
    ``visible_cpus`` is how many CPUs the node's workloads must see (its cpuset), checked when readable.
    """
    expected = {"cpus": cpus, "memory_bytes": memory_bytes, "visible_cpus": visible_cpus}
    state = read(container)
    if state is None:
        logger.warning("lane cgroup: cannot read the cgroup of %s; limits are unverified", container)
        return None
    if not cgroup_matches(state, **expected):
        logger.warning(
            "lane cgroup: %s holds %s, expected cpus=%s memory=%s visible_cpus=%s; re-applying",
            container,
            state,
            cpus,
            memory_bytes,
            visible_cpus,
        )
        reapply()
        state = read(container) or state
        if not cgroup_matches(state, cpus=cpus, memory_bytes=memory_bytes):
            raise CgroupMismatch(
                f"eval: {container} cgroup holds cpus={state.cpus} memory={state.memory_bytes} after docker update; "
                f"expected cpus={cpus} memory={memory_bytes}"
            )
        if not cgroup_matches(state, cpus=None, memory_bytes=None, visible_cpus=visible_cpus):
            raise CgroupMismatch(
                f"eval: {container} exposes {state.visible_cpus} CPUs to its workloads but is capped at {cpus} CPUs "
                f"after docker update --cpuset-cpus; JVM and Go runtimes size thread pools and direct buffers from "
                f"the CPUs they see and overrun the cap (expected {visible_cpus} visible CPUs)"
            )
    if parent is not None and not state.under(parent):
        logger.info(
            "lane cgroup: %s is not under %s (created before the slice); per-container limits only", container, parent
        )
    return state
