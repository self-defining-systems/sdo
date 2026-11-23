"""Host memory admission for lanes and eval cells: reserve, pressure and a budget for our own lane caps.

Three read-only checks keep a shared host from thrashing:

* ``MemAvailable >= lane cap + reserve``: a lane may be started only if its whole cap would still leave the reserve;
* memory PSI ``some avg60`` below a threshold: a host already stalling on memory takes no new work;
* the caps of our running lanes (Docker node ``--memory`` limits under our cluster prefix) plus the new lane stay
  within a configured budget, so lanes we start cannot add up to more than we agreed to use. Foreign clusters are
  never counted and never touched.

A missing or unparsable measurement refuses admission. Production code stays benchmark-neutral: nothing here knows
about a fault or a verdict.
"""

from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.adapter.budgets import WaitKind, wait_for
from benchmarks.sregym.adapter.lane_resources import host_available_memory

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

_GIB = 1024**3
_SIZE = re.compile(r"([1-9][0-9]*)([bkmgBKMG]?)")
_UNITS = {"": 1, "b": 1, "k": 1024, "m": 1024**2, "g": 1024**3}
PRESSURE_FILE = Path("/proc/pressure/memory")
CGROUP_FILE = Path("/proc/self/cgroup")
CGROUP_ROOT = Path("/sys/fs/cgroup")
MEMINFO_FILE = Path("/proc/meminfo")
KIND_CLUSTER_LABEL = "io.x-k8s.kind.cluster"
HOST_ENV_FILE = "~/.config/sdo/host.env"


class HostMemoryRefused(ValueError):
    """The host has no memory room for this lane or cell; the message is the logged reason."""


@dataclass(frozen=True)
class HostMemoryPolicy:
    reserve_bytes: int = 32 * _GIB
    #: Refuse at or above this memory PSI ``some avg60`` (percent of time some task stalled on memory).
    max_pressure_avg60: float = 10.0
    #: Upper bound on the summed caps of our running lanes; ``None`` leaves the sum unchecked.
    lane_budget_bytes: int | None = None

    def __post_init__(self) -> None:
        if type(self.reserve_bytes) is not int:
            raise TypeError("host memory reserve_bytes must be an integer")
        if self.reserve_bytes < 0:
            raise ValueError("host memory reserve_bytes must not be negative")
        if not isinstance(self.max_pressure_avg60, (int, float)) or isinstance(self.max_pressure_avg60, bool):
            raise TypeError("host memory max_pressure_avg60 must be a number")
        if not 0 < self.max_pressure_avg60 <= 100:
            raise ValueError("host memory max_pressure_avg60 must be in (0, 100]")
        if self.lane_budget_bytes is not None:
            if type(self.lane_budget_bytes) is not int:
                raise TypeError("host memory lane_budget_bytes must be an integer")
            if self.lane_budget_bytes <= 0:
                raise ValueError("host memory lane_budget_bytes must be positive")

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> HostMemoryPolicy:
        """``SDO_HOST_MEMORY_RESERVE_GIB``, ``SDO_HOST_MEMORY_PSI_MAX``, ``SDO_HOST_LANE_BUDGET_GIB``."""
        default = cls()
        budget = env.get("SDO_HOST_LANE_BUDGET_GIB")
        return cls(
            reserve_bytes=int(float(env.get("SDO_HOST_MEMORY_RESERVE_GIB", default.reserve_bytes / _GIB)) * _GIB),
            max_pressure_avg60=float(env.get("SDO_HOST_MEMORY_PSI_MAX", default.max_pressure_avg60)),
            lane_budget_bytes=None if budget is None else int(float(budget) * _GIB),
        )


def host_env(environ: Mapping[str, str]) -> dict[str, str]:
    """``environ`` over the ``SDO_HOST_*`` lines of the host guard file, so a set-up host needs no exported variables.

    The file is ``SDO_HOST_ENV_FILE`` or ``~/.config/sdo/host.env`` (written by ``python -m benchmarks.sregym.hosts``);
    a missing or unreadable file contributes nothing and an explicit environment variable always wins.
    """
    path = Path(environ.get("SDO_HOST_ENV_FILE") or HOST_ENV_FILE).expanduser()
    values: dict[str, str] = {}
    try:
        lines = path.read_text().splitlines()
    except OSError:
        lines = []
    for line in lines:
        match = re.match(r"\s*(?:export\s+)?(SDO_HOST_[A-Z_]+)=(.*)", line)
        if match:
            values[match[1]] = match[2].strip().strip("'\"")
    return {**values, **environ}


def parse_memory_size(text: str) -> int:
    """A Docker size such as ``16g``, ``512M`` or a plain byte count."""
    match = _SIZE.fullmatch(text.strip())
    if match is None:
        raise ValueError(f"eval: memory size {text!r} is not a Docker size such as 16g")
    return int(match[1]) * _UNITS[match[2].lower()]


def memory_pressure_avg60(path: Path = PRESSURE_FILE) -> float:
    """``some avg60`` from the kernel's memory PSI file."""
    try:
        for line in path.read_text().splitlines():
            fields = line.split()
            if fields[:1] == ["some"]:
                values = dict(field.split("=", 1) for field in fields[1:] if "=" in field)
                return float(values["avg60"])
    except (OSError, ValueError, KeyError) as exc:
        raise ValueError(f"eval: host memory pressure (PSI) unavailable at {path}") from exc
    raise ValueError(f"eval: host memory pressure (PSI) unavailable at {path}: no 'some avg60' line")


def own_cgroup_dir(cgroup_file: Path = CGROUP_FILE, root: Path = CGROUP_ROOT) -> Path:
    """This process's cgroup (v2) directory: its ``*.pressure`` files are the stalls of the runner and its children.

    A row on a remote lane (EKS) puts only the runner and the agent CLI on this host, so their own cgroup, not the
    whole host, says whether a row started now would stall. Without a cgroup v2 entry the pressure is unknown and the
    caller refuses.
    """
    try:
        lines = cgroup_file.read_text().splitlines()
    except OSError as exc:
        raise ValueError(f"eval: no cgroup v2 entry readable at {cgroup_file}") from exc
    for line in lines:
        if line.startswith("0::"):
            return root / line.removeprefix("0::").lstrip("/")
    raise ValueError(f"eval: no cgroup v2 entry in {cgroup_file}; this process's pressure is unknown")


def cgroup_pressure_refusal(directory: Path, max_avg60: float) -> str | None:
    """Why the cgroup at ``directory`` stalls too much on CPU, IO or memory (PSI ``some avg60``), or ``None``.

    An unreadable pressure file is a refusal too.
    """
    for resource in ("cpu", "io", "memory"):
        try:
            avg60 = memory_pressure_avg60(directory / f"{resource}.pressure")
        except ValueError as exc:
            return str(exc).removeprefix("eval: ")
        if avg60 >= max_avg60:
            return f"runner cgroup {resource} pressure some avg60={avg60:.2f} >= {max_avg60:.2f}"
    return None


def host_memory_refusal(
    cap_bytes: int, policy: HostMemoryPolicy, *, available_bytes: int, pressure_avg60: float
) -> str | None:
    """Why the host cannot take a lane of ``cap_bytes`` now, or ``None`` when it can."""
    if pressure_avg60 >= policy.max_pressure_avg60:
        return (
            f"host memory pressure some avg60={pressure_avg60:.2f} >= {policy.max_pressure_avg60:.2f}; "
            "no new lane or cell while the host stalls on memory"
        )
    needed = cap_bytes + policy.reserve_bytes
    if available_bytes < needed:
        return (
            f"host MemAvailable {available_bytes} bytes < lane cap {cap_bytes} + reserve {policy.reserve_bytes} "
            f"= {needed} bytes"
        )
    return None


def assert_host_memory_admission(
    cap_bytes: int,
    policy: HostMemoryPolicy | None = None,
    *,
    meminfo: Path = MEMINFO_FILE,
    pressure: Path = PRESSURE_FILE,
) -> dict[str, Any]:
    """Read /proc and refuse (``HostMemoryRefused``) a lane that would eat the reserve or start under pressure."""
    policy = policy or HostMemoryPolicy()
    available = host_available_memory(meminfo)
    avg60 = memory_pressure_avg60(pressure)
    if (reason := host_memory_refusal(cap_bytes, policy, available_bytes=available, pressure_avg60=avg60)) is not None:
        raise HostMemoryRefused(f"eval: {reason}")
    return {
        "passed": True,
        "host_available_bytes": available,
        "lane_cap_bytes": cap_bytes,
        "reserve_bytes": policy.reserve_bytes,
        "pressure_avg60": avg60,
        "max_pressure_avg60": policy.max_pressure_avg60,
    }


def host_check(
    cap_bytes: int,
    policy: HostMemoryPolicy | None = None,
    *,
    meminfo: Path = MEMINFO_FILE,
    pressure: Path = PRESSURE_FILE,
) -> Callable[[], str | None]:
    """A polling probe: the refusal reason now, or ``None``. An unreadable host is a reason too."""
    policy = policy or HostMemoryPolicy()

    def check() -> str | None:
        try:
            assert_host_memory_admission(cap_bytes, policy, meminfo=meminfo, pressure=pressure)
        except ValueError as exc:
            return str(exc).removeprefix("eval: ")
        return None

    return check


def own_cgroup_check(
    policy: HostMemoryPolicy,
    max_avg60: float,
    *,
    meminfo: Path = MEMINFO_FILE,
    own_cgroup: Callable[[], Path] = own_cgroup_dir,
) -> Callable[[], str | None]:
    """A polling probe for work whose cluster is elsewhere: the runner's own cgroup stalls and the host reserve.

    The refusal reason now, or ``None``: CPU, IO or memory PSI of this process's cgroup at or above ``max_avg60``, or
    MemAvailable under the policy's reserve (no lane cap: the lane's nodes are not on this host). An unknown cgroup
    or an unreadable measurement is a reason too.
    """

    def check() -> str | None:
        try:
            directory = own_cgroup()
            available = host_available_memory(meminfo)
        except ValueError as exc:
            return str(exc).removeprefix("eval: ")
        if (reason := cgroup_pressure_refusal(directory, max_avg60)) is not None:
            return reason
        return host_memory_refusal(0, policy, available_bytes=available, pressure_avg60=0.0)

    return check


def inspect_listed(
    template: str,
    ids: Sequence[str],
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] | None = None,
    timeout: float,
) -> list[str]:
    """``docker inspect --format TEMPLATE`` of containers ``docker ps`` just listed: a line per one still there.

    On a shared host other users' containers come and go: one that exited after the listing is not running, so docker's
    "No such object" for it is skipped (docker still prints the others). Any other failure raises.
    """
    inspected = (run or subprocess.run)(
        ["docker", "inspect", "--format", template, *ids], capture_output=True, text=True, check=False, timeout=timeout
    )
    errors = [line for line in inspected.stderr.splitlines() if line.strip()]
    gone = ("no such object", "no such container")  # docker 27+ prints "error: no such object: ID"
    if inspected.returncode and (not errors or not all(any(g in line.lower() for g in gone) for line in errors)):
        raise subprocess.CalledProcessError(inspected.returncode, inspected.args, inspected.stdout, inspected.stderr)
    return inspected.stdout.splitlines()


def running_lane_caps(
    cluster_prefix: str,
    *,
    default_node_bytes: int,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, int]:
    """Summed Docker memory caps per running kind cluster named ``cluster_prefix*``; read-only Docker queries.

    An uncapped node (limit 0) of ours counts as ``default_node_bytes``: unknown is never free.
    """
    timeout = wait_for(WaitKind.PROBE)
    listed = run(["docker", "ps", "-q"], capture_output=True, text=True, check=True, timeout=timeout).stdout.split()
    if not listed:
        return {}
    template = '{{index .Config.Labels "' + KIND_CLUSTER_LABEL + '"}} {{.Name}} {{.HostConfig.Memory}}'
    rows = inspect_listed(template, listed, run=run, timeout=timeout)
    caps: dict[str, int] = {}
    for row in rows:
        fields = row.split()
        if len(fields) != 3 or not fields[0].startswith(cluster_prefix):
            continue  # not a kind node, or not one of ours
        caps[fields[0]] = caps.get(fields[0], 0) + (int(fields[2]) or default_node_bytes)
    return caps


def assert_lane_budget(
    cluster: str, cap_bytes: int, running: Mapping[str, int], policy: HostMemoryPolicy | None = None
) -> dict[str, Any]:
    """Refuse a lane whose cap would take the summed caps of our running lanes past the budget."""
    policy = policy or HostMemoryPolicy()
    others = sum(cap for name, cap in running.items() if name != cluster)
    after = others + cap_bytes
    if policy.lane_budget_bytes is not None and after > policy.lane_budget_bytes:
        raise HostMemoryRefused(
            f"eval: lane {cluster} cap {cap_bytes} bytes plus {others} bytes already capped by our running lanes "
            f"= {after} bytes exceeds the host lane budget {policy.lane_budget_bytes} bytes"
        )
    return {"passed": True, "lane_caps_after_bytes": after, "budget_bytes": policy.lane_budget_bytes}
