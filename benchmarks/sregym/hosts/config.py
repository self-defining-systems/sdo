"""Per-host eval setup configuration and the limits derived from a host's cores and RAM.

``hosts.toml`` is checked in and holds paths and numbers only, never a secret. ``[defaults]`` is merged under
every ``[hosts.<name>]`` table; a host overrides any key. Everything here is pure: the live machine is read by
``facts.gather_facts`` and compared against these values by ``steps``.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import tomllib
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

GIB = 1024**3
KIB_PER_GIB = 1024**2
#: Mounts that cannot hold lanes, images or evidence: a lane under NFS stalls every cell on one metadata storm.
REFUSED_FILESYSTEMS = frozenset({"nfs", "nfs4", "cifs", "smb3", "smbfs", "fuse.sshfs", "afs", "ceph", "9p"})
DEFAULT_CONFIG = Path(__file__).with_name("hosts.toml")
DEFAULT_GUARD_ENV = "~/.config/sdo/host.env"


class HostConfigError(ValueError):
    """A hosts file or entry that cannot describe a safe eval host."""


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SliceSettings(_Model):
    """``sdo-eval.slice``: shares of the host's ``MemTotal`` and core count (each floored), overridable absolutely.

    ``MemoryHigh`` is a fraction of ``MemoryMax``, so a lane storm is throttled before the hard cap kills it.
    """

    name: str = Field(default="sdo-eval.slice", pattern=r"^[A-Za-z0-9_.-]+\.slice$")
    memory_max_pct: float = Field(default=50.0, gt=0, le=100)
    memory_high_fraction: float = Field(default=0.94, gt=0, le=1)
    cpu_quota_pct: float = Field(default=44.0, gt=0, le=100)
    cpu_weight: int = Field(default=100, ge=1, le=10000)
    tasks_max: int = Field(default=16384, ge=1)
    io_weight: int = Field(default=50, ge=1, le=10000)
    memory_high_gib: int | None = Field(default=None, ge=1)
    memory_max_gib: int | None = Field(default=None, ge=1)
    cpu_cores: float | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def _high_below_max(self) -> SliceSettings:
        if self.memory_high_gib and self.memory_max_gib and self.memory_high_gib > self.memory_max_gib:
            raise ValueError("memory_high_gib must not exceed memory_max_gib")
        return self


class MglruSettings(_Model):
    enabled: bool = True
    min_ttl_ms: int = Field(default=1000, ge=0)


class SysctlSettings(_Model):
    """``/etc/sysctl.d/90-sdo-eval.conf``: what kind needs, plus an optional ``vm.min_free_kbytes``."""

    gc_thresh: tuple[int, int, int] = (16384, 32768, 65536)
    inotify_max_user_instances: int = Field(default=8192, ge=1)
    inotify_max_user_watches: int = Field(default=1048576, ge=1)
    min_free_pct: float | None = Field(default=None, gt=0, le=10)
    min_free_kbytes: int | None = Field(default=None, ge=1024)

    @model_validator(mode="after")
    def _one_source(self) -> SysctlSettings:
        if self.min_free_pct is not None and self.min_free_kbytes is not None:
            raise ValueError("set min_free_pct or min_free_kbytes, not both")
        if not self.gc_thresh[0] <= self.gc_thresh[1] <= self.gc_thresh[2]:
            raise ValueError("gc_thresh must be ascending")
        return self


class GuardSettings(_Model):
    """The values ``fastloop up`` and the eval scheduler read as ``SDO_HOST_*``."""

    lane_prefix: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
    reserve_gib: float = Field(default=32.0, ge=0)
    psi_max: float = Field(default=10.0, gt=0, le=100)
    env_file: str = DEFAULT_GUARD_ENV


class DiskwatchSettings(_Model):
    """The disk watcher timer. With no ``script`` the host's installed unit is adopted; a new host gets the default."""

    enabled: bool = True
    script: str | None = None
    collector: str | None = None
    interval_minutes: int = Field(default=2, ge=1, le=60)


class CredentialSettings(_Model):
    """Where the ClaudeGCP profile lives on this host. Paths only; copying is an explicit opt-in."""

    profile_dir: str
    key_path: str = "~/.config/gcloud/application_default_credentials.json"
    source_host: str | None = None


class HostConfig(_Model):
    name: str
    #: Name passed to ``ssh`` when the tool runs somewhere else; defaults to ``name``.
    ssh: str | None = None
    lane_root: str
    evidence_root: str | None = None
    slice: SliceSettings = SliceSettings()
    mglru: MglruSettings = MglruSettings()
    sysctl: SysctlSettings = SysctlSettings()
    guard: GuardSettings
    diskwatch: DiskwatchSettings = DiskwatchSettings()
    credentials: CredentialSettings | None = None

    @property
    def evidence_dir(self) -> str:
        return self.evidence_root or f"{self.lane_root.rstrip('/')}/evidence"


@dataclass(frozen=True)
class HostFacts:
    """What the machine reports; the only input the derivation needs besides the config."""

    cores: int
    mem_total_kib: int
    uid: int = 1000
    has_lru_gen: bool = True

    def __post_init__(self) -> None:
        if self.cores <= 0 or self.mem_total_kib <= 0:
            raise ValueError("host facts need positive cores and memory")


@dataclass(frozen=True)
class SliceSpec:
    memory_high_bytes: int
    memory_max_bytes: int
    cpu_quota_pct: int
    cores: int
    cpu_weight: int
    tasks_max: int
    io_weight: int

    @property
    def memory_max_gib(self) -> int:
        return self.memory_max_bytes // GIB


def derive_slice(settings: SliceSettings, facts: HostFacts) -> SliceSpec:
    ram_gib = facts.mem_total_kib / KIB_PER_GIB
    maximum = settings.memory_max_gib or max(1, math.floor(ram_gib * settings.memory_max_pct / 100))
    high = settings.memory_high_gib or max(1, math.floor(maximum * settings.memory_high_fraction))
    if high > maximum:
        raise HostConfigError(f"derived MemoryHigh {high}G exceeds MemoryMax {maximum}G")
    cores = settings.cpu_cores or facts.cores * settings.cpu_quota_pct / 100
    return SliceSpec(
        memory_high_bytes=high * GIB,
        memory_max_bytes=maximum * GIB,
        cpu_quota_pct=max(1, math.floor(cores)) * 100,
        cores=facts.cores,
        cpu_weight=settings.cpu_weight,
        tasks_max=settings.tasks_max,
        io_weight=settings.io_weight,
    )


def sysctl_values(settings: SysctlSettings, facts: HostFacts) -> dict[str, int]:
    """Every sysctl the host should carry, in file order."""
    values = {
        "net.ipv4.neigh.default.gc_thresh1": settings.gc_thresh[0],
        "net.ipv4.neigh.default.gc_thresh2": settings.gc_thresh[1],
        "net.ipv4.neigh.default.gc_thresh3": settings.gc_thresh[2],
        "fs.inotify.max_user_instances": settings.inotify_max_user_instances,
        "fs.inotify.max_user_watches": settings.inotify_max_user_watches,
    }
    kbytes = min_free_kbytes(settings, facts)
    if kbytes is not None:
        values["vm.min_free_kbytes"] = kbytes
    return values


def min_free_kbytes(settings: SysctlSettings, facts: HostFacts) -> int | None:
    if settings.min_free_kbytes is not None:
        return settings.min_free_kbytes
    if settings.min_free_pct is None:
        return None
    return int(facts.mem_total_kib * settings.min_free_pct / 100)


def _merge(base: dict[str, Any], over: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for key, value in over.items():
        out[key] = _merge(out[key], value) if isinstance(value, dict) and isinstance(out.get(key), dict) else value
    return out


def parse_hosts(text: str) -> dict[str, HostConfig]:
    """Every host in a ``hosts.toml`` document, defaults merged and validated."""
    try:
        document = tomllib.loads(text)
    except tomllib.TOMLDecodeError as exc:
        raise HostConfigError(f"hosts file is not valid TOML: {exc}") from exc
    unknown = set(document) - {"defaults", "hosts"}
    if unknown:
        raise HostConfigError(f"hosts file: unknown top-level keys {sorted(unknown)}")
    defaults = document.get("defaults", {})
    hosts: dict[str, HostConfig] = {}
    for name, table in document.get("hosts", {}).items():
        try:
            hosts[name] = HostConfig(name=name, **_merge(defaults, table))
        except ValidationError as exc:
            raise HostConfigError(f"host {name}: {exc}") from exc
    if not hosts:
        raise HostConfigError("hosts file defines no [hosts.<name>] table")
    return hosts


def load_hosts(path: Path = DEFAULT_CONFIG) -> dict[str, HostConfig]:
    try:
        return parse_hosts(path.read_text())
    except OSError as exc:
        raise HostConfigError(f"cannot read hosts file {path}: {exc}") from exc
