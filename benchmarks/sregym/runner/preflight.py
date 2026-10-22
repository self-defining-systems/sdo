"""Launch preflight: refuse to start an experiment whose numbers could not be trusted.

Every experiment and pipeline runs these checks before it launches. A failure
aborts with an actionable error before any directory, cluster or agent is
touched:

- ``disk``: at least 100 GB free on the logs disk and on Docker's data disk
  (a full data disk once crashed MongoDB pods mid-run);
- ``codex-cli-pins``: one concrete Codex CLI version everywhere. The stock
  Codex arm's ``agents.yaml`` pin, the host CLI (which runs the judge) and the
  SDO image build argument must agree, and the pin must resolve on npm with
  its platform package (``@latest`` once resolved to a version whose package
  returned 404);
- ``agentshim``: the host's agentshim is the ``uv.lock`` version;
- ``sdo-images``: each SDO image is present, and its Codex CLI, agentshim and
  entry-point imports are probed inside the image (an old agentshim pin once
  broke imports at run time);
- ``lane-isolation``: the lane's clusters are not locked by another
  experiment, and every kubeconfig the lane would use reaches only its own
  cluster (lanes once shared a kubeconfig and contaminated each other);
- ``model-policy``: every agent role and the judge resolve to the one allowed
  model, provider and effort (:class:`ModelPolicy`);
- ``arm-parity``: every compared arm and stage shares the model, reasoning
  effort, judge and environment that affect grading or timing;
- ``codex-quota``: the weekly Codex quota has headroom, read offline from the
  newest rate-limit snapshot in the local Codex session logs; ``unknown`` when
  there is none or it is stale.

``SDO_PREFLIGHT=warn`` downgrades failures to warnings. The manifest then
records the preflight as waived, and the validity checker counts such a run as
``invalid_infra``: a waived preflight never yields a reportable number.

Compare arms before launching them::

    uv run python -m benchmarks.sregym.runner.preflight \\
        benchmarks/sregym/experiments/sdo_example.toml \\
        benchmarks/sregym/experiments/codex_baseline_example.toml
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Protocol, cast

import yaml

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10
    import tomli as tomllib

from collections.abc import Callable

from benchmarks.sregym.runner.experiment import ExperimentConfig, load_experiment_config, resolve_config
from benchmarks.sregym.runner.pipeline import is_pipeline_config, load_pipeline_config, merge_stage_config

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

CheckStatus = Literal["pass", "fail", "unknown"]
PreflightMode = Literal["enforce", "warn"]
PREFLIGHT_MODE_ENV = "SDO_PREFLIGHT"
MAX_QUOTA_USED_ENV = "SDO_PREFLIGHT_MAX_QUOTA_USED_PERCENT"

GB = 10**9
DEFAULT_KIND_CLUSTER_PREFIX = "sregym-w"
#: SREGym's default kind topology when ``kind_worker_nodes`` is 0.
DEFAULT_KIND_WORKERS = 3
AGENT_BASE_IMAGE = "sregym-agent-base:latest"
#: Where each SDO image is defaulted when ``[agent.sdo_codex]`` names none (``config_to_env``).
SDO_IMAGE_DEFAULTS = {
    "controller_image": "sdo-controller:v0.1.0",
    "responder_image": "sdo-sregym-responder:v0.1.0",
    "validator_image": "sdo-detector-validator:v0.1.0",
}
#: Python modules imported by the worker image. An empty tuple means the image
#: runs the Go operator binary and must not need a Python interpreter.
SDO_IMAGE_ENTRY_POINTS = {
    "controller_image": (),
    "responder_image": ("sdo.agents.responder.worker", "libs.agent_cli", "agentshim"),
    "validator_image": (),
}
#: Only the worker image runs agent turns and needs these Python/CLI pins.
AGENT_IMAGES = ("responder_image",)
#: The SDO driver's model when neither ``[agent.sdo_codex].model`` nor ``MODEL_ID`` is set.
SDO_DRIVER_DEFAULT_MODEL = "gpt-6-luna"
#: Env overrides some SDO entry points read when they are given no model.
SDO_MODEL_ENV_OVERRIDES = ("SDO_RESPONDER_MODEL", "SDO_LIFECYCLE_MODEL", "SDO_DEPLOYMENT_MODEL")
JUDGE_EFFORT_ENV = "JUDGE_REASONING_EFFORT"
#: SREGym's Codex judge effort when ``JUDGE_REASONING_EFFORT`` is unset.
SREGYM_DEFAULT_JUDGE_EFFORT = "xhigh"
_SEMVER = re.compile(r"^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$")
_CODEX_VERSION = re.compile(r"codex-cli\s+(\S+)")


# --------------------------------------------------------------------------- policy and settings


@dataclass(frozen=True)
class ModelPolicy:
    """The one model every agent role and the judge may resolve to.

    User rule (2026-09-28): SDO's responder, reflection, lifecycle and health
    judge use only Codex gpt-6-luna; the Codex baselines use gpt-6-luna; the
    SREGym judge is only ``codex-gpt-6-luna`` at ``xhigh``. ``agent_effort`` is
    the effort SDO pins its incident agents to in code
    (``sdo.agents.responder.INCIDENT_REASONING_EFFORT``; a test keeps
    the two equal), so every compared arm declares it.
    """

    provider: str = "codex"
    model: str = "gpt-6-luna"
    judge_model_id: str = "codex-gpt-6-luna"
    judge_effort: str = "xhigh"
    agent_effort: str = "medium"
    agents: frozenset[str] = frozenset({"codex", "sdo_codex"})

    def __post_init__(self) -> None:
        for name in ("provider", "model", "judge_model_id", "judge_effort", "agent_effort"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"ModelPolicy.{name} must be a non-empty string, got {value!r}")
        if not self.agents:
            raise ValueError("ModelPolicy.agents must name at least one agent")


@dataclass(frozen=True)
class PreflightSettings:
    min_free_bytes: int = 100 * GB
    #: Fail when the newest Codex quota snapshot shows at least this much of a window used.
    max_quota_used_percent: float = 85.0
    #: A snapshot older than this says nothing reliable about the quota now.
    quota_snapshot_max_age_seconds: float = 6 * 3600
    policy: ModelPolicy = field(default_factory=ModelPolicy)

    def __post_init__(self) -> None:
        if self.min_free_bytes < 0:
            raise ValueError(f"min_free_bytes must not be negative, got {self.min_free_bytes}")
        if not 0 < self.max_quota_used_percent <= 100:
            raise ValueError(f"max_quota_used_percent must be in (0, 100], got {self.max_quota_used_percent}")
        if self.quota_snapshot_max_age_seconds <= 0:
            raise ValueError("quota_snapshot_max_age_seconds must be positive")

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> PreflightSettings:
        raw = env.get(MAX_QUOTA_USED_ENV, "").strip()
        return cls(max_quota_used_percent=float(raw)) if raw else cls()


# --------------------------------------------------------------------------- report


@dataclass(frozen=True)
class PreflightCheck:
    name: str
    status: CheckStatus
    detail: str
    remedy: str = ""


@dataclass(frozen=True)
class PreflightReport:
    checks: tuple[PreflightCheck, ...]
    #: Resolved facts (versions, digests, lanes, roles, quota) for the run manifest.
    facts: dict[str, Any]
    mode: PreflightMode = "enforce"

    @property
    def failures(self) -> tuple[PreflightCheck, ...]:
        return tuple(check for check in self.checks if check.status == "fail")

    @property
    def ok(self) -> bool:
        return not self.failures

    @property
    def waived(self) -> bool:
        """Whether the run launched despite failures (``SDO_PREFLIGHT=warn``)."""

        return self.mode == "warn" and not self.ok

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "mode": self.mode,
            "waived": self.waived,
            "checks": [asdict(check) for check in self.checks],
            "facts": self.facts,
        }


class PreflightError(RuntimeError):
    """A launch preflight check failed; the message lists every failure and its remedy."""

    def __init__(self, report: PreflightReport) -> None:
        self.report = report
        lines = ["SREGym launch preflight failed; nothing was launched."]
        for check in report.failures:
            lines.append(f"  [{check.name}] {check.detail}")
            if check.remedy:
                lines.append(f"      fix: {check.remedy}")
        lines.append(f"Set {PREFLIGHT_MODE_ENV}=warn to launch anyway; the run is then recorded as invalid_infra.")
        super().__init__("\n".join(lines))


# --------------------------------------------------------------------------- host probes


@dataclass(frozen=True)
class ImageInfo:
    ref: str
    id: str
    repo_digests: tuple[str, ...] = ()
    created: str = ""


@dataclass(frozen=True)
class ImageVersions:
    """What an agent image actually contains, probed by running it."""

    codex: str | None
    agentshim: str | None
    #: Import failure of the image's entry points, ``None`` when all imported.
    import_error: str | None = None


class HostProbe(Protocol):
    """Everything preflight and the manifest read from the host; tests substitute a fake."""

    home: Path

    def ipv4_neighbor_limit(self) -> int | None: ...
    def free_bytes(self, path: Path) -> int | None: ...
    def docker_root(self) -> Path | None: ...
    def image(self, ref: str) -> ImageInfo | None: ...
    def image_versions(self, ref: str, modules: Sequence[str]) -> ImageVersions | None: ...
    def npm_resolves(self, spec: str) -> bool | None: ...
    def host_codex_version(self) -> str | None: ...
    def python_package_version(self, name: str) -> str | None: ...
    def kind_clusters(self) -> list[str] | None: ...
    def kind_nodes(self, cluster: str) -> list[str] | None: ...
    def control_plane_port(self, cluster: str) -> int | None: ...
    def cluster_lock_owner(self, cluster: str) -> str | None: ...
    def git(self, repository: Path, *args: str) -> str | None: ...
    def load_average(self) -> tuple[float, float, float] | None: ...


Runner = Callable[..., subprocess.CompletedProcess[str]]


class SystemHost:
    """The real host: Docker, kind, npm, git, and the local Codex session logs."""

    def __init__(self, *, run: Runner = subprocess.run, home: Path | None = None) -> None:
        self._run = run
        self.home = home or Path.home()

    def _text(self, argv: list[str], *, timeout: float = 60.0) -> str | None:
        try:
            completed = self._run(argv, capture_output=True, text=True, timeout=timeout, check=False)
        except (OSError, subprocess.SubprocessError):
            return None
        return completed.stdout if completed.returncode == 0 else None

    def ipv4_neighbor_limit(self) -> int | None:
        """Read the Linux host's global IPv4 neighbor allocation ceiling."""
        try:
            return int(Path("/proc/sys/net/ipv4/neigh/default/gc_thresh3").read_text().strip())
        except (OSError, ValueError):
            return None

    def free_bytes(self, path: Path) -> int | None:
        probe = path
        while not probe.exists() and probe != probe.parent:
            probe = probe.parent
        try:
            return shutil.disk_usage(probe).free
        except OSError:
            return None

    def docker_root(self) -> Path | None:
        out = self._text(["docker", "info", "--format", "{{.DockerRootDir}}"], timeout=20)
        return Path(out.strip()) if out and out.strip() else None

    def image(self, ref: str) -> ImageInfo | None:
        out = self._text(["docker", "image", "inspect", ref, "--format", "{{json .}}"], timeout=20)
        if not out:
            return None
        document = cast("dict[str, Any]", json.loads(out))
        return ImageInfo(
            ref=ref,
            id=str(document.get("Id", "")),
            repo_digests=tuple(str(item) for item in document.get("RepoDigests") or ()),
            created=str(document.get("Created", "")),
        )

    def image_versions(self, ref: str, modules: Sequence[str]) -> ImageVersions | None:
        if not modules:
            identity = self._text(
                [
                    "docker",
                    "run",
                    "--rm",
                    "--workdir",
                    "/opt/sdo/sdo/operator",
                    "--entrypoint",
                    "/usr/local/bin/sdo-operator",
                    ref,
                    "schema-identity",
                ],
                timeout=120,
            )
            if identity is None:
                return ImageVersions(
                    codex=None, agentshim=None, import_error="Go operator schema-identity probe failed"
                )
            if not identity.strip().startswith("sdo.dev/"):
                return ImageVersions(
                    codex=None, agentshim=None, import_error="Go operator returned an invalid schema identity"
                )
            return ImageVersions(codex=None, agentshim=None)
        codex_out = self._text(["docker", "run", "--rm", "--entrypoint", "codex", ref, "--version"], timeout=120)
        script = (
            "import importlib, importlib.metadata as m, json, sys\n"
            "error = None\n"
            f"for name in {list(modules)!r}:\n"
            "    try:\n"
            "        importlib.import_module(name)\n"
            "    except Exception as exc:\n"
            "        error = f'{name}: {type(exc).__name__}: {exc}'\n"
            "        break\n"
            "try:\n"
            "    version = m.version('agentshim')\n"
            "except m.PackageNotFoundError:\n"
            "    version = None\n"
            "print(json.dumps({'agentshim': version, 'import_error': error}))\n"
        )
        python_out = self._text(["docker", "run", "--rm", "--entrypoint", "python3", ref, "-c", script], timeout=120)
        if python_out is None and codex_out is None:
            return None
        python = cast("dict[str, Any]", json.loads(python_out.strip().splitlines()[-1])) if python_out else {}
        return ImageVersions(
            codex=parse_codex_version(codex_out or ""),
            agentshim=python.get("agentshim"),
            import_error=python.get("import_error") if python_out else "python3 probe failed to run",
        )

    def npm_resolves(self, spec: str) -> bool | None:
        try:
            completed = self._run(["npm", "view", spec, "version"], capture_output=True, text=True, timeout=60)
        except (OSError, subprocess.SubprocessError):
            return None
        if completed.returncode == 0:
            return bool(completed.stdout.strip())
        return False if "E404" in completed.stderr or "404" in completed.stderr else None

    def host_codex_version(self) -> str | None:
        return parse_codex_version(self._text(["codex", "--version"], timeout=30) or "")

    def python_package_version(self, name: str) -> str | None:
        import importlib.metadata

        try:
            return importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            return None

    def kind_clusters(self) -> list[str] | None:
        out = self._text(["kind", "get", "clusters"], timeout=30)
        return None if out is None else [line.strip() for line in out.splitlines() if line.strip()]

    def kind_nodes(self, cluster: str) -> list[str] | None:
        out = self._text(["kind", "get", "nodes", "--name", cluster], timeout=30)
        return None if out is None else sorted(line.strip() for line in out.splitlines() if line.strip())

    def control_plane_port(self, cluster: str) -> int | None:
        out = self._text(["docker", "port", f"{cluster}-control-plane", "6443/tcp"], timeout=20)
        match = re.search(r":(\d+)\s*$", (out or "").strip().splitlines()[0] if out and out.strip() else "")
        return int(match.group(1)) if match else None

    def cluster_lock_owner(self, cluster: str) -> str | None:
        return flock_owner(self.home / ".cache" / "sregym" / "locks" / f"{cluster}.lock")

    def git(self, repository: Path, *args: str) -> str | None:
        out = self._text(["git", "-C", str(repository), *args], timeout=30)
        return None if out is None else out.strip()

    def load_average(self) -> tuple[float, float, float] | None:
        try:
            return os.getloadavg()
        except OSError:
            return None


def parse_codex_version(output: str) -> str | None:
    """The version in ``codex --version`` output, which may follow warning lines."""

    match = _CODEX_VERSION.search(output)
    return match.group(1) if match else None


def flock_owner(path: Path, *, proc_locks: Path = Path("/proc/locks")) -> str | None:
    """Who holds SREGym's per-cluster ``flock``, without taking it (``None`` when free or unknown).

    Reads ``/proc/locks`` by inode, so probing never makes a starting
    experiment fail its own lock.
    """

    if not path.is_file() or not proc_locks.is_file():
        return None
    inode = path.stat().st_ino
    for line in proc_locks.read_text(encoding="utf-8", errors="replace").splitlines():
        parts = line.split()
        if len(parts) >= 6 and parts[1] == "FLOCK" and parts[5].rsplit(":", 1)[-1] == str(inode):
            owner = path.read_text(encoding="utf-8", errors="replace").strip() or "unknown"
            return f"{owner} (lock pid {parts[4]})"
    return None


# --------------------------------------------------------------------------- resolution


@dataclass(frozen=True)
class RoleModel:
    provider: str | None
    model: str | None
    effort: str | None
    source: str


def resolve_roles(config: ExperimentConfig, env: Mapping[str, str], policy: ModelPolicy) -> dict[str, RoleModel]:
    """The provider, model and effort each agent role and the judge of *config* resolve to.

    SDO's driver gives every role (responder, reflection, lifecycle deployer
    and health judge) ``[agent.sdo_codex]``'s provider and model, falling back
    to ``MODEL_ID`` and then to its built-in default. Their efforts are pinned
    in code (``policy.agent_effort``).
    """

    judge_effort = env.get(JUDGE_EFFORT_ENV, "").strip() or SREGYM_DEFAULT_JUDGE_EFFORT
    roles: dict[str, RoleModel] = {
        "sregym_judge": RoleModel(
            provider="codex" if config.env.judge_model_id.startswith("codex-") else None,
            model=config.env.judge_model_id or None,
            effort=judge_effort,
            source="runner.env.judge_model_id",
        )
    }
    if config.agent == "sdo_codex":
        section = config.agent_config.get("sdo_codex") or {}
        model = str(section.get("model") or env.get("MODEL_ID") or SDO_DRIVER_DEFAULT_MODEL)
        provider = str(section.get("provider") or "codex")
        for role in ("responder", "reflection", "lifecycle_deployer", "health_judge"):
            roles[role] = RoleModel(provider, model, policy.agent_effort, "agent.sdo_codex")
    elif config.agent == "codex":
        roles["baseline_agent"] = RoleModel("codex", config.model, config.reasoning_effort or None, "runner.model")
    else:
        roles["agent"] = RoleModel(None, config.model, config.reasoning_effort or None, "runner.model")
    return roles


def lane_clusters(config: ExperimentConfig, env: Mapping[str, str]) -> list[str]:
    """The kind clusters this experiment's workers drive: ``<prefix><offset + worker>``."""

    prefix = env.get("SREGYM_KIND_CLUSTER_PREFIX", "").strip() or DEFAULT_KIND_CLUSTER_PREFIX
    raw = env.get("SREGYM_WORKER_ID_OFFSET", "").strip()
    offset = int(raw) if raw else 0
    if offset < 0:
        raise ValueError(f"SREGYM_WORKER_ID_OFFSET must be non-negative, got {offset}")
    return [f"{prefix}{offset + worker}" for worker in range(max(1, config.parallel))]


def sdo_images(config: ExperimentConfig) -> dict[str, str]:
    section = config.agent_config.get("sdo_codex") or {}
    return {key: str(section.get(key) or default) for key, default in SDO_IMAGE_DEFAULTS.items()}


def dockerfile_arg(dockerfile: Path, name: str) -> str | None:
    match = re.search(rf"^ARG {re.escape(name)}=(\S+)\s*$", dockerfile.read_text(encoding="utf-8"), re.MULTILINE)
    return match.group(1) if match else None


def locked_version(lockfile: Path, package: str) -> str | None:
    lock = tomllib.loads(lockfile.read_text(encoding="utf-8"))
    for entry in lock.get("package", []):
        if str(entry.get("name", "")).lower().replace("_", "-") == package:
            return str(entry["version"])
    return None


def _as_dict(value: object) -> dict[str, Any]:
    return cast("dict[str, Any]", value) if isinstance(value, dict) else {}


def _as_dicts(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [cast("dict[str, Any]", item) for item in cast("list[object]", value) if isinstance(item, dict)]


def stock_agent_version(sregym_dir: Path, agent: str) -> tuple[bool, str | None]:
    """``(registered, agent_version)`` of a stock SREGym agent in ``agents.yaml``."""

    path = sregym_dir / "agents.yaml"
    if not path.is_file():
        return False, None
    document = _as_dict(yaml.safe_load(path.read_text(encoding="utf-8")))
    for entry in _as_dicts(document.get("agents")):
        if entry.get("name") == agent:
            version: object = entry.get("agent_version")
            return True, None if version is None else str(version)
    return False, None


# --------------------------------------------------------------------------- quota


@dataclass(frozen=True)
class QuotaWindow:
    used_percent: float
    window_minutes: int | None
    resets_at: float | None


@dataclass(frozen=True)
class QuotaSnapshot:
    observed_at: float
    source: str
    windows: dict[str, QuotaWindow]


def read_quota_snapshot(codex_home: Path, *, newest_files: int = 20) -> QuotaSnapshot | None:
    """The newest Codex rate-limit snapshot in the local session rollouts (offline, no API call).

    Every Codex response writes a ``token_count`` event whose ``rate_limits``
    carries the account's window usage; the newest one is the latest reading.
    """

    sessions = codex_home / "sessions"
    if not sessions.is_dir():
        return None
    rollouts = sorted(sessions.rglob("rollout-*.jsonl"), key=lambda path: path.stat().st_mtime, reverse=True)
    for path in rollouts[:newest_files]:
        latest: QuotaSnapshot | None = None
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if '"rate_limits"' not in line:
                continue
            try:
                record = _as_dict(json.loads(line))
            except json.JSONDecodeError:
                continue
            limits = _as_dict(_as_dict(record.get("payload")).get("rate_limits"))
            observed = _iso_seconds(record.get("timestamp"))
            if not limits or observed is None:
                continue
            windows: dict[str, QuotaWindow] = {}
            for name in ("primary", "secondary"):
                window = _as_dict(limits.get(name))
                if window.get("used_percent") is not None:
                    windows[name] = QuotaWindow(
                        used_percent=float(window["used_percent"]),
                        window_minutes=int(window["window_minutes"]) if window.get("window_minutes") else None,
                        resets_at=float(window["resets_at"]) if window.get("resets_at") else None,
                    )
            if windows:
                latest = QuotaSnapshot(observed_at=observed, source=str(path), windows=windows)
        if latest is not None:
            return latest
    return None


def _iso_seconds(value: object) -> float | None:
    from datetime import datetime

    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


# --------------------------------------------------------------------------- checks


@dataclass
class _Context:
    configs: list[ExperimentConfig]
    project_root: Path
    sregym_dir: Path
    env: Mapping[str, str]
    host: HostProbe
    settings: PreflightSettings
    now: float
    facts: dict[str, Any] = field(default_factory=dict)  # pyright: ignore[reportUnknownVariableType]
    #: Images to probe instead of those the configs name (``check_images``).
    images: Mapping[str, str] | None = None

    @property
    def agents(self) -> set[str]:
        return {config.agent for config in self.configs}


# Shared kind hosts allocate neighbors across node network namespaces, including
# loopback. A full global table can stall apiserver/etcd without interface drops.
MIN_IPV4_NEIGHBOR_LIMIT = 65536


def _check_neighbor_cache(ctx: _Context) -> PreflightCheck:
    limit = ctx.host.ipv4_neighbor_limit()
    ctx.facts["ipv4_neighbor_gc_thresh3"] = limit
    if limit is None:
        return PreflightCheck(
            "neighbor-cache",
            "unknown",
            "IPv4 neighbor capacity is unavailable on this host",
            "check net.ipv4.neigh.default.gc_thresh3 on the Linux Docker host",
        )
    if limit < MIN_IPV4_NEIGHBOR_LIMIT:
        return PreflightCheck(
            "neighbor-cache",
            "fail",
            f"host net.ipv4.neigh.default.gc_thresh3={limit}; shared kind hosts need at least "
            f"{MIN_IPV4_NEIGHBOR_LIMIT} (exhaustion can stall localhost API/etcd traffic)",
            "ask the host administrator to approve a runtime capacity increase: sysctl -w "
            "net.ipv4.neigh.default.gc_thresh1=16384 net.ipv4.neigh.default.gc_thresh2=32768 "
            "net.ipv4.neigh.default.gc_thresh3=65536",
        )
    return PreflightCheck("neighbor-cache", "pass", f"IPv4 neighbor capacity {limit} >= {MIN_IPV4_NEIGHBOR_LIMIT}")


def _check_disk(ctx: _Context) -> PreflightCheck:
    paths = {"logs": ctx.sregym_dir / "logs"}
    docker_root = ctx.host.docker_root()
    if docker_root is not None:
        paths["docker"] = docker_root
    free = {name: ctx.host.free_bytes(path) for name, path in paths.items()}
    ctx.facts["disk_free_bytes"] = dict(free)
    low = [
        f"{name} disk {paths[name]} has {value / GB:.0f} GB free"
        for name, value in free.items()
        if value is not None and value < ctx.settings.min_free_bytes
    ]
    need = f"{ctx.settings.min_free_bytes / GB:.0f} GB"
    if low:
        return PreflightCheck(
            "disk",
            "fail",
            f"{'; '.join(low)}, below the {need} minimum",
            "free space on that disk (old experiment logs, stopped kind clusters, unused images) before launching",
        )
    if free.get("logs") is None:
        return PreflightCheck("disk", "unknown", f"cannot read free space of {paths['logs']}")
    return PreflightCheck("disk", "pass", ", ".join(f"{n} {v / GB:.0f} GB free" for n, v in free.items() if v))


def _check_application_sources(ctx: _Context) -> PreflightCheck | None:
    """A source deploy needs SREGym's nested ``SREGym-applications`` submodule checked out.

    A fresh worktree that initialized only the top-level SREGym submodule has
    an empty directory there, and every deploy attempt then fails on a
    missing Helm chart after the lane has already started.
    """

    if not any(config.deploy_from_source for config in ctx.configs):
        return None
    applications = ctx.sregym_dir / "SREGym-applications"
    entries = (
        [entry for entry in applications.iterdir() if not entry.name.startswith(".")] if applications.is_dir() else []
    )
    if not entries:
        return PreflightCheck(
            "application-sources",
            "fail",
            f"{applications} is empty or missing; source deploys have no Helm charts",
            f"run `git -C {ctx.sregym_dir} submodule update --init --recursive`",
        )
    return PreflightCheck("application-sources", "pass", f"{len(entries)} application sources present")


def _check_codex_pins(ctx: _Context) -> PreflightCheck:
    dockerfile = ctx.project_root / "sdo" / "operator" / "Dockerfile.runtime"
    pin = dockerfile_arg(dockerfile, "CODEX_VERSION") if dockerfile.is_file() else None
    host = ctx.host.host_codex_version()
    codex_facts: dict[str, Any] = {"pin": pin, "host": host}
    ctx.facts["codex_cli"] = codex_facts
    problems: list[str] = []
    remedies: list[str] = []
    if pin is None or not _SEMVER.match(pin):
        problems.append(f"sdo/operator/Dockerfile.runtime pins CODEX_VERSION={pin!r}, not a concrete version")
        remedies.append("pin ARG CODEX_VERSION to an exact release")
    judged_by_codex = any(config.env.judge_model_id.startswith("codex-") for config in ctx.configs)
    if judged_by_codex and host != pin:
        problems.append(f"host Codex CLI (runs the judge) is {host!r}, not the pinned {pin!r}")
        remedies.append(f"npm install -g @openai/codex@{pin}")
    if "codex" in ctx.agents:
        registered, stock = stock_agent_version(ctx.sregym_dir, "codex")
        codex_facts["stock_arm"] = stock
        if not registered:
            problems.append(f"{ctx.sregym_dir / 'agents.yaml'} registers no codex agent")
        elif stock is None or stock == "latest" or not _SEMVER.match(stock):
            problems.append(f"the stock Codex arm installs @openai/codex@{stock or 'latest'} (floating)")
            remedies.append(f"pin agent_version: {pin} for codex in third_party/sregym/agents.yaml")
        elif stock != pin:
            problems.append(f"the stock Codex arm pins {stock}, SDO's images pin {pin}")
            remedies.append("use one Codex CLI version for every arm")
        else:
            missing: list[str] = []
            unknown: list[str] = []
            for spec in (f"@openai/codex@{stock}", f"@openai/codex@{stock}-linux-x64"):
                resolved = ctx.host.npm_resolves(spec)
                if resolved is False:
                    missing.append(spec)
                elif resolved is None:
                    unknown.append(spec)
            codex_facts["npm_unverified"] = unknown
            if missing:
                problems.append(f"npm does not resolve {', '.join(missing)} (the 404 the stock arm would hit)")
                remedies.append("pin a Codex CLI release whose package and linux-x64 platform package are published")
    if problems:
        return PreflightCheck("codex-cli-pins", "fail", "; ".join(problems), "; ".join(remedies))
    unverified = codex_facts.get("npm_unverified")
    if unverified:
        return PreflightCheck("codex-cli-pins", "unknown", f"pinned {pin}; npm unreachable for {unverified}")
    return PreflightCheck("codex-cli-pins", "pass", f"Codex CLI {pin} everywhere")


def _check_agentshim(ctx: _Context) -> PreflightCheck:
    lock = ctx.project_root / "uv.lock"
    pin = locked_version(lock, "agentshim") if lock.is_file() else None
    host = ctx.host.python_package_version("agentshim")
    ctx.facts["agentshim"] = {"pin": pin, "host": host}
    if pin is None:
        return PreflightCheck("agentshim", "fail", "uv.lock locks no agentshim", "run uv lock")
    if host != pin:
        return PreflightCheck("agentshim", "fail", f"host agentshim is {host!r}, uv.lock pins {pin}", "run uv sync")
    return PreflightCheck("agentshim", "pass", f"agentshim {pin}")


def _check_sdo_images(ctx: _Context) -> PreflightCheck | None:
    sdo_configs = [config for config in ctx.configs if config.agent == "sdo_codex"]
    images: dict[str, str] = {}
    for config in sdo_configs:
        images.update({f"{key}:{ref}": ref for key, ref in sdo_images(config).items()})
    if "codex" in ctx.agents:
        images[f"agent_base:{AGENT_BASE_IMAGE}"] = AGENT_BASE_IMAGE
    if ctx.images is not None:
        images = {f"{key}:{ref}": ref for key, ref in ctx.images.items()}
    if not images:
        return None
    codex_pin = ctx.facts.get("codex_cli", {}).get("pin")
    agentshim_pin = ctx.facts.get("agentshim", {}).get("pin")
    facts: dict[str, Any] = {}
    problems: list[str] = []
    for label, ref in sorted(images.items()):
        key = label.split(":", 1)[0]
        info = ctx.host.image(ref)
        facts[label] = {
            "ref": ref,
            "id": info.id if info else None,
            "repo_digests": list(info.repo_digests) if info else [],
        }
        if info is None:
            if key != "agent_base":  # SREGym builds its agent base image from this checkout when missing.
                problems.append(f"image {ref} is not present")
            continue
        if key not in SDO_IMAGE_ENTRY_POINTS:
            continue
        versions = ctx.host.image_versions(ref, SDO_IMAGE_ENTRY_POINTS[key])
        if versions is None:
            problems.append(f"image {ref} could not be probed")
            continue
        facts[label].update(asdict(versions))
        if versions.import_error:
            problems.append(f"image {ref} fails to import its entry points: {versions.import_error}")
        if key in AGENT_IMAGES:
            if versions.codex != codex_pin:
                problems.append(f"image {ref} has Codex CLI {versions.codex!r}, not the pinned {codex_pin!r}")
            if versions.agentshim != agentshim_pin:
                problems.append(f"image {ref} has agentshim {versions.agentshim!r}, uv.lock pins {agentshim_pin!r}")
    ctx.facts["images"] = facts
    if problems:
        return PreflightCheck(
            "sdo-images",
            "fail",
            "; ".join(problems),
            "rebuild the images from this checkout with scripts/build_sdo_images.sh",
        )
    return PreflightCheck("sdo-images", "pass", f"{len(images)} images present and probed")


def _kubeconfig_targets(path: Path) -> tuple[set[str], str | None, set[int]]:
    """``(context cluster names, current context, server ports)`` of a kubeconfig."""

    document = _as_dict(yaml.safe_load(path.read_text(encoding="utf-8")))
    clusters = {str(_as_dict(item.get("context")).get("cluster")) for item in _as_dicts(document.get("contexts"))}
    ports: set[int] = set()
    for item in _as_dicts(document.get("clusters")):
        match = re.search(r":(\d+)/?$", str(_as_dict(item.get("cluster")).get("server", "")))
        if match:
            ports.add(int(match.group(1)))
    current: object = document.get("current-context")
    return clusters, str(current) if current else None, ports


def _check_lane_isolation(ctx: _Context) -> PreflightCheck:
    problems: list[str] = []
    lanes: dict[str, Any] = {}
    stable_dir = ctx.host.home / ".cache" / "sregym" / "kubeconfigs"
    existing = ctx.host.kind_clusters()
    clusters = sorted({cluster for config in ctx.configs for cluster in lane_clusters(config, ctx.env)})
    reuse = any(config.env.reuse_cluster for config in ctx.configs)
    for cluster in clusters:
        lane: dict[str, Any] = {"exists": None if existing is None else cluster in existing}
        owner = ctx.host.cluster_lock_owner(cluster)
        if owner:
            problems.append(f"cluster {cluster} is locked by another experiment: {owner}")
        stable = stable_dir / f"{cluster}.kubeconfig"
        if reuse and stable.is_file():
            names, current, ports = _kubeconfig_targets(stable)
            expected = f"kind-{cluster}"
            lane["stable_kubeconfig"] = str(stable)
            if names != {expected} or current != expected:
                problems.append(f"{stable} targets {sorted(names)} (current {current!r}), not only {expected}")
            port = ctx.host.control_plane_port(cluster) if lane["exists"] else None
            lane["api_port"] = port
            if port is not None and ports != {port}:
                problems.append(f"{stable} points at port(s) {sorted(ports)}, but {cluster}'s API is on {port}")
        lanes[cluster] = lane
    ambient = ctx.env.get("KUBECONFIG", "").strip()
    if ambient and ":" not in ambient and Path(ambient).is_file():
        _, current, _ = _kubeconfig_targets(Path(ambient))
        lanes["ambient_kubeconfig"] = {"path": ambient, "current_context": current}
        if current is not None and current.startswith("kind-") and current.removeprefix("kind-") not in clusters:
            problems.append(f"KUBECONFIG={ambient} selects {current}, outside this lane {clusters}")
    ctx.facts["lanes"] = lanes
    if problems:
        return PreflightCheck(
            "lane-isolation",
            "fail",
            "; ".join(problems),
            "give each concurrent experiment its own SREGYM_KIND_CLUSTER_PREFIX or SREGYM_WORKER_ID_OFFSET, "
            "delete a stale stable kubeconfig so SREGym rewrites it, and unset or correct KUBECONFIG",
        )
    return PreflightCheck("lane-isolation", "pass", f"lane {clusters} is free and its kubeconfigs reach only it")


def _check_model_policy(ctx: _Context) -> PreflightCheck:
    policy = ctx.settings.policy
    problems: list[str] = []
    roles_by_arm: list[dict[str, dict[str, Any]]] = []
    for config in ctx.configs:
        roles = resolve_roles(config, ctx.env, policy)
        roles_by_arm.append({role: asdict(resolved) for role, resolved in roles.items()})
        arm = config.agent
        if arm not in policy.agents:
            problems.append(f"agent {arm!r} is outside the allowed arms {sorted(policy.agents)}")
        if config.model != policy.model:
            problems.append(f"{arm}: runner.model is {config.model!r}, not {policy.model!r}")
        if config.reasoning_effort != policy.agent_effort:
            problems.append(
                f"{arm}: runner.reasoning_effort is {config.reasoning_effort!r}, "
                f"not SDO's pinned {policy.agent_effort!r}"
            )
        for role, resolved in roles.items():
            if role == "sregym_judge":
                if resolved.model != policy.judge_model_id:
                    problems.append(f"{arm}: judge is {resolved.model!r}, not {policy.judge_model_id!r}")
                if resolved.effort != policy.judge_effort:
                    problems.append(f"{arm}: judge effort is {resolved.effort!r}, not {policy.judge_effort!r}")
                continue
            if resolved.provider not in (None, policy.provider) or resolved.model != policy.model:
                problems.append(
                    f"{arm}: {role} resolves to {resolved.provider}:{resolved.model}, "
                    f"not {policy.provider}:{policy.model}"
                )
    for name in SDO_MODEL_ENV_OVERRIDES:
        value = ctx.env.get(name, "").strip()
        if value and value != policy.model:
            problems.append(f"{name}={value} would override an SDO role's model")
    ctx.facts["roles"] = roles_by_arm
    if problems:
        return PreflightCheck(
            "model-policy",
            "fail",
            "; ".join(problems),
            f"every arm runs {policy.provider}:{policy.model} at {policy.agent_effort} and is judged by "
            f"{policy.judge_model_id} at {policy.judge_effort}; fix the config or unset the override",
        )
    return PreflightCheck(
        "model-policy", "pass", f"all roles {policy.provider}:{policy.model}, judge {policy.judge_model_id}"
    )


#: Settings that affect grading or timing and so must match across compared arms and stages.
PARITY_FIELDS: dict[str, Callable[[ExperimentConfig], object]] = {
    "model": lambda c: c.model,
    "reasoning_effort": lambda c: c.reasoning_effort,
    "judge_model_id": lambda c: c.env.judge_model_id,
    "app_filter": lambda c: c.app_filter,
    "deploy_from_source": lambda c: c.deploy_from_source,
    "worker_cpu_limit": lambda c: c.env.worker_cpu_limit,
    "kind_worker_nodes": lambda c: c.env.kind_worker_nodes,
    "agent_timeout": lambda c: c.agent_timeout,
}


def _check_parity(ctx: _Context) -> PreflightCheck | None:
    if len(ctx.configs) < 2:
        return None
    differences: list[str] = []
    for name, read in PARITY_FIELDS.items():
        values = sorted({repr(read(config)) for config in ctx.configs})
        if len(values) > 1:
            differences.append(f"{name} differs: {', '.join(values)}")
    if differences:
        return PreflightCheck(
            "arm-parity", "fail", "; ".join(differences), "make every compared arm and stage use the same settings"
        )
    return PreflightCheck("arm-parity", "pass", f"{len(ctx.configs)} arms/stages share {', '.join(PARITY_FIELDS)}")


def _check_quota(ctx: _Context) -> PreflightCheck:
    codex_home = Path(ctx.env.get("CODEX_HOME", "").strip() or ctx.host.home / ".codex")
    snapshot = read_quota_snapshot(codex_home)
    if snapshot is None:
        ctx.facts["codex_quota"] = {"status": "unknown", "reason": f"no rate-limit snapshot under {codex_home}"}
        return PreflightCheck("codex-quota", "unknown", f"no offline Codex rate-limit snapshot under {codex_home}")
    age = ctx.now - snapshot.observed_at
    ctx.facts["codex_quota"] = {
        "observed_at": snapshot.observed_at,
        "age_seconds": round(age, 1),
        "source": snapshot.source,
        "windows": {name: asdict(window) for name, window in snapshot.windows.items()},
    }
    windows = {
        name: window
        for name, window in snapshot.windows.items()
        if window.resets_at is None or window.resets_at > ctx.now
    }
    if age > ctx.settings.quota_snapshot_max_age_seconds or not windows:
        ctx.facts["codex_quota"]["status"] = "unknown"
        return PreflightCheck(
            "codex-quota", "unknown", f"newest Codex rate-limit snapshot is {age / 3600:.1f} h old or past its reset"
        )
    exhausted = [
        f"{name} window {window.used_percent:.0f}% used (resets {_fmt_epoch(window.resets_at)})"
        for name, window in windows.items()
        if window.used_percent >= ctx.settings.max_quota_used_percent
    ]
    if exhausted:
        ctx.facts["codex_quota"]["status"] = "exhausted"
        return PreflightCheck(
            "codex-quota",
            "fail",
            f"Codex quota has no headroom: {'; '.join(exhausted)}; limit {ctx.settings.max_quota_used_percent:.0f}%",
            f"wait for the reset, or raise {MAX_QUOTA_USED_ENV} if the run is small",
        )
    ctx.facts["codex_quota"]["status"] = "ok"
    used = ", ".join(f"{name} {window.used_percent:.0f}%" for name, window in windows.items())
    return PreflightCheck("codex-quota", "pass", f"Codex quota used: {used}")


def _fmt_epoch(value: float | None) -> str:
    if value is None:
        return "unknown"
    return time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(value))


def run_preflight(
    configs: Sequence[ExperimentConfig],
    *,
    project_root: Path,
    sregym_dir: Path,
    env: Mapping[str, str],
    host: HostProbe,
    settings: PreflightSettings | None = None,
    mode: PreflightMode = "enforce",
    now: float | None = None,
) -> PreflightReport:
    """Run every check over the arms or stages about to launch; never raises for a failed check."""

    if not configs:
        raise ValueError("preflight needs at least one experiment config")
    ctx = _Context(
        configs=list(configs),
        project_root=project_root,
        sregym_dir=sregym_dir,
        env=env,
        host=host,
        settings=settings or PreflightSettings(),
        now=time.time() if now is None else now,
    )
    checks = [
        _check_neighbor_cache(ctx),
        _check_disk(ctx),
        _check_application_sources(ctx),
        _check_codex_pins(ctx),
        _check_agentshim(ctx),
        _check_sdo_images(ctx),
        _check_lane_isolation(ctx),
        _check_model_policy(ctx),
        _check_parity(ctx),
        _check_quota(ctx),
    ]
    return PreflightReport(tuple(check for check in checks if check is not None), ctx.facts, mode)


def check_images(
    images: Mapping[str, str],
    *,
    project_root: Path,
    sregym_dir: Path,
    host: HostProbe,
) -> PreflightReport:
    """Probe freshly built SDO images (``role -> ref``) against the pins, without any experiment config."""

    unknown = sorted(set(images) - set(SDO_IMAGE_ENTRY_POINTS))
    if unknown or not images:
        raise ValueError(f"image roles must be among {sorted(SDO_IMAGE_ENTRY_POINTS)}, got {sorted(images)}")
    ctx = _Context(
        configs=[],
        project_root=project_root,
        sregym_dir=sregym_dir,
        env={},
        host=host,
        settings=PreflightSettings(),
        now=time.time(),
        images=dict(images),
    )
    checks = [_check_codex_pins(ctx), _check_agentshim(ctx), _check_sdo_images(ctx)]
    return PreflightReport(tuple(check for check in checks if check is not None), ctx.facts, "enforce")


def preflight_mode(env: Mapping[str, str]) -> PreflightMode:
    raw = env.get(PREFLIGHT_MODE_ENV, "").strip().lower() or "enforce"
    if raw not in ("enforce", "warn"):
        raise ValueError(f"{PREFLIGHT_MODE_ENV} must be 'enforce' or 'warn', got {raw!r}")
    return raw


@dataclass
class LaunchAssurance:
    """What the runner consults before and while launching: the preflight, then the manifest."""

    host: HostProbe = field(default_factory=SystemHost)
    settings: PreflightSettings = field(default_factory=PreflightSettings)
    mode: PreflightMode = "enforce"

    def __post_init__(self) -> None:
        if self.mode not in ("enforce", "warn"):
            raise ValueError(f"LaunchAssurance.mode must be 'enforce' or 'warn', got {self.mode!r}")

    def preflight(
        self,
        configs: Sequence[ExperimentConfig],
        *,
        project_root: Path,
        sregym_dir: Path,
        env: Mapping[str, str],
    ) -> PreflightReport:
        """Run the preflight; raise :class:`PreflightError` on a failure unless in warn mode."""

        report = run_preflight(
            configs,
            project_root=project_root,
            sregym_dir=sregym_dir,
            env=env,
            host=self.host,
            settings=self.settings,
            mode=self.mode,
        )
        print(render_report(report), flush=True)
        if not report.ok and self.mode == "enforce":
            raise PreflightError(report)
        return report

    def write_manifest(
        self,
        run_dir: Path,
        configs: Sequence[ExperimentConfig],
        *,
        snapshot: Path,
        report: PreflightReport,
        project_root: Path,
        sregym_dir: Path,
        env: Mapping[str, str],
        kind: Literal["experiment", "pipeline", "stage"],
        source: Path | None = None,
    ) -> Path:
        """Write ``run_manifest.json`` into *run_dir* (see :mod:`benchmarks.sregym.runner.manifest`)."""

        from benchmarks.sregym.runner.manifest import build_run_manifest, write_run_manifest

        manifest = build_run_manifest(
            run_dir=run_dir,
            configs=configs,
            snapshot=snapshot,
            report=report,
            host=self.host,
            project_root=project_root,
            sregym_dir=sregym_dir,
            env=env,
            kind=kind,
            source=source,
            settings=self.settings,
        )
        return write_run_manifest(run_dir, manifest)


def default_assurance(env: Mapping[str, str] | None = None) -> LaunchAssurance:
    env = os.environ if env is None else env
    return LaunchAssurance(settings=PreflightSettings.from_env(env), mode=preflight_mode(env))


def render_report(report: PreflightReport) -> str:
    marks = {"pass": "ok", "fail": "FAIL", "unknown": "??"}
    lines = [f"preflight ({report.mode}):"]
    lines.extend(f"  {marks[check.status]:>4} {check.name}: {check.detail}" for check in report.checks)
    if report.waived:
        lines.append(f"  preflight failures WAIVED by {PREFLIGHT_MODE_ENV}=warn; this run will be invalid_infra")
    return "\n".join(lines)


def load_arm_configs(paths: Iterable[Path], env: Mapping[str, str]) -> list[ExperimentConfig]:
    """Every stage of every config file, with env overrides applied as the runner would."""

    configs: list[ExperimentConfig] = []
    for path in paths:
        if is_pipeline_config(path):
            pipeline = load_pipeline_config(path)
            configs.extend(
                resolve_config(merge_stage_config(pipeline.defaults, stage.runner_overrides), dict(env))
                for stage in pipeline.stages
            )
        else:
            configs.append(resolve_config(load_experiment_config(path), dict(env)))
    return configs


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the SREGym launch preflight over the arms of a comparison.")
    parser.add_argument("configs", type=Path, nargs="*", help="experiment or pipeline TOMLs compared together")
    parser.add_argument(
        "--image",
        action="append",
        default=[],
        metavar="ROLE=REF",
        help=f"probe only these built images (roles: {', '.join(SDO_IMAGE_ENTRY_POINTS)}); used by CI",
    )
    parser.add_argument("--json", type=Path, help="also write the report as JSON")
    args = parser.parse_args(argv)
    if bool(args.configs) == bool(args.image):
        parser.error("give either experiment configs or --image ROLE=REF, not both or neither")
    project_root = Path(__file__).resolve().parents[3]
    sregym_dir = Path(os.environ.get("SDO_SREGYM_DIR", project_root / "third_party" / "sregym")).resolve()
    env = dict(os.environ)
    if args.image:
        pairs = [str(item).partition("=") for item in args.image]
        if any(not role or not ref for role, _, ref in pairs):
            parser.error("--image takes ROLE=REF")
        try:
            report = check_images(
                {role: ref for role, _, ref in pairs},
                project_root=project_root,
                sregym_dir=sregym_dir,
                host=SystemHost(),
            )
        except ValueError as exc:
            parser.error(str(exc))
    else:
        report = run_preflight(
            load_arm_configs(args.configs, env),
            project_root=project_root,
            sregym_dir=sregym_dir,
            env=env,
            host=SystemHost(),
            settings=PreflightSettings.from_env(env),
        )
    print(render_report(report))
    if args.json:
        args.json.write_text(json.dumps(report.to_dict(), indent=2) + "\n", encoding="utf-8")
    if not report.ok:
        print(PreflightError(report), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
