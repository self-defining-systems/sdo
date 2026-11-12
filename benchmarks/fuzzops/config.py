"""Operator configuration for benchmark launches (``docs/config.md``).

A benchmark never passes an operator knob as its own flag. ``sdo-operator host`` and ``install-existing`` get the
benchmark profile (``sdo-bench.yaml``, only the fields that differ from production defaults) as the first
``--config``, then the harness's overlay files in order, and the per-experiment, per-lane and per-run values as
``SDO_*`` environment variables (``operator_env``). An overlay is a nested mapping of config sections written as
JSON, which is YAML.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

BENCH_PROFILE = Path(__file__).resolve().with_name("sdo-bench.yaml")
MODES = ("operate", "frozen", "watch-only", "external")

# Pre-config ``install-existing``/``host`` flags and the config key each one set (``--flag`` -> (path, kind)).
# Only translating a stored format-1 fuzzops baseline uses this table.
LEGACY_FLAGS: dict[str, tuple[tuple[str, ...], str]] = {
    "--code-path": (("agent", "code_path"), "string"),
    "--code-path-overrides": (("agent", "code_path_overrides"), "string"),
    "--lifecycle-timeout": (("agent", "lifecycle_timeout"), "string"),
    "--self-heal-hold": (("detection", "self_heal_hold"), "string"),
    "--closure-stability-window": (("detection", "stability_window"), "string"),
    "--drift-batch-window": (("detection", "drift_batch_window"), "string"),
    "--responder-env": (("agent", "extra_env"), "list"),
    "--response-timeout": (("response", "timeout"), "string"),
    "--verification-timeout": (("response", "verification_timeout"), "string"),
    "--max-response-loops": (("response", "max_loops"), "int"),
    "--repair-policy": (("response", "repair_policy"), "string"),
    "--responder-standby": (("response", "standby"), "bool"),
    "--permissive-responder": (("response", "permissive"), "bool"),
    "--responder-namespace": (("response", "permissive_namespaces"), "list"),
    "--replay": (("learning", "replay"), "string"),
    "--reflection-guidance": (("learning", "reflection_guidance"), "string"),
    "--force-reflection": (("learning", "force_reflection"), "bool"),
    "--healthy-baseline": (("learning", "healthy_baseline"), "bool"),
    "--max-attempts": (("deployment", "max_attempts"), "int"),
    "--build-timeout": (("deployment", "build_timeout"), "string"),
    "--build-budget": (("deployment", "build_budget"), "string"),
    "--rollout-timeout": (("deployment", "rollout_timeout"), "string"),
    "--allow-registry": (("deployment", "policy", "allowed_registries"), "csv"),
    "--scope-blocking": (("deployment", "policy", "scope_blocking"), "bool"),
    "--grant-namespace": (("deployment", "policy", "granted_namespaces"), "csv"),
    "--grant-cluster-kind": (("deployment", "policy", "granted_cluster_kinds"), "csv"),
    "--telemetry-profile": (("telemetry", "profile"), "string"),
    "--telemetry-source": (("telemetry", "sources"), "list"),
}
_MODE_FLAGS = ("--frozen-memory", "--watch-only", "--external-incidents")


def config_args(files: Iterable[Path]) -> list[str]:
    """``--config FILE`` for each file, in order (later files override earlier ones)."""

    args: list[str] = []
    for path in files:
        args.extend(("--config", str(path)))
    return args


def merge(*overlays: Mapping[str, Any]) -> dict[str, Any]:
    """Deep-merge overlays, later ones winning; a list or scalar replaces, a mapping merges."""

    merged: dict[str, Any] = {}
    for overlay in overlays:
        for key, value in overlay.items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key] = merge(merged[key], value)
            else:
                merged[key] = value
    return merged


def write_overlay(settings: Mapping[str, Any], path: Path) -> Path:
    """Write an overlay mapping as a config file (JSON is YAML)."""

    if not isinstance(settings, dict):
        raise TypeError("an operator config overlay must be a mapping of config sections")
    path.write_text(json.dumps(settings, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def temporary_overlay(settings: Mapping[str, Any], *, prefix: str = "sdo-config-") -> Path:
    """Write the overlay to a new temporary file the caller removes after the launch."""

    descriptor, name = tempfile.mkstemp(prefix=prefix, suffix=".yaml")
    os.close(descriptor)
    return write_overlay(settings, Path(name))


def operator_env(
    *,
    provider: str = "",
    model: str = "",
    effort: str = "",
    evidence_host_path: str = "",
    evidence_run_id: str = "",
    mode: str = "",
) -> dict[str, str]:
    """The ``SDO_*`` variables of one launch; an empty value is left to the config files and defaults."""

    if mode and mode not in MODES:
        raise ValueError(f"mode must be one of {MODES}, got {mode!r}")
    if evidence_host_path and not evidence_host_path.startswith("/"):
        raise ValueError("the evidence host path must be an absolute node path")
    if evidence_run_id and not evidence_host_path:
        raise ValueError("an evidence run ID requires an evidence host path")
    values = {
        "SDO_AGENT_PROVIDER": provider,
        "SDO_AGENT_MODEL": model,
        "SDO_AGENT_EFFORT": effort,
        "SDO_EVIDENCE_HOST_PATH": evidence_host_path,
        "SDO_EVIDENCE_RUN_ID": evidence_run_id,
        "SDO_MODE": mode,
    }
    return {name: value for name, value in values.items() if value}


def translate_flags(flags: Sequence[str]) -> dict[str, Any]:
    """The config overlay equivalent to pre-config ``install-existing`` flags; an unknown flag is an error."""

    overlay: dict[str, Any] = {}
    modes: dict[str, bool] = {}
    index = 0
    while index < len(flags):
        token = flags[index]
        index += 1
        if not token.startswith("--"):
            raise ValueError(f"expected a flag, got {token!r}")
        name, separator, inline = token.partition("=")
        kind = "bool" if name in _MODE_FLAGS else LEGACY_FLAGS.get(name, ((), ""))[1]
        if not kind:
            raise ValueError(f"flag {name} has no operator config key; see docs/config.md")
        if separator:
            value = inline
        elif kind == "bool":
            value = "true"
        elif index < len(flags):
            value = flags[index]
            index += 1
        else:
            raise ValueError(f"flag {name} needs a value")
        if name in _MODE_FLAGS:
            modes[name] = _bool(name, value)
            continue
        path = LEGACY_FLAGS[name][0]
        section = overlay
        for key in path[:-1]:
            section = section.setdefault(key, {})
        if kind == "int":
            section[path[-1]] = int(value)
        elif kind == "bool":
            section[path[-1]] = _bool(name, value)
        elif kind == "list":
            section.setdefault(path[-1], []).append(value)
        elif kind == "csv":
            section[path[-1]] = [item.strip() for item in value.split(",") if item.strip()]
        else:
            section[path[-1]] = value
    if modes:
        overlay["mode"] = _mode(modes)
    return overlay


def _bool(name: str, value: str) -> bool:
    lowered = value.lower()
    if lowered in ("1", "t", "true"):
        return True
    if lowered in ("0", "f", "false"):
        return False
    raise ValueError(f"flag {name} needs a boolean, got {value!r}")


def _mode(flags: Mapping[str, bool]) -> str:
    frozen, watch, external = (flags.get(name, False) for name in _MODE_FLAGS)
    if watch and external:
        raise ValueError("--watch-only conflicts with --external-incidents")
    if (watch or external) and not frozen:
        raise ValueError("--watch-only and --external-incidents require --frozen-memory")
    return "watch-only" if watch else "external" if external else "frozen" if frozen else "operate"


def resolved(
    operator: Sequence[str],
    files: Iterable[Path],
    env: Mapping[str, str],
    *,
    complete: bool = True,
    timeout: float = 120,
) -> dict[str, Any]:
    """The configuration ``sdo-operator config print`` resolves from these files and variables.

    ``complete=False`` accepts a document that does not validate yet (for example, no model), for a local runner
    that reads only the defaults it needs.
    """

    import yaml

    environment = {**os.environ, **env}
    completed = subprocess.run(
        [*operator, "config", "print", *config_args(files)],
        capture_output=True,
        text=True,
        env=environment,
        timeout=timeout,
        check=False,
    )
    document = yaml.safe_load(completed.stdout or "null") if completed.returncode in (0, 1) else None
    if (completed.returncode != 0 and complete) or not isinstance(document, dict):
        raise ValueError(f"sdo-operator config print failed: {(completed.stderr or completed.stdout).strip()[-1024:]}")
    return document


_UNITS = {"h": 3600.0, "m": 60.0, "s": 1.0, "ms": 1e-3, "us": 1e-6, "µs": 1e-6, "ns": 1e-9}
_DURATION = re.compile(r"(\d+(?:\.\d*)?)(h|ms|us|µs|ns|m|s)")


def duration_seconds(value: str) -> float:
    """Seconds of a Go duration string as the config prints it (``15m0s``)."""

    text = str(value).strip()
    if text in ("0", ""):
        return 0.0
    parts = _DURATION.findall(text)
    if not parts or "".join(number + unit for number, unit in parts) != text:
        raise ValueError(f"not a Go duration: {value!r}")
    return sum(float(number) * _UNITS[unit] for number, unit in parts)


def local_operator() -> list[str]:
    """The ``sdo-operator`` command a local runner uses: ``SDO_OPERATOR_BIN``, the one on PATH, else ``go run``."""

    if specified := os.environ.get("SDO_OPERATOR_BIN", "").strip():
        return [specified]
    if installed := shutil.which("sdo-operator"):
        return [installed]
    module = Path(__file__).resolve().parents[2] / "sdo" / "operator"
    return ["go", "-C", str(module), "run", "./cmd/sdo-operator"]


#: Worker variables the controller sets in every worker Job from the resolved config (``docs/config.md``).
_WORKER_LIMITS = {
    "SDO_LIFECYCLE_TIMEOUT_SECONDS": ("agent", "lifecycle_timeout"),
    "SDO_MEMORY_CHECK_TIMEOUT_SECONDS": ("agent", "memory_check_timeout"),
}


def worker_environment(operator: Sequence[str], files: Iterable[Path] = (BENCH_PROFILE,)) -> dict[str, str]:
    """The agent variables a controller would set in a worker Job, for a runner that starts workers without one.

    The values come from ``sdo-operator config print``, so a local worker runs at the Go defaults.
    """

    settings = resolved(operator, files, {}, complete=False)
    agent = settings["agent"]
    environment = {"SDO_AGENT_PROVIDER": str(agent["provider"]), "SDO_AGENT_EFFORT": str(agent["effort"])}
    for name, (section, key) in _WORKER_LIMITS.items():
        environment[name] = str(int(duration_seconds(str(settings[section][key]))))
    return environment
