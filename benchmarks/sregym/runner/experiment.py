"""Centralized experiment configuration for SREGym runs.

Provides a single ExperimentConfig dataclass that captures all settings
needed to reproduce an experiment: runner args, env vars, problem selection,
and agent-specific config.
"""

from __future__ import annotations

import copy
import dataclasses
import json
import os
import shutil
from pathlib import Path
from typing import Any, Literal

from pydantic import model_validator
from pydantic.dataclasses import dataclass

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

import yaml

from benchmarks.sregym.runner.codex_baseline import ALLOW_EXEC_ENV, PROMPT_APPENDIX_ENV, CodexBaselineConfig

_VARIANT_ORDERS = ("flat", "round_robin", "grouped", "adaptive")
#: The efforts SREGym's --reasoning-effort accepts.
REASONING_EFFORTS = frozenset({"none", "minimal", "low", "medium", "high", "xhigh", "max"})
VariantOrder = Literal["flat", "round_robin", "grouped", "adaptive"]
ApplicationWorkspaceMode = Literal["persistent", "ephemeral"]
ApplicationWorkspaceSetting = ApplicationWorkspaceMode | bool


@dataclass
class VariantConfig:
    enabled: bool = False
    count: int = 0
    offset: int = 0
    seed: int = 42
    order: VariantOrder = "round_robin"
    max_per_class: int | None = None
    consec_solves_to_stop: int | None = None
    spec_names: list[str] = dataclasses.field(default_factory=list)  # pyright: ignore[reportUnknownVariableType]

    @model_validator(mode="after")
    def _validate(self) -> VariantConfig:
        if self.max_per_class is not None:
            if self.order not in ("grouped", "adaptive"):
                raise ValueError("variants.max_per_class is only valid when variants.order in {'grouped', 'adaptive'}")
            if self.max_per_class <= 0:
                raise ValueError("variants.max_per_class must be > 0")
        if self.order == "adaptive":
            if self.max_per_class is None:
                raise ValueError("variants.max_per_class is required when variants.order='adaptive'")
            if self.consec_solves_to_stop is None:
                raise ValueError("variants.consec_solves_to_stop is required when variants.order='adaptive'")
            if self.consec_solves_to_stop <= 0:
                raise ValueError("variants.consec_solves_to_stop must be > 0")
        elif self.consec_solves_to_stop is not None:
            raise ValueError("variants.consec_solves_to_stop is only valid when variants.order='adaptive'")
        if self.spec_names and not self.enabled:
            raise ValueError("variants.spec_names requires variants.enabled = true")
        return self


_BOOL_ENV_TRUE = {"1", "true", "yes", "on"}


def _parse_bool_env(value: str) -> bool:
    return value.strip().lower() in _BOOL_ENV_TRUE


def application_workspace_enabled(value: ApplicationWorkspaceSetting) -> bool:
    return application_workspace_mode(value) is not None


def application_workspace_mode(value: ApplicationWorkspaceSetting) -> ApplicationWorkspaceMode | None:
    if value is True:
        return "persistent"
    if value is False:
        return None
    return value


#: Keys that only configured the removed Crucible agent. A config that still sets one
#: was written for Crucible, so it is rejected instead of being silently ignored.
REMOVED_CRUCIBLE_KEYS = frozenset({"chain_kb", "enable_summary", "crucible_seed_kb_dir"})


def reject_removed_crucible_keys(section: dict[str, Any], where: str) -> None:
    """Raise ``ValueError`` if *section* sets a key of the removed Crucible agent."""
    removed = sorted(REMOVED_CRUCIBLE_KEYS & section.keys())
    if removed:
        raise ValueError(f"{where}: {', '.join(removed)} configured the removed Crucible agent; delete it")


@dataclass
class RunnerEnv:
    judge_model_id: str = ""
    worker_cpu_limit: str = ""
    reuse_cluster: bool = False
    force_recreate_cluster: bool = False
    preserve_infrastructure: bool = False
    submit_done_returns_feedback: bool = False
    defer_diagnosis_grading: bool = False
    lifecycle_validation_cache: bool = False
    source_build_cache: bool = False
    fast_namespace_teardown: bool = False
    cleanup_defer_timeout_seconds: int = 0
    docker_builder: str = ""
    # Worker nodes of a kind cluster the harness creates for this lane; 0
    # keeps SREGym's default topology (1 control plane + 3 workers). A reused
    # cluster with another worker count is recreated.
    kind_worker_nodes: int = 0

    def __post_init__(self) -> None:
        if self.kind_worker_nodes < 0:
            raise ValueError(f"env.kind_worker_nodes must not be negative, got {self.kind_worker_nodes}")


@dataclass
class ExperimentConfig:
    # Runner settings (become CLI args to third_party/sregym/main.py)
    agent: str = "sdo_codex"
    model: str = "google-vertex:gemini-2.5-flash"
    parallel: int = 4
    agent_timeout: int = 1800
    app_filter: str = ""
    deploy_from_source: bool = False
    application_workspace: ApplicationWorkspaceSetting = False
    # Independent end-to-end attempts per selected problem, passed to SREGym as
    # --n-attempts. The parallel runner expands each problem into ``repeat``
    # consecutive tasks (A A B B), each with its own runs/<seq>_<problem>/ dir,
    # fresh agent launch, and fault injection; attempts are not retries.
    repeat: int = 1
    sequence_len: int = 0
    sequence_seed: int = 42
    require_strict_receipt: bool = False
    # A pipeline stage whose diagnosis or mitigation was graded false still counts as completed, so a
    # long incident stream continues past failures. Ungraded rows and agent errors still fail the stage.
    allow_failed_verdicts: bool = False
    # Agent reasoning effort, passed to SREGym as --reasoning-effort. Empty
    # leaves the agent's default. SDO's incident agents pin theirs in code
    # (sdo.agents.responder.INCIDENT_REASONING_EFFORT); a config test
    # keeps every compared arm declaring that value.
    reasoning_effort: str = ""

    # Problem selection (mutually exclusive with variants)
    tasklist: str = ""  # named set or path to YAML
    problems: list[str] = dataclasses.field(default_factory=list)  # pyright: ignore[reportUnknownVariableType]
    spec_names: list[str] = dataclasses.field(default_factory=list)  # pyright: ignore[reportUnknownVariableType]

    # Variant mode
    variants: VariantConfig = dataclasses.field(default_factory=VariantConfig)

    # Env vars passed to the main.py process
    env: RunnerEnv = dataclasses.field(default_factory=RunnerEnv)

    # Agent-specific config (keyed by agent name)
    agent_config: dict[str, dict[str, Any]] = dataclasses.field(default_factory=dict)  # pyright: ignore[reportUnknownVariableType]

    @model_validator(mode="after")
    def _validate_mutual_exclusion(self) -> ExperimentConfig:
        if self.repeat < 1:
            raise ValueError(f"runner.repeat must be at least 1, got {self.repeat}")
        if self.reasoning_effort and self.reasoning_effort not in REASONING_EFFORTS:
            raise ValueError(
                f"runner.reasoning_effort must be one of {sorted(REASONING_EFFORTS)}, got {self.reasoning_effort!r}"
            )
        if self.variants.enabled and (self.tasklist or self.problems):
            raise ValueError("runner.variants.enabled is mutually exclusive with runner.tasklist and runner.problems")
        if self.tasklist and self.problems:
            raise ValueError("runner.tasklist and runner.problems are mutually exclusive")
        if self.spec_names and self.variants.enabled:
            raise ValueError("runner.spec_names cannot be used with runner.variants.enabled")
        if self.spec_names and (self.tasklist or self.problems):
            raise ValueError("runner.spec_names is mutually exclusive with runner.tasklist and runner.problems")
        if self.agent == "codex":
            CodexBaselineConfig.from_agent_config(self.agent_config.get("codex") or {})
        if application_workspace_enabled(self.application_workspace):
            if not self.app_filter:
                raise ValueError("application_workspace requires runner.app_filter")
            if not self.deploy_from_source:
                raise ValueError("application_workspace requires runner.deploy_from_source = true")
        return self


def variant_config_from_raw(variants_raw: dict[str, Any]) -> VariantConfig:
    """Build a VariantConfig from a TOML-like dict, accepting the legacy
    ``round_robin`` field on read for backward compatibility with snapshots
    written by older versions of the runner.
    """
    if "order" in variants_raw:
        order = variants_raw["order"]
    elif "round_robin" in variants_raw:
        order = "round_robin" if variants_raw["round_robin"] else "flat"
    else:
        order = "round_robin"

    return VariantConfig(
        enabled=variants_raw.get("enabled", False),
        count=variants_raw.get("count", 0),
        offset=variants_raw.get("offset", 0),
        seed=variants_raw.get("seed", 42),
        order=order,
        max_per_class=variants_raw.get("max_per_class"),
        consec_solves_to_stop=variants_raw.get("consec_solves_to_stop"),
        spec_names=list(variants_raw.get("spec_names", [])),
    )


def load_experiment_config(path: Path) -> ExperimentConfig:
    """Read a TOML experiment config file and return an ExperimentConfig."""
    with open(path, "rb") as f:
        raw = tomllib.load(f)

    runner = raw.get("runner", {})
    variants_raw = runner.pop("variants", {})
    env_raw = runner.pop("env", {})
    reject_removed_crucible_keys(runner, f"{path} [runner]")
    reject_removed_crucible_keys(env_raw, f"{path} [runner.env]")

    variants = variant_config_from_raw(variants_raw)

    env = RunnerEnv(
        judge_model_id=env_raw.get("judge_model_id", ""),
        worker_cpu_limit=str(env_raw.get("worker_cpu_limit", "")),
        reuse_cluster=bool(env_raw.get("reuse_cluster", False)),
        force_recreate_cluster=bool(env_raw.get("force_recreate_cluster", False)),
        preserve_infrastructure=bool(env_raw.get("preserve_infrastructure", False)),
        submit_done_returns_feedback=bool(env_raw.get("submit_done_returns_feedback", False)),
        defer_diagnosis_grading=bool(env_raw.get("defer_diagnosis_grading", False)),
        lifecycle_validation_cache=bool(env_raw.get("lifecycle_validation_cache", False)),
        source_build_cache=bool(env_raw.get("source_build_cache", False)),
        fast_namespace_teardown=bool(env_raw.get("fast_namespace_teardown", False)),
        cleanup_defer_timeout_seconds=int(env_raw.get("cleanup_defer_timeout_seconds", 0)),
        docker_builder=str(env_raw.get("docker_builder", "")),
        kind_worker_nodes=int(env_raw.get("kind_worker_nodes", 0)),
    )

    agent = runner.get("agent", "sdo_codex")
    agent_config = copy.deepcopy(raw.get("agent", {}))

    return ExperimentConfig(
        agent=agent,
        model=runner.get("model", "google-vertex:gemini-2.5-flash"),
        parallel=runner.get("parallel", 4),
        agent_timeout=runner.get("agent_timeout", 1800),
        app_filter=runner.get("app_filter", ""),
        deploy_from_source=runner.get("deploy_from_source", False),
        application_workspace=runner.get("application_workspace", False),
        repeat=runner.get("repeat", 1),
        sequence_len=runner.get("sequence_len", 0),
        sequence_seed=runner.get("sequence_seed", 42),
        require_strict_receipt=runner.get("require_strict_receipt", False),
        allow_failed_verdicts=runner.get("allow_failed_verdicts", False),
        reasoning_effort=str(runner.get("reasoning_effort", "")),
        tasklist=runner.get("tasklist", ""),
        problems=runner.get("problems", []),
        spec_names=runner.get("spec_names", []),
        variants=variants,
        env=env,
        agent_config=agent_config,
    )


def resolve_config(
    config: ExperimentConfig,
    env_overrides: dict[str, str] | None = None,
) -> ExperimentConfig:
    """Apply environment variable overrides on top of the loaded config.

    Recognized env vars: MODEL, PARALLEL, JUDGE_MODEL_ID,
    SREGYM_WORKER_CPU_LIMIT, SREGYM_REUSE_CLUSTER,
    SREGYM_FORCE_RECREATE_CLUSTER, SREGYM_SUBMIT_DONE_RETURNS_FEEDBACK,
    SREGYM_CLEANUP_DEFER_TIMEOUT_SECONDS, SREGYM_KIND_WORKER_NODES.
    """
    if env_overrides is None:
        env_overrides = dict(os.environ)

    updates: dict[str, Any] = {}
    env_updates: dict[str, Any] = {}

    if "MODEL" in env_overrides:
        updates["model"] = env_overrides["MODEL"]
    if "PARALLEL" in env_overrides:
        updates["parallel"] = int(env_overrides["PARALLEL"])
    if "SREGYM_DEPLOY_FROM_SOURCE" in env_overrides:
        updates["deploy_from_source"] = _parse_bool_env(env_overrides["SREGYM_DEPLOY_FROM_SOURCE"])
    if "JUDGE_MODEL_ID" in env_overrides:
        env_updates["judge_model_id"] = env_overrides["JUDGE_MODEL_ID"]
    if "SREGYM_WORKER_CPU_LIMIT" in env_overrides:
        env_updates["worker_cpu_limit"] = env_overrides["SREGYM_WORKER_CPU_LIMIT"]
    if "SREGYM_REUSE_CLUSTER" in env_overrides:
        env_updates["reuse_cluster"] = _parse_bool_env(env_overrides["SREGYM_REUSE_CLUSTER"])
    if "SREGYM_FORCE_RECREATE_CLUSTER" in env_overrides:
        env_updates["force_recreate_cluster"] = _parse_bool_env(env_overrides["SREGYM_FORCE_RECREATE_CLUSTER"])
    if "SREGYM_PRESERVE_INFRASTRUCTURE" in env_overrides:
        env_updates["preserve_infrastructure"] = _parse_bool_env(env_overrides["SREGYM_PRESERVE_INFRASTRUCTURE"])
    if "SREGYM_DEFER_DIAGNOSIS_GRADING" in env_overrides:
        env_updates["defer_diagnosis_grading"] = _parse_bool_env(env_overrides["SREGYM_DEFER_DIAGNOSIS_GRADING"])
    if "SDO_LIFECYCLE_VALIDATION_CACHE" in env_overrides:
        env_updates["lifecycle_validation_cache"] = _parse_bool_env(env_overrides["SDO_LIFECYCLE_VALIDATION_CACHE"])
    if "SREGYM_SOURCE_BUILD_CACHE" in env_overrides:
        env_updates["source_build_cache"] = _parse_bool_env(env_overrides["SREGYM_SOURCE_BUILD_CACHE"])
    if "SREGYM_FAST_NAMESPACE_TEARDOWN" in env_overrides:
        env_updates["fast_namespace_teardown"] = _parse_bool_env(env_overrides["SREGYM_FAST_NAMESPACE_TEARDOWN"])
    if "SREGYM_SUBMIT_DONE_RETURNS_FEEDBACK" in env_overrides:
        env_updates["submit_done_returns_feedback"] = _parse_bool_env(
            env_overrides["SREGYM_SUBMIT_DONE_RETURNS_FEEDBACK"]
        )
    if "SREGYM_CLEANUP_DEFER_TIMEOUT_SECONDS" in env_overrides:
        env_updates["cleanup_defer_timeout_seconds"] = int(env_overrides["SREGYM_CLEANUP_DEFER_TIMEOUT_SECONDS"])
    if "SREGYM_KIND_WORKER_NODES" in env_overrides:
        env_updates["kind_worker_nodes"] = int(env_overrides["SREGYM_KIND_WORKER_NODES"])
    if updates or env_updates:
        new_env = dataclasses.replace(config.env, **env_updates) if env_updates else config.env
        config = dataclasses.replace(config, **updates, env=new_env)

    return config


def effective_agent_config(config: ExperimentConfig) -> dict[str, Any]:
    """Return a copy of the selected agent's config."""
    return copy.deepcopy(config.agent_config.get(config.agent, {}))


_SNAPSHOT_FILENAME = "experiment_config.toml"


def write_snapshot(config: ExperimentConfig, exp_dir: Path) -> Path:
    """Write the resolved config as a TOML file in the experiment directory."""
    dest = exp_dir / _SNAPSHOT_FILENAME
    dest.write_text(_serialize_config(config))
    return dest


def read_snapshot(exp_dir: Path) -> ExperimentConfig:
    """Read an experiment config from an existing experiment directory."""
    return load_experiment_config(exp_dir / _SNAPSHOT_FILENAME)


_TASKLIST_FILENAME = "tasklist.yml"


def resolve_tasklist(
    config: ExperimentConfig,
    sregym_dir: Path,
    exp_dir: Path,
) -> Path | None:
    """Resolve the tasklist and write it into the experiment directory.

    Returns the path to the resolved tasklist YAML, or None if no tasklist
    is needed (variant mode or run-all-problems).
    """
    if config.variants.enabled:
        return None

    dest = exp_dir / _TASKLIST_FILENAME

    if config.problems:
        # SREGym's parallel runner accepts all.problems as an ordered list and
        # runs every entry, so repeated problem ids stay separate runs. A stage
        # mapping keyed by problem id would silently collapse repeats.
        tasklist_data = {"all": {"problems": list(config.problems)}}
        with open(dest, "w") as f:
            yaml.dump(tasklist_data, f, default_flow_style=False)
        return dest

    if config.tasklist:
        source = _resolve_tasklist_source(config.tasklist, sregym_dir)
        shutil.copy2(source, dest)
        return dest

    # No tasklist specified = run all problems
    return None


def _resolve_tasklist_source(tasklist_ref: str, sregym_dir: Path) -> Path:
    """Resolve a tasklist reference to an actual file path.

    A tasklist_ref can be:
    - A named pre-built set (e.g. "count_train") that maps to
      sregym/conductor/tasklist.count_train.yml
    - A path to a custom YAML file
    """
    # Check if it's a path to an existing file
    as_path = Path(tasklist_ref)
    if as_path.is_file():
        return as_path

    # Try as a named pre-built set
    prebuilt = sregym_dir / "sregym" / "conductor" / f"tasklist.{tasklist_ref}.yml"
    if prebuilt.is_file():
        return prebuilt

    raise FileNotFoundError(
        f"Tasklist '{tasklist_ref}' not found. Tried: {as_path} (custom path), {prebuilt} (pre-built set)"
    )


#: Agents launched from this repository's registry (``benchmarks/sregym/registry.yaml``) instead of the
#: harness's ``agents.yaml``. ``codex`` is here only to pin its CLI version to the one the SDO images use.
_EXTERNAL_AGENTS = {"codex", "sdo_codex"}
#: Namespace label the SDO installer puts on a separate controller namespace;
#: the harness keeps namespaces with this label across problems when told to.
PERSISTENT_CONTROLLER_NAMESPACE_LABEL = "sdo.dev/controller-namespace"


def persistent_controller_enabled(config: ExperimentConfig) -> bool:
    """Whether SDO keeps one controller per application across this experiment's problems."""

    if config.agent != "sdo_codex":
        return False
    return bool((config.agent_config.get("sdo_codex") or {}).get("persistent_controller", False))


def config_to_main_args(
    config: ExperimentConfig,
    exp_dir: Path,
    tasklist_path: Path | None,
) -> list[str]:
    """Convert ExperimentConfig to CLI args for third_party/sregym/main.py."""
    args = [
        "--agent",
        config.agent,
        "--model",
        config.model,
        "--parallel",
        str(config.parallel),
        "--agent-timeout",
        str(config.agent_timeout),
        "--experiment-dir",
        str(exp_dir),
    ]

    if config.env.judge_model_id:
        args.extend(["--judge-model", config.env.judge_model_id])

    if config.app_filter:
        args.extend(["--app-filter", config.app_filter])
    if config.deploy_from_source:
        args.append("--deploy-from-source")
    # main.py declares --application-workspace as store_true (boolean enable);
    # it takes no value. Emit the bare flag for every enabled mode; a trailing
    # mode token here is an argparse usage error (exit 2).
    if application_workspace_mode(config.application_workspace) is not None:
        args.append("--application-workspace")
    if config.repeat > 1:
        args.extend(["--n-attempts", str(config.repeat)])
    if config.reasoning_effort:
        # SREGym clears AGENT_REASONING_EFFORT unless this flag is passed.
        args.extend(["--reasoning-effort", config.reasoning_effort])

    if tasklist_path is not None:
        args.extend(["--tasklist", str(tasklist_path)])

    for name in config.spec_names:
        args.extend(["--problem-spec", name])

    if config.variants.enabled:
        args.append("--variants")
        # In adaptive mode --variant-count is ignored and the runner does not
        # require it; emit it only when set, to keep the CLI clean.
        if config.variants.order != "adaptive" or config.variants.count > 0:
            args.extend(["--variant-count", str(config.variants.count)])
        args.extend(["--variant-offset", str(config.variants.offset)])
        args.extend(["--variant-seed", str(config.variants.seed)])
        args.extend(["--variant-order", config.variants.order])
        if config.variants.max_per_class is not None:
            args.extend(["--variant-max-per-class", str(config.variants.max_per_class)])
        if config.variants.consec_solves_to_stop is not None:
            args.extend(
                [
                    "--variant-adaptive-consec-solves",
                    str(config.variants.consec_solves_to_stop),
                ]
            )
        for name in config.variants.spec_names:
            args.extend(["--variant-spec", name])
    elif config.sequence_len > 0:
        args.extend(["--sequence-len", str(config.sequence_len)])
        args.extend(["--sequence-seed", str(config.sequence_seed)])

    return args


def config_to_env(config: ExperimentConfig, project_root: Path, exp_dir: Path | None = None) -> dict[str, str]:
    """Build env var dict from ExperimentConfig.

    Starts with the current environment and adds/overrides entries.
    """
    env = dict(os.environ)

    # Agent registry
    if config.agent in _EXTERNAL_AGENTS:
        registry = project_root / "benchmarks" / "sregym" / "registry.yaml"
        env["SREGYM_AGENT_REGISTRY"] = str(registry)

    # Runner env vars
    if config.env.judge_model_id:
        env["JUDGE_MODEL_ID"] = config.env.judge_model_id
    if config.env.worker_cpu_limit:
        env["SREGYM_WORKER_CPU_LIMIT"] = config.env.worker_cpu_limit
    if config.env.reuse_cluster:
        env["SREGYM_REUSE_CLUSTER"] = "1"
    if config.env.force_recreate_cluster:
        env["SREGYM_FORCE_RECREATE_CLUSTER"] = "1"
    if config.env.preserve_infrastructure:
        env["SREGYM_PRESERVE_INFRASTRUCTURE"] = "1"
    if config.env.defer_diagnosis_grading:
        env["SREGYM_DEFER_DIAGNOSIS_GRADING"] = "1"
    if config.env.lifecycle_validation_cache:
        # Shared by every pipeline of this checkout; entries are keyed by the
        # validator image identity and the diagnostics digest.
        env["SDO_LIFECYCLE_VALIDATION_CACHE_DIR"] = str(project_root / ".sdo-runtime" / "lifecycle-validation-cache")
    if config.env.source_build_cache:
        env["SREGYM_SOURCE_BUILD_CACHE"] = "1"
    if config.env.fast_namespace_teardown:
        env["SREGYM_FAST_NAMESPACE_TEARDOWN"] = "1"
    env["SREGYM_SUBMIT_DONE_RETURNS_FEEDBACK"] = "1" if config.env.submit_done_returns_feedback else "0"
    if config.env.cleanup_defer_timeout_seconds > 0:
        env["SREGYM_CLEANUP_DEFER_TIMEOUT_SECONDS"] = str(config.env.cleanup_defer_timeout_seconds)
    if config.env.docker_builder:
        env["SREGYM_DOCKER_BUILDER"] = config.env.docker_builder
    if config.env.kind_worker_nodes > 0:
        env["SREGYM_KIND_WORKER_NODES"] = str(config.env.kind_worker_nodes)

    # SREGym's base kind config disables the default CNI; Calico is installed
    # only on this request. Every arm asks for it, so a baseline lane is not
    # left without a CNI and network-policy faults are enforced on every arm.
    env["SREGYM_KIND_REQUIRE_NETWORK_POLICY"] = "1"
    env["SREGYM_PROGRESS_MODE"] = "rich"
    if exp_dir is not None:
        env["SREGYM_EXPERIMENT_DIR"] = str(exp_dir)

    # Agent-specific config via JSON env var
    agent_cfg = effective_agent_config(config)
    if agent_cfg:
        env["SREGYM_EXPERIMENT_AGENT_CONFIG"] = json.dumps(agent_cfg)
    # Only an arm that asks for a prompt appendix gets one; a value left in the
    # shell must never alter a stock baseline.
    env.pop(PROMPT_APPENDIX_ENV, None)
    env.pop(ALLOW_EXEC_ENV, None)
    if config.agent == "codex":
        baseline = CodexBaselineConfig.from_agent_config(config.agent_config.get("codex") or {})
        appendix = baseline.prompt_appendix()
        if appendix:
            env[PROMPT_APPENDIX_ENV] = appendix
        if baseline.allow_exec:
            env[ALLOW_EXEC_ENV] = "1"
    if config.agent == "sdo_codex":
        sdo_cfg = config.agent_config.get("sdo_codex") or {}
        validator_image = str(sdo_cfg.get("validator_image", "sdo-detector-validator:v0.1.0"))
        env["SREGYM_KIND_REQUIRED_IMAGES"] = json.dumps(
            [
                str(sdo_cfg.get("controller_image", "sdo-controller:v0.1.0")),
                str(sdo_cfg.get("responder_image", "sdo-responder:v0.1.0")),
                validator_image,
            ]
        )
        # Production validator Jobs execute untrusted detector code behind a
        # deny-all NetworkPolicy. A CNI that merely accepts NetworkPolicy
        # objects without enforcing them is not a production-equivalent test
        # environment, so SDO workers require a real enforcement preflight.
        env["SREGYM_KIND_REQUIRE_NETWORK_POLICY"] = "1"
        env["SREGYM_KIND_NETWORK_POLICY_CANARY_IMAGE"] = validator_image
        if persistent_controller_enabled(config):
            env["SREGYM_PRESERVE_NAMESPACE_LABEL"] = PERSISTENT_CONTROLLER_NAMESPACE_LABEL

    return env


def _toml_value(value: bool | int | str | list[Any]) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    items = ", ".join(_toml_value(item) for item in value)
    return f"[{items}]"


def _serialize_config(config: ExperimentConfig) -> str:
    """Serialize ExperimentConfig to TOML string."""
    lines = ["[runner]"]
    lines.append(f"agent = {_toml_value(config.agent)}")
    lines.append(f"model = {_toml_value(config.model)}")
    lines.append(f"parallel = {_toml_value(config.parallel)}")
    lines.append(f"agent_timeout = {_toml_value(config.agent_timeout)}")
    lines.append(f"app_filter = {_toml_value(config.app_filter)}")
    lines.append(f"deploy_from_source = {_toml_value(config.deploy_from_source)}")
    lines.append(f"application_workspace = {_toml_value(config.application_workspace)}")
    lines.append(f"repeat = {_toml_value(config.repeat)}")
    lines.append(f"sequence_len = {_toml_value(config.sequence_len)}")
    lines.append(f"sequence_seed = {_toml_value(config.sequence_seed)}")
    lines.append(f"require_strict_receipt = {_toml_value(config.require_strict_receipt)}")
    lines.append(f"allow_failed_verdicts = {_toml_value(config.allow_failed_verdicts)}")
    if config.reasoning_effort:
        lines.append(f"reasoning_effort = {_toml_value(config.reasoning_effort)}")

    if config.tasklist:
        lines.append(f"tasklist = {_toml_value(config.tasklist)}")
    if config.problems:
        lines.append(f"problems = {_toml_value(config.problems)}")
    if config.spec_names:
        lines.append(f"spec_names = {_toml_value(config.spec_names)}")

    lines.append("")
    lines.append("[runner.variants]")
    lines.append(f"enabled = {_toml_value(config.variants.enabled)}")
    lines.append(f"count = {_toml_value(config.variants.count)}")
    lines.append(f"offset = {_toml_value(config.variants.offset)}")
    lines.append(f"seed = {_toml_value(config.variants.seed)}")
    lines.append(f"order = {_toml_value(config.variants.order)}")
    if config.variants.max_per_class is not None:
        lines.append(f"max_per_class = {_toml_value(config.variants.max_per_class)}")
    if config.variants.consec_solves_to_stop is not None:
        lines.append(f"consec_solves_to_stop = {_toml_value(config.variants.consec_solves_to_stop)}")
    if config.variants.spec_names:
        lines.append(f"spec_names = {_toml_value(config.variants.spec_names)}")

    lines.append("")
    lines.append("[runner.env]")
    lines.append(f"judge_model_id = {_toml_value(config.env.judge_model_id)}")
    lines.append(f"worker_cpu_limit = {_toml_value(config.env.worker_cpu_limit)}")
    lines.append(f"reuse_cluster = {_toml_value(config.env.reuse_cluster)}")
    lines.append(f"force_recreate_cluster = {_toml_value(config.env.force_recreate_cluster)}")
    lines.append(f"preserve_infrastructure = {_toml_value(config.env.preserve_infrastructure)}")
    lines.append(f"submit_done_returns_feedback = {_toml_value(config.env.submit_done_returns_feedback)}")
    lines.append(f"defer_diagnosis_grading = {_toml_value(config.env.defer_diagnosis_grading)}")
    lines.append(f"lifecycle_validation_cache = {_toml_value(config.env.lifecycle_validation_cache)}")
    lines.append(f"source_build_cache = {_toml_value(config.env.source_build_cache)}")
    lines.append(f"fast_namespace_teardown = {_toml_value(config.env.fast_namespace_teardown)}")
    lines.append(f"cleanup_defer_timeout_seconds = {_toml_value(config.env.cleanup_defer_timeout_seconds)}")
    lines.append(f"docker_builder = {_toml_value(config.env.docker_builder)}")
    lines.append(f"kind_worker_nodes = {_toml_value(config.env.kind_worker_nodes)}")

    for agent_name, agent_cfg in config.agent_config.items():
        lines.append("")
        lines.append(f"[agent.{agent_name}]")
        for k, v in agent_cfg.items():
            lines.append(f"{k} = {_toml_value(v)}")

    lines.append("")
    return "\n".join(lines) + "\n"
