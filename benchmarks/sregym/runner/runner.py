"""SREGym experiment + pipeline runner.

Pure orchestration — resolving configs, creating experiment directories,
translating :class:`ExperimentConfig` into CLI args + env vars for
``third_party/sregym/main.py``, and running pipelines with resume/rerun support.

This runner stays agent-agnostic.
"""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Literal, cast

from benchmarks.sregym.adapter import bind_state
from benchmarks.sregym.protocol import ProductionReceiptValidationError, validate_production_receipt
from benchmarks.sregym.protocol.infrastructure import has_injection_evidence, infrastructure_error
from benchmarks.sregym.runner.experiment import (
    ExperimentConfig,
    application_workspace_mode,
    config_to_env,
    config_to_main_args,
    persistent_controller_enabled,
    read_snapshot,
    resolve_config,
    resolve_tasklist,
    write_snapshot,
)
from benchmarks.sregym.runner.pipeline import (
    APP_WORKSPACE_SEED_ENV_VAR,
    PipelineConfig,
    PipelineState,
    StageState,
    initial_workspace_seed_env,
    merge_stage_config,
    reconcile_pipeline_state,
    write_pipeline_snapshot,
    write_pipeline_state,
)
from benchmarks.sregym.runner.preflight import LaunchAssurance, default_assurance

_APP_WORKSPACE_SEED_ENV_VAR = APP_WORKSPACE_SEED_ENV_VAR
_PERSISTENT_STATE_ENV_VAR = "SDO_PERSISTENT_CONTROLLER_STATE"
_PERSISTENT_STATE_FILENAME = "sdo_persistent_controller.json"


def _load_agent_hooks(agent_name: str, project_root: Path) -> tuple[str | None, str | None]:
    """Return (before_benchmark, after_benchmark) for *agent_name*."""
    import yaml

    agents_yaml = project_root / "benchmarks" / "sregym" / "registry.yaml"
    if not agents_yaml.exists():
        return None, None

    raw_data: object = yaml.safe_load(agents_yaml.read_text())
    if not isinstance(raw_data, dict):
        return None, None
    data = cast("dict[str, object]", raw_data)
    agents_raw = data.get("agents", [])
    if not isinstance(agents_raw, list):
        return None, None

    for raw_agent in cast("list[object]", agents_raw):
        if not isinstance(raw_agent, dict):
            continue
        agent = cast("dict[str, object]", raw_agent)
        if agent.get("name") == agent_name:
            before = agent.get("before_benchmark")
            after = agent.get("after_benchmark")
            return (
                before if isinstance(before, str) else None,
                after if isinstance(after, str) else None,
            )
    return None, None


def _run_hook(cmd: str, env: dict[str, str], label: str, project_root: Path) -> None:
    """Run a lifecycle hook shell command from the project root."""
    print(f"  {label}: {cmd}")
    result = subprocess.run(cmd, shell=True, cwd=str(project_root), env=env)  # noqa: S602
    if result.returncode != 0:
        print(f"  ⚠️  {label} exited with code {result.returncode}", flush=True)


# ---------------------------------------------------------------------------
# Single experiment
# ---------------------------------------------------------------------------


def _claim_run_dir(logs_root: Path, dir_name: str) -> Path:
    """Create a run directory no concurrent launch shares; a same-second clash gets a ``_2``, ``_3``... suffix."""

    logs_root.mkdir(parents=True, exist_ok=True)
    candidate = logs_root / dir_name
    attempt = 1
    while True:
        try:
            candidate.mkdir()  # atomic: exactly one launcher wins each name
            return candidate
        except FileExistsError:
            attempt += 1
            candidate = logs_root / f"{dir_name}_{attempt}"


def _create_experiment_dir(config: ExperimentConfig, sregym_dir: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return _claim_run_dir(sregym_dir / "logs", f"{timestamp}_{config.agent}")


def _print_experiment_info(config: ExperimentConfig, env: dict[str, str]) -> None:
    print(f"  agent={config.agent}  model={config.model}  parallel={config.parallel}")
    if config.variants.enabled:
        variants = config.variants
        print(f"  variants: count={variants.count}  offset={variants.offset}  seed={variants.seed}")
    elif config.tasklist:
        print(f"  tasklist={config.tasklist}")
    elif config.problems:
        print(f"  problems={config.problems}")
    agent_cfg_json = env.get("SREGYM_EXPERIMENT_AGENT_CONFIG", "")
    if agent_cfg_json:
        print(f"  agent config: {agent_cfg_json}")


def _verify_sregym(sregym_dir: Path) -> None:
    main_py = sregym_dir / "main.py"
    if not main_py.exists():
        print(
            f"Error: {main_py} not found. Is the sregym submodule checked out?",
            file=sys.stderr,
        )
        sys.exit(1)


def run_single_experiment(
    target: Path,
    extra_args: list[str],
    *,
    project_root: Path,
    sregym_dir: Path,
    assurance: LaunchAssurance | None = None,
) -> None:
    """Run or resume a single experiment; the launch preflight runs first and raises on a failure."""
    assurance = assurance or default_assurance()

    if target.is_dir():
        exp_dir = target.resolve()
        config = read_snapshot(exp_dir)
        config = resolve_config(config)
        report = assurance.preflight([config], project_root=project_root, sregym_dir=sregym_dir, env=dict(os.environ))
        snapshot = exp_dir / "experiment_config.toml"
        source: Path | None = None
        tasklist_path: Path | None = exp_dir / "tasklist.yml"
        if not tasklist_path.exists():
            tasklist_path = None
        print(f"Resuming experiment from: {exp_dir}")
    else:
        config = load_experiment_config_or_resolve(target)
        report = assurance.preflight([config], project_root=project_root, sregym_dir=sregym_dir, env=dict(os.environ))
        exp_dir = _create_experiment_dir(config, sregym_dir)
        snapshot = write_snapshot(config, exp_dir)
        source = target
        tasklist_path = resolve_tasklist(config, sregym_dir, exp_dir)
        print(f"New experiment: {exp_dir}")
    assurance.write_manifest(
        exp_dir,
        [config],
        snapshot=snapshot,
        report=report,
        project_root=project_root,
        sregym_dir=sregym_dir,
        env=dict(os.environ),
        kind="experiment",
        source=source,
    )

    bind_state(exp_dir / _PERSISTENT_STATE_FILENAME)
    _verify_sregym(sregym_dir)

    cli_args = config_to_main_args(config, exp_dir, tasklist_path)
    cli_args.extend(extra_args)
    env = config_to_env(config, project_root, exp_dir=exp_dir)

    _print_experiment_info(config, env)
    print()

    before_hook, after_hook = _load_agent_hooks(config.agent, project_root)
    if before_hook:
        _run_hook(before_hook, env, "before_benchmark", project_root)

    os.chdir(sregym_dir)
    argv = ["uv", "run", "main.py"] + cli_args
    print(f"  exec: {' '.join(argv)}")

    if after_hook:
        result = subprocess.run(argv, env=env)
        _run_hook(after_hook, env, "after_benchmark", project_root)
        sys.exit(result.returncode)

    os.execvpe("uv", argv, env)


def load_experiment_config_or_resolve(path: Path) -> ExperimentConfig:
    """Load a TOML experiment config and apply env-var overrides."""
    from benchmarks.sregym.runner.experiment import load_experiment_config

    return resolve_config(load_experiment_config(path))


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


def _create_pipeline_dir(config: PipelineConfig, sregym_dir: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = f"_{config.name}" if config.name else ""
    return _claim_run_dir(sregym_dir / "logs", f"{timestamp}_pipeline{suffix}")


def _run_stage(
    exp_config: ExperimentConfig,
    stage_exp_dir: Path,
    tasklist_path: Path | None,
    sregym_dir: Path,
    project_root: Path,
    extra_env: dict[str, str] | None = None,
) -> int:
    """Run one stage; a persistent-controller stage's strict receipt is validated after its drain."""

    cli_args = config_to_main_args(exp_config, stage_exp_dir, tasklist_path)
    env = config_to_env(exp_config, project_root, exp_dir=stage_exp_dir)
    if extra_env:
        env.update(extra_env)

    _print_experiment_info(exp_config, env)

    argv = ["uv", "run", "main.py"] + cli_args
    print(f"  exec: {' '.join(argv)}")
    print()
    result = subprocess.run(argv, cwd=str(sregym_dir), env=env)
    if result.returncode != 0:
        print(f"  ⚠️  Stage exited with code {result.returncode}", flush=True)
        if result.returncode < 0:
            import signal as _sig

            try:
                sig_name = _sig.Signals(-result.returncode).name
            except (ValueError, AttributeError):
                sig_name = f"signal {-result.returncode}"
            print(f"  ⚠️  Process was killed by {sig_name}", flush=True)
    if result.returncode == 0:
        stage_error = _stage_results_error(
            stage_exp_dir,
            require_strict_receipt=exp_config.require_strict_receipt and not persistent_controller_enabled(exp_config),
            allow_failed_verdicts=exp_config.allow_failed_verdicts,
        )
        if stage_error is not None:
            print(f"  ⚠️  Stage artifacts failed validation: {stage_error}", flush=True)
            _write_stage_failure(stage_exp_dir, stage_error)
            return STAGE_ARTIFACTS_FAILED
    return result.returncode


#: ``_run_stage`` result when the benchmark exited cleanly but its artifacts
#: failed validation (an oracle failed, or no valid receipt): an agent outcome.
STAGE_ARTIFACTS_FAILED = 2
_STAGE_FAILURE_FILENAME = "stage_failure.txt"


def _write_stage_failure(stage_exp_dir: Path, error: str) -> None:
    (stage_exp_dir / _STAGE_FAILURE_FILENAME).write_text(error + "\n", encoding="utf-8")


def _read_stage_failure(stage_exp_dir: Path) -> str:
    path = stage_exp_dir / _STAGE_FAILURE_FILENAME
    return path.read_text(encoding="utf-8").strip() if path.is_file() else ""


def _csv_semantic_graded(row: dict[str, str | None], stage: str) -> bool:
    """Whether a row carries an explicit true/false grade for ``stage`` (not blank, absent or malformed)."""

    flattened = f"{stage}.success"
    raw: object
    if flattened in row:
        raw = row[flattened]
    elif stage in row and isinstance(row[stage], str):
        try:
            parsed: object = json.loads(str(row[stage]))
        except json.JSONDecodeError:
            return False
        if not isinstance(parsed, dict) or "success" not in parsed:
            return False
        raw = cast("dict[str, object]", parsed)["success"]
    else:
        return False
    if isinstance(raw, bool):
        return True
    return isinstance(raw, str) and raw.strip().lower() in {"true", "false", "1", "0", "yes", "no"}


def _csv_semantic_success(row: dict[str, str | None], stage: str) -> bool:
    """Return whether a benchmark row explicitly records a successful stage.

    SREGym's current writer flattens conductor outcomes into columns such as
    ``Diagnosis.success``.  Older/external harnesses can preserve the outcome
    as a JSON object in a ``Diagnosis`` column instead.  Only explicit boolean
    truth is accepted; absent, blank, malformed, or contradictory values must
    never turn an incomplete benchmark row into a successful pipeline stage.
    """

    flattened = f"{stage}.success"
    if flattened in row:
        raw: object = row[flattened]
    elif stage in row:
        nested_raw = row[stage]
        if not isinstance(nested_raw, str) or not nested_raw.strip():
            return False
        try:
            parsed: object = json.loads(nested_raw)
        except json.JSONDecodeError:
            return False
        if not isinstance(parsed, dict) or "success" not in parsed:
            return False
        nested = cast("dict[str, object]", parsed)
        raw = nested["success"]
    else:
        return False

    if isinstance(raw, bool):
        return raw
    if isinstance(raw, int):
        return raw == 1
    if not isinstance(raw, str):
        return False
    return raw.strip().lower() in {"true", "1", "yes"}


def _strict_receipt_error(receipt_path: Path) -> str | None:
    """Parse and validate one SDO receipt at the production schema boundary."""

    try:
        parsed_document: object = json.loads(receipt_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return f"strict receipt is unreadable or malformed: {exc}"
    if not isinstance(parsed_document, dict):
        return "strict receipt must contain one JSON object"
    document = cast("dict[str, Any]", parsed_document)

    try:
        validate_production_receipt(document)
    except ProductionReceiptValidationError as exc:
        return f"strict receipt failed production validation: {exc}"
    return None


def _stage_result_paths(stage_exp_dir: Path) -> list[Path]:
    results = sorted((stage_exp_dir / "problem_runs").glob("*/results_*.csv"))
    results.extend(sorted((stage_exp_dir / "runs").glob("*/worker_*/results/*/*/run_*/*_results.csv")))
    # Invalid injections finish before a run_<attempt> directory exists.
    results.extend(sorted((stage_exp_dir / "runs").glob("*/worker_*/results/*/*/*_results.csv")))
    return results


def _stage_infrastructure_outcomes(stage_exp_dir: Path) -> list[str]:
    outcomes: list[str] = []
    for result in _stage_result_paths(stage_exp_dir):
        with result.open(newline="", encoding="utf-8") as stream:
            for row in csv.DictReader(stream):
                reason = infrastructure_error(row)
                if reason:
                    outcomes.append(f"{row.get('problem_id') or result.parent.name}: {reason}")
    return outcomes


def _stage_results_error(
    stage_exp_dir: Path, *, require_strict_receipt: bool = False, allow_failed_verdicts: bool = False
) -> str | None:
    """Reject benchmark-zero stages without complete, valid per-problem evidence."""
    results = _stage_result_paths(stage_exp_dir)
    if not results:
        return "no per-problem result CSV was produced"
    for result in results:
        with result.open(newline="", encoding="utf-8") as stream:
            rows = list(csv.DictReader(stream))
        if not rows:
            return f"{result.name} contains no result rows"
        graded_rows: list[dict[str, str | None]] = []
        for row in rows:
            reason = infrastructure_error(row)
            if reason:
                if reason == "injection_invalid" and not has_injection_evidence(row):
                    return f"problem {row.get('problem_id')} injection_invalid has no injection evidence"
                continue
            graded_rows.append(row)
            if str(row.get("agent_error", "")).strip().lower() in {"1", "true", "yes"}:
                problem = row.get("problem_id") or result.parent.name
                return f"problem {problem} recorded agent_error=true"
            for stage in ("Diagnosis", "Mitigation"):
                accepted = (
                    _csv_semantic_graded(row, stage) if allow_failed_verdicts else _csv_semantic_success(row, stage)
                )
                if not accepted:
                    problem = row.get("problem_id") or result.parent.name
                    return f"problem {problem} requires {stage}.success=true"
        if require_strict_receipt and graded_rows:
            receipt_dir = result.parent if result.parent.name.startswith("run_") else result.parent / "agent"
            problem = result.parent.name
            receipts = sorted(receipt_dir.glob("sdo_production_receipt_*.json"))
            expected = receipt_dir / "sdo_production_receipt_strict.json"
            if not receipts:
                return f"problem {problem} produced no strict receipt"
            if receipts != [expected]:
                names = ", ".join(path.name for path in receipts)
                return f"problem {problem} must produce exactly one standalone strict receipt, found {names}"
            receipt_error = _strict_receipt_error(expected)
            if receipt_error is not None:
                return f"problem {result.parent.name} {receipt_error}"
    return None


def _resolve_workspace_seed_env(
    *,
    current_stage: int,
    state: PipelineState,
    exp_config: ExperimentConfig,
) -> dict[str, str]:
    """Return env vars needed to seed a stage-local application workspace."""
    if current_stage <= 0:
        raise ValueError("chain_application_workspace requires a previous stage to copy from")
    if application_workspace_mode(exp_config.application_workspace) != "persistent":
        raise ValueError("chain_application_workspace requires application_workspace = 'persistent' for the stage")

    prev_state = state.stages[current_stage - 1]
    if not prev_state.experiment_dir:
        raise FileNotFoundError("Previous stage has no experiment directory to copy application workspace from")

    prev_stage_dir = Path(prev_state.experiment_dir)
    prev_workspace_dir = prev_stage_dir / "application_workspace"
    if not prev_workspace_dir.is_dir():
        raise FileNotFoundError(f"Previous stage application workspace is missing: {prev_workspace_dir}")

    prev_config = read_snapshot(prev_stage_dir)
    if application_workspace_mode(prev_config.application_workspace) != "persistent":
        raise ValueError(
            "chain_application_workspace requires the previous stage to enable application_workspace = 'persistent'"
        )
    if prev_config.app_filter != exp_config.app_filter:
        raise ValueError(
            "chain_application_workspace requires matching app_filter values between consecutive stages "
            f"(previous={prev_config.app_filter!r}, current={exp_config.app_filter!r})"
        )

    current_exp_dir = state.stages[current_stage].experiment_dir
    if current_exp_dir:
        pipeline_dir = Path(current_exp_dir).parent
        lifecycle_seed = pipeline_dir / f"lifecycle_seed_stage{current_stage}"
        if (lifecycle_seed / ".git").exists() and (lifecycle_seed / ".sdo" / "lifecycle-provenance.yaml").is_file():
            return {_APP_WORKSPACE_SEED_ENV_VAR: str(lifecycle_seed)}

    return {_APP_WORKSPACE_SEED_ENV_VAR: str(prev_workspace_dir)}


def _teardown_persistent_controllers(state_path: Path, project_root: Path, env: dict[str, str]) -> str | None:
    """Drain the last incidents, publish every deferred strict receipt, and stop every persistent controller."""

    if not state_path.exists():
        return None
    argv = [
        "uv",
        "run",
        "python",
        "-m",
        "benchmarks.sregym.adapter.persistent",
        "teardown",
        "--state",
        str(state_path),
        "--publish-root",
        str(state_path.parent),
    ]
    print(f"  persistent controller teardown: {' '.join(argv)}", flush=True)
    result = subprocess.run(argv, cwd=str(project_root), env=env)
    if result.returncode != 0:
        return f"persistent controller teardown exited with code {result.returncode}"
    return None


def run_pipeline(
    config: PipelineConfig,
    *,
    project_root: Path,
    sregym_dir: Path,
    pipeline_dir: Path | None = None,
    state: PipelineState | None = None,
    assurance: LaunchAssurance | None = None,
) -> int:
    """Run a multi-stage pipeline with application-workspace chaining.

    The launch preflight covers every stage and runs before the pipeline
    directory exists; a failure raises :class:`PreflightError`.
    """
    assurance = assurance or default_assurance()

    _verify_sregym(sregym_dir)
    stage_configs = [
        resolve_config(merge_stage_config(config.defaults, stage.runner_overrides)) for stage in config.stages
    ]
    report = assurance.preflight(stage_configs, project_root=project_root, sregym_dir=sregym_dir, env=dict(os.environ))

    if pipeline_dir is None:
        pipeline_dir = _create_pipeline_dir(config, sregym_dir)
        write_pipeline_snapshot(config, pipeline_dir)
        state = PipelineState(
            stages=[StageState(index=i, name=s.name, status="pending") for i, s in enumerate(config.stages)]
        )
        write_pipeline_state(state, pipeline_dir)
        print(f"New pipeline: {pipeline_dir}")
    else:
        print(f"Resuming pipeline from: {pipeline_dir}")

    assert state is not None
    reconcile_pipeline_state(config, state)
    write_pipeline_state(state, pipeline_dir)

    def write_manifest(
        run_dir: Path, configs: list[ExperimentConfig], snapshot: Path, kind: Literal["pipeline", "stage"]
    ) -> None:
        assurance.write_manifest(
            run_dir,
            configs,
            snapshot=snapshot,
            report=report,
            project_root=project_root,
            sregym_dir=sregym_dir,
            env=dict(os.environ),
            kind=kind,
        )

    write_manifest(pipeline_dir, stage_configs, pipeline_dir / "pipeline_config.toml", "pipeline")

    print(f"  stages: {len(config.stages)}")
    print()

    before_hook = after_hook = None
    hook_env: dict[str, str] = dict(os.environ)
    if config.stages:
        hook_exp = merge_stage_config(config.defaults, {})
        hook_exp = resolve_config(hook_exp)
        hook_env = config_to_env(hook_exp, project_root)
        before_hook, after_hook = _load_agent_hooks(hook_exp.agent, project_root)

    persistent_state = pipeline_dir / _PERSISTENT_STATE_FILENAME
    bind_state(persistent_state)
    if before_hook:
        _run_hook(before_hook, hook_env, "before_benchmark", project_root)
    # Stages whose strict receipts are written by the next stage's drain or by teardown.
    deferred_receipt_stages: list[tuple[int, Path]] = []
    pipeline_succeeded = False

    try:
        for i, stage_cfg in enumerate(config.stages):
            stage_state = state.stages[i]

            if stage_state.status in ("completed", "agent_failure", "invalid_infra"):
                print(
                    f"Stage {i}/{len(config.stages) - 1}: "
                    f"{stage_cfg.name or f'stage_{i}'} [skipped — already {stage_state.status}]"
                )
                continue

            exp_config = merge_stage_config(config.defaults, stage_cfg.runner_overrides)
            exp_config = resolve_config(exp_config)

            stage_name = stage_cfg.name or f"stage_{i}"
            stage_exp_dir = pipeline_dir / f"stage_{i}_{stage_name}"
            stage_exp_dir.mkdir(parents=True, exist_ok=True)

            stage_snapshot = write_snapshot(exp_config, stage_exp_dir)
            tasklist_path = resolve_tasklist(exp_config, sregym_dir, stage_exp_dir)
            write_manifest(stage_exp_dir, [exp_config], stage_snapshot, "stage")

            stage_state.status = "running"
            stage_state.experiment_dir = str(stage_exp_dir)
            write_pipeline_state(state, pipeline_dir)

            print("=" * 60)
            print(f"Stage {i}/{len(config.stages) - 1}: {stage_name}")
            stage_extra_env: dict[str, str] = {}
            try:
                if i == 0 and not stage_cfg.chain_application_workspace:
                    stage_extra_env = initial_workspace_seed_env(config)
                    if stage_extra_env:
                        print(f"  Application workspace seed: {stage_extra_env[_APP_WORKSPACE_SEED_ENV_VAR]}")
                if stage_cfg.chain_application_workspace:
                    stage_extra_env = _resolve_workspace_seed_env(
                        current_stage=i,
                        state=state,
                        exp_config=exp_config,
                    )
                    print(f"  Application workspace seed: {stage_extra_env[_APP_WORKSPACE_SEED_ENV_VAR]}")
            except Exception as exc:
                stage_state.status = "failed"
                stage_state.error = str(exc)
                write_pipeline_state(state, pipeline_dir)
                print(f"\nStage {i} failed before launch: {exc}")
                print(f"Resume with: run_sregym.sh {pipeline_dir}")
                return 1
            if persistent_controller_enabled(exp_config):
                stage_extra_env[_PERSISTENT_STATE_ENV_VAR] = str(persistent_state)
                if exp_config.require_strict_receipt:
                    deferred_receipt_stages.append((i, stage_exp_dir))
            print("=" * 60)

            try:
                returncode = _run_stage(
                    exp_config,
                    stage_exp_dir,
                    tasklist_path,
                    sregym_dir,
                    project_root,
                    stage_extra_env,
                )
            except KeyboardInterrupt:
                print(f"\nInterrupted during stage {i}. Saving state for resume.")
                stage_state.status = "failed"
                stage_state.error = "interrupted"
                write_pipeline_state(state, pipeline_dir)
                print(f"Resume with: run_sregym.sh {pipeline_dir}")
                return 1

            if returncode == STAGE_ARTIFACTS_FAILED and config.continue_on_agent_failure:
                stage_state.status = "agent_failure"
                stage_state.error = _read_stage_failure(stage_exp_dir) or "stage artifacts failed validation"
                write_pipeline_state(state, pipeline_dir)
                if persistent_state.exists():
                    # Same state an abort plus resume leaves: the next stage
                    # installs a fresh controller over the chained workspace.
                    _teardown_persistent_controllers(persistent_state, project_root, hook_env)
                print(f"\nStage {i} agent failure ({stage_state.error}); continuing the pipeline.\n")
                continue

            if returncode != 0:
                stage_state.status = "failed"
                stage_state.error = f"exit code {returncode}"
                write_pipeline_state(state, pipeline_dir)
                print(f"\nStage {i} failed (exit code {returncode}). Pipeline aborted.")
                print(f"Resume with: run_sregym.sh {pipeline_dir}")
                return 1

            infrastructure = _stage_infrastructure_outcomes(stage_exp_dir)
            stage_state.status = "invalid_infra" if infrastructure else "completed"
            stage_state.error = "; ".join(infrastructure)
            write_pipeline_state(state, pipeline_dir)
            print(f"\nStage {i} {stage_state.status}.\n")

        if deferred_receipt_stages or persistent_state.exists():
            teardown_error = _teardown_persistent_controllers(persistent_state, project_root, hook_env)
            for index, stage_dir in deferred_receipt_stages:
                if state.stages[index].status == "agent_failure":
                    continue  # already an agent outcome; its receipt is not expected to validate
                # Each stage is judged by its own published receipt; a teardown
                # failure is reported with the stages it left without one.
                receipt_error = _stage_results_error(
                    stage_dir,
                    require_strict_receipt=True,
                    allow_failed_verdicts=merge_stage_config(
                        config.defaults, config.stages[index].runner_overrides
                    ).allow_failed_verdicts,
                )
                if receipt_error is not None and teardown_error is not None:
                    receipt_error = f"{receipt_error} ({teardown_error})"
                if receipt_error is not None and config.continue_on_agent_failure and teardown_error is None:
                    # A rejected receipt is that stage's own agent outcome; the
                    # other stages' evidence stands and the teardown itself succeeded.
                    state.stages[index].status = "agent_failure"
                    state.stages[index].error = f"deferred strict receipt: {receipt_error}"
                    write_pipeline_state(state, pipeline_dir)
                    print(f"\nStage {index} agent failure after the persistent controller drain: {receipt_error}")
                    continue
                if receipt_error is not None:
                    state.stages[index].status = "failed"
                    state.stages[index].error = f"deferred strict receipt: {receipt_error}"
                    write_pipeline_state(state, pipeline_dir)
                    print(f"\nStage {index} failed after the persistent controller drain: {receipt_error}")
                    return 1
        pipeline_succeeded = True
        print("=" * 60)
        print("Pipeline completed successfully.")
        print(f"  directory: {pipeline_dir}")
        print("=" * 60)
        return 0
    finally:
        if not pipeline_succeeded and persistent_state.exists():
            # Never leave a controller running after an aborted pipeline.
            _teardown_persistent_controllers(persistent_state, project_root, hook_env)
        if after_hook:
            _run_hook(after_hook, hook_env, "after_benchmark", project_root)
