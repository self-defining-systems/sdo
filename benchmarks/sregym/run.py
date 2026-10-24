#!/usr/bin/env python3
"""Legacy SREGym experiment launcher; every launch requires --allow-non-fastloop.

Use benchmarks.sregym.fastloop for normal runs.

Supports both single experiments and multi-stage pipelines with
application-workspace chaining.

Usage:
    # New SDO experiment (a pipeline is auto-detected by [[stages]] in the TOML) and its
    # memoryless Codex baseline (see the example configs):
    uv run python -m benchmarks.sregym.run benchmarks/sregym/experiments/sdo_example.toml
    uv run python -m benchmarks.sregym.run benchmarks/sregym/experiments/codex_baseline_example.toml

    # Resume experiment or pipeline:
    uv run python -m benchmarks.sregym.run third_party/sregym/logs/<exp_or_pipeline_dir>/

    # Rerun a specific pipeline stage:
    uv run python -m benchmarks.sregym.run third_party/sregym/logs/<pipeline_dir>/ --stage 1

This script is a thin integration layer: launcher orchestration lives in
:mod:`benchmarks.sregym.runner`.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from benchmarks.sregym.runner import (
    has_pipeline_state,
    is_pipeline_config,
    load_pipeline_config,
    read_pipeline_snapshot,
    read_pipeline_state,
    reset_stages_for_rerun,
    run_pipeline,
    run_single_experiment,
    write_pipeline_state,
)

_PROJECT_ROOT = Path(__file__).resolve().parents[2]

_SREGYM_DIR = Path(os.environ.get("SDO_SREGYM_DIR", _PROJECT_ROOT / "third_party" / "sregym")).resolve()


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------


def _parse_stage_arg(args: list[str]) -> tuple[int | None, list[str]]:
    """Extract --stage N from args. Returns (stage_index, remaining_args)."""
    remaining: list[str] = []
    stage_index: int | None = None
    i = 0
    while i < len(args):
        if args[i] == "--stage" and i + 1 < len(args):
            stage_index = int(args[i + 1])
            i += 2
        else:
            remaining.append(args[i])
            i += 1
    return stage_index, remaining


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    if "--allow-non-fastloop" not in sys.argv[1:]:
        raise SystemExit(
            "Use benchmarks.sregym.fastloop for warm lanes; legacy live launches require --allow-non-fastloop"
        )
    sys.argv.remove("--allow-non-fastloop")
    if len(sys.argv) < 2:
        print(
            "Usage: run_sregym.py <config.toml | experiment_dir> [--stage N]",
            file=sys.stderr,
        )
        sys.exit(1)

    target = Path(sys.argv[1])
    extra_args = sys.argv[2:]
    stage_index, extra_args = _parse_stage_arg(extra_args)

    if target.is_dir():
        target = target.resolve()
        if has_pipeline_state(target):
            config = read_pipeline_snapshot(target)
            state = read_pipeline_state(target)
            if stage_index is not None:
                reset_stages_for_rerun(config, state, stage_index, target)
                write_pipeline_state(state, target)
                print(f"Resetting from stage {stage_index} for rerun.")
            sys.exit(
                run_pipeline(
                    config,
                    project_root=_PROJECT_ROOT,
                    sregym_dir=_SREGYM_DIR,
                    pipeline_dir=target,
                    state=state,
                )
            )
        else:
            if stage_index is not None:
                print(
                    "Error: --stage is only supported for pipeline directories.",
                    file=sys.stderr,
                )
                sys.exit(1)
            run_single_experiment(
                target,
                extra_args,
                project_root=_PROJECT_ROOT,
                sregym_dir=_SREGYM_DIR,
            )

    elif target.is_file() and target.suffix == ".toml":
        if stage_index is not None:
            print(
                "Error: --stage is only supported when resuming a pipeline directory.",
                file=sys.stderr,
            )
            sys.exit(1)
        if is_pipeline_config(target):
            config = load_pipeline_config(target)
            sys.exit(
                run_pipeline(
                    config,
                    project_root=_PROJECT_ROOT,
                    sregym_dir=_SREGYM_DIR,
                )
            )
        else:
            run_single_experiment(
                target,
                extra_args,
                project_root=_PROJECT_ROOT,
                sregym_dir=_SREGYM_DIR,
            )

    else:
        print(
            f"Error: '{target}' is neither a .toml config file nor an existing experiment directory.",
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
