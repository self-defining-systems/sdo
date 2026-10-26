"""Resolve runner selection in the harness subprocess before production launch."""

from __future__ import annotations

import dataclasses
import json
import os
from pathlib import Path
from typing import TYPE_CHECKING

from benchmarks.sregym.faults.description import FaultDescriptionError, ProblemDescription
from benchmarks.sregym.faults.sregym_subprocess import run_sregym_json, sregym_env

if TYPE_CHECKING:
    from benchmarks.sregym.runner.experiment import ExperimentConfig


def describe_suite(config: ExperimentConfig, sregym_dir: Path) -> dict[str, ProblemDescription]:
    selection = dataclasses.asdict(config)
    if config.tasklist:
        from benchmarks.sregym.runner.experiment import resolve_tasklist_source

        selection["tasklist"] = str(resolve_tasklist_source(config.tasklist, sregym_dir).resolve())
    env = sregym_env(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[3]))
    reply = run_sregym_json(
        "suite discovery",
        ["uv", "run", "--directory", str(sregym_dir), "python", "-m", "benchmarks.sregym.faults.worker"],
        input_text=json.dumps({"selection": selection}),
        env=env,
        timeout=180,
        error=FaultDescriptionError,
    )
    return {key: ProblemDescription.model_validate(value) for key, value in reply.items()}
