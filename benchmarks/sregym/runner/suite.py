"""Resolve runner selection in the harness subprocess before production launch."""

from __future__ import annotations

import dataclasses
import json
import os
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

from benchmarks.sregym.faults.description import ProblemDescription

if TYPE_CHECKING:
    from benchmarks.sregym.runner.experiment import ExperimentConfig


def describe_suite(config: ExperimentConfig, sregym_dir: Path) -> dict[str, ProblemDescription]:
    selection = dataclasses.asdict(config)
    if config.tasklist:
        from benchmarks.sregym.runner.experiment import resolve_tasklist_source

        selection["tasklist"] = str(resolve_tasklist_source(config.tasklist, sregym_dir).resolve())
    env = {**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[3])}
    reply = subprocess.run(
        [
            "uv",
            "run",
            "--directory",
            str(sregym_dir),
            "python",
            "-m",
            "benchmarks.sregym.faults.worker",
        ],
        input=json.dumps({"selection": selection}),
        capture_output=True,
        text=True,
        env=env,
        timeout=180,
        check=False,
    )
    if reply.returncode:
        raise ValueError(f"suite discovery failed: {(reply.stderr or reply.stdout)[-2000:]}")
    return {key: ProblemDescription.model_validate(value) for key, value in json.loads(reply.stdout).items()}
