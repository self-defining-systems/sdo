"""Run the conductor's diagnosis oracle in SREGym's own Python environment."""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

from benchmarks.sregym.fastloop.records import DiagnosisGrade


@dataclass(frozen=True)
class SregymDiagnosisGrader:
    sregym_dir: Path
    judge_model_id: str
    timeout_seconds: float = 1800.0

    def grade(self, problem_ids: tuple[str, ...], diagnosis: str) -> DiagnosisGrade:
        root = Path(__file__).resolve().parents[3]
        env = {
            **os.environ,
            "PYTHONPATH": os.pathsep.join(filter(None, (str(root), os.environ.get("PYTHONPATH", "")))),
            "JUDGE_MODEL_ID": self.judge_model_id,
            "JUDGE_NUM_ROUNDS": "3",
        }
        completed = subprocess.run(
            [
                "uv",
                "run",
                "--directory",
                str(self.sregym_dir),
                "python",
                "-m",
                "benchmarks.sregym.fastloop.grade_worker",
            ],
            input=json.dumps({"problem_ids": problem_ids, "diagnosis": diagnosis}),
            capture_output=True,
            text=True,
            env=env,
            timeout=self.timeout_seconds,
            check=False,
        )
        if completed.returncode != 0:
            raise RuntimeError(f"SREGym diagnosis judge failed: {(completed.stderr or completed.stdout)[-2000:]}")
        return DiagnosisGrade.model_validate_json(completed.stdout)
