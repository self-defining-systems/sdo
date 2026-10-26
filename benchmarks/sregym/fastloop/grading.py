"""Run the conductor's diagnosis oracle in SREGym's own Python environment."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

from benchmarks.sregym.fastloop.records import DiagnosisGrade
from benchmarks.sregym.faults.sregym_subprocess import SregymSubprocessError, run_sregym_json, sregym_env


class DiagnosisJudgeError(SregymSubprocessError):
    """The SREGym diagnosis judge produced no verdict; `cause` is why."""


@dataclass(frozen=True)
class SregymDiagnosisGrader:
    sregym_dir: Path
    judge_model_id: str
    timeout_seconds: float = 1800.0

    def grade(self, problem_ids: tuple[str, ...], diagnosis: str) -> DiagnosisGrade:
        root = Path(__file__).resolve().parents[3]
        env = sregym_env(
            os.environ,
            PYTHONPATH=os.pathsep.join(filter(None, (str(root), os.environ.get("PYTHONPATH", "")))),
            JUDGE_MODEL_ID=self.judge_model_id,
            JUDGE_NUM_ROUNDS="3",
        )
        result = run_sregym_json(
            "diagnosis judge",
            [
                "uv",
                "run",
                "--directory",
                str(self.sregym_dir),
                "python",
                "-m",
                "benchmarks.sregym.fastloop.grade_worker",
            ],
            input_text=json.dumps({"problem_ids": problem_ids, "diagnosis": diagnosis}),
            env=env,
            timeout=self.timeout_seconds,
            error=DiagnosisJudgeError,
        )
        return DiagnosisGrade.model_validate(result)
