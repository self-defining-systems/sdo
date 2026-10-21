from __future__ import annotations

import json
from typing import TYPE_CHECKING

from benchmarks.sregym.fastloop.grade_worker import MeteredBackend, diagnosis_problem
from benchmarks.sregym.fastloop.grading import SregymDiagnosisGrader

if TYPE_CHECKING:
    from pathlib import Path


def test_explicit_composite_uses_one_whole_set_diagnosis_oracle() -> None:
    grades: list[str] = []

    class Oracle:
        def evaluate(self, diagnosis: str) -> dict[str, bool]:
            grades.append(diagnosis)
            return {"success": True}

    class Problem:
        diagnosis_oracle = Oracle()

    class Composite:
        def __init__(self, problems: list[Problem]) -> None:
            self.problems = problems
            self.diagnosis_oracle = Oracle()

    seen: list[str] = []

    def factory(problem_id: str) -> Problem:
        seen.append(problem_id)
        return Problem()

    problem = diagnosis_problem(("port", "image"), factory=factory, composite=Composite)
    assert seen == ["port", "image"]
    assert isinstance(problem, Composite)
    assert len(problem.problems) == 2
    assert problem.diagnosis_oracle.evaluate("both root causes") == {"success": True}
    assert grades == ["both root causes"]


def test_grader_sends_diagnosis_and_problem_ids_to_sregym_judge(monkeypatch, tmp_path: Path) -> None:
    from benchmarks.sregym.fastloop import grading

    seen: list[dict[str, object]] = []

    def run(argv: list[str], **kwargs: object) -> object:
        seen.append({"argv": argv, **kwargs})
        return type(
            "Completed",
            (),
            {
                "returncode": 0,
                "stdout": json.dumps(
                    {"success": True, "accuracy": 100, "judge_tokens": {"input_tokens": 13}, "latency_seconds": 1.2}
                ),
            },
        )()

    monkeypatch.setattr(grading.subprocess, "run", run)
    result = SregymDiagnosisGrader(tmp_path, "codex-gpt-6-luna").grade(("port", "image"), "both causes")

    assert json.loads(seen[0]["input"]) == {"problem_ids": ["port", "image"], "diagnosis": "both causes"}
    assert seen[0]["env"]["JUDGE_MODEL_ID"] == "codex-gpt-6-luna"
    assert seen[0]["env"]["JUDGE_NUM_ROUNDS"] == "3"
    assert result.success is True
    assert result.judge_tokens.input_tokens == 13


def test_non_codex_judge_usage_is_recorded_from_response_metadata() -> None:
    class Backend:
        temperature = 0.0

        def inference(self, messages: object, system_prompt: str | None = None, tools: list | None = None) -> object:
            return type(
                "Response",
                (),
                {
                    "usage_metadata": {"input_tokens": 8, "output_tokens": 2, "input_token_details": {"cache_read": 3}},
                },
            )()

    backend = MeteredBackend(Backend())
    backend.temperature = 0.7
    backend.inference("diagnosis")

    assert backend.temperature == 0.7
    assert backend.usage == {"input_tokens": 8, "cached_input_tokens": 3, "output_tokens": 2}
