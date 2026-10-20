"""One-shot SREGym diagnosis grading using its own oracle and prompt."""

from __future__ import annotations

import contextlib
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable


class MeteredBackend:
    """Count usage exposed by SREGym's non-Codex judge backends."""

    def __init__(self, backend: Any) -> None:
        self.backend = backend
        self.usage = {"input_tokens": 0, "cached_input_tokens": 0, "output_tokens": 0}
        self.lock = threading.Lock()

    @property
    def temperature(self) -> float | None:
        return getattr(self.backend, "temperature", None)

    @temperature.setter
    def temperature(self, value: float | None) -> None:
        if hasattr(self.backend, "temperature"):
            self.backend.temperature = value

    def inference(self, messages: Any, system_prompt: str | None = None, tools: list | None = None) -> Any:
        response = self.backend.inference(messages, system_prompt=system_prompt, tools=tools)
        usage = getattr(response, "usage_metadata", None)
        if isinstance(usage, dict):
            details = usage.get("input_token_details") or {}
            with self.lock:
                self.usage["input_tokens"] += int(usage.get("input_tokens") or 0)
                self.usage["cached_input_tokens"] += int(details.get("cache_read") or 0)
                self.usage["output_tokens"] += int(usage.get("output_tokens") or 0)
        return response


def diagnosis_problem(problem_ids: tuple[str, ...], *, factory: Callable[[str], Any], composite: type) -> Any:
    """Match the conductor: explicit faults form one composite diagnosis problem."""

    problems = [factory(problem_id) for problem_id in problem_ids]
    return composite(problems) if len(problems) > 1 else problems[0]


def grade(problem_ids: tuple[str, ...], diagnosis: str) -> dict[str, Any]:
    from langchain_core.messages import AIMessage
    from llm_backend.codex_cli_backend import CodexCLIBackend
    from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle
    from sregym.conductor.problems.multiple_failures import MultipleIndependentFailures
    from sregym.conductor.problems.registry import ProblemRegistry

    class MeteredCodexBackend(CodexCLIBackend):
        def __init__(self, model: str, reasoning_effort: str) -> None:
            super().__init__(model, reasoning_effort)
            self.usage = {"input_tokens": 0, "cached_input_tokens": 0, "output_tokens": 0}
            self.lock = threading.Lock()

        def inference(self, messages: Any, system_prompt: str | None = None, tools: list | None = None) -> AIMessage:
            if tools:
                raise ValueError("Codex judge backend does not support tool binding")
            with tempfile.TemporaryDirectory(prefix="sregym-codex-judge-") as workdir:
                output = Path(workdir) / "last-message.txt"
                completed = subprocess.run(
                    [
                        "codex",
                        "exec",
                        "--json",
                        "--model",
                        self.model,
                        "-c",
                        f"model_reasoning_effort={self.reasoning_effort}",
                        "--sandbox",
                        "read-only",
                        "--ephemeral",
                        "--skip-git-repo-check",
                        "--cd",
                        workdir,
                        "--output-last-message",
                        str(output),
                        "-",
                    ],
                    input=self._prompt(messages, system_prompt),
                    capture_output=True,
                    text=True,
                    timeout=self.timeout_seconds,
                    check=False,
                )
                if completed.returncode != 0:
                    raise RuntimeError(f"codex judge exited {completed.returncode}: {completed.stderr[-2000:]}")
                answer = output.read_text(encoding="utf-8").strip()
            if not answer:
                raise RuntimeError("codex judge returned no final message")
            with self.lock:
                for line in completed.stdout.splitlines():
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if event.get("type") == "turn.completed":
                        usage = event.get("usage") or {}
                        for key in self.usage:
                            self.usage[key] += int(usage.get(key) or 0)
            return AIMessage(content=answer)

    started = time.monotonic()
    registry = ProblemRegistry()

    def factory(problem_id: str) -> Any:
        constructor = registry.get_problem(problem_id)
        if constructor is None:
            raise ValueError(f"Problem ID {problem_id} not found in registry")
        return constructor()

    problem = diagnosis_problem(problem_ids, factory=factory, composite=MultipleIndependentFailures)
    oracle = problem.diagnosis_oracle
    if not isinstance(oracle, LLMAsAJudgeOracle):
        raise RuntimeError("problem has no SREGym LLM diagnosis oracle")
    backend = oracle.judge.backend
    metered: Any = None
    if isinstance(backend, CodexCLIBackend):
        metered = MeteredCodexBackend(backend.model, backend.reasoning_effort)
    elif backend is not None:
        metered = MeteredBackend(backend)
    if metered is not None:
        oracle.judge._backend = metered
    result = oracle.evaluate(diagnosis)
    return {
        "success": result.get("success"),
        "accuracy": result.get("accuracy"),
        "details": result,
        "judge_tokens": metered.usage if metered else {},
        "judge_model_id": os.environ.get("JUDGE_MODEL_ID"),
        "latency_seconds": time.monotonic() - started,
        "error": result.get("error") or ("judge returned no verdict" if result.get("success") is None else None),
    }


def main() -> None:
    request = json.load(sys.stdin)
    with contextlib.redirect_stdout(sys.stderr):
        result = grade(tuple(request["problem_ids"]), str(request["diagnosis"]))
    print(json.dumps(result))


if __name__ == "__main__":
    main()
