"""Every `uv run` into SREGym's environment must treat uv warnings as warnings."""

from __future__ import annotations

import json
import subprocess
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.sregym.fastloop.grading import DiagnosisJudgeError, SregymDiagnosisGrader
from benchmarks.sregym.faults import sregym_subprocess
from benchmarks.sregym.faults.sregym_subprocess import (
    SregymSubprocessError,
    run_sregym_json,
    sregym_env,
    stderr_without_warnings,
)

if TYPE_CHECKING:
    from pathlib import Path

VIRTUAL_ENV_WARNING = (
    "warning: `VIRTUAL_ENV=/work/p4run/.venv` does not match the project environment path `.venv` "
    "and will be ignored; use `--active` to target the active environment instead"
)
GRADE = {"success": True, "accuracy": 100, "judge_tokens": {"input_tokens": 13}, "latency_seconds": 1.2}


def _completed(returncode: int, stdout: str, stderr: str) -> Any:
    return type("Completed", (), {"returncode": returncode, "stdout": stdout, "stderr": stderr})()


def _grade(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, completed: Any) -> Any:
    monkeypatch.setattr(sregym_subprocess.subprocess, "run", lambda argv, **kwargs: completed)
    return SregymDiagnosisGrader(tmp_path, "codex-gpt-6-luna").grade(("port",), "cause")


def test_uv_warning_on_stderr_does_not_fail_a_successful_judge(monkeypatch, tmp_path: Path) -> None:
    result = _grade(monkeypatch, tmp_path, _completed(0, json.dumps(GRADE), VIRTUAL_ENV_WARNING + "\n"))
    assert result.success is True


def test_uv_warning_or_log_noise_before_the_result_line_is_ignored(monkeypatch, tmp_path: Path) -> None:
    stdout = "uv: resolving\n" + json.dumps(GRADE) + "\n"
    assert _grade(monkeypatch, tmp_path, _completed(0, stdout, VIRTUAL_ENV_WARNING)).success is True


def test_failed_judge_reports_its_traceback_not_the_uv_warning(monkeypatch, tmp_path: Path) -> None:
    stderr = VIRTUAL_ENV_WARNING + "\nTraceback (most recent call last):\nRuntimeError: codex judge exited 1: quota\n"
    with pytest.raises(DiagnosisJudgeError) as caught:
        _grade(monkeypatch, tmp_path, _completed(1, "", stderr))
    assert caught.value.cause == "exit"
    assert "quota" in str(caught.value)
    assert "VIRTUAL_ENV" not in str(caught.value)


def test_judge_that_only_warned_is_typed_as_no_result(monkeypatch, tmp_path: Path) -> None:
    with pytest.raises(DiagnosisJudgeError) as caught:
        _grade(monkeypatch, tmp_path, _completed(0, "", VIRTUAL_ENV_WARNING))
    assert caught.value.cause == "no_result"
    assert "VIRTUAL_ENV" not in str(caught.value)


def test_judge_timeout_is_typed(monkeypatch, tmp_path: Path) -> None:
    def run(argv: list[str], **kwargs: object) -> object:
        raise subprocess.TimeoutExpired(argv, 1.0)

    monkeypatch.setattr(sregym_subprocess.subprocess, "run", run)
    with pytest.raises(DiagnosisJudgeError) as caught:
        SregymDiagnosisGrader(tmp_path, "codex-gpt-6-luna").grade(("port",), "cause")
    assert caught.value.cause == "timeout"


def test_environment_handed_to_sregym_drops_the_inherited_virtualenv() -> None:
    env = sregym_env({"VIRTUAL_ENV": "/elsewhere/.venv", "PATH": "/bin"}, JUDGE_MODEL_ID="m")
    assert "VIRTUAL_ENV" not in env
    assert env["PATH"] == "/bin"
    assert env["JUDGE_MODEL_ID"] == "m"


def test_grader_runs_sregym_without_the_inherited_virtualenv(monkeypatch, tmp_path: Path) -> None:
    seen: list[dict[str, str]] = []

    def run(argv: list[str], **kwargs: Any) -> Any:
        seen.append(kwargs["env"])
        return _completed(0, json.dumps(GRADE), "")

    monkeypatch.setenv("VIRTUAL_ENV", "/elsewhere/.venv")
    monkeypatch.setattr(sregym_subprocess.subprocess, "run", run)
    SregymDiagnosisGrader(tmp_path, "codex-gpt-6-luna").grade(("port",), "cause")
    assert "VIRTUAL_ENV" not in seen[0]


def test_warning_filter_keeps_every_non_warning_line() -> None:
    text = f"{VIRTUAL_ENV_WARNING}\nreal failure\nlater line\n"
    assert stderr_without_warnings(text) == "real failure\nlater line"


def test_generic_runner_raises_a_typed_error_with_the_real_cause(monkeypatch) -> None:
    monkeypatch.setattr(
        sregym_subprocess.subprocess, "run", lambda argv, **kw: _completed(2, "", VIRTUAL_ENV_WARNING + "\nboom\n")
    )
    with pytest.raises(SregymSubprocessError) as caught:
        run_sregym_json("fault description", ["uv"], input_text="{}", env={}, timeout=1)
    assert caught.value.cause == "exit"
    assert str(caught.value).endswith("boom")


def _composite(*faults: tuple[str, str]) -> Any:
    from benchmarks.sregym.faults.description import FaultSpec, ProblemDescription

    return ProblemDescription(
        problem_id="mf__social_network__n2",
        composite=True,
        faults=tuple(
            FaultSpec(kind=kind, component=component, declared_index=index)
            for index, (kind, component) in enumerate(faults)
        ),
        injection_order=tuple(range(len(faults))),
    )


def test_composite_whose_later_fault_waits_on_an_unsettled_component_is_rejected_before_deploy() -> None:
    """mf__social_network__n2__s3496062482 spent 796 s in SREGym's settle timeouts before InvalidInjectionError."""

    from benchmarks.sregym.faults.description import CompositeConflictError, check_composites

    description = _composite(
        ("rolling_update_misconfigured", "post-storage-service"), ("missing_service", "post-storage-service")
    )
    with pytest.raises(CompositeConflictError) as caught:
        check_composites({"mf__social_network__n2": description})
    assert caught.value.reason == "composite_fault_conflict"
    assert set(caught.value.conflicts) == {"mf__social_network__n2"}


@pytest.mark.parametrize(
    "faults",
    [
        (("rolling_update_misconfigured", "post-storage-service"), ("missing_service", "user-service")),
        (("missing_service", "post-storage-service"), ("rolling_update_misconfigured", "post-storage-service")),
    ],
)
def test_composites_that_install_cleanly_are_admitted(faults: tuple[tuple[str, str], ...]) -> None:
    from benchmarks.sregym.faults.description import check_composites

    check_composites({"mf__ok": _composite(*faults)})
