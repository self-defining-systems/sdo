"""Controller pod logs are exported into the run's results as diagnostics."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

import benchmarks.sregym.adapter.runtime as runtime

if TYPE_CHECKING:
    import pytest


def _completed(
    args: list[str], returncode: int, stdout: str = "", stderr: str = ""
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args, returncode, stdout, stderr)


def test_controller_logs_of_every_job_pod_are_exported_with_timestamps(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    commands: list[list[str]] = []

    def fake_kubectl(
        args: list[str], *, namespace: str, check: bool = True, **_: object
    ) -> subprocess.CompletedProcess[str]:
        assert namespace == "demo"
        assert check is False
        commands.append(args)
        if args[:2] == ["get", "pods"]:
            return _completed(args, 0, "sdo-controller-run-a sdo-controller-run-b")
        return _completed(args, 0, f"2026-09-27T13:00:02Z log of {args[1]}\n")

    monkeypatch.setattr(runtime, "kubectl", fake_kubectl)

    summary = runtime.export_controller_logs("demo", tmp_path)

    destination = tmp_path / "sdo_runtime" / "controller_logs"
    assert summary == {"directory": str(destination), "error": None}
    assert commands[0][:4] == ["get", "pods", "--selector", "job-name=sdo-controller-run"]
    for pod in ("sdo-controller-run-a", "sdo-controller-run-b"):
        assert (destination / f"{pod}.log").read_text(encoding="utf-8") == (f"2026-09-27T13:00:02Z log of pod/{pod}\n")
    log_commands = [command for command in commands if command[0] == "logs"]
    assert all("--timestamps=true" in command and "--all-containers=true" in command for command in log_commands)


def test_controller_log_export_failure_is_recorded_not_raised(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def fake_kubectl(args: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        if args[:2] == ["get", "pods"]:
            return _completed(args, 0, "sdo-controller-run-a")
        return _completed(args, 1, stderr="container not found")

    monkeypatch.setattr(runtime, "kubectl", fake_kubectl)

    summary = runtime.export_controller_logs("demo", tmp_path)

    assert "container not found" in str(summary["error"])
    error_file = tmp_path / "sdo_runtime" / "controller_logs" / "export_error.txt"
    assert "container not found" in error_file.read_text(encoding="utf-8")


def test_controller_log_export_survives_a_missing_kubectl(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def missing_kubectl(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        raise FileNotFoundError("kubectl")

    monkeypatch.setattr(runtime, "kubectl", missing_kubectl)

    summary = runtime.export_controller_logs("demo", tmp_path)

    assert summary["directory"] is None or Path(str(summary["directory"])).is_dir()
    assert "kubectl" in str(summary["error"])


def test_controller_log_export_without_a_destination_is_skipped() -> None:
    assert runtime.export_controller_logs("demo", None) == {"directory": None, "error": "no artifacts directory"}
