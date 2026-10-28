"""Bounded CLI transport for the complete Go memory acceptance check."""

from __future__ import annotations

import contextlib
import os
import signal
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from sdo.contracts import ReflectionResult, to_canonical_json

if TYPE_CHECKING:
    from collections.abc import Callable

VALIDATION_TIMEOUT_SECONDS = 300


@dataclass(frozen=True)
class ValidationResult:
    exit_code: int
    timed_out: bool
    stdout: str
    stderr: str


def validation_output(raw: bytes) -> str:
    return raw.decode("utf-8", errors="replace")


def communicate_within(process: subprocess.Popen[bytes], timeout_seconds: float) -> tuple[bytes, bytes]:
    """Collect output, raising subprocess.TimeoutExpired once the deadline passes."""
    return process.communicate(timeout=timeout_seconds)


def check_reflection_result(
    root: Path,
    decision: ReflectionResult,
    timeout_seconds: float,
    communicate: Callable[[subprocess.Popen[bytes], float], tuple[bytes, bytes]] = communicate_within,
) -> ValidationResult:
    """Check the exact returned decision and final files, independently of model tool use."""
    if timeout_seconds <= 0:
        return ValidationResult(2, True, "", "reflection deadline expired before memory check")
    with tempfile.TemporaryDirectory(prefix="sdo-final-decision-") as temporary:
        path = Path(temporary) / "decision.json"
        path.write_text(to_canonical_json(decision), encoding="utf-8")
        try:
            process = subprocess.Popen(
                ["sdo-operator", "memory", "check", "--json", "--decision", str(path)],
                cwd=root,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,
            )
        except OSError as exc:
            return ValidationResult(2, False, "", f"cannot start memory check: {exc}")
        try:
            stdout, stderr = communicate(process, timeout_seconds)
        except subprocess.TimeoutExpired:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            stdout, stderr = process.communicate()
            return ValidationResult(
                2,
                True,
                validation_output(stdout),
                f"memory check timed out after {timeout_seconds:g}s\n{validation_output(stderr)}",
            )
        return ValidationResult(process.returncode, False, validation_output(stdout), validation_output(stderr))
