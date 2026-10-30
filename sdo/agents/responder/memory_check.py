"""Bounded CLI transport for the complete Go memory acceptance check."""

from __future__ import annotations

import contextlib
import json
import os
import signal
import subprocess
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from sdo.contracts import ReflectionResult, to_canonical_json

if TYPE_CHECKING:
    from collections.abc import Callable

VALIDATION_TIMEOUT_SECONDS = 300
VALIDATION_TIMEOUT_FILE_ENV = "SDO_MEMORY_CHECK_TIMEOUT_FILE"


@dataclass(frozen=True)
class ValidationResult:
    exit_code: int
    timed_out: bool
    stdout: str
    stderr: str


class MemoryCheckTimeout(RuntimeError):
    """Validation exhausted its bound; a provider correction cannot fix a timeout."""

    def __init__(self, check: ValidationResult) -> None:
        self.check = check
        # Worker summaries are capped. Keep the active stage (the stderr tail)
        # ahead of potentially large JSON output so the cause survives the cap.
        super().__init__(f"reflection memory check timed out: {check.stderr[-3000:]}\n{check.stdout[:512]}")


def remember_timeout(check: ValidationResult) -> None:
    """Record a terminal timeout for this worker turn, across MCP process boundaries."""
    if path := os.environ.get(VALIDATION_TIMEOUT_FILE_ENV):
        Path(path).write_text(json.dumps(asdict(check)), encoding="utf-8")


def prior_timeout() -> ValidationResult | None:
    if (path := os.environ.get(VALIDATION_TIMEOUT_FILE_ENV)) and Path(path).is_file():
        try:
            check = ValidationResult(**json.loads(Path(path).read_text(encoding="utf-8")))
        except (TypeError, ValueError) as exc:
            raise ValueError("invalid terminal memory-check timeout record") from exc
        # This cross-process signal can only stop a check; never accept a
        # worker-writable record as a successful validation attestation.
        if check.timed_out is not True or not isinstance(check.exit_code, int) or check.exit_code == 0:
            raise ValueError("terminal memory-check timeout record cannot authorize a pass")
        return check
    return None


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
    if check := prior_timeout():
        return check
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
