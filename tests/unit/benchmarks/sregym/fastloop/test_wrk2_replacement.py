"""Adapter seam contracts for the pinned load-generator replacement lifecycle."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest


@pytest.mark.parametrize("scenario", ["terminating", "gone", "active", "finalizer", "api-error", "delete-timeout"])
def test_real_wrk2_replacement_is_bounded_and_checks_old_pods(scenario: str) -> None:
    root = Path(__file__).resolve().parents[5]
    reply = subprocess.run(
        [
            "uv",
            "run",
            "--directory",
            str(root / "third_party/sregym"),
            "python",
            "-m",
            "tests.unit.benchmarks.sregym.fastloop.wrk2_replacement_worker",
            scenario,
        ],
        env={**os.environ, "PYTHONPATH": str(root)},
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert reply.returncode == 0, (reply.stdout + reply.stderr)[-4000:]
    assert f"bounded wrk2 replacement {scenario}: pass" in reply.stdout
