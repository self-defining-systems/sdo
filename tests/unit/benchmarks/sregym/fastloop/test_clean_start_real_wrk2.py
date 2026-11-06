"""Real SREGym Wrk2 check, nightly tier: PR CI ignores this file.

It runs `uv run --directory third_party/sregym`, which resolves and imports the real harness
environment in a fresh interpreter. That costs 11-12 s on a hosted runner (over the 10 s
slow-test budget); the clean-start contracts in test_clean_start.py cover the rest with fakes.
"""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.mark.requires_sregym
def test_real_wrk2_creates_harness_objects_and_grader_reads_the_same_namespace() -> None:
    import os
    import subprocess

    root = Path(__file__).resolve().parents[5]
    reply = subprocess.run(
        [
            "uv",
            "run",
            "--directory",
            str(root / "third_party/sregym"),
            "python",
            "-m",
            "tests.unit.benchmarks.sregym.fastloop.clean_start_wrk2_worker",
        ],
        env={**os.environ, "PYTHONPATH": str(root)},
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert reply.returncode == 0, (reply.stdout + reply.stderr)[-4000:]
    assert "real Wrk2 Job, payload, settings and grader log namespace: pass" in reply.stdout
