"""Exercise the image CI audit against recording code and hidden benchmark data."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[4]


@pytest.mark.parametrize(
    ("path", "accepted"),
    [
        ("adapter/submission.py", True),
        ("fastloop/__init__.py", True),
        ("fastloop/model_recording.py", True),
        ("fastloop/file_edits.py", True),
        ("fastloop/recording_worker.py", True),
        ("fastloop/__pycache__/recording_worker.cpython-312.pyc", True),
        ("fastloop/__pycache__/model_recording.cpython-312.opt-1.pyc", True),
        ("fastloop/fault_tracker.py", False),
        ("fastloop/__pycache__/fault_tracker.cpython-312.pyc", False),
        ("fastloop/scripted_program/diagnose.go", False),
        ("experiments/assurance/composites.toml", False),
        ("assurance/scripted_codex/faults.py", False),
        ("analysis/incident_cost.py", False),
        ("runner/incident_stream.py", False),
        ("agents/answer.py", False),
        ("docs/answer.md", False),
        ("tests/fixture.json", False),
    ],
)
def test_image_ci_accepts_only_recording_files_from_fastloop(tmp_path: Path, path: str, accepted: bool) -> None:
    workflow = yaml.safe_load((ROOT / ".github/workflows/nightly-fuzz.yml").read_text(encoding="utf-8"))
    step = next(
        step
        for step in workflow["jobs"]["images"]["steps"]
        if step.get("name") == "Confirm the responder image carries no SREGym answer key"
    )
    fixture = tmp_path / "image-files.txt"
    fixture.write_text(f"/opt/sdo/benchmarks/sregym/{path}\n", encoding="utf-8")
    # Execute the real CI shell gate, replacing only Docker's file-list transport.
    script = 'set -euo pipefail\nscope_fixture=$1\ndocker() { cat "$scope_fixture"; }\n' + step["run"]
    completed = subprocess.run(
        ["bash", "-c", script, "image-scope-test", str(fixture)], capture_output=True, text=True, check=False
    )
    assert (completed.returncode == 0) is accepted, completed.stdout + completed.stderr
