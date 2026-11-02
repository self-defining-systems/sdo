"""Recorded SDK gaps stay visible even when stored only in older ledgers."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "list_sdk_gaps.py"


def test_lists_every_gap_deterministically(tmp_path: Path) -> None:
    gap = {"fault_class": "db", "primitive": "EnsureExec", "reason": "old SDK", "scratch_script": "fixture command"}
    (tmp_path / "outcome.json").write_text(json.dumps({"incident_id": "i", "sdk_gaps": [gap]}))
    (tmp_path / "ledger.json").write_text(
        json.dumps(
            {
                "incident_id": "i",
                "reflection_decision": {"sdk_gaps": [gap]},
                "reflection_results": [{"sdk_gaps": [gap]}],
            }
        )
    )
    (tmp_path / "other.json").write_text(
        json.dumps({"incident_id": "j", "reflection_results": [{"sdk_gaps": [gap, {**gap, "primitive": "Other"}]}]})
    )
    result = subprocess.run([sys.executable, str(SCRIPT), str(tmp_path)], check=False, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    rows = [json.loads(line) for line in result.stdout.splitlines()]
    assert [(row["incident_id"], row["primitive"]) for row in rows] == [
        ("i", "EnsureExec"),
        ("j", "EnsureExec"),
        ("j", "Other"),
    ]
    assert rows[0]["scratch_script"] == "fixture command"
    assert len(rows[0]["sources"]) == 2
    failed = subprocess.run(
        [sys.executable, str(SCRIPT), "--fail-on-gap", str(tmp_path)], check=False, capture_output=True, text=True
    )
    assert failed.returncode == 1


def test_malformed_input_is_visible(tmp_path: Path) -> None:
    (tmp_path / "broken.json").write_text("{")
    result = subprocess.run([sys.executable, str(SCRIPT), str(tmp_path)], check=False, capture_output=True, text=True)
    assert result.returncode == 2
    assert "broken.json" in result.stderr
