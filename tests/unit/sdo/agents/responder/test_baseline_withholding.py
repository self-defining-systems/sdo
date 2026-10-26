"""Only a typed, context-bound missing capture may degrade detector publication."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from sdo.agents.responder.baseline_withholding import missing_healthy_capture
from sdo.agents.responder.memory_check import ValidationResult

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize(
    ("change", "allowed"),
    [
        ({}, True),
        ({"diagnostics": []}, False),
        ({"diagnostics": [{"rule": "detector.healthy-baseline", "message": "detector fired on healthy state"}]}, False),
        ({"diagnostics": [{"rule": "program.repeat-unreachable", "message": "no finding"}]}, False),
        ({"actor": "health-judge"}, False),
        ({"phase": "initial"}, False),
        ({"base": "different-accepted-commit"}, False),
        ({"root": "/different/proposal"}, False),
        ({"unexpected": True}, False),
    ],
)
def test_missing_capture_requires_exact_diagnostic_and_context(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: dict, allowed: bool
) -> None:
    monkeypatch.setenv("SDO_BASE_COMMIT", "accepted-commit")
    report = {
        "root": str(tmp_path),
        "base": "accepted-commit",
        "actor": "responder",
        "phase": "reflection",
        "diagnostics": [
            {
                "rule": "detector.healthy-baseline",
                "message": "controller pre-incident healthy capture is unavailable",
            }
        ],
    }
    report.update(change)
    check = ValidationResult(1, False, json.dumps(report), "")
    assert missing_healthy_capture(check, tmp_path) == allowed
    assert not missing_healthy_capture(ValidationResult(2, False, check.stdout, ""), tmp_path)
    assert not missing_healthy_capture(ValidationResult(1, True, check.stdout, ""), tmp_path)
    if allowed:
        report["diagnostics"].append({"rule": "reflection.program-required", "message": "missing program"})
        assert not missing_healthy_capture(ValidationResult(1, False, json.dumps(report), ""), tmp_path)


def test_unstructured_or_truncated_diagnostics_stay_fatal(tmp_path: Path) -> None:
    for stdout in ["detector.healthy-baseline", '{"diagnostics":[', "null", "[]"]:
        assert not missing_healthy_capture(ValidationResult(1, False, stdout, ""), tmp_path)
