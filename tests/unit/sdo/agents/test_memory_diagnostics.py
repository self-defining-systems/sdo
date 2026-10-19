"""Typed broker diagnostics render into agent prompts without losing structure."""

from __future__ import annotations

from sdo.agents.memory_diagnostics import render_validation_feedback
from sdo.contracts import MemoryDiagnostic


def test_first_attempt_renders_nothing() -> None:
    assert render_validation_feedback(1, []) == ""


def test_retry_renders_every_diagnostic_field() -> None:
    text = render_validation_feedback(
        3,
        [
            MemoryDiagnostic(
                rule="detector.manifest", path="a/manifest.yaml", line=7, message="bad id", hint="use kebab"
            ),
            MemoryDiagnostic(rule="playbook.text", message="physical path"),
        ],
    )
    assert "attempt 3" in text
    assert "[detector.manifest] a/manifest.yaml:7: bad id (hint: use kebab)" in text
    assert "[playbook.text] physical path" in text
