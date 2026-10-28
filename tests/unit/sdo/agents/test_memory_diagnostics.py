"""Typed broker diagnostics render into agent prompts without losing structure."""

from __future__ import annotations

from sdo.agents.memory_diagnostics import render_validation_feedback
from sdo.contracts import MemoryDiagnostic
from tests.context_helpers import expand_context


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


def test_memory_conflict_renders_paths_provenance_and_validator_errors() -> None:
    diagnostic = MemoryDiagnostic(rule="memory.conflict", message="Accepted memory advanced")
    conflict = diagnostic.memory_conflict
    conflict.base_commit = "a" * 40
    conflict.accepted_commit = "b" * 40
    conflict.conflicting_paths.append(".sdo/playbooks/base/README.md")
    conflict.accepted_changes.add(commit="b" * 40, incident_id="peer-incident", paths=[".sdo/lib/shared.go"])
    conflict.validator_errors.add(
        rule="program.replay", path=".sdo/playbooks/base/diagnose.go", message="replay failed"
    )
    text = expand_context(render_validation_feedback(2, [diagnostic]))
    assert "peer-incident" in expand_context(text)
    assert "b" * 40 in text
    assert ".sdo/playbooks/base/README.md" in text
    assert "program.replay" in text
    assert "replay failed" in text
