"""Logical reflection guidance preserves class-level and spec-first learning."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

import pytest

from sdo.agents.responder.reflection import REFLECTION_GUIDANCE_MODES, SessionReflector
from tests.unit.sdo.agents.responder.test_reflection import _CapturingBackend, _fresh_closure, _outcome

if TYPE_CHECKING:
    from pathlib import Path


_LEAK_DENYLIST = ("hotel", "geo", "reservation", "SREGym", "benchmark verdict")


def _request_text(tmp_path: Path, guidance: str) -> str:
    backend = _CapturingBackend()
    SessionReflector(backend, guidance=guidance).resume(  # type: ignore[arg-type]
        session_id="session-1",
        incident_id="inc-1",
        worktree=tmp_path,
        outcome=_outcome(),
        history=[],
        outcome_commit="outcome-sha",
        closure=_fresh_closure(tmp_path),
    )
    return str(backend.calls[0]["prompt"])


@pytest.mark.parametrize("guidance", REFLECTION_GUIDANCE_MODES)
def test_logical_reflection_prompt_is_problem_agnostic(tmp_path: Path, guidance: str) -> None:
    prompt = _request_text(tmp_path, guidance)
    static = prompt.split("Current outcome:", 1)[0]
    for private_fact in _LEAK_DENYLIST:
        assert re.search(re.escape(private_fact), static, re.IGNORECASE) is None
    assert "Edit final operational-memory files only" in prompt
    for recipe in (
        "Finding.ParameterBindings",
        "originatingIncident",
        "merge",
        "widen",
        "create_corpus",
        "capture_fields",
        "check_memory",
    ):
        assert recipe not in static


def test_unknown_guidance_is_rejected() -> None:
    with pytest.raises(ValueError, match="unsupported reflection guidance"):
        SessionReflector(_CapturingBackend(), guidance="unknown")  # type: ignore[arg-type]
