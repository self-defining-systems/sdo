"""Interrupted evidence must remain a stop across an outer-runner restart."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.fastloop.reflection_gate import require_terminal_drain

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize(
    "evidence",
    [
        {},
        {"terminal": False, "interrupted": True},
        {"terminal": True, "interrupted": True},
        {"terminal": True},
        {"terminal": 1, "interrupted": False},
        {"terminal": True, "interrupted": 0},
    ],
)
def test_nonterminal_drain_is_rejected_before_and_after_cache_reload(
    tmp_path: Path, evidence: dict[str, object]
) -> None:
    cache = tmp_path / "terminal-reflection.json"
    cache.write_text(json.dumps(evidence))
    for attempt in (evidence, json.loads(cache.read_text())):
        with pytest.raises(RuntimeError, match="durable terminal reflection"):
            require_terminal_drain(attempt)


def test_explicit_terminal_uninterrupted_evidence_admits_recovery(tmp_path: Path) -> None:
    evidence = {"terminal": True, "interrupted": False}
    cache = tmp_path / "terminal-reflection.json"
    cache.write_text(json.dumps(evidence))
    require_terminal_drain(evidence)
    require_terminal_drain(json.loads(cache.read_text()))
