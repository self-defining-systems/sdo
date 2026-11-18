"""A claimed lane's accepted memory is prepared as an install of the new build would prepare it."""

from __future__ import annotations

from pathlib import Path

import pytest

from benchmarks.devswap import tree
from benchmarks.devswap.kube import SwapError


def steps(calls: list[str], prepared: list[str]) -> tree.TreeSteps:
    queue = iter(prepared)
    return tree.TreeSteps(
        export=lambda: calls.append("export") or Path("/clone"),
        prepare=lambda _clone: calls.append("prepare") or next(queue),
        import_head=lambda _clone, head: calls.append(f"import {head}"),
    )


def test_a_stale_tree_is_prepared_and_restored_into_the_lane() -> None:
    calls: list[str] = []
    head = tree.prepare_accepted_tree(steps(calls, ["new", "new"]), "old", lambda _line: None)
    assert head == "new"
    assert calls == ["export", "prepare", "prepare", "import new"]


def test_a_prepared_tree_leaves_the_lane_alone() -> None:
    calls: list[str] = []
    assert tree.prepare_accepted_tree(steps(calls, ["same"]), "same", lambda _line: None) == "same"
    assert calls == ["export", "prepare"]


def test_a_tree_that_does_not_prepare_to_a_fixed_point_is_refused() -> None:
    calls: list[str] = []
    with pytest.raises(SwapError, match="not stable"):
        tree.prepare_accepted_tree(steps(calls, ["one", "two"]), "old", lambda _line: None)
    assert "import one" not in calls
