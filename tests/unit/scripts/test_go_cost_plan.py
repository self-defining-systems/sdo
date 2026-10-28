"""Cost planning guards the next expensive test, without placement edits."""

import importlib.util
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location(
    "go_cost_plan", Path(__file__).resolve().parents[3] / "scripts/go_cost_plan.py"
)
assert SPEC and SPEC.loader
planner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(planner)


def test_new_expensive_test_creates_capacity_without_placement_edits():
    costs = {"TestOld": 140, "TestNewExpensive": 140}
    bins, nightly = planner.pack(list(costs), costs, budget=210)
    assert len(bins) == 2
    assert sorted(n for b in bins for n in b) == sorted(costs)
    assert nightly == []


def test_budget_guard_rejects_overfull_shard():
    with pytest.raises(ValueError, match="budget"):
        planner.check_budget(["TestOld", "TestNewExpensive"], {"TestOld": 140, "TestNewExpensive": 140}, budget=210)


def test_unknown_names_have_conservative_cost_and_deterministic_ownership():
    names = [f"TestNew{i}" for i in range(30)]
    bins, nightly = planner.pack(names, {}, budget=210)
    assert bins == planner.pack(list(reversed(names)), {}, budget=210)[0]
    assert len(bins) >= 3
    assert sorted(n for b in bins for n in b) == sorted(names)
    assert not nightly


def test_single_test_over_budget_runs_nightly():
    assert planner.pack(["TestHuge"], {"TestHuge": 211}, budget=210) == ([], ["TestHuge"])


@pytest.mark.parametrize("cost", [0, -1, float("nan"), float("inf"), True])
def test_invalid_measurements_fail_closed(cost):
    with pytest.raises(ValueError, match="cost"):
        planner.pack(["TestOne"], {"TestOne": cost}, budget=210)
