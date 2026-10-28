"""Fixed PR capacity sends excess work to a complete, disjoint nightly inventory."""

import importlib.util
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location(
    "pr_test_plan", Path(__file__).resolve().parents[3] / "scripts/pr_test_plan.py"
)
assert SPEC is not None
assert SPEC.loader is not None
planner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(planner)


def test_fixed_capacity_prioritizes_cheap_checks_and_keeps_every_test():
    source = {
        "runtime": {"packages": {"TestFast": ["pkg"], "TestSlow": ["pkg"]}, "costs": {"TestFast": 1, "TestSlow": 200}}
    }
    python = {"costs": {"test_a.py::test_fast": 1, "test_a.py::test_unknown": 210}}
    policy = {"contract_work_seconds": 180, "contract_jobs": 2}
    plan = planner.build(source, python, policy)
    assert len(plan["bins"]) == 2
    assert sorted(n for b in plan["bins"] for n in b) == ["go:runtime:TestFast", "python:test_a.py::test_fast"]
    assert plan["nightly"] == ["go:runtime:TestSlow", "python:test_a.py::test_unknown"]
    assert {n for b in plan["bins"] for n in b}.isdisjoint(plan["nightly"])


def test_new_expensive_test_never_creates_an_extra_job():
    names = ["TestOld", "TestNew", "TestThird"]
    bins, nightly = planner.costs.pack(names, dict.fromkeys(names, 140), budget=180, max_bins=2)
    assert len(bins) == 2
    assert len(nightly) == 1
    assert sorted(n for b in bins for n in b) + nightly != []
    for b in bins:
        planner.costs.check_budget(b, dict.fromkeys(names, 140), budget=180)
    with pytest.raises(ValueError, match="budget"):
        planner.costs.check_budget(names, dict.fromkeys(names, 140), budget=180)
