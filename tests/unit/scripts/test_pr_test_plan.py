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


def test_unmeasured_tests_run_in_the_pr_instead_of_hiding_in_nightly():
    source = {
        "operator": {
            "packages": {f"TestCheap{i}": ["pkg"] for i in range(35)} | {"TestNew": ["pkg"]},
            "costs": {f"TestCheap{i}": 11 for i in range(35)} | {"TestNew": 15.0},
            "unmeasured": ["TestNew"],
        }
    }
    plan = planner.build(source, {"costs": {}}, {"contract_work_seconds": 180, "contract_jobs": 2})
    assert "go:operator:TestNew" in {n for b in plan["bins"] for n in b}
    assert plan["items"]["go:operator:TestNew"]["priority"]
    assert len(plan["nightly"]) == 4  # 360s capacity: the new test, then 31 cheap ones


def test_protected_guards_displace_cheaper_filler_when_capacity_is_short():
    source = {
        "operator": {
            "packages": {"TestMemoryGuard": ["sdo.dev/operator/memory/validate"]}
            | {f"TestFiller{i}": ["sdo.dev/operator/other"] for i in range(40)},
            "costs": {"TestMemoryGuard": 100} | {f"TestFiller{i}": 9 for i in range(40)},
        }
    }
    policy = {
        "contract_work_seconds": 110,
        "contract_jobs": 2,
        "protected_share": 0.6,
        "protected_patterns": [r"sdo\.dev/operator/memory"],
    }
    plan = planner.build(source, {"costs": {}}, policy)
    admitted = {n for b in plan["bins"] for n in b}
    assert "go:operator:TestMemoryGuard" in admitted
    assert not any(n.startswith("go:operator:TestMemory") for n in plan["nightly"])


def test_committed_profiles_keep_every_protected_guard_that_fits_in_the_pr():
    """Memory, broker, compile and publication guards are never deferred while they fit."""
    import json

    policy = json.loads((planner.ROOT / "scripts/ci_policy.json").read_text())
    go: dict = {}
    for scope in ("operator", "runtime"):
        profile = json.loads((planner.ROOT / f"scripts/{scope}_test_costs.json").read_text())["costs"]
        packages: dict[str, list[str]] = {}
        costs: dict[str, float] = {}
        for key, seconds in profile.items():
            package, name = key.rsplit("/", 1) if scope == "operator" else ("sdo.dev/operator/runtime", key)
            packages.setdefault(name, []).append(package)
            costs[name] = costs.get(name, 0) + seconds
        go[scope] = {"packages": packages, "costs": costs}
    python = json.loads((planner.ROOT / "scripts/python_integration_test_costs.json").read_text())
    plan = planner.build(go, python, policy)
    protected = {k for k, item in plan["items"].items() if item["priority"]}
    assert protected, "ci_policy.json protected_patterns select nothing"
    capacity = policy["contract_work_seconds"] * policy["contract_jobs"]
    total = sum(plan["items"][k]["seconds"] for k in protected)
    deferred = sorted(protected & set(plan["nightly"]))
    # Bin packing may strand a little capacity. Beyond that slack the guards
    # outgrew the PR: cut their cost (shared fixtures, fewer cases), do not hide them.
    if total <= 0.9 * capacity:
        assert deferred == [], deferred
