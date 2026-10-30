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


@pytest.mark.parametrize(
    "guard",
    [
        "TestNativeReflectionSharesIncidentSessionWithResponder",
        "TestNativeLaunchRequiresProposalOnlyMountAndBuildsBroker",
        "TestCoveredRepeatBrokerSkipsReflectionWorker",
        "TestRecoveryProofSharesClearingWindowAndToolVerdict",
        "TestClosureRolloutWaitsBeforePublishingSuccess",
        "TestSourceInventoryMissingAuthorityPreservesMonitoringAndIsInformation",
        "TestLatentAllHealthClearClosureCarriesInventoryDriftAsInformation",
        "TestContinuationStillBrokenExcludesFindingAwaitingClearing",
        "TestControllerWaitsForFreshClearSamplesInsteadOfRedispatch",
    ],
)
def test_reflection_launch_guards_displace_cheap_filler(guard: str):
    """Optional launch adapters must not escape the memory/publication guard set."""
    import json

    policy = json.loads((planner.ROOT / "scripts/ci_policy.json").read_text())
    names = {guard: 50} | {f"TestFiller{i}": 9 for i in range(80)}
    source = {"runtime": {"packages": {name: ["sdo.dev/operator/runtime"] for name in names}, "costs": names}}
    plan = planner.build(source, {"costs": {}}, policy)
    selected = {name for bucket in plan["bins"] for name in bucket}
    assert f"go:runtime:{guard}" in selected
    assert len(plan["bins"]) == policy["contract_jobs"]


def test_nightly_runs_full_runtime_even_when_pr_selected_it():
    source = {
        "runtime": {
            "packages": {"TestFast": ["pkg"], "TestSlow": ["pkg"]},
            "costs": {"TestFast": 1, "TestSlow": 200},
        },
        "operator": {"packages": {"TestOther": ["other"]}, "costs": {"TestOther": 1}},
    }
    plan = planner.build(
        source, {"costs": {"test_a.py::test_slow": 200}}, {"contract_work_seconds": 180, "contract_jobs": 2}
    )
    pr_bins = [list(members) for members in plan["bins"]]
    bins = planner.nightly_bins(plan)
    selected = [key for members in bins for key in members]
    assert sorted(selected) == ["go:runtime:TestFast", "go:runtime:TestSlow", "python:test_a.py::test_slow"]
    assert len(selected) == len(set(selected))
    assert plan["bins"] == pr_bins  # Nightly coverage never expands the PR budget.
    for members in bins:
        planner.costs.check_budget(members, {key: plan["items"][key]["seconds"] for key in members})


@pytest.mark.go_integration
def test_compiled_runtime_inventory_reaches_nightly_runner(tmp_path, monkeypatch):
    """The real compiler inventory and execution cover PR-selected and deferred tests."""
    import json
    import subprocess

    module = tmp_path / "sdo/operator/runtime"
    module.mkdir(parents=True)
    (module / "go.mod").write_text("module synthetic.example/nightly\n\ngo 1.24\n")
    (module / "runtime_test.go").write_text("""package nightly
import "testing"
func TestSelectedByPR(t *testing.T) {}
func TestDeferred(t *testing.T) {}
func FuzzRuntimeSeed(f *testing.F) {
    f.Add("synthetic")
    f.Fuzz(func(t *testing.T, s string) {})
}
""")
    profiles = tmp_path / "scripts"
    profiles.mkdir()
    (profiles / "runtime_test_costs.json").write_text(
        json.dumps(
            {
                "costs": {
                    "TestSelectedByPR": 1,
                    "TestDeferred": 200,
                    "FuzzRuntimeSeed": 1,
                }
            }
        )
    )
    monkeypatch.setattr(planner.costs, "ROOT", tmp_path)
    inventory = planner.costs.inventory("runtime")
    go = {"runtime": planner.costs.plan("runtime", inventory, include_dedicated=True)}
    plan = planner.build(go, {"costs": {}}, {"contract_work_seconds": 180, "contract_jobs": 2})
    assert "go:runtime:TestSelectedByPR" in {key for members in plan["bins"] for key in members}
    assert "go:runtime:TestDeferred" in plan["nightly"]
    passed = set()
    selected = []
    for members in planner.nightly_bins(plan):
        names = [plan["items"][key]["name"] for key in members]
        selected.extend(names)
        output = subprocess.check_output(planner.costs.go_command(go["runtime"], names), cwd=module, text=True)
        for line in output.splitlines():
            event = json.loads(line)
            if event.get("Action") == "pass" and event.get("Test"):
                passed.add(event["Test"])
    expected = {name for names in inventory.values() for name in names}
    assert set(selected) == expected
    assert len(selected) == len(expected)
    assert {name for name in passed if "/" not in name} == expected
    assert "FuzzRuntimeSeed/seed#0" in passed


def test_deterministic_response_contracts_cannot_be_displaced_by_cheap_filler():
    import json

    policy = json.loads((planner.ROOT / "scripts/ci_policy.json").read_text())
    names = {
        "TestDeterministicResponseStageRecoveryWaitsWithoutRepeatingWrites": 3,
        "TestDeterministicResponseStageRolloutWaitIsBoundedByBudget": 25,
        "TestDeterministicResponseStageFutureDeadlineContract": 5,
        "TestRolloutBudgetNeverInvokesConsumerAfterDeadline": 1,
    }
    guards = {f"go:runtime:{name}" for name in names}
    names |= {f"TestFiller{i}": 0.1 for i in range(4000)}
    source = {
        "runtime": {
            "packages": {name: ["sdo.dev/operator/runtime"] for name in names},
            "costs": names,
            "unmeasured": [f"TestFiller{i}" for i in range(4000)],
        }
    }
    plan = planner.build(source, {"costs": {}}, policy)
    assert guards <= {name for bucket in plan["bins"] for name in bucket}
    assert len(plan["bins"]) == policy["contract_jobs"]
    for bucket in plan["bins"]:
        planner.costs.check_budget(
            bucket,
            {key: item["seconds"] for key, item in plan["items"].items()},
            budget=policy["contract_work_seconds"],
        )


def test_required_contract_overflow_fails_instead_of_silently_deferring():
    source = {"runtime": {"packages": {"TestRequired": ["pkg"]}, "costs": {"TestRequired": 181}}}
    policy = {"contract_work_seconds": 180, "contract_jobs": 2, "required_patterns": ["TestRequired"]}
    with pytest.raises(ValueError, match="required PR contracts exceed fixed budget"):
        planner.build(source, {"costs": {}}, policy)
