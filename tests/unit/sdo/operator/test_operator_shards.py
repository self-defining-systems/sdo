"""Operator partitions preserve the compiled inventory and name budget regressions."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[4]
SPEC = importlib.util.spec_from_file_location("operator_shards", ROOT / "scripts/test_operator_shard.py")
assert SPEC is not None
assert SPEC.loader is not None
shards = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(shards)


def inventory() -> dict[str, list[str]]:
    source = {
        "sdo.dev/operator/builder": [
            *shards.MOVED_BUILDER_TESTS,
            "TestBuildWorkspaceCompilesApplicationDetectors",
            "FuzzNewCompiler",
            "Example_compiler",
        ],
        "sdo.dev/operator/broker": [
            *(
                full.removeprefix("sdo.dev/operator/broker/")
                for full in shards.MOVED_TESTS
                if full.startswith("sdo.dev/operator/broker/")
            ),
            "TestCLIAndBrokerAgreeOnAcceptance",
            "TestCLIAndBrokerAgreeOnAcceptanceSecondShard",
            "TestNewBrokerBehavior",
        ],
        "sdo.dev/operator/memory/validate": ["TestAssuranceSeedsPassHealthJudgeCheck"],
        "sdo.dev/operator/operatorcli": ["TestAdoptionCompilesAsIsAndLaterValidationStillRejects"],
        "sdo.dev/operator/worker_registry": ["TestKubernetesExecutorRejectsRequestOverBudgetNamingTheComponent"],
    }
    # Every measured move participates in this synthetic compiled inventory;
    # negative controls below remove anchors after construction.
    for full in shards.MOVED_TESTS:
        package, _, name = full.rpartition("/")
        names = source.setdefault(package, [])
        if name not in names:
            names.append(name)
    return source


def test_compiler_work_is_exhaustive_disjoint_and_keeps_new_names() -> None:
    source = inventory()
    groups = shards.partition(source)
    assigned = [(package, name) for group in groups.values() for package, names in group.items() for name in names]
    assert sorted(assigned) == sorted((package, name) for package, names in source.items() for name in names)
    assert len(assigned) == len(set(assigned))
    assert groups["compiler"]["sdo.dev/operator/builder"] == [
        "TestBuildWorkspaceCompilesApplicationDetectors",
        "FuzzNewCompiler",
        "Example_compiler",
    ]
    for name, shard in shards.MOVED_BUILDER_TESTS.items():
        assert name in groups[shard]["sdo.dev/operator/builder"]


def test_removed_or_renamed_cost_anchor_fails_closed() -> None:
    source = inventory()
    source["sdo.dev/operator/builder"].remove(next(iter(shards.MOVED_BUILDER_TESTS)))
    with pytest.raises(ValueError, match="missing.*builder"):
        shards.partition(source)


def test_same_name_in_differently_partitioned_packages_fails_closed() -> None:
    source = inventory()
    source["sdo.dev/operator/builder"].append("TestNewBrokerBehavior")
    with pytest.raises(ValueError, match="different shards"):
        shards.partition(source)


def test_slow_tests_and_process_are_report_only(capsys: pytest.CaptureFixture[str]) -> None:
    shards.check_budgets(
        [
            {"Action": "pass", "Package": "builder", "Test": "TestSlowCompile", "Elapsed": 181},
            {"Action": "run", "Package": "builder", "Test": "TestStillCompiling"},
        ],
        elapsed=241,
    )
    output = capsys.readouterr().out
    assert "TestSlowCompile=181.00s" in output
    assert "TestStillCompiling" in output
    assert "report-only" in output


def test_fast_complete_suite_passes_budget_guard() -> None:
    shards.check_budgets(
        [
            {"Action": "pass", "Package": "builder", "Test": "TestCompile", "Elapsed": 20},
            {"Action": "pass", "Package": "builder", "Elapsed": 21},
        ],
        elapsed=22,
    )


def test_operator_runner_changes_trigger_go_and_keep_existing_matrix_jobs() -> None:
    import yaml

    jobs = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"]
    matrix = jobs["controller"]["strategy"]["matrix"]["include"]
    selected = [row["operator"] for row in matrix if "operator" in row]
    assert sorted(selected) == sorted(shards.SHARDS)
    replay = next(row for row in matrix if row.get("operator") == "compiler-replay")
    assert replay["module"] == "foundation-contracts"
    assert len([row for row in matrix if row["module"].startswith("operator")]) == 5
    filters = yaml.safe_load(jobs["changes"]["steps"][1]["with"]["filters"])
    assert "scripts/test_operator_shard.py" in filters["go"]
    assert "scripts/test_operator_shard.sh" in filters["go"]
    assert "scripts/operator_test_costs.json" in filters["go"]
    assert all(jobs[name]["timeout-minutes"] <= 5 for name in jobs)


def test_shard_guard_does_not_kill_a_successful_process(monkeypatch: pytest.MonkeyPatch) -> None:
    import sys
    import time

    monkeypatch.setattr(shards, "SHARD_BUDGET_SECONDS", 0.01)
    command = [sys.executable, "-c", "import time; time.sleep(0.05); print('{}')"]
    shards.run_tests(command, started=time.monotonic())


def test_go_test_failure_still_fails() -> None:
    import subprocess
    import sys
    import time

    with pytest.raises(subprocess.CalledProcessError):
        shards.run_tests([sys.executable, "-c", "print('{}'); raise SystemExit(1)"], started=time.monotonic())


def test_malformed_tool_output_fails_closed() -> None:
    import sys
    import time

    with pytest.raises(ValueError, match="invalid Go test JSON"):
        shards.run_tests([sys.executable, "-c", "print('{')"], started=time.monotonic())


def test_parallel_children_cannot_hide_parent_wall_cost() -> None:
    events = [
        {"Action": "run", "Package": "builder", "Test": "TestParallel", "Time": "2026-10-26T00:00:00Z"},
        {"Action": "pass", "Package": "builder", "Test": "TestParallel", "Time": "2026-10-26T00:03:01Z", "Elapsed": 0},
    ]
    shards.check_budgets(events, elapsed=182)


def test_top_level_pause_does_not_become_execution_cost() -> None:
    events = [
        {"Action": "run", "Package": "builder", "Test": "TestParallel", "Time": "2026-10-26T00:00:00Z"},
        {"Action": "pause", "Package": "builder", "Test": "TestParallel", "Time": "2026-10-26T00:00:00Z"},
        {"Action": "cont", "Package": "builder", "Test": "TestParallel", "Time": "2026-10-26T00:03:00Z"},
        {"Action": "pass", "Package": "builder", "Test": "TestParallel", "Time": "2026-10-26T00:03:01Z", "Elapsed": 1},
    ]
    shards.check_budgets(events, elapsed=182)


def test_hosted_expensive_nonbuilder_work_shares_existing_runners() -> None:
    source = inventory()
    source[shards.PREFIX + "operatorcli"] = [
        "TestAdoptionCompilesAsIsAndLaterValidationStillRejects",
        "TestNewCLIBehavior",
    ]
    source[shards.PREFIX + "worker_registry"] = [
        "TestKubernetesExecutorRejectsRequestOverBudgetNamingTheComponent",
        "FuzzWorker",
    ]
    groups = shards.partition(source)
    assert groups["compiler-replay"][shards.PREFIX + "operatorcli"] == [
        "TestAdoptionCompilesAsIsAndLaterValidationStillRejects",
    ]
    assert groups["compiler"][shards.PREFIX + "worker_registry"] == [
        "TestKubernetesExecutorRejectsRequestOverBudgetNamingTheComponent",
    ]
    assert groups["remaining"][shards.PREFIX + "operatorcli"] == ["TestNewCLIBehavior"]
    assert groups["remaining"][shards.PREFIX + "worker_registry"] == ["FuzzWorker"]


@pytest.mark.parametrize("package", ["broker", "operatorcli", "worker_registry"])
def test_removed_or_renamed_nonbuilder_cost_anchor_fails_closed(package: str) -> None:
    source = inventory()
    source[shards.PREFIX + package].clear()
    with pytest.raises(ValueError, match="missing measured operator"):
        shards.partition(source)


def test_hosted_profiles_keep_provenance_and_positive_costs() -> None:
    import json
    import math

    for filename in ("operator_test_costs.json", "runtime_test_costs.json"):
        profile = json.loads((ROOT / "scripts" / filename).read_text())
        assert isinstance(profile["source_run"], int)
        assert profile["source_run"] > 0
        assert len(profile["source_head"]) == 40
        assert all(math.isfinite(cost) and cost > 0 for cost in profile["costs"].values())
        assert profile.get("placements", {}).keys() <= profile["costs"].keys()


def test_measured_slow_acceptance_cases_share_existing_compiler_runner() -> None:
    source = inventory()
    name = "TestCLIAndBrokerAgreeOnAcceptanceThirdShard"
    source[shards.PREFIX + "broker"].append(name)
    groups = shards.partition(source)
    assert name in groups["compiler-replay"][shards.PREFIX + "broker"]
    assert sum(names.count(name) for group in groups.values() for names in group.values()) == 1


def test_advisory_acceptance_group_shares_existing_broker_runner() -> None:
    source = inventory()
    name = "TestCLIAndBrokerAgreeOnAcceptanceFourthShard"
    source[shards.PREFIX + "broker"].append(name)
    groups = shards.partition(source)
    assert name in groups["broker"][shards.PREFIX + "broker"]
    assert sum(names.count(name) for group in groups.values() for names in group.values()) == 1
