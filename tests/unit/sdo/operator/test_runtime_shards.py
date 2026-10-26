"""Runtime CI partitions cover discovered tests once and retain the race suite."""

from __future__ import annotations

import importlib.util
import json
import re
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[4]
SPEC = importlib.util.spec_from_file_location("runtime_shards", ROOT / "scripts/test_runtime_shard.py")
assert SPEC is not None
assert SPEC.loader is not None
shards = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(shards)


def test_partitions_include_tests_fuzz_seeds_and_examples() -> None:
    names = [
        "TestAdmission",
        "TestHermeticFastloopRealGoGate",
        "TestReflectionDirectFilesCLIAndBroker",
        "TestReflectionLearnedProgramCLIAndBroker",
        "TestReflectionMongoAuthUpgradesReadmeThroughCLIAndBroker",
        "TestRegisteredReflectionWorker",
        "TestRenderedLockImageInputScope",
        "TestSimRenderedLockSchedule",
        "TestRejectedRepeatCorpusFallsBackToReflectionWorker",
        "TestSimSchedule",
        "TestSimIncidentStream",
        "TestSimRV6HealthRefreshCrashSchedules",
        "TestState",
        "TestZero",
        "FuzzController",
        "FuzzSimIncidentSchedule",
        "ExampleController",
        "ExampleRuntime",
    ]
    assigned = shards.partition(names, shards.PATTERNS)
    assert sorted(name for group in assigned.values() for name in group) == sorted(names)
    assert all(assigned.values())
    assert assigned["schedules"] == ["TestSimRenderedLockSchedule", "TestSimSchedule", "FuzzSimIncidentSchedule"]


@pytest.mark.parametrize("patterns", [{"one": "^TestA$"}, {"one": "^Test", "two": "^Test"}])
def test_guard_rejects_missing_or_duplicate_coverage(patterns: dict[str, str]) -> None:
    with pytest.raises(ValueError, match="exactly one shard"):
        shards.partition(["TestNewBehavior"], patterns)


def test_discovery_retains_fuzz_and_examples_but_not_benchmarks() -> None:
    assert shards.discovered_names(
        [
            {"Action": "output", "Output": name + "\n"}
            for name in ["TestOne", "FuzzOne", "Example_one", "BenchmarkOne", "ok  package"]
        ]
    ) == ["Example_one", "FuzzOne", "TestOne"]


def test_every_matrix_shard_is_aggregated_and_script_changes_trigger_go() -> None:
    jobs = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"]
    matrix = jobs["controller"]["strategy"]["matrix"]["include"]
    runtime = [row for row in matrix if row["module"].startswith("runtime")]
    partitions = [part for row in runtime for part in row["shard"].split(",")]
    assert set(partitions) == shards.PATTERNS.keys()
    assert len(partitions) == len(shards.PATTERNS)
    assert {tuple(row["shard"].split(",")) for row in runtime} == set(shards.RUNNER_GROUPS.values())
    assert len(runtime) == 8
    assert "scripts/runtime_test_costs.json" in yaml.safe_load(jobs["changes"]["steps"][1]["with"]["filters"])["go"]
    assert "controller" in jobs["required-pr-ci"]["needs"]
    filters = yaml.safe_load(jobs["changes"]["steps"][1]["with"]["filters"])
    assert "scripts/test_runtime_shard.py" in filters["go"]
    assert "scripts/test_operator_shard.py" in filters["go"]


def test_schedule_shard_runs_all_seeds_with_a_five_minute_race_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    names = [
        "TestAdmission",
        "TestRendered",
        "TestSimIncidentStream",
        "TestSimSchedule",
        "TestReflectionDirectFilesCLIAndBroker",
        "TestRejectedRepeatCorpusFallsBackToReflectionWorker",
        "TestReflectionMongoAuthUpgradesReadmeThroughCLIAndBroker",
        "TestReflectionLearnedProgramCLIAndBroker",
        "TestRestart",
        "TestState",
        "FuzzSimIncidentSchedule",
    ]
    calls: list[list[str]] = []

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        output = "\n".join(json.dumps({"Action": "output", "Output": name + "\n"}) for name in names)
        return subprocess.CompletedProcess(command, 0, stdout=output)

    monkeypatch.setattr(shards.subprocess, "run", run)
    monkeypatch.setattr(shards, "run_tests", lambda command, **kwargs: calls.append(command))
    monkeypatch.setattr(sys, "argv", ["test_runtime_shard.py", "--shard", "schedules"])
    shards.main()
    command = calls[-1]
    assert "-race" in command
    assert "-parallel=2" in command
    assert "-timeout=5m" in command
    selection = command[command.index("-run") + 1]
    costs = json.loads(shards.COST_FILE.read_text())["costs"]
    expected = shards.balanced_partition(names, shards.PATTERNS, costs)["schedules"]
    assert {name for name in names if re.fullmatch(selection, name)} == set(expected)


def test_compiling_reflection_contracts_have_independent_budgets() -> None:
    mongo = [
        "TestReflectionMongoAuthUpgradesReadmeThroughCLIAndBroker",
        "TestReflectionHotelMongoAuthUpgradesReadmeThroughCLIAndBroker",
    ]
    programs = [
        "TestReflectionHotelRunScriptUpgradesReadmeThroughCLIAndBroker",
        "TestReflectionLearnedProgramCLIAndBroker",
        "TestReflectionLearnsDeploymentRepairWithUnwrittenServiceSymptoms",
    ]
    assigned = shards.partition([*mongo, *programs, "TestAdmission"], shards.PATTERNS)
    assert assigned["reflection-mongo"] == mongo
    assert assigned["reflection-programs"] == programs
    assert assigned["contracts"] == ["TestAdmission"]


def test_balanced_runtime_plan_preserves_every_compiled_name_once() -> None:
    names = [
        "TestReflectionDirectFilesCLIAndBroker",
        "TestRejectedRepeatCorpusFallsBackToReflectionWorker",
        "TestReflectionMongoAuthUpgradesReadmeThroughCLIAndBroker",
        "TestReflectionLearnedProgramCLIAndBroker",
        "TestRestartClassHermetic",
        "TestHermeticSharedMemoryControllers",
        "TestSimSchedule",
        "TestAdmission",
        "TestRendered",
        "TestSimIncidentStream",
        "TestState",
        "FuzzSimIncidentSchedule",
    ]
    costs = {name: float(20 + i) for i, name in enumerate(names)}
    planned = shards.balanced_partition(names, shards.PATTERNS, costs)
    assert sorted(name for members in planned.values() for name in members) == sorted(names)
    assert planned == shards.balanced_partition(list(reversed(names)), shards.PATTERNS, costs)
    assert set(planned) == set(shards.PATTERNS)


def test_measured_costs_rebalance_heavy_alphabetical_groups() -> None:
    names = ["TestRenderOne", "TestRenderTwo", "TestRenderThree", "TestAdmission", "TestState"]
    costs = dict.fromkeys(names[:3], 50.0)
    planned = shards.balanced_partition(names, shards.PATTERNS, costs)
    owners = [group for group, members in planned.items() for name in names[:3] if name in members]
    assert len(set(owners)) == 3


def test_cost_profile_rejects_invalid_or_unmeasured_values() -> None:
    for cost in [0, -1, float("nan"), float("inf")]:
        with pytest.raises(ValueError, match="cost"):
            shards.balanced_partition(["TestAdmission"], shards.PATTERNS, {"TestAdmission": cost})


def test_measured_corrections_retain_exact_coverage() -> None:
    names = [*shards.PLACEMENTS, "TestAdmission", "TestRendered", "TestState"]
    planned = shards.balanced_partition(names, shards.PATTERNS, dict.fromkeys(shards.PLACEMENTS, 20.0))
    for name, target in shards.PLACEMENTS.items():
        assert planned[target].count(name) == 1
    assert sorted(name for members in planned.values() for name in members) == sorted(names)


def test_runtime_report_uses_seventy_percent_of_hosted_job(monkeypatch: pytest.MonkeyPatch) -> None:
    # Exercise the actual runtime wrapper's report threshold.
    sys.path.insert(0, str(ROOT / "scripts"))
    import test_operator_shard

    calls: list[dict] = []
    monkeypatch.setattr(test_operator_shard, "run_tests", lambda _command, **kwargs: calls.append(kwargs))
    shards.run_tests(["go", "test"], started=0)
    controller = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"]["controller"]
    assert calls[0]["budget_seconds"] == controller["timeout-minutes"] * 60 * 0.7 == 210
