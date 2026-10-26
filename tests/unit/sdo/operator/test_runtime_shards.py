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
    assert "controller" in jobs["required-pr-ci"]["needs"]
    filters = yaml.safe_load(jobs["changes"]["steps"][1]["with"]["filters"])
    assert "scripts/test_runtime_shard.py" in filters["go"]


def test_schedule_shard_runs_all_seeds_with_a_five_minute_race_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    names = [
        "TestAdmission",
        "TestRendered",
        "TestSimIncidentStream",
        "TestSimSchedule",
        "TestReflectionDirectFilesCLIAndBroker",
        "TestRejectedRepeatCorpusFallsBackToReflectionWorker",
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
    monkeypatch.setattr(sys, "argv", ["test_runtime_shard.py", "--shard", "schedules"])
    shards.main()
    command = calls[-1]
    assert "-race" in command
    assert "-parallel=2" in command
    assert "-timeout=5m" in command
    selection = command[command.index("-run") + 1]
    assert {name for name in names if re.fullmatch(selection, name)} == {"TestSimSchedule", "FuzzSimIncidentSchedule"}
