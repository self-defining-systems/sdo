"""Runtime CI partitions cover discovered tests once and retain the race suite."""

from __future__ import annotations

import importlib.util
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
    assert set(assigned) == {"contracts", "integration"}


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


def test_two_runtime_partitions_preserve_the_gate_selectors() -> None:
    jobs = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"]
    assert "strategy" not in jobs["go"]
    assert "go" in jobs["required-pr-ci"]["needs"]
    script = (ROOT / "scripts/test_go_ci.sh").read_text()
    assert "scripts/test_runtime_shard.py --shard contracts" in script
    assert any("--shard integration" in step.get("run", "") for step in jobs["runtime-integration"]["steps"])
    for name in [
        "TestReplayIncident",
        "TestChaosRestart",
        "TestExhaustiveHistory",
        "TestExplorerState",
        "TestHermeticFastloopRealGoGate",
        "TestHermeticReflectionLearnsThenAgentSelectsIdenticalRecurrence",
    ]:
        assert shards.partition([name], shards.PATTERNS)["contracts"] == [name]
