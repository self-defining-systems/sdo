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
    assert "test_runtime_shard.py" not in script
    assert any("--shard all" in step.get("run", "") for step in jobs["runtime-integration"]["steps"])
    for name in [
        "TestReplayIncident",
        "TestChaosRestart",
        "TestExhaustiveHistory",
        "TestExplorerState",
        "TestHermeticFastloopRealGoGate",
        "TestHermeticReflectionLearnsThenAgentSelectsIdenticalRecurrence",
    ]:
        assert shards.partition([name], shards.PATTERNS)["contracts"] == [name]


@pytest.mark.parametrize("fail_contracts", [False, True])
def test_all_shards_run_real_discovered_tests_once_and_preserve_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capfd: pytest.CaptureFixture[str], fail_contracts: bool
) -> None:
    """Exercise the runner against a real compiled Go module, including failure."""
    import json
    import sys

    (tmp_path / "go.mod").write_text("module example.com/runtime-partitions\n\ngo 1.26.0\n")
    (tmp_path / "runner_test.go").write_text(
        'package partitions\nimport ("os"; "testing")\n'
        "func mark(t *testing.T) {\n"
        'if err := os.WriteFile(t.Name(), []byte("ran"), 0600); err != nil { t.Fatal(err) } }\n'
        "func TestRestartClassHermetic(t *testing.T) { mark(t) }\n"
        "func TestContracts(t *testing.T) { mark(t);\n"
        'if os.Getenv("FAIL_CONTRACTS") == "1" { t.Fatal("deliberate contract failure") } }\n'
    )
    monkeypatch.setattr(shards, "RUNTIME", tmp_path)
    monkeypatch.setattr(sys, "argv", ["runtime-shards", "--shard", "all"])
    monkeypatch.setenv("FAIL_CONTRACTS", "1" if fail_contracts else "0")
    with pytest.raises(SystemExit) as result:
        shards.main()
    assert result.value.code == int(fail_contracts)
    output = capfd.readouterr().out
    assert output.count("Coverage guard:") == 1
    events = [json.loads(line) for line in output.splitlines() if line.startswith("{")]
    assert sorted(event["Test"] for event in events if event.get("Action") == "run") == [
        "TestContracts",
        "TestRestartClassHermetic",
    ]
    assert (tmp_path / "TestContracts").read_text() == "ran"
    assert (tmp_path / "TestRestartClassHermetic").read_text() == "ran"
