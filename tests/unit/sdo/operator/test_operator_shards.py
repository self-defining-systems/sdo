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
    return {
        "sdo.dev/operator/builder": [
            *shards.MOVED_BUILDER_TESTS,
            "TestBuildWorkspaceCompilesApplicationDetectors",
            "FuzzNewCompiler",
            "Example_compiler",
        ],
        "sdo.dev/operator/broker": [
            "TestCLIAndBrokerAgreeOnAcceptance",
            "TestCLIAndBrokerAgreeOnAcceptanceSecondShard",
            "TestNewBrokerBehavior",
        ],
        "sdo.dev/operator/memory/validate": ["TestAssuranceSeedsPassHealthJudgeCheck"],
    }


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


def test_budget_regression_reports_slow_test_and_package() -> None:
    events = [
        {"Action": "pass", "Package": "sdo.dev/operator/builder", "Test": "TestSlowCompile", "Elapsed": 181},
        {"Action": "pass", "Package": "sdo.dev/operator/builder", "Elapsed": 182},
    ]
    with pytest.raises(ValueError, match=r"builder/TestSlowCompile.*181.*180"):
        shards.check_budgets(events, elapsed=182)


def test_aggregate_regression_and_timeout_keep_named_evidence() -> None:
    events = [
        {"Action": "pass", "Package": "builder", "Test": "TestFirst", "Elapsed": 100},
        {"Action": "run", "Package": "builder", "Test": "TestStillCompiling"},
    ]
    with pytest.raises(ValueError, match=r"210.*TestFirst.*TestStillCompiling"):
        shards.check_budgets(events, elapsed=241)


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
    assert all(jobs[name]["timeout-minutes"] <= 5 for name in jobs)


@pytest.mark.parametrize("partial_line", [False, True])
def test_wall_budget_stops_a_stalled_test_and_keeps_its_name(
    monkeypatch: pytest.MonkeyPatch, partial_line: bool
) -> None:
    import sys
    import time

    monkeypatch.setattr(shards, "SHARD_BUDGET_SECONDS", 0.5)
    command = [
        sys.executable,
        "-c",
        (
            "import json,time,sys; "
            "print(json.dumps({'Action':'run','Package':'builder','Test':'TestStalledCompile'}),flush=True); "
            + ("sys.stdout.write('{'); sys.stdout.flush(); " if partial_line else "")
            + "time.sleep(60)"
        ),
    ]
    started = time.monotonic()
    with pytest.raises(ValueError, match="TestStalledCompile"):
        shards.run_tests(command, started=started)
    assert time.monotonic() - started < 5


def test_peer_partitions_only_report_compiler_budgets() -> None:
    shards.check_budgets(
        [
            {"Action": "pass", "Package": "broker", "Test": "TestExistingAcceptance", "Elapsed": 211},
        ],
        elapsed=212,
        enforce=False,
    )


def test_malformed_tool_output_fails_closed() -> None:
    import sys
    import time

    with pytest.raises(ValueError, match="invalid Go test JSON"):
        shards.run_tests([sys.executable, "-c", "print('{')"], started=time.monotonic())
