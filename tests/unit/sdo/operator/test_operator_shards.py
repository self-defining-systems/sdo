"""Operator partitions preserve the compiled inventory and name budget regressions."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[4]
SPEC = importlib.util.spec_from_file_location("operator_shards", ROOT / "scripts/go_test_timing.py")
assert SPEC is not None
assert SPEC.loader is not None
shards = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(shards)


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
