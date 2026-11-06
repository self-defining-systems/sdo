"""CI speed and reliability ratchets: timeouts, concurrency, slow tests, cache size and health budgets."""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
import yaml

if TYPE_CHECKING:
    from types import ModuleType

ROOT = Path(__file__).resolve().parents[4]
BUDGETS = json.loads((ROOT / ".github/ci_budgets.json").read_text())


def load_script(name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


slow = load_script("check_slow_tests")
health = load_script("ci_health")


def workflow(name: str) -> dict:
    return yaml.safe_load((ROOT / ".github/workflows" / name).read_text())


@pytest.mark.parametrize("name", ["ci.yml", "nightly.yml", "ci-health.yml"])
def test_timeouts_never_exceed_the_checked_in_ratchet(name: str) -> None:
    table = BUDGETS["timeout_minutes"][name]
    jobs = workflow(name)["jobs"]
    assert set(jobs) == set(table), "add every job to timeout_minutes in .github/ci_budgets.json"
    for job, limit in table.items():
        assert jobs[job].get("timeout-minutes", 360) <= limit, f"{name}:{job} timeout rose above its ratchet"


def test_main_push_runs_are_never_cancelled_and_each_commit_has_its_own_group() -> None:
    concurrency = workflow("ci.yml")["concurrency"]
    assert concurrency["cancel-in-progress"] == "${{ github.event_name == 'pull_request' }}"
    group = concurrency["group"]
    assert "github.event_name == 'push'" in group
    assert "github.sha" in group
    assert "github.ref" in group, "PRs keep one group per ref"


def test_one_aggregate_check_covers_every_shard() -> None:
    jobs = workflow("ci.yml")["jobs"]
    aggregates = [j for j in jobs.values() if j.get("name") == "Required PR CI"]
    assert len(aggregates) == 1
    assert set(aggregates[0]["needs"]) == set(jobs) - {"required-pr-ci"}


def test_main_cache_saves_are_guarded_by_the_size_check() -> None:
    for name, job in workflow("ci.yml")["jobs"].items():
        steps = job["steps"]
        saves = [s for s in steps if s.get("uses", "").startswith("actions/cache/save@")]
        if not saves:
            continue
        guards = [s for s in steps if "ci_cache_guard.sh" in s.get("run", "")]
        assert len(guards) == 1, name
        assert steps.index(guards[0]) < steps.index(saves[0]), name
        assert f"steps.{guards[0]['id']}.outputs.oversize != 'true'" in saves[0]["if"], name
        assert any(
            s.get("run") == "exit 1" and f"steps.{guards[0]['id']}.outputs.oversize == 'true'" in s["if"] for s in steps
        ), f"{name}: an oversize cache must fail the job loudly"


def test_slow_test_checks_run_in_every_pr_test_job_and_budgets_are_consistent() -> None:
    jobs = workflow("ci.yml")["jobs"]
    for name, flag in (("python", "--pytest"), ("tests", "--go")):
        assert any("check_slow_tests.py" in s.get("run", "") and flag in s["run"] for s in jobs[name]["steps"]), name
    assert BUDGETS["slow_test_seconds"] <= 10
    for reason in [*BUDGETS["slow_tests"]["go"].values(), *BUDGETS["slow_tests"]["python"].values()]:
        assert reason.strip()
    for node in BUDGETS["slow_tests"]["python"]:
        assert (ROOT / node.split("::")[0]).is_file(), f"stale allowlist entry {node}"


def go_event(action: str, package: str, test: str, elapsed: float) -> str:
    return json.dumps({"Time": "t", "Action": action, "Package": package, "Test": test, "Elapsed": elapsed})


def test_go_slow_tests_report_parallel_subtests_under_their_top_level_test() -> None:
    lines = [
        go_event("pass", "p", "TestA", 0),
        go_event("pass", "p", "TestA/sub", 12.5),
        go_event("pass", "p", "TestB", 9.9),
        "ordinary text",
        "2026-01-01T00:00:00Z " + go_event("fail", "q", "TestC", 11),
    ]
    assert slow.go_slow(lines, 10) == {"p TestA": 12.5, "q TestC": 11}
    assert slow.offenders({"p TestA": 12.5, "q TestC": 11}, {"p TestA": "why"}) == {"q TestC": 11}


def test_pytest_slow_tests_ignore_parameter_suffixes_and_fast_tests() -> None:
    lines = [
        "============ slowest 30 durations ============",
        "11.15s call     tests/unit/test_a.py::test_x[a-b]",
        " 3.00s setup    tests/unit/test_a.py::test_y",
        "10.00s call     tests/unit/test_a.py::test_z",
    ]
    assert slow.pytest_slow(lines, 10) == {"tests/unit/test_a.py::test_x": 11.15}


def test_slow_test_script_fails_on_an_unlisted_offender(tmp_path: Path) -> None:
    log = tmp_path / "go.json"
    log.write_text(go_event("pass", "sdo.dev/new", "TestNewSlow", 30) + "\n")
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts/check_slow_tests.py"), "--go", str(log)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1
    assert "sdo.dev/new TestNewSlow took 30.0s" in result.stdout
    log.write_text(go_event("pass", "sdo.dev/new", "TestFast", 1) + "\n")
    ok = subprocess.run([sys.executable, str(ROOT / "scripts/check_slow_tests.py"), "--go", str(log)], check=False)
    assert ok.returncode == 0


@pytest.mark.parametrize(("limit", "oversize"), [(1, True), (100000, False)])
def test_cache_guard_flags_an_oversize_cache(tmp_path: Path, limit: int, oversize: bool) -> None:
    (tmp_path / "cache").mkdir()
    (tmp_path / "cache" / "blob").write_bytes(os.urandom(3 * 1024 * 1024))
    output = tmp_path / "out"
    result = subprocess.run(
        [
            "bash",
            str(ROOT / "scripts/ci_cache_guard.sh"),
            str(limit),
            str(tmp_path / "cache"),
            str(tmp_path / "missing"),
        ],
        env={**os.environ, "GITHUB_OUTPUT": str(output)},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    values = dict(line.split("=") for line in output.read_text().splitlines())
    assert 3 <= int(values["size_mb"]) <= 4, "compressed archive of incompressible data"
    assert values["oversize"] == str(oversize).lower()
    assert ("::error::" in result.stdout) is oversize


def test_percentile_is_nearest_rank() -> None:
    assert health.percentile(range(1, 101), 0.95) == 95
    assert health.percentile([7], 0.5) == 7
    assert health.percentile([], 0.5) != health.percentile([], 0.5)  # NaN


def job(name: str, start: str, end: str, conclusion: str = "success") -> dict:
    return {"name": name, "conclusion": conclusion, "started_at": start, "completed_at": end}


def test_health_budgets_fail_on_slow_jobs_and_slow_queue() -> None:
    budget = {"job_p95_seconds": 180, "queue_p95_minutes": 8}
    durations = health.job_durations(
        [
            [job("fast", "2026-01-01T00:00:00Z", "2026-01-01T00:01:00Z")],
            [job("slow", "2026-01-01T00:00:00Z", "2026-01-01T00:04:00Z")],
            [job("slow", "2026-01-01T00:00:00Z", "2026-01-01T00:10:00Z", "failure")],
        ]
    )
    assert durations == {"fast": [60.0], "slow": [240.0]}
    failures = health.evaluate(budget, durations, [60.0, 9 * 60.0])
    assert [f.split(":")[0] for f in failures] == ["slow", "merge queue"]
    assert health.evaluate(budget, {"fast": [60.0]}, [120.0]) == []


def test_queue_latency_and_outcome_rates() -> None:
    runs = [
        {
            "event": "merge_group",
            "head_branch": "gh-readonly-queue/main/pr-7-abc",
            "created_at": "2026-01-01T00:00:00Z",
            "conclusion": "success",
            "run_attempt": 1,
        },
        {
            "event": "merge_group",
            "head_branch": "gh-readonly-queue/main/pr-8-abc",
            "created_at": "2026-01-01T00:00:00Z",
            "conclusion": "cancelled",
            "run_attempt": 2,
        },
        {
            "event": "pull_request",
            "head_branch": "x",
            "created_at": "2026-01-01T00:00:00Z",
            "conclusion": "success",
            "run_attempt": 1,
        },
    ]
    merged = {7: "2026-01-01T00:06:00Z", 8: None}
    assert health.queue_latencies(runs, merged.get) == [360.0]
    assert health.outcome_rates(runs) == {"cancelled": 1 / 3, "rerun": 1 / 3, "ejected": 1 / 3}


def test_health_workflow_is_hourly_read_only() -> None:
    config = workflow("ci-health.yml")
    triggers = config.get("on", config.get(True, {}))
    assert triggers["schedule"] == [{"cron": "23 * * * *"}]
    assert set(config["permissions"].values()) == {"read"}
