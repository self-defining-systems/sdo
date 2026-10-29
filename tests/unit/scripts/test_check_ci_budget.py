"""Queue-free CI execution budgets and the required workflow policy."""

from __future__ import annotations

import copy
import importlib.util
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location("ci_budget", ROOT / "scripts/check_ci_budget.py")
assert SPEC is not None
assert SPEC.loader is not None
budget = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = budget
SPEC.loader.exec_module(budget)


def workflow() -> dict:
    return {
        "jobs": {
            "changes": {"name": "affected jobs"},
            "tests": {
                "name": "test (${{ matrix.shard }})",
                "needs": "changes",
                "strategy": {"matrix": {"include": [{"shard": "a"}, {"shard": "b"}]}},
            },
            "build": {"name": "build", "needs": ["changes"]},
            "deploy": {"name": "deploy", "needs": ["build", "tests"]},
            "required-pr-ci": {
                "name": "Required PR CI",
                "needs": ["deploy", "changes", "tests", "build"],
                "timeout-minutes": 1,
            },
        }
    }


def job(name: str, seconds: int, offset: int = 0, *, status: str = "completed", conclusion: str = "success") -> dict:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=offset)
    return {
        "name": name,
        "status": status,
        "conclusion": conclusion,
        "started_at": start.isoformat(),
        "completed_at": (start + timedelta(seconds=seconds)).isoformat(),
    }


def test_longest_dependency_chain_excludes_queues_and_parallel_siblings() -> None:
    jobs = [
        job("affected jobs", 10),
        job("test (a)", 100, 5000),
        job("test (b)", 200, 9000),
        job("build", 300, 100),
        job("deploy", 50, 20000),
    ]
    report = budget.measure(workflow(), jobs)
    assert report.ancestor_seconds == 360  # changes -> build -> deploy
    assert report.guarded_seconds == 420  # reserve the entire aggregator timeout
    assert report.path == ("affected jobs", "build", "deploy")
    assert "build" in report.summary()


@pytest.mark.parametrize(("seconds", "passes"), [(530, True), (531, False)])
def test_ten_minute_boundary_reserves_required_job_tail(seconds: int, passes: bool) -> None:
    jobs = [job("affected jobs", 10), job("test (a)", seconds), job("test (b)", 1), job("build", 1), job("deploy", 0)]
    assert budget.measure(workflow(), jobs).within_budget is passes


def test_skipped_jobs_add_no_duration_but_retain_dependency_edges() -> None:
    jobs = [
        job("affected jobs", 10),
        job("test (a)", 99999, conclusion="skipped"),
        job("test (b)", 99999, conclusion="skipped"),
        job("build", 20),
        job("deploy", 0),
    ]
    assert budget.measure(workflow(), jobs).ancestor_seconds == 30


@pytest.mark.parametrize("mutation", ["missing", "running", "unknown", "timestamps", "cycle"])
def test_incomplete_or_ambiguous_evidence_fails_closed(mutation: str) -> None:
    config = workflow()
    jobs = [job("affected jobs", 10), job("test (a)", 1), job("test (b)", 1), job("build", 20), job("deploy", 0)]
    if mutation == "missing":
        jobs.pop()
    elif mutation == "running":
        jobs[-1]["status"] = "in_progress"
    elif mutation == "unknown":
        jobs.append(job("unaccounted work", 700))
    elif mutation == "timestamps":
        jobs[0]["completed_at"] = None
    else:
        config["jobs"]["changes"]["needs"] = "deploy"
    with pytest.raises(ValueError, match="Missing|not completed|Unknown|timestamps|Cyclic"):
        budget.measure(config, jobs)


def test_real_pr_workflow_enforces_budget_and_job_timeouts() -> None:
    config = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    budget.validate_policy(config)


@pytest.mark.parametrize(
    "mutation",
    [
        "timeout",
        "missing_timeout",
        "removed",
        "conditional",
        "continued",
        "permission",
        "job_count",
        "matrix_count",
        "matrix_axes",
    ],
)
def test_policy_cannot_silently_disable_enforcement(mutation: str) -> None:
    config = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    config = copy.deepcopy(config)
    required = config["jobs"]["required-pr-ci"]
    if mutation == "job_count":
        config["jobs"]["extra1"] = {"timeout-minutes": 1}
        config["jobs"]["extra2"] = {"timeout-minutes": 1}
    elif mutation == "matrix_axes":
        config["jobs"]["go"]["strategy"] = {"matrix": {"part": [1, 2, 3]}}
    elif mutation == "matrix_count":
        config["jobs"]["go"]["strategy"] = {"matrix": {"include": [{"part": i} for i in range(3)]}}
    elif mutation == "timeout":
        config["jobs"]["images"]["timeout-minutes"] = 11
    elif mutation == "missing_timeout":
        config["jobs"]["images"].pop("timeout-minutes", None)
    elif mutation == "permission":
        required["permissions"] = {"contents": "read"}
    else:
        gates = [s for s in required["steps"] if "scripts/check_ci_budget.py" in s.get("run", "")]
        assert gates
        gate = gates[0]
        if mutation == "removed":
            required["steps"].remove(gate)
        elif mutation == "conditional":
            gate["if"] = "false"
        else:
            gate["continue-on-error"] = True
    with pytest.raises(ValueError, match="timeout|Required PR CI|test jobs"):
        budget.validate_policy(config)


def test_policy_rejects_a_fourth_work_job() -> None:
    config = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    # Four independent work jobs exceed the user's runner concurrency target.
    assert len(config["jobs"]) - 1 == 3
    config["jobs"]["extra"] = {"timeout-minutes": 9}
    config["jobs"]["required-pr-ci"]["needs"].append("extra")
    with pytest.raises(ValueError, match="maximum is three"):
        budget.validate_policy(config)


def test_nightly_extends_pr_checks_without_hiding_large_response_failure() -> None:
    pr = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    nightly = yaml.safe_load((ROOT / ".github/workflows/nightly.yml").read_text())
    # PyYAML follows YAML 1.1 and parses the workflow key `on` as True.
    triggers = nightly.get("on", nightly.get(True))
    assert triggers["schedule"]
    assert "workflow_dispatch" in triggers
    pr_latency = next(
        s["run"] for s in pr["jobs"]["go"]["steps"] if s.get("name") == "Normal-build proxy latency smoke"
    )
    nightly_latency = nightly["jobs"]["acceptance"]["steps"][2]["run"]
    for name in ("TestProxyWarmAddedP95", "TestLargeResponseWarmAddedP95", "TestProjectionTailLatency"):
        assert name in pr_latency
        assert name in nightly_latency
    assert "-count=1" in pr_latency
    assert "-count=3" in nightly_latency
    assert any(s.get("run") == "bash scripts/test_ci_fuzz.sh 1s" for s in pr["jobs"]["go"]["steps"])
    assert any(s.get("run") == "bash scripts/test_ci_fuzz.sh 30s" for s in nightly["jobs"]["acceptance"]["steps"])


def test_independent_smokes_still_execute_after_a_module_test_failure() -> None:
    config = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    steps = config["jobs"]["go"]["steps"]
    for name in ("Normal-build proxy latency smoke", "Fuzz boundary smoke"):
        step = next(step for step in steps if step.get("name") == name)
        assert step.get("if") == "!cancelled()"
        assert not step.get("continue-on-error", False)


def test_heavy_image_cache_publication_is_nightly_and_prs_consume_it() -> None:
    pr = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    nightly = yaml.safe_load((ROOT / ".github/workflows/nightly.yml").read_text())
    pr_build = next(s for s in pr["jobs"]["images"]["steps"] if "docker/bake-action@" in s.get("uses", ""))
    nightly_build = next(s for s in nightly["jobs"]["images"]["steps"] if "docker/bake-action@" in s.get("uses", ""))
    assert "*.cache-from=type=gha,scope=sdo-images" in pr_build["with"]["set"]
    assert "cache-to" not in pr_build["with"]["set"]
    assert ".cache-to=type=gha,scope=sdo-images,mode=max" in nightly_build["with"]["set"]


def test_go_cache_publication_is_nightly_without_cached_test_executables() -> None:
    pr = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    nightly = yaml.safe_load((ROOT / ".github/workflows/nightly.yml").read_text())
    assert not any(
        "actions/cache/save@" in step.get("uses", "") for config in pr["jobs"].values() for step in config["steps"]
    )
    cache = next(s for s in nightly["jobs"]["go-cache"]["steps"] if "actions/cache@" in s.get("uses", ""))
    assert set(cache["with"]["path"].splitlines()) == {"~/.cache/go-build", "~/go/pkg/mod"}
    warm = nightly["jobs"]["go-cache"]["steps"][-1]["run"]
    assert "go test -race -run '^$'" in warm
    assert "sdo/operator/runtime" in warm
    assert "go1.24.5" in warm
