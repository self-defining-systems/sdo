"""CI speed and reliability ratchets: timeouts, concurrency, slow tests, cache size and health budgets."""

from __future__ import annotations

import importlib.util
import json
import os
import re
import subprocess
import sys
import time
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
cache_budget = load_script("ci_cache_budget")


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


def test_a_run_cancelled_by_concurrency_never_reports_a_failing_required_check() -> None:
    aggregate = workflow("ci.yml")["jobs"]["required-pr-ci"]
    # always() runs the gate in a cancelled run, where its cancelled needs fail it.
    assert aggregate["if"] == "${{ !cancelled() }}"


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


def test_saved_go_caches_hold_only_the_entries_the_run_used() -> None:
    """Each saving job ages the restored entries, then drops what the run never refreshed, before the size check."""
    for name, job in workflow("ci.yml")["jobs"].items():
        steps = job["steps"]
        if not any(s.get("uses", "").startswith("actions/cache/save@") for s in steps):
            continue

        def position(command: str, steps: list[dict] = steps) -> int:
            return next(i for i, s in enumerate(steps) if command in s.get("run", ""))

        restore = next(i for i, s in enumerate(steps) if s.get("uses", "").startswith("actions/cache/restore@"))
        assert restore < position("ci_go_cache.sh mark") < position("ci_go_cache.sh trim"), name
        assert position("ci_go_cache.sh trim") < position("ci_cache_guard.sh"), name


def test_python_shards_build_the_operator_binary_before_pytest_and_hand_it_to_the_tests() -> None:
    steps = workflow("ci.yml")["jobs"]["python"]["steps"]
    text = [s.get("run", "") for s in steps]
    build = next(i for i, t in enumerate(text) if "operator_test_binary.py" in t)
    wait = next(i for i, t in enumerate(text) if "SDO_OPERATOR_BIN=" in t)
    pytest_step = next(i for i, t in enumerate(text) if "test_python.sh" in t)
    assert build < wait < pytest_step, "no test may wait on, or be charged for, the operator link"
    assert "SDO_SKIP_OPERATOR_PREBUILD=1" in text[wait]


def _aged(path: Path, hours: float) -> None:
    stamp = time.time() - hours * 3600
    os.utime(path, (stamp, stamp))


def test_go_cache_trim_keeps_only_entries_the_run_refreshed(tmp_path: Path) -> None:
    build = tmp_path / "go-build"
    modules = tmp_path / "mod"
    (build / "ab").mkdir(parents=True)
    (modules / "cache/download/m/@v").mkdir(parents=True)
    used, unused = build / "ab/used-d", build / "ab/unused-d"
    for entry in (used, unused):
        entry.write_text("archive")
        _aged(entry, 0.5)  # both were used within the last hour of an earlier run
    zipped = modules / "cache/download/m/@v/v1.zip"
    kept = modules / "cache/download/m/@v/v1.mod"
    zipped.write_text("zip")
    kept.write_text("module m")
    env = {**os.environ, "GOCACHE": str(build), "GOMODCACHE": str(modules)}

    def run(mode: str) -> None:
        subprocess.run(["bash", str(ROOT / "scripts/ci_go_cache.sh"), mode], env=env, check=True)

    run("trim")
    assert used.exists(), "an hour-old age cutoff cannot tell the two apart; this is why entries are aged first"
    assert unused.exists()
    run("mark")
    _aged(used, 0)  # Go refreshes an entry it reads that is more than an hour old
    run("trim")
    assert used.exists()
    assert not unused.exists()
    assert kept.exists()
    assert not zipped.exists(), "extracted modules are read without their zip archives"


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


def test_slow_test_script_warns_about_a_test_over_half_the_budget(tmp_path: Path) -> None:
    log = tmp_path / "go.json"
    log.write_text(go_event("pass", "sdo.dev/new", "TestVaries", 6) + "\n")
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts/check_slow_tests.py"), "--go", str(log)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, "a warning never fails the job"
    assert "::warning::sdo.dev/new TestVaries took 6.0s" in result.stdout


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


def test_health_workflow_runs_hourly_and_after_every_main_ci_run_read_only() -> None:
    config = workflow("ci-health.yml")
    triggers = config.get("on", config.get(True, {}))
    assert triggers["schedule"] == [{"cron": "23 * * * *"}]
    assert triggers["workflow_run"] == {"workflows": ["CI"], "types": ["completed"], "branches": ["main"]}
    assert set(config["permissions"].values()) == {"read"}


GO_FAIL = '{"Time":"t","Action":"fail","Package":"sdo.dev/x","Test":"TestA","Elapsed":1}'


def test_failed_tests_come_from_pytest_go_json_and_the_slow_guard() -> None:
    log = "\n".join(
        [
            "2026-01-01T00:00:00.0000000Z " + GO_FAIL,
            "2026-01-01T00:00:00.0000000Z " + GO_FAIL.replace("TestA", "TestA/sub"),
            "2026-01-01T00:00:01Z \x1b[31mFAILED tests/unit/t.py::test_b[x] - AssertionError: nope\x1b[0m",
            "2026-01-01T00:00:02Z ##[error]tests/unit/t.py::test_slow took 10.0s (PR budget 10s)",
            "2026-01-01T00:00:03Z PASSED tests/unit/t.py::test_ok",
        ]
    )
    assert health.failed_tests(log) == ["sdo.dev/x TestA", "tests/unit/t.py::test_b[x]", "tests/unit/t.py::test_slow"]


def attempt(job_name: str, conclusion: str, at: str, sha: str = "s1", run: int = 1) -> dict:
    return {"sha": sha, "job": job_name, "conclusion": conclusion, "at": at, "run_id": run, "job_id": run, "step": "t"}


def test_a_flake_is_a_failure_followed_by_a_pass_on_the_same_commit() -> None:
    rows = [
        attempt("py", "failure", "2026-01-01T00:00:00Z"),
        attempt("py", "success", "2026-01-01T00:05:00Z"),
        attempt("go", "failure", "2026-01-01T00:00:00Z"),  # never passed: a real failure
        attempt("py", "failure", "2026-01-02T00:00:00Z", sha="s2"),
        attempt("py", "success", "2026-01-02T00:05:00Z", sha="s3"),  # another commit: not a rerun
        attempt("go", "success", "2026-01-01T00:05:00Z", sha="s9"),
    ]
    assert [(r["job"], r["sha"]) for r in health.flaky_jobs(rows)] == [("py", "s1")]


def test_flakes_are_ranked_by_count_and_untestable_jobs_name_their_step() -> None:
    rows = [attempt("py", "failure", "t", run=run) for run in (1, 2)] + [attempt("go", "failure", "t", run=3)]
    logs = {1: "FAILED a.py::t - x", 2: "FAILED a.py::t - x", 3: ""}
    ranked = health.rank_flakes(rows, lambda row: logs[row["run_id"]])
    assert [(e["test"], e["count"]) for e in ranked] == [("a.py::t", 2), ("(no test id; failed step: t)", 1)]
    assert "a.py::t" in health.render_flakes(ranked, [], "o/r", 100)


def test_flake_report_reads_attempts_through_the_api() -> None:
    runs = [
        {
            "id": 1,
            "head_sha": "s",
            "run_attempt": 2,
            "conclusion": "success",
            "event": "pull_request",
            "created_at": "2026-01-01T00:00:00Z",
        },
        {
            "id": 2,
            "head_sha": "t",
            "run_attempt": 1,
            "conclusion": "cancelled",
            "event": "push",
            "created_at": "2026-01-01T00:00:00Z",
        },
    ]
    jobs = {
        1: [
            {
                "id": 11,
                "name": "py",
                "conclusion": "failure",
                "completed_at": "2026-01-01T00:01:00Z",
                "steps": [{"name": "pytest", "conclusion": "failure"}],
            }
        ],
        2: [{"id": 12, "name": "py", "conclusion": "success", "completed_at": "2026-01-01T00:09:00Z", "steps": []}],
    }

    def api(path: str) -> dict:
        if path.startswith("actions/workflows"):
            return {"workflow_runs": runs}
        attempt_number = int(re.search(r"attempts/(\d)", path)[1])  # type: ignore[index]
        return {"jobs": jobs[attempt_number]}

    report = health.flake_report(
        api, lambda path: "FAILED t.py::test_x - boom", "o/r", health.parse_time("2025-01-01T00:00:00Z")
    )
    assert "| 1 | py | `t.py::test_x` |" in report
    assert "| [2](https://github.com/o/r/actions/runs/2) | push | cancelled |" in report


def test_queue_latency_counts_from_the_enqueue_event_when_it_is_known() -> None:
    runs = [
        {
            "event": "merge_group",
            "head_branch": "gh-readonly-queue/main/pr-7-abc",
            "created_at": "2026-01-01T00:02:00Z",
            "conclusion": "success",
            "run_attempt": 1,
        }
    ]
    merged = {7: "2026-01-01T00:06:00Z"}
    enqueued = {7: "2026-01-01T00:00:30Z"}
    assert health.queue_latencies(runs, merged.get, enqueued.get) == [330.0]
    assert health.queue_latencies(runs, merged.get, {}.get) == [240.0], "falls back to the merge-group run"


def push(sha: str, created: str, conclusion: str) -> dict:
    return {"event": "push", "head_sha": sha, "created_at": created, "conclusion": conclusion}


def test_main_health_reports_completion_red_time_and_post_merge_failures() -> None:
    runs = [
        push("a", "2026-01-01T00:00:00Z", "success"),
        push("b", "2026-01-01T00:10:00Z", "failure"),
        push("c", "2026-01-01T00:25:00Z", "cancelled"),
        push("d", "2026-01-01T00:40:00Z", "success"),
    ]
    main = health.main_health(runs)
    assert main["completion"] == 3 / 4
    assert main["red_minutes"] == [30.0], "red from the failing push until the next green one"
    assert [r["head_sha"] for r in main["failures"]] == ["b"]


def test_a_cancelled_run_that_a_later_push_superseded_is_counted_apart() -> None:
    def run(number: int, created: str, conclusion: str) -> dict:
        return {
            "id": number,
            "event": "pull_request",
            "head_branch": "feature",
            "created_at": created,
            "conclusion": conclusion,
            "run_attempt": 1,
        }

    runs = [
        run(3, "2026-01-01T00:10:00Z", "success"),
        run(2, "2026-01-01T00:05:00Z", "cancelled"),
        run(1, "2026-01-01T00:00:00Z", "cancelled"),
    ]
    assert [r["id"] for r in health.superseded(runs)] == [2, 1]
    assert health.superseded(runs[:1] + [run(9, "2026-01-01T00:20:00Z", "cancelled")]) == []


def test_the_health_report_covers_queue_enqueue_time_and_main() -> None:
    runs = [
        {
            "id": 1,
            "event": "merge_group",
            "head_branch": "gh-readonly-queue/main/pr-7-abc",
            "head_sha": "m",
            "created_at": "2026-01-01T00:01:00Z",
            "conclusion": "success",
            "run_attempt": 1,
        },
        push("a", "2026-01-01T00:07:00Z", "failure"),
        push("b", "2026-01-01T00:20:00Z", "success"),
    ]
    jobs = [
        {
            "name": "py",
            "conclusion": "success",
            "started_at": "2026-01-01T00:01:00Z",
            "completed_at": "2026-01-01T00:03:00Z",
        }
    ]

    def api(path: str) -> object:
        if path.startswith("actions/workflows"):
            return {"workflow_runs": runs}
        if path.startswith("actions/runs/"):
            return {"jobs": jobs}
        if path == "pulls/7":
            return {"merged_at": "2026-01-01T00:06:00Z"}
        assert path.startswith("issues/7/timeline")
        return [{"event": "added_to_merge_queue", "created_at": "2026-01-01T00:00:00Z"}]

    measured = health.gather(api, {"since": "2025-01-01T00:00:00Z", "runs": 50})
    assert measured.latencies == [360.0]
    report = health.render(measured)
    assert "| merge_group | 1 | 6.0 | 6.0 |" in report
    assert "| push runs | 100% | 1 | 1 | 13 |" in report


CACHE = BUDGETS["cache"]
HASH64 = "a" * 64
SHA40 = "b" * 40


def cache_row(key: str, cache_id: int, created: str, used: str, mb: int = 100) -> dict:
    return {
        "id": cache_id,
        "key": key,
        "size_in_bytes": mb * 1024 * 1024,
        "created_at": created,
        "last_accessed_at": used,
    }


def test_cache_family_drops_the_lockfile_hash_commit_and_tool_versions() -> None:
    key = f"ci-py-v3-Linux-X64-1.26.8-core-{HASH64}-{SHA40}"
    assert cache_budget.family_of(key) == "ci-py-v3-Linux-X64-core"
    assert cache_budget.family_of(f"ci-shared-v1-Linux-X64-1.26.8-{HASH64}-{SHA40}") == "ci-shared-v1-Linux-X64"


def test_prune_keeps_the_newest_key_and_spares_a_key_used_seconds_ago() -> None:
    now = cache_budget.parse_time("2026-01-01T12:00:00Z")
    family = f"ci-tests-v2-Linux-X64-1.26.8-rest-{HASH64}"
    rows = [
        cache_row(f"{family}-{'1' * 40}", 1, "2026-01-01T09:00:00Z", "2026-01-01T09:30:00Z"),
        cache_row(f"{family}-{'2' * 40}", 2, "2026-01-01T10:00:00Z", "2026-01-01T11:59:40Z"),
        cache_row(f"{family}-{'3' * 40}", 3, "2026-01-01T11:00:00Z", "2026-01-01T11:00:00Z"),
        cache_row(f"{family}-{'4' * 40}", 4, "2026-01-01T07:00:00Z", "2026-01-01T07:00:00Z"),
        # The previous main push's key, restored by the next push's job at its start two minutes
        # before this prune (two merges a minute or two apart): superseded, not "in use".
        cache_row(f"{family}-{'5' * 40}", 5, "2026-01-01T10:30:00Z", "2026-01-01T11:58:00Z"),
    ]
    deleted: list[int] = []
    removed = cache_budget.prune(CACHE, rows, deleted.append, now)
    assert sorted(deleted) == [1, 4, 5]  # 3 is the newest; 2 was restored seconds ago
    assert sorted(row["id"] for row in removed) == [1, 4, 5]


def test_prune_honours_a_family_prefix() -> None:
    now = cache_budget.parse_time("2026-01-01T12:00:00Z")
    rows = [
        cache_row(
            f"ci-shared-v1-Linux-X64-1.26.8-{HASH64}-{'1' * 40}", 1, "2026-01-01T09:00:00Z", "2026-01-01T09:00:00Z"
        ),
        cache_row(
            f"ci-shared-v1-Linux-X64-1.26.8-{HASH64}-{'2' * 40}", 2, "2026-01-01T10:00:00Z", "2026-01-01T10:00:00Z"
        ),
        cache_row(
            f"ci-py-v3-Linux-X64-1.26.8-core-{HASH64}-{'1' * 40}", 3, "2026-01-01T09:00:00Z", "2026-01-01T09:00:00Z"
        ),
        cache_row(
            f"ci-py-v3-Linux-X64-1.26.8-core-{HASH64}-{'2' * 40}", 4, "2026-01-01T10:00:00Z", "2026-01-01T10:00:00Z"
        ),
    ]
    deleted: list[int] = []
    cache_budget.prune(CACHE, rows, deleted.append, now, "ci-py-v3-Linux-X64-core")
    assert deleted == [3]


NOW = cache_budget.parse_time("2026-01-01T12:00:00Z")


def shared_key(sha_digit: str) -> str:
    return f"ci-shared-v1-Linux-X64-1.26.8-{HASH64}-{sha_digit * 40}"


def test_cache_alarm_fires_at_eighty_percent_and_on_unknown_keys() -> None:
    limit = CACHE["repo_limit_mb"] * 1024 * 1024
    quiet = [cache_row("envtest-assets-1.34.0-Linux-X64", 1, "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", 40)]
    assert cache_budget.usage_failures(CACHE, int(limit * 0.79), quiet, NOW) == []
    at_limit = cache_budget.usage_failures(CACHE, int(limit * 0.8), quiet, NOW)
    assert any("80%" in f and "envtest-assets-1.34.0-Linux-X64" in f and "Action:" in f for f in at_limit)
    stray = [cache_row("mystery-cache-v1", 3, "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")]
    assert any("mystery-cache-v1" in f and "no family" in f for f in cache_budget.usage_failures(CACHE, 0, stray, NOW))


def test_overlapping_runs_leaving_two_keys_for_a_few_minutes_do_not_alarm() -> None:
    # The state seen at 08:39, 09:18 and 09:31 UTC: every family held two generations because pushes and PR
    # runs landed minutes apart. The prune converges from it, so it must not fail the hourly health job.
    rows = [
        cache_row(shared_key("1"), 1, "2026-01-01T11:50:00Z", "2026-01-01T11:57:00Z", 650),
        cache_row(shared_key("2"), 2, "2026-01-01T11:56:00Z", "2026-01-01T11:56:00Z", 650),
    ]
    total = sum(row["size_in_bytes"] for row in rows)
    assert cache_budget.usage_failures(CACHE, total, rows, NOW) == []


def test_a_superseded_key_that_survives_past_the_stale_window_alarms_with_key_id_and_action() -> None:
    old = cache_row(shared_key("1"), 4242, "2026-01-01T07:00:00Z", "2026-01-01T07:30:00Z", 650)
    new = cache_row(shared_key("2"), 4243, "2026-01-01T11:56:00Z", "2026-01-01T11:56:00Z", 650)
    failures = cache_budget.usage_failures(CACHE, old["size_in_bytes"] * 2, [old, new], NOW)
    assert len(failures) == 1
    assert old["key"] in failures[0]
    assert "id 4242" in failures[0]
    assert new["key"] not in failures[0]
    assert "prune ci-shared-v1-Linux-X64" in failures[0]
    assert "Action:" in failures[0]
    # The same key one minute inside the window is still the transient.
    recent = {**old, "last_accessed_at": "2026-01-01T09:01:00Z"}
    assert cache_budget.usage_failures(CACHE, old["size_in_bytes"] * 2, [recent, new], NOW) == []


def test_a_newest_key_over_the_family_budget_alarms_and_names_it() -> None:
    bulky = cache_row(shared_key("1"), 2, "2026-01-01T11:00:00Z", "2026-01-01T11:00:00Z", 5000)
    failures = cache_budget.usage_failures(CACHE, 0, [bulky], NOW)
    assert len(failures) == 1
    assert bulky["key"] in failures[0]
    assert "max_mb" in failures[0]


def test_cache_family_budgets_sum_to_at_most_seventy_percent_of_the_repository_limit() -> None:
    families = CACHE["families"].values()
    steady = sum(f["max_mb"] * f["keep"] for f in families)
    assert steady <= 0.7 * CACHE["repo_limit_mb"]


def workflow_cache_keys(name: str) -> list[tuple[str, str]]:
    """(job, key) for every cache save in a workflow (explicit saves; a plain cache action saves after the job)."""
    return [
        (job_name, step["with"]["key"])
        for job_name, job in workflow(name)["jobs"].items()
        for step in job.get("steps", [])
        if str(step.get("uses", "")).startswith("actions/cache/save@")
    ]


@pytest.mark.parametrize("name", ["ci.yml", "nightly.yml"])
def test_every_cache_save_has_a_family_budget_and_a_prune_step(name: str) -> None:
    keys = workflow_cache_keys(name)
    assert keys
    for job_name, key in keys:
        steps = workflow(name)["jobs"][job_name]["steps"]
        job = workflow(name)["jobs"][job_name]
        shards = job.get("strategy", {}).get("matrix", {}).get("shard", ["-"])
        for shard in shards:
            rendered = re.sub(r"\$\{\{ hashFiles\([^}]*\) \}\}|\$\{\{ github\.sha \}\}", HASH64, key)
            rendered = rendered.replace("${{ runner.os }}", "Linux").replace("${{ runner.arch }}", "X64")
            rendered = rendered.replace("${{ matrix.shard }}", shard)
            assert cache_budget.budget_for(cache_budget.family_of(rendered), CACHE["families"]) is not None, rendered
        assert any("ci_cache_budget.py prune" in str(step.get("run", "")) for step in steps), job_name
        assert workflow(name)["jobs"][job_name]["permissions"]["actions"] == "write"


def test_health_workflow_checks_the_cache_budget_read_only() -> None:
    steps = workflow("ci-health.yml")["jobs"]["health"]["steps"]
    assert any("ci_cache_budget.py check" in str(step.get("run", "")) for step in steps)


TEST_SCRIPTS = [
    "check_errors.sh",
    "test_controller_e2e.sh",
    "test_go_ci.sh",
    "test_go_foundation.sh",
    "test_python.sh",
    "test_rendered_lock_kind.sh",
]


@pytest.mark.parametrize("name", TEST_SCRIPTS)
def test_test_scripts_run_with_an_isolated_git_config(name: str) -> None:
    assert "isolate_git_config.sh" in (ROOT / "scripts" / name).read_text()


def test_isolated_git_config_makes_an_identity_dependent_commit_fail_like_a_hosted_runner(tmp_path: Path) -> None:
    ambient = {
        **os.environ,
        "GIT_AUTHOR_NAME": "dev",
        "GIT_AUTHOR_EMAIL": "dev@example.com",
        "GIT_COMMITTER_NAME": "dev",
        "GIT_COMMITTER_EMAIL": "dev@example.com",
    }
    script = (
        f'source "{ROOT}/scripts/isolate_git_config.sh"; cd "{tmp_path}"; git init -q . && '
        'git commit -q --allow-empty -m x; echo "rc=$?"; '
        'git -c user.name=t -c user.email=t@example.com commit -q --allow-empty -m y; echo "rc=$?"'
    )
    result = subprocess.run(["bash", "-c", script], env=ambient, capture_output=True, text=True, check=False)
    assert "rc=128" in result.stdout, result
    assert result.stdout.rstrip().endswith("rc=0"), result
    assert "Author identity unknown" in result.stderr or "empty ident" in result.stderr
