#!/usr/bin/env python3
"""Hourly CI health: per-job duration percentiles, merge-queue latency, run outcomes and flakes.

Read-only GitHub API use through the `gh` CLI (GH_TOKEN, default workflow token).
Writes a Markdown summary to $GITHUB_STEP_SUMMARY (or stdout) and exits non-zero when a
budget in .github/ci_budgets.json ("health") is exceeded. A flake is a job that failed and
later passed on the same commit (a rerun or a second run); the report ranks the failing
tests behind those jobs from their logs. Flakes are reported, never a budget failure: the
policy (fix or quarantine within a day) lives in docs/testing-guide.md. Runs created before
health.since are ignored so a fixed regression stops failing once it leaves the window.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import subprocess
import sys
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping
from datetime import datetime
from pathlib import Path
from typing import Any, NamedTuple

from ci_github_api import GithubApiUnavailable, gh_json, gh_text

BUDGETS = Path(__file__).resolve().parents[1] / ".github" / "ci_budgets.json"
PR_EVENTS = ("pull_request", "merge_group")
AGGREGATE_JOB = "Required PR CI"
ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
LOG_PREFIX = re.compile(r"^\ufeff?\d{4}-\d\d-\d\dT[\d:.]+Z ")
PYTEST_FAILED = re.compile(r"^(?:FAILED|ERROR) (\S+?::\S+?)(?: - .*)?$")
SLOW_TEST = re.compile(r"^##\[error\](\S+) took [\d.]+s \(")

Json = Mapping[str, Any]
Api = Callable[[str], Any]
Text = Callable[[str], str]


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def percentile(values: Iterable[float], fraction: float) -> float:
    """Nearest-rank percentile; NaN for no samples."""
    ordered = sorted(values)
    if not ordered:
        return math.nan
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


def job_durations(jobs_by_run: Iterable[Iterable[Json]]) -> dict[str, list[float]]:
    """Seconds from runner start to completion of each successful job, by job name."""
    durations: dict[str, list[float]] = defaultdict(list)
    for jobs in jobs_by_run:
        for job in jobs:
            if job.get("conclusion") != "success" or not job.get("started_at") or not job.get("completed_at"):
                continue
            seconds = (parse_time(job["completed_at"]) - parse_time(job["started_at"])).total_seconds()
            durations[job["name"]].append(seconds)
    return durations


def queue_latencies(
    runs: Iterable[Json],
    merged_at: Callable[[int], str | None],
    enqueued_at: Callable[[int], str | None] = lambda _number: None,
) -> list[float]:
    """Seconds from a PR entering the merge queue to its merge.

    The enqueue time is the PR timeline's ``added_to_merge_queue`` event; without one the merge-group
    run's creation stands in for it (it starts a little after the enqueue).
    """
    latencies = []
    for run in runs:
        if run["event"] != "merge_group":
            continue
        match = re.search(r"/pr-(\d+)-", run.get("head_branch") or "")
        merged = merged_at(int(match[1])) if match else None
        if merged:
            start = enqueued_at(int(match[1])) or run["created_at"]
            latencies.append((parse_time(merged) - parse_time(start)).total_seconds())
    return latencies


def superseded(runs: Iterable[Json]) -> list[Json]:
    """Cancelled pull-request runs that a later run on the same branch replaced (concurrency doing its job)."""
    listed = list(runs)
    return [
        r
        for r in listed
        if r["conclusion"] == "cancelled"
        and r["event"] == "pull_request"
        and any(
            o["event"] == r["event"]
            and o["head_branch"] == r["head_branch"]
            and parse_time(o["created_at"]) > parse_time(r["created_at"])
            for o in listed
        )
    ]


def main_health(push_runs: Iterable[Json]) -> dict[str, Any]:
    """Main branch: share of pushes that ran to completion, red intervals, and the failing runs."""
    ordered = sorted(push_runs, key=lambda r: parse_time(r["created_at"]))
    red_since: datetime | None = None
    red_minutes: list[float] = []
    for run in ordered:
        created = parse_time(run["created_at"])
        if run["conclusion"] == "failure" and red_since is None:
            red_since = created
        elif run["conclusion"] == "success" and red_since is not None:
            red_minutes.append((created - red_since).total_seconds() / 60)
            red_since = None
    done = sum(r["conclusion"] in ("success", "failure") for r in ordered)
    return {
        "completion": done / len(ordered) if ordered else 1.0,
        "red_minutes": red_minutes,
        "still_red": red_since is not None,
        "failures": [r for r in ordered if r["conclusion"] == "failure"],
    }


def outcome_rates(runs: list[Json]) -> dict[str, float]:
    total = len(runs) or 1
    return {
        "cancelled": sum(r["conclusion"] == "cancelled" for r in runs) / total,
        "rerun": sum(r.get("run_attempt", 1) > 1 for r in runs) / total,
        "ejected": sum(r["event"] == "merge_group" and r["conclusion"] != "success" for r in runs) / total,
    }


def evaluate(budget: Json, durations: Mapping[str, list[float]], latencies: list[float]) -> list[str]:
    failures = []
    for name, values in sorted(durations.items()):
        p95 = percentile(values, 0.95)
        if p95 > budget["job_p95_seconds"]:
            failures.append(f"{name}: p95 {p95:.0f}s exceeds {budget['job_p95_seconds']}s")
    if latencies:
        p95 = percentile(latencies, 0.95) / 60
        if p95 > budget["queue_p95_minutes"]:
            failures.append(
                f"merge queue: p95 enqueue-to-merge {p95:.1f} min exceeds {budget['queue_p95_minutes']} min"
            )
    return failures


class Measured(NamedTuple):
    durations: dict[str, list[float]]
    latencies: list[float]
    pr_runs: list[Json]
    push_runs: list[Json]


def render(measured: Measured) -> str:
    lines = ["## CI health", "", "| job | samples | p50 s | p95 s |", "|---|---|---|---|"]
    for name, values in sorted(measured.durations.items()):
        lines.append(f"| {name} | {len(values)} | {percentile(values, 0.5):.0f} | {percentile(values, 0.95):.0f} |")
    latencies = measured.latencies
    lines += ["", "| merge queue (enqueue to merge) | samples | p50 min | p95 min |", "|---|---|---|---|"]
    p50, p95 = percentile(latencies, 0.5) / 60, percentile(latencies, 0.95) / 60
    lines.append(f"| merge_group | {len(latencies)} | {p50:.1f} | {p95:.1f} |")
    pr_rates, push_rates = outcome_rates(measured.pr_runs), outcome_rates(measured.push_runs)
    replaced = len(superseded(measured.pr_runs)) / (len(measured.pr_runs) or 1)
    lines += ["", "| runs | samples | cancelled | of which superseded by a push | rerun | ejected |"]
    lines.append("|---|---|---|---|---|---|")
    for label, runs, rates, shown in (
        ("PR and merge group", measured.pr_runs, pr_rates, f"{replaced:.0%}"),
        ("push to main", measured.push_runs, push_rates, "n/a"),
    ):
        cells = [
            label,
            len(runs),
            f"{rates['cancelled']:.0%}",
            shown,
            f"{rates['rerun']:.0%}",
            f"{rates['ejected']:.0%}",
        ]
        lines.append("| " + " | ".join(str(cell) for cell in cells) + " |")
    main = main_health(measured.push_runs)
    red = main["red_minutes"]
    lines += [
        "",
        "| main | completed | failed pushes | red intervals | longest red min |",
        "|---|---|---|---|---|",
        f"| push runs | {main['completion']:.0%} | {len(main['failures'])} | {len(red)}"
        f"{' (still red)' if main['still_red'] else ''} | {max(red, default=0):.0f} |",
    ]
    return "\n".join(lines) + "\n"


def gather(api: Api, budget: Json) -> Measured:
    since = parse_time(budget["since"])
    listing = api("actions/workflows/ci.yml/runs?per_page=100&status=completed")["workflow_runs"]
    runs = [r for r in listing if parse_time(r["created_at"]) >= since]
    pr_runs = [r for r in runs if r["event"] in PR_EVENTS][: budget["runs"]]
    push_runs = [r for r in runs if r["event"] == "push"][: budget["runs"]]
    jobs = [api(f"actions/runs/{r['id']}/jobs?per_page=100&filter=latest")["jobs"] for r in pr_runs]

    def merged_at(number: int) -> str | None:
        return api(f"pulls/{number}").get("merged_at")

    def enqueued_at(number: int) -> str | None:
        events = [e for e in api(f"issues/{number}/timeline?per_page=100") if e.get("event") == "added_to_merge_queue"]
        return events[-1]["created_at"] if events else None

    latencies = queue_latencies(pr_runs, merged_at, enqueued_at)
    return Measured(job_durations(jobs), latencies, pr_runs, push_runs)


def failed_tests(log: str) -> list[str]:
    """Test ids that failed in one job log: pytest summary lines, Go -json fail events, slow-test guard errors."""
    found: dict[str, None] = {}
    for raw in log.splitlines():
        line = LOG_PREFIX.sub("", ANSI.sub("", raw)).strip()
        if line.startswith("{") and '"Action":"fail"' in line:
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if event.get("Test") and "/" not in event["Test"]:
                found[f"{event['Package']} {event['Test']}"] = None
            continue
        match = PYTEST_FAILED.match(line) or SLOW_TEST.match(line)
        if match:
            found[match[1]] = None
    return list(found)


def job_attempts(api: Api, runs: Iterable[Json]) -> list[dict[str, Any]]:
    """Every job of every attempt of the given runs, with its commit and run."""
    rows = []
    for run in runs:
        for attempt in range(1, run.get("run_attempt", 1) + 1):
            jobs = api(f"actions/runs/{run['id']}/attempts/{attempt}/jobs?per_page=100")["jobs"]
            rows += [
                {
                    "sha": run["head_sha"],
                    "job": job["name"],
                    "conclusion": job["conclusion"],
                    "job_id": job["id"],
                    "step": next((st["name"] for st in job.get("steps", []) if st.get("conclusion") == "failure"), ""),
                    "run_id": run["id"],
                    "at": job.get("completed_at") or run["created_at"],
                }
                for job in jobs
                if job["name"] != AGGREGATE_JOB
            ]
    return rows


def flaky_jobs(rows: Iterable[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    """Failed job executions followed by a success of the same job on the same commit."""
    by_key: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in sorted(rows, key=lambda r: parse_time(r["at"])):
        by_key[(row["sha"], row["job"])].append(row)
    flaky = []
    for history in by_key.values():
        for index, row in enumerate(history):
            if row["conclusion"] == "failure" and any(
                later["conclusion"] == "success" for later in history[index + 1 :]
            ):
                flaky.append(row)
    return flaky


def rank_flakes(flaky: Iterable[Mapping[str, Any]], log_of: Callable[[Mapping[str, Any]], str]) -> list[dict[str, Any]]:
    """Failures per (job, test), most frequent first; a job with no parsable test is listed under its own name."""
    table: dict[tuple[str, str], dict[str, Any]] = {}
    for row in flaky:
        for test in failed_tests(log_of(row)) or [f"(no test id; failed step: {row.get('step') or 'unknown'})"]:
            entry = table.setdefault((row["job"], test), {"job": row["job"], "test": test, "count": 0, "runs": []})
            entry["count"] += 1
            entry["runs"].append(row["run_id"])
    return sorted(table.values(), key=lambda e: (-e["count"], e["job"], e["test"]))


def non_green_runs(runs: Iterable[Json]) -> list[Json]:
    """Cancelled, rerun or ejected (failed merge-group) runs, newest first as listed."""
    return [
        r
        for r in runs
        if r["conclusion"] == "cancelled"
        or r.get("run_attempt", 1) > 1
        or (r["event"] == "merge_group" and r["conclusion"] != "success")
    ]


def render_flakes(ranked: list[dict[str, Any]], odd_runs: list[Json], repo: str, total: int) -> str:
    lines = ["", f"## Flakes (failed, then passed on the same commit; last {total} runs)", ""]
    if ranked:
        lines += ["| flakes | job | test | runs |", "|---|---|---|---|"]
        for entry in ranked:
            links = ", ".join(f"[{run}](https://github.com/{repo}/actions/runs/{run})" for run in entry["runs"][:5])
            lines.append(f"| {entry['count']} | {entry['job']} | `{entry['test']}` | {links} |")
    else:
        lines.append("None.")
    lines += ["", "### Cancelled, rerun or ejected runs", ""]
    if odd_runs:
        lines += ["| run | event | outcome | attempt | branch |", "|---|---|---|---|---|"]
        for r in odd_runs:
            link = f"[{r['id']}](https://github.com/{repo}/actions/runs/{r['id']})"
            attempt, branch = r.get("run_attempt", 1), r.get("head_branch", "")
            lines.append(f"| {link} | {r['event']} | {r['conclusion']} | {attempt} | {branch} |")
    else:
        lines.append("None.")
    return "\n".join(lines) + "\n"


def flake_report(api: Api, text: Text, repo: str, since: datetime, limit: int = 100) -> str:
    runs = api(f"actions/workflows/ci.yml/runs?per_page={limit}&status=completed")["workflow_runs"][:limit]
    candidates = [r for r in runs if r.get("run_attempt", 1) > 1 or r["conclusion"] == "failure"]
    # Only commits that ran more than once can hold a flake.
    shas = defaultdict(int)
    for run in runs:
        shas[run["head_sha"]] += 1
    candidates = [r for r in candidates if r.get("run_attempt", 1) > 1 or shas[r["head_sha"]] > 1]
    flaky = flaky_jobs(job_attempts(api, candidates))

    def log_of(row: Mapping[str, Any]) -> str:
        try:
            return text(f"actions/jobs/{row['job_id']}/logs")
        except (subprocess.CalledProcessError, GithubApiUnavailable):  # logs expire; one log is not worth the report
            return ""

    ranked = rank_flakes(flaky, log_of)
    recent = [r for r in runs if parse_time(r["created_at"]) >= since]
    return render_flakes(ranked, non_green_runs(recent)[:25], repo, len(runs))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since", help="ISO time overriding health.since (measure a different window)")
    parser.add_argument("--runs", type=int, help="most recent runs per event, overriding health.runs")
    args = parser.parse_args()
    budget = json.loads(BUDGETS.read_text())["health"]
    budget = {**budget, **{k: v for k, v in (("since", args.since), ("runs", args.runs)) if v}}
    try:
        return report_health(budget)
    except GithubApiUnavailable as error:
        return report_unavailable(error)


def report_unavailable(error: GithubApiUnavailable) -> int:
    """GitHub's API stayed down through every retry: say so; the next run measures again."""
    message = f"GitHub API unavailable: {error}. No budget was evaluated; the next CI health run measures again."
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with Path(summary).open("a") as handle:
            handle.write(f"## CI health\n\n{message}\n")
    print(f"::warning::{message}")
    return 0


def report_health(budget: Json) -> int:
    measured = gather(gh_json, budget)
    report = render(measured)
    failures = evaluate(budget, measured.durations, measured.latencies)
    if failures:
        report += "\n### Budget exceeded\n\n" + "\n".join(f"- {f}" for f in failures) + "\n"
    report += flake_report(gh_json, gh_text, os.environ["GITHUB_REPOSITORY"], parse_time(budget["since"]))
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with Path(summary).open("a") as handle:
            handle.write(report)
    print(report)
    for failure in failures:
        print(f"::error::{failure}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
