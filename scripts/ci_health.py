#!/usr/bin/env python3
"""Hourly CI health: per-job duration percentiles, merge-queue latency and run outcomes.

Read-only GitHub API use through the `gh` CLI (GH_TOKEN, default workflow token).
Writes a Markdown summary to $GITHUB_STEP_SUMMARY (or stdout) and exits non-zero when a
budget in .github/ci_budgets.json ("health") is exceeded. Runs created before
health.since are ignored so a fixed regression stops failing once it leaves the window.
"""

from __future__ import annotations

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
from typing import Any

BUDGETS = Path(__file__).resolve().parents[1] / ".github" / "ci_budgets.json"
PR_EVENTS = ("pull_request", "merge_group")

Json = Mapping[str, Any]
Api = Callable[[str], Any]


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


def queue_latencies(runs: Iterable[Json], merged_at: Callable[[int], str | None]) -> list[float]:
    """Seconds from a merge-group run's creation (enqueue) to the PR's merge."""
    latencies = []
    for run in runs:
        if run["event"] != "merge_group":
            continue
        match = re.search(r"/pr-(\d+)-", run.get("head_branch") or "")
        merged = merged_at(int(match[1])) if match else None
        if merged:
            latencies.append((parse_time(merged) - parse_time(run["created_at"])).total_seconds())
    return latencies


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


def render(
    durations: Mapping[str, list[float]],
    latencies: list[float],
    pr_rates: dict[str, float],
    push_rates: dict[str, float],
) -> str:
    lines = ["## CI health", "", "| job | samples | p50 s | p95 s |", "|---|---|---|---|"]
    for name, values in sorted(durations.items()):
        lines.append(f"| {name} | {len(values)} | {percentile(values, 0.5):.0f} | {percentile(values, 0.95):.0f} |")
    lines += ["", "| merge queue (enqueue to merge) | samples | p50 min | p95 min |", "|---|---|---|---|"]
    p50, p95 = percentile(latencies, 0.5) / 60, percentile(latencies, 0.95) / 60
    lines.append(f"| merge_group | {len(latencies)} | {p50:.1f} | {p95:.1f} |")
    lines += ["", "| runs | cancelled | rerun | ejected |", "|---|---|---|---|"]
    for label, rates in (("PR and merge group", pr_rates), ("push to main", push_rates)):
        lines.append(f"| {label} | {rates['cancelled']:.0%} | {rates['rerun']:.0%} | {rates['ejected']:.0%} |")
    return "\n".join(lines) + "\n"


def gather(api: Api, budget: Json) -> tuple[dict[str, list[float]], list[float], dict[str, float], dict[str, float]]:
    since = parse_time(budget["since"])
    listing = api("actions/workflows/ci.yml/runs?per_page=100&status=completed")["workflow_runs"]
    runs = [r for r in listing if parse_time(r["created_at"]) >= since]
    pr_runs = [r for r in runs if r["event"] in PR_EVENTS][: budget["runs"]]
    push_runs = [r for r in runs if r["event"] == "push"][: budget["runs"]]
    jobs = [api(f"actions/runs/{r['id']}/jobs?per_page=100&filter=latest")["jobs"] for r in pr_runs]
    durations = job_durations(jobs)

    def merged_at(number: int) -> str | None:
        return api(f"pulls/{number}").get("merged_at")

    return durations, queue_latencies(pr_runs, merged_at), outcome_rates(pr_runs), outcome_rates(push_runs)


def gh_api(path: str) -> Any:
    repo = os.environ["GITHUB_REPOSITORY"]
    out = subprocess.run(["gh", "api", f"repos/{repo}/{path}"], check=True, capture_output=True, text=True).stdout
    return json.loads(out)


def main() -> int:
    budget = json.loads(BUDGETS.read_text())["health"]
    durations, latencies, pr_rates, push_rates = gather(gh_api, budget)
    report = render(durations, latencies, pr_rates, push_rates)
    failures = evaluate(budget, durations, latencies)
    if failures:
        report += "\n### Budget exceeded\n\n" + "\n".join(f"- {f}" for f in failures) + "\n"
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
