#!/usr/bin/env python3
"""Fail required PR CI when its queue-free dependency path can exceed ten minutes.

Completed jobs use the Actions API's started_at/completed_at, never created_at.
The still-running aggregator reserves its full one-minute timeout, including its
checkout, this API call, and cleanup. This conservatively enforces the complete
path without pretending its completion timestamp is already available.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import yaml

BUDGET_SECONDS = 600
GATE_COMMAND = "uv run --locked python scripts/check_ci_budget.py"
REQUIRED = "required-pr-ci"


@dataclass(frozen=True)
class Report:
    ancestor_seconds: float
    guarded_seconds: float
    path: tuple[str, ...]
    slowest: tuple[tuple[str, float], ...]

    @property
    def within_budget(self) -> bool:
        return self.guarded_seconds <= BUDGET_SECONDS

    def summary(self) -> str:
        return (
            f"CI critical path (excluding queues): {self.ancestor_seconds:.1f}s completed ancestors; "
            f"{self.guarded_seconds:.1f}s including the required job's full timeout "
            f"(budget {BUDGET_SECONDS}s).\n"
            f"Path: {' -> '.join(self.path)} -> Required PR CI.\n"
            "Slowest jobs: " + ", ".join(f"{name}: {seconds:.1f}s" for name, seconds in self.slowest)
        )


def dependencies(job: dict) -> list[str]:
    needs = job.get("needs", [])
    return [needs] if isinstance(needs, str) else needs


def validate_policy(workflow: dict) -> None:
    jobs = workflow["jobs"]
    for name, job in jobs.items():
        matrix = job.get("strategy", {}).get("matrix", {})
        if matrix and set(matrix) != {"include"}:
            raise ValueError(f"{name}: test jobs must use an explicit include matrix for the job-count policy")
    count = sum(
        len(job.get("strategy", {}).get("matrix", {}).get("include", [{}]))
        for name, job in jobs.items()
        if name != REQUIRED
    )
    if count > 3:
        raise ValueError(f"PR CI has {count} test jobs; maximum is three (including every matrix entry)")
    for name, job in jobs.items():
        timeout = job.get("timeout-minutes")
        if type(timeout) is not int or not 1 <= timeout <= 10:
            raise ValueError(f"{name}: timeout-minutes must be an integer in 1..10")
    required = jobs[REQUIRED]
    if required.get("timeout-minutes") != 1:
        raise ValueError("Required PR CI must reserve exactly its one-minute timeout")
    if required.get("if") != "always()" or required.get("continue-on-error", False):
        raise ValueError("Required PR CI must always run and fail on error")
    if set(dependencies(required)) != jobs.keys() - {REQUIRED}:
        raise ValueError("Required PR CI must depend on every PR job")
    if required.get("permissions", {}).get("actions") != "read":
        raise ValueError("Required PR CI needs actions: read to measure execution")
    gates = [step for step in required["steps"] if step.get("run") == GATE_COMMAND]
    if len(gates) != 1 or gates[0].get("if") != "always()" or gates[0].get("continue-on-error", False):
        raise ValueError("Required PR CI must always enforce scripts/check_ci_budget.py")


def expanded_names(workflow: dict) -> dict[str, str]:
    names: dict[str, str] = {}
    for job_id, job in workflow["jobs"].items():
        matrix = job.get("strategy", {}).get("matrix", {})
        if matrix and set(matrix) != {"include"}:
            raise ValueError(f"{job_id}: budget expansion requires an explicit include matrix")
        for row in matrix.get("include", [{}]):
            name = re.sub(
                r"\$\{\{\s*matrix\.(\w+)\s*\}\}",
                lambda match, values=row: str(values[match[1]]),
                job.get("name", job_id),
            )
            if "${{" in name or name in names:
                raise ValueError(f"Ambiguous job name: {name}")
            names[name] = job_id
    return names


def measure(workflow: dict, jobs: list[dict]) -> Report:
    names = expanded_names(workflow)
    durations: dict[str, tuple[str, float]] = {}
    for job in jobs:
        name = job["name"]
        if name not in names or name in durations:
            raise ValueError(f"Unknown or duplicate API job: {name}")
        if names[name] == REQUIRED:
            continue
        if job["status"] != "completed":
            raise ValueError(f"{name}: predecessor is not completed")
        if job["conclusion"] == "skipped":
            seconds = 0.0
        else:
            if not job.get("started_at") or not job.get("completed_at"):
                raise ValueError(f"{name}: missing execution timestamps")
            seconds = (
                datetime.fromisoformat(job["completed_at"].replace("Z", "+00:00"))
                - datetime.fromisoformat(job["started_at"].replace("Z", "+00:00"))
            ).total_seconds()
            if seconds < 0:
                raise ValueError(f"{name}: negative execution duration")
        durations[name] = (name, seconds)
    missing = {name for name, job_id in names.items() if job_id != REQUIRED} - durations.keys()
    if missing:
        raise ValueError(f"Missing predecessor job evidence: {sorted(missing)}")
    costs: dict[str, tuple[float, tuple[str, ...]]] = {}
    visiting: set[str] = set()

    def path(job_id: str) -> tuple[float, tuple[str, ...]]:
        if job_id in visiting:
            raise ValueError(f"Cyclic job dependency: {job_id}")
        if job_id not in costs:
            visiting.add(job_id)
            parents = [path(parent) for parent in dependencies(workflow["jobs"][job_id])]
            before, chain = max(parents, default=(0.0, ()), key=lambda value: value[0])
            name, seconds = max(
                (durations[name] for name, owner in names.items() if owner == job_id), key=lambda value: value[1]
            )
            costs[job_id] = (before + seconds, chain + (name,))
            visiting.remove(job_id)
        return costs[job_id]

    ancestors = dependencies(workflow["jobs"][REQUIRED])
    seconds, chain = max((path(parent) for parent in ancestors), key=lambda value: value[0])
    tail = workflow["jobs"][REQUIRED]["timeout-minutes"] * 60
    return Report(
        seconds, seconds + tail, chain, tuple(sorted(durations.values(), key=lambda item: item[1], reverse=True)[:5])
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workflow", type=Path, default=Path(".github/workflows/ci.yml"))
    parser.add_argument("--jobs", type=Path, help="Recorded Actions jobs JSON for offline measurement")
    args = parser.parse_args()
    workflow = yaml.safe_load(args.workflow.read_text())
    validate_policy(workflow)
    if args.jobs:
        payload = json.loads(args.jobs.read_text())
        pages = payload if isinstance(payload, list) else [payload]
    else:
        endpoint = (
            f"repos/{os.environ['GITHUB_REPOSITORY']}/actions/runs/{os.environ['GITHUB_RUN_ID']}"
            "/jobs?filter=latest&per_page=100"
        )
        response = subprocess.run(
            ["gh", "api", "--paginate", "--slurp", endpoint], check=True, capture_output=True, text=True, timeout=25
        )
        pages = json.loads(response.stdout)
    report = measure(workflow, [job for page in pages for job in page["jobs"]])
    print(report.summary())
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(summary, "a") as output:
            output.write(report.summary() + "\n")
    if not report.within_budget:
        raise SystemExit("CI execution exceeds the ten-minute critical-path budget")


if __name__ == "__main__":
    main()
