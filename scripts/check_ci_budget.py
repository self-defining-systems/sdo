#!/usr/bin/env python3
"""Fail closed on PR job growth and a configured critical path over ten minutes."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def cardinality(job: dict) -> int:
    matrix = job.get("strategy", {}).get("matrix")
    if matrix is None:
        return 1
    if not isinstance(matrix, dict):
        raise ValueError("PR matrices must be fixed, never computed from test cost or inventory")
    if "exclude" in matrix:
        raise ValueError("use an explicit fixed include matrix for PR jobs")
    axes = [value for key, value in matrix.items() if key != "include"]
    if any(not isinstance(axis, list) or not axis for axis in axes):
        raise ValueError("PR matrix axes must be fixed nonempty lists")
    if axes and "include" in matrix:
        raise ValueError("use fixed axes or fixed includes, not both")
    return len(matrix["include"]) if "include" in matrix else math.prod(len(axis) for axis in axes)


def check(jobs: dict) -> tuple[int, int]:
    count = sum(cardinality(job) for name, job in jobs.items() if name not in {"prepare-go", "required-pr-ci"})
    if count > 4:
        raise ValueError(f"{count} PR test jobs exceeds the fixed limit of 4")
    durations: dict[str, int] = {}
    visiting: set[str] = set()

    def longest(name: str) -> int:
        if name in durations:
            return durations[name]
        if name in visiting or name not in jobs:
            raise ValueError(f"invalid CI dependency: {name}")
        visiting.add(name)
        job = jobs[name]
        minutes = job.get("timeout-minutes")
        if isinstance(minutes, bool) or not isinstance(minutes, int) or not 0 < minutes <= 5:
            raise ValueError(f"{name}: literal unchanged-or-lower timeout <=5 required")
        needs = job.get("needs", [])
        if isinstance(needs, str):
            needs = [needs]
        durations[name] = minutes * 60 + max((longest(parent) for parent in needs), default=0)
        visiting.remove(name)
        return durations[name]

    path = max((longest(name) for name in jobs), default=0)
    if path > 600:
        raise ValueError(f"critical path {path}s exceeds 600s / 10 minutes")
    return count, path


def main() -> None:
    import yaml

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workflow", type=Path, default=ROOT / ".github/workflows/ci.yml")
    args = parser.parse_args()
    jobs = yaml.safe_load(args.workflow.read_text())["jobs"]
    count, path = check(jobs)
    policy = json.loads((ROOT / "scripts/ci_policy.json").read_text())
    if (
        cardinality(jobs["contracts"]) != policy["contract_jobs"]
        or cardinality(jobs["python"]) != policy["python_jobs"]
    ):
        raise ValueError("fixed workflow job counts must match ci_policy.json")
    if policy["contract_work_seconds"] + policy["setup_margin_seconds"] > jobs["contracts"]["timeout-minutes"] * 60:
        raise ValueError("contract work plus setup margin exceeds the job deadline")
    print(f"CI budget: {count}/4 fixed test jobs; {path}/600s maximum executed critical path")


if __name__ == "__main__":
    main()
