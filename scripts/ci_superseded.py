#!/usr/bin/env python3
"""Whether a cancelled pull_request CI run was superseded by a newer run.

A stack restack force-pushes head and base within a second, so GitHub starts two
pull_request runs for one commit; the workflow's concurrency group cancels the
older one. Its cancelled jobs would fail `Required PR CI` next to the newer run's
verdict. A run is superseded only when a job was cancelled AND a newer run exists
for the same pull request and head commit, so a manual cancel with no newer run
still fails the gate.

    ci_superseded.py RUN_ID PULL_REQUEST HEAD_SHA RESULT...   (reads runs JSON on stdin)

Prints `true` or `false`.
"""

from __future__ import annotations

import json
import sys


def superseded(runs: list[dict], run_id: int, pull_request: int, head_sha: str, results: list[str]) -> bool:
    if "cancelled" not in results:
        return False
    return any(
        run["id"] > run_id
        and run["head_sha"] == head_sha
        and run["event"] == "pull_request"
        and any(pr["number"] == pull_request for pr in run.get("pull_requests", []))
        for run in runs
    )


def main(argv: list[str]) -> int:
    run_id, pull_request, head_sha, *results = argv
    runs = json.load(sys.stdin)["workflow_runs"]
    print(str(superseded(runs, int(run_id), int(pull_request), head_sha, results)).lower())
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
