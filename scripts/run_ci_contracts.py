#!/usr/bin/env python3
"""Execute one fixed PR or cost-packed nightly bucket without dropping assertions."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from go_cost_plan import check_budget, go_command
from go_test_timing import run_tests
from operator_test_binary import build_operator

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--bin", type=int, required=True)
    parser.add_argument("--nightly", action="store_true")
    args = parser.parse_args()
    data = json.loads(args.plan.read_text())
    if args.bin < 0 or args.bin >= len(data["bins"]):
        parser.error("bucket outside computed plan")
    members = data["bins"][args.bin]
    if not args.nightly:
        if len(data["bins"]) != data["policy"]["contract_jobs"]:
            raise ValueError("PR contract job count is fixed")
        check_budget(
            members,
            {key: item["seconds"] for key, item in data["items"].items()},
            budget=data["policy"]["contract_work_seconds"],
        )
    if not members:
        print("No tests in this bucket")
        return
    os.environ["SDO_TEST_OPERATOR_BINARY"] = str(build_operator(ROOT / "sdo/operator", ROOT / ".fastloop/test-bin"))
    started = time.monotonic()
    for scope in ("operator", "runtime"):
        names = [
            data["items"][key]["name"]
            for key in members
            if data["items"][key]["kind"] == "go" and data["items"][key]["scope"] == scope
        ]
        if names:
            run_tests(
                go_command(data["go"][scope], names),
                started=started,
                cwd=ROOT / "sdo/operator" / ("runtime" if scope == "runtime" else ""),
            )
    nodes = [data["items"][key]["name"] for key in members if data["items"][key]["kind"] == "python"]
    if nodes:
        subprocess.run([sys.executable, "scripts/init_test_prereqs.py"], cwd=ROOT, check=True)
        subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                "-v",
                "--durations=0",
                "-p",
                "no:tach",
                "-p",
                "scripts.pytest_ci",
                "-n",
                str(min(2, len(nodes))),
                *nodes,
            ],
            cwd=ROOT,
            check=True,
        )


if __name__ == "__main__":
    main()
