#!/usr/bin/env python3
"""Collect and run Go-backed Python contracts with the shared CI cost planner."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import TypedDict

ROOT = Path(__file__).resolve().parents[1]
# The Go planner owns the one budget/bin-packing implementation.
SPEC = importlib.util.spec_from_file_location("go_cost_plan", Path(__file__).with_name("go_cost_plan.py"))
assert SPEC is not None
assert SPEC.loader is not None
_go = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(_go)


class PythonPlan(TypedDict):
    bins: list[list[str]]
    nightly: list[str]
    costs: dict[str, float]


def build_plan(names: list[str], costs: dict[str, float]) -> PythonPlan:
    bins, nightly = _go.pack(names, costs)
    return {"bins": bins, "nightly": nightly, "costs": {n: costs.get(n, _go.DEFAULT_COST) for n in names}}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--plan", type=Path)
    parser.add_argument("--bin")
    parser.add_argument("--nightly-index", type=int)
    parser.add_argument("--inventory-only", action="store_true")
    args = parser.parse_args()
    if args.output:
        inventory = args.output.with_suffix(".inventory.json")
        inventory.parent.mkdir(parents=True, exist_ok=True)
        environment = {**os.environ, "SDO_PYTEST_INVENTORY": str(inventory.resolve())}
        subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                "--collect-only",
                "-q",
                "-p",
                "no:tach",
                "-p",
                "scripts.pytest_ci",
                "-m",
                "go_integration",
                "tests",
            ],
            cwd=ROOT,
            env=environment,
            check=True,
        )
        names = json.loads(inventory.read_text())
        if not names:
            raise ValueError("integration discovery returned no tests")
        costs = json.loads((ROOT / "scripts/python_integration_test_costs.json").read_text())["costs"]
        data = build_plan(names, costs)
        args.output.write_text(json.dumps(data, sort_keys=True) + "\n")
        for index, members in enumerate(data["bins"]):
            print(f"python-{index}: {sum(data['costs'][n] for n in members):.2f}s, {len(members)} tests")
        print(f"Python nightly: {data['nightly']}")
        return
    if not args.plan or args.bin is None:
        parser.error("--output or both --plan/--bin required")
    data = json.loads(args.plan.read_text())
    if args.bin == "none":
        print("All Python integration contracts require nightly budgets")
        return
    if args.bin == "nightly":
        names = data["nightly"]
        if args.nightly_index is not None:
            if args.nightly_index < 0 or (names and args.nightly_index >= len(names)):
                parser.error("nightly index is outside prepared integration plan")
            names = names[args.nightly_index : args.nightly_index + 1]
    else:
        index = int(args.bin)
        if index < 0 or index >= len(data["bins"]):
            parser.error("bin outside prepared integration plan")
        names = data["bins"][index]
        _go.check_budget(names, data["costs"])
    if not names:
        print("No oversized Python contracts")
        return
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
            str(min(2, len(names))),
            *names,
        ],
        cwd=ROOT,
        check=True,
    )


if __name__ == "__main__":
    main()
