#!/usr/bin/env python3
"""Compile-inventory-driven CI bins. Budgets include a 90s job setup margin."""

from __future__ import annotations

import argparse
import json
import math
import re
import subprocess
from pathlib import Path
from typing import TypedDict

ROOT = Path(__file__).resolve().parents[1]
JOB_SECONDS = 300.0
SETUP_MARGIN = 90.0
BUDGET = JOB_SECONDS - SETUP_MARGIN
# Unmeasured names own a whole bin, so a new slow test cannot overload peers.
DEFAULT_COST = BUDGET
DEDICATED = re.compile(r"^Test(?:Chaos|Exhaustive|Explorer)")


class CostPlan(TypedDict):
    scope: str
    bins: list[list[str]]
    nightly: list[str]
    packages: dict[str, list[str]]
    costs: dict[str, float]


def validate_costs(costs: dict[str, float]) -> None:
    if any(
        isinstance(c, bool) or not isinstance(c, (int, float)) or not math.isfinite(c) or c <= 0 for c in costs.values()
    ):
        raise ValueError("cost must be finite positive seconds")


def check_budget(names: list[str], costs: dict[str, float], *, budget: float = BUDGET) -> None:
    if not math.isfinite(budget) or budget <= 0:
        raise ValueError("budget must be finite and positive")
    validate_costs(costs)
    total = sum(costs.get(name, DEFAULT_COST) for name in names)
    if total > budget:
        raise ValueError(f"shard cost {total:.2f}s exceeds {budget:.2f}s work budget; regenerate the CI plan")


def pack(
    names: list[str],
    costs: dict[str, float],
    *,
    budget: float = BUDGET,
    max_bins: int | None = None,
    max_test_seconds: float | None = None,
) -> tuple[list[list[str]], list[str]]:
    """First-fit decreasing grows capacity, never a job deadline or placement list."""
    check_budget([], costs, budget=budget)
    if len(names) != len(set(names)):
        raise ValueError("inventory contains duplicate names")
    if max_bins is not None and (isinstance(max_bins, bool) or max_bins <= 0):
        raise ValueError("fixed bin count must be positive")
    bins: list[list[str]] = [[] for _ in range(max_bins or 0)]
    totals: list[float] = [0.0 for _ in bins]
    nightly: list[str] = []
    candidates = names
    if max_bins is not None:
        # Keep the broadest cheap regression set; growing cost becomes nightly
        # work, never another PR job. Pack that set longest-first afterwards.
        remaining = max_bins * budget
        candidates = []
        for name in sorted(names, key=lambda n: (costs.get(n, DEFAULT_COST), n)):
            cost = costs.get(name, DEFAULT_COST)
            if cost > budget or cost > remaining or (max_test_seconds is not None and cost > max_test_seconds):
                nightly.append(name)
            else:
                candidates.append(name)
                remaining -= cost
    for name in sorted(candidates, key=lambda n: (-costs.get(n, DEFAULT_COST), n)):
        cost = costs.get(name, DEFAULT_COST)
        if cost > budget or (max_test_seconds is not None and cost > max_test_seconds):
            nightly.append(name)
            continue
        available = [i for i, total in enumerate(totals) if total + cost <= budget]
        if available:
            target = min(available, key=lambda i: (totals[i], i))
        elif max_bins is not None:
            nightly.append(name)
            continue
        else:
            target = len(bins)
            bins.append([])
            totals.append(0.0)
        bins[target].append(name)
        totals[target] += cost
    for members in bins:
        check_budget(members, costs, budget=budget)
    return bins, nightly


def inventory(module: str, *, prepared: bool = False) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    cached = ROOT / f".fastloop/test-bin/inventory-{module or 'operator'}.json"
    output = (
        cached.read_text()
        if prepared
        else subprocess.check_output(
            ["go", "test", "-race", "-list", ".", "-json", "./..."], cwd=ROOT / "sdo/operator" / module, text=True
        )
    )
    for line in output.splitlines():
        event = json.loads(line)
        name = event.get("Output", "").strip()
        if event.get("Action") == "output" and re.fullmatch(r"(?:Test|Fuzz|Example)\w*", name):
            result.setdefault(event["Package"], []).append(name)
    if not result:
        raise ValueError(f"compiled inventory is empty: {module or 'operator'}")
    return result


def plan(
    scope: str, source: dict[str, list[str]], *, replay_only: bool = False, include_dedicated: bool = False
) -> CostPlan:
    recorded = json.loads((ROOT / f"scripts/{scope}_test_costs.json").read_text())
    profile = recorded["costs"]
    local = recorded.get("local_costs", {})
    validate_costs(profile)
    validate_costs(local)
    # Equal names stay together: go test's union regex applies to every package.
    packages: dict[str, list[str]] = {}
    costs: dict[str, float] = {}
    for package, names in sorted(source.items()):
        for name in names:
            if replay_only and (scope != "runtime" or not name.startswith("TestReplay")):
                continue
            if scope == "runtime" and DEDICATED.match(name) and not include_dedicated:
                continue
            packages.setdefault(name, []).append(package)
            key = name if scope == "runtime" else package + "/" + name
            # Local measurements are only a conservative bootstrap. Hosted
            # observations always take precedence; truly unknown work is isolated.
            estimate = max(10.0, 10 * local[key]) if key in local else DEFAULT_COST
            costs[name] = costs.get(name, 0.0) + profile.get(key, estimate)
    bins, nightly = pack(list(packages), costs)
    return {"scope": scope, "bins": bins, "nightly": nightly, "packages": packages, "costs": costs}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--prepared", action="store_true", help="use the inventory compiled by prepare_go_ci.sh")
    parser.add_argument("--replay-only", action="store_true", help="preserve Python-only PR Go replay coverage")
    parser.add_argument("--inventory-only", action="store_true")
    parser.add_argument("--include-dedicated", action="store_true")
    args = parser.parse_args()
    plans = {
        scope: plan(
            scope,
            inventory(module, prepared=args.prepared),
            replay_only=args.replay_only,
            include_dedicated=args.include_dedicated,
        )
        for scope, module in [("operator", ""), ("runtime", "runtime")]
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(plans, sort_keys=True) + "\n")
    print("Recorded compiled inventory; PR execution has fixed ci_policy.json capacity")


def run_main(scope: str) -> None:
    """Run the prepared plan, or discover it for standalone local invocations."""
    import os
    import time

    from go_test_timing import run_tests
    from operator_test_binary import build_operator

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bin", help="zero-based automatic bin, or nightly")
    parser.add_argument("--plan", type=Path)
    parser.add_argument("--nightly-index", type=int)
    args = parser.parse_args()
    started = time.monotonic()
    data = (
        json.loads(args.plan.read_text())[scope]
        if args.plan
        else plan(scope, inventory("runtime" if scope == "runtime" else ""))
    )
    if args.bin == "nightly":
        names = data["nightly"]
        if args.nightly_index is not None:
            if args.nightly_index < 0 or (names and args.nightly_index >= len(names)):
                parser.error("nightly index is outside the prepared plan")
            names = names[args.nightly_index : args.nightly_index + 1]
    else:
        index = int(args.bin)
        if index < 0 or index >= len(data["bins"]):
            parser.error(f"bin must be between 0 and {len(data['bins']) - 1}")
        names = data["bins"][index]
        check_budget(names, data["costs"])
    if not names:
        print(f"{scope}: no nightly overflow")
        return
    os.environ["SDO_TEST_OPERATOR_BINARY"] = str(build_operator(ROOT / "sdo/operator", ROOT / ".fastloop/test-bin"))
    run_tests(
        go_command(data, names),
        started=started,
        cwd=ROOT / "sdo/operator" / ("runtime" if scope == "runtime" else ""),
    )


def go_command(data: CostPlan, names: list[str]) -> list[str]:
    """A whole-name union preserves all subtests and seeds in their owner bin."""
    scope = data["scope"]
    packages = sorted({p for n in names for p in data["packages"][n]})
    selection = "^(" + "|".join(re.escape(n) for n in names) + ")$"
    return [
        "go",
        "test",
        "-race",
        "-parallel=2",
        "-count=1",
        "-timeout=" + ("5m" if scope == "runtime" else "4m"),
        "-json",
        "-run",
        selection,
        *packages,
    ]


if __name__ == "__main__":
    main()
