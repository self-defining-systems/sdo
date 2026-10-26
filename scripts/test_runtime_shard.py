#!/usr/bin/env python3
"""Run one exhaustive runtime test-name partition, preserving all subtests/seeds.

Discovery uses Go's compiled test inventory, including fuzz seed tests and examples,
across every package in the runtime module. Every eligible name must match exactly
one partition before any shard runs. Replay/chaos/explorers retain their dedicated
CI jobs. Keep PATTERNS and the controller matrix's shard values in sync.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import subprocess
import sys
import time
from pathlib import Path

PATTERNS = {
    "reflection": r"^TestReflectionDirectFilesCLIAndBroker$",
    "reflection-mongo": r"^TestReflection(?:MongoAuth|HotelMongoAuth)UpgradesReadmeThroughCLIAndBroker$",
    "reflection-programs": (
        r"^TestReflection(?:HotelRunScriptUpgradesReadmeThroughCLIAndBroker|LearnedProgramCLIAndBroker"
        r"|LearnsDeploymentRepairWithUnwrittenServiceSymptoms)$"
    ),
    "contracts": (
        r"^(Test(?:[A-G]|Reflection(?!(?:DirectFilesCLIAndBroker|(?:MongoAuth|HotelMongoAuth|HotelRunScript)"
        r"UpgradesReadmeThroughCLIAndBroker|LearnedProgramCLIAndBroker|LearnsDeploymentRepairWithUnwrittenServiceSymptoms)$)"
        r"|RegisteredReflection|HermeticFastloopRealGoGate$)"
        r"|Fuzz[A-M]|Example(?:[A-M_]|$))"
    ),
    "rendered": (
        r"^(Test(?!(?:HermeticFastloopRealGoGate|SimRenderedLockSchedule)$)(?:[H-P]|Rendered|SimRendered)"
        r"|Fuzz(?!SimIncidentSchedule$)[N-Z]|Example[N-Z])"
    ),
    "simulation": r"^TestSim[A-M]",
    "schedules": r"^(TestSimSchedule|TestSimRenderedLockSchedule$|FuzzSimIncidentSchedule$)",
    "corpus": r"^TestRejectedRepeatCorpusFallsBackToReflectionWorker$",
    "recovery": (
        r"^Test(?:Q|R(?!eflection|egisteredReflection|endered|ejectedRepeatCorpusFallsBackToReflectionWorker$)"
        r"|Sim(?!Rendered|Schedule)(?:[N-Z]|$))"
    ),
    "remaining": r"^Test(?:S(?!im)|[T-Z])",
}
DEDICATED = re.compile(r"^Test(?:Replay|Chaos|Exhaustive|Explorer)")
RUNTIME = Path(__file__).resolve().parents[1] / "sdo/operator/runtime"


def discovered_names(events: list[dict]) -> list[str]:
    """Names are sufficient: identical names in different packages share a shard."""
    return sorted(
        {
            event["Output"].strip()
            for event in events
            if event.get("Action") == "output"
            and re.fullmatch(r"(?:Test|Fuzz|Example)\w*", event.get("Output", "").strip())
        }
    )


def partition(names: list[str], patterns: dict[str, str]) -> dict[str, list[str]]:
    groups: dict[str, list[str]] = {shard: [] for shard in patterns}
    for name in names:
        matches = [shard for shard, pattern in patterns.items() if re.search(pattern, name)]
        if len(matches) != 1:
            raise ValueError(f"{name} must land in exactly one shard; matched {matches}")
        groups[matches[0]].append(name)
    return groups


# These are the eight existing runner allocations, not additional CI jobs.
RUNNER_GROUPS = {
    "reflection": ("reflection", "corpus"),
    "reflection-mongo": ("reflection-mongo",),
    "reflection-programs": ("reflection-programs",),
    "contracts": ("contracts",),
    "rendered": ("rendered",),
    "simulation": ("simulation", "remaining"),
    "schedules": ("schedules",),
    "recovery": ("recovery",),
}
COST_FILE = Path(__file__).with_name("runtime_test_costs.json")
# Use 70% of the unchanged five-minute controller job: reserve the remaining
# 90 seconds for checkout, toolchain/cache setup and linking on hosted runners.
SHARD_BUDGET_SECONDS = 5 * 60 * 70 // 100
# Hosted costs drive all measured placements; no local-only overrides.
PLACEMENTS: dict[str, str] = {}


def balanced_partition(names: list[str], patterns: dict[str, str], costs: dict[str, float]) -> dict[str, list[str]]:
    """Keep unmeasured names exhaustive; place measured work by longest first."""
    if any(
        isinstance(cost, bool) or not isinstance(cost, (int, float)) or not math.isfinite(cost) or cost <= 0
        for cost in costs.values()
    ):
        raise ValueError("measured runtime costs must be finite positive seconds")
    original = partition(sorted(names), patterns)
    groups: dict[str, list[str]] = {shard: [] for shard in patterns}
    totals = dict.fromkeys(RUNNER_GROUPS, 0.0)
    owners = {part: runner for runner, parts in RUNNER_GROUPS.items() for part in parts}
    measured: list[str] = []
    for shard, members in original.items():
        for name in members:
            # Keep the small logical corpus partition usable by existing callers.
            if name in costs and shard != "corpus":
                measured.append(name)
            else:
                groups[shard].append(name)
                totals[owners[shard]] += costs.get(name, 0.1)
    for name in sorted(measured, key=lambda name: (-costs[name], name)):
        runner = min(totals, key=lambda runner: (totals[runner], runner))
        groups[runner].append(name)
        totals[runner] += costs[name]
    for name, target in PLACEMENTS.items():
        for members in groups.values():
            if name in members:
                members.remove(name)
                groups[target].append(name)
                break
    return {shard: sorted(members) for shard, members in groups.items()}


def run_tests(command: list[str], *, started: float) -> None:
    # Both runners use one process-group watchdog and JSON diagnostic path.
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from test_operator_shard import run_tests as run_go_tests

    run_go_tests(command, started=started, cwd=RUNTIME, budget_seconds=SHARD_BUDGET_SECONDS)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shard", required=True, help="comma-separated runtime partitions")
    args = parser.parse_args()
    selected = args.shard.split(",")
    if len(selected) != len(set(selected)) or set(selected) - PATTERNS.keys():
        parser.error("select distinct known runtime partitions: " + ",".join(PATTERNS))
    started = time.monotonic()
    discovery = subprocess.run(
        ["go", "test", "-race", "-list", ".", "-json", "./..."],
        cwd=RUNTIME,
        text=True,
        capture_output=True,
        check=True,
    )
    names = discovered_names([json.loads(line) for line in discovery.stdout.splitlines()])
    if not names:
        raise ValueError("Go test discovery returned no tests")
    eligible = [name for name in names if not DEDICATED.match(name)]
    costs = json.loads(COST_FILE.read_text())["costs"]
    groups = balanced_partition(eligible, PATTERNS, costs)
    if not all(any(groups[part] for part in parts) for parts in RUNNER_GROUPS.values()):
        raise ValueError("Every runtime runner must contain tests")
    print(
        f"Coverage guard: {len(eligible)} names assigned exactly once; "
        f"{len(names) - len(eligible)} names covered by dedicated jobs",
        flush=True,
    )
    for shard, members in groups.items():
        print(f"{shard}: {len(members)} test names", flush=True)
    # Anchoring whole top-level names retains all nested scenarios and fuzz seeds.
    selection = "^(" + "|".join(re.escape(name) for shard in selected for name in groups[shard]) + ")$"
    run_tests(
        ["go", "test", "-race", "-parallel=2", "-count=1", "-timeout=5m", "-json", "-run", selection, "./..."],
        started=started,
    )


if __name__ == "__main__":
    main()
