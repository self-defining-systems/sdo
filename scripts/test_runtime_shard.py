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
import re
import subprocess
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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shard", required=True, help="comma-separated runtime partitions")
    args = parser.parse_args()
    selected = args.shard.split(",")
    if len(selected) != len(set(selected)) or set(selected) - PATTERNS.keys():
        parser.error("select distinct known runtime partitions: " + ",".join(PATTERNS))
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
    groups = partition(eligible, PATTERNS)
    if not all(groups.values()):
        raise ValueError("Every runtime shard must contain tests")
    print(
        f"Coverage guard: {len(eligible)} names assigned exactly once; "
        f"{len(names) - len(eligible)} names covered by dedicated jobs",
        flush=True,
    )
    for shard, members in groups.items():
        print(f"{shard}: {len(members)} test names", flush=True)
    # Anchoring whole top-level names retains all nested scenarios and fuzz seeds.
    selection = "^(" + "|".join(re.escape(name) for shard in selected for name in groups[shard]) + ")$"
    subprocess.run(
        ["go", "test", "-race", "-parallel=2", "-count=1", "-timeout=5m", "-json", "-run", selection, "./..."],
        cwd=RUNTIME,
        check=True,
    )


if __name__ == "__main__":
    main()
