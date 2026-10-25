#!/usr/bin/env python3
"""Run one exhaustive runtime test-name partition, preserving all subtests/seeds.

Discovery uses Go's compiled test inventory, including replay, chaos, fuzz seeds
and examples, across every package. Every name must match exactly one partition.
PR CI uses two measured partitions, sharing dependency build caches.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path

# Real compiled CLI/large-repository paths exceeded eight minutes alongside
# all remaining contracts. Retain every case, including the 83MiB inventories.
INTEGRATION = (
    "TestRestartClassHermetic",
    "TestReflectionDirectFilesCLIAndBroker",
    "TestReflectionLearnedProgramCLIAndBroker",
    "TestT1ScriptedBrokerMatrix",
    "TestRenderedLockImageInputScope",
    "TestExhaustiveReflectionLifecycle",
    "TestSimSchedule",
    "TestSimScheduleQueuedDriftNeverRedispatchesAfterVerifiedClosure",
    "TestSimRenderedLockSchedule",
    "TestSimIncidentStream",
)
_INTEGRATION = "(?:" + "|".join(INTEGRATION) + ")"
PATTERNS = {
    "integration": "^" + _INTEGRATION + "$",
    "contracts": "^(?!" + _INTEGRATION + "$)(?:Test|Fuzz|Example)",
}
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
    parser.add_argument("--shard", choices=PATTERNS, required=True)
    args = parser.parse_args()
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
    eligible = names
    groups = partition(eligible, PATTERNS)
    if not all(groups.values()):
        raise ValueError("Every runtime shard must contain tests")
    print(
        f"Coverage guard: {len(eligible)} names assigned exactly once; "
        "including replay, chaos, examples and fuzz seeds",
        flush=True,
    )
    for shard, members in groups.items():
        print(f"{shard}: {len(members)} test names", flush=True)
    # Anchoring whole top-level names retains all nested scenarios and fuzz seeds.
    selection = "^(" + "|".join(re.escape(name) for name in groups[args.shard]) + ")$"
    result = subprocess.run(
        ["go", "test", "-race", "-parallel=2", "-count=1", "-timeout=8m", "-json", "-run", selection, "./..."],
        cwd=RUNTIME,
        check=False,
    )
    # Go printed the failing test; avoid dumping an 800-name argv on failure.
    raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
