#!/usr/bin/env python3
"""Refresh CI cost profiles from completed hosted Go JSON events, without moving or omitting tests."""

from __future__ import annotations

import argparse
import json
import math
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable

ROOT = Path(__file__).resolve().parents[1]


def read_events(lines: Iterable[str]) -> list[dict[str, Any]]:
    events = []
    for line in lines:
        # gh run view --log prefixes the original JSON line with job and step.
        start = line.find('{"Time":')
        if start < 0:
            continue
        event = json.loads(line[start:])
        if "Action" in event and "Package" in event:
            events.append(event)
    return events


def measured_costs(events: Iterable[dict[str, Any]]) -> dict[tuple[str, str], float]:
    starts: dict[tuple[str, str], float] = {}
    pauses: dict[tuple[str, str], float] = {}
    paused: dict[tuple[str, str], float] = {}
    result: dict[tuple[str, str], float] = {}
    for event in events:
        name = event.get("Test", "")
        if not name or "/" in name:
            continue
        key = (event["Package"], name)
        action = event["Action"]
        at = datetime.fromisoformat(event["Time"].replace("Z", "+00:00")).timestamp()
        if action == "run":
            starts[key] = at
            paused[key] = 0.0
        elif action == "pause":
            pauses[key] = at
        elif action == "cont" and key in pauses:
            paused[key] = paused.get(key, 0.0) + at - pauses.pop(key)
        elif action in ("pass", "fail", "skip"):
            elapsed = float(event.get("Elapsed", 0))
            if not math.isfinite(elapsed) or elapsed < 0:
                raise ValueError(f"{key}: timing must be finite and nonnegative")
            active = at - starts[key] - paused.get(key, 0.0) if key in starts else elapsed
            seconds = max(elapsed, active)
            if not math.isfinite(seconds) or seconds < 0:
                raise ValueError(f"{key}: active timing must be finite and nonnegative")
            result[key] = max(result.get(key, 0.0), round(seconds, 3), 0.001)
            starts.pop(key, None)
    if not result:
        raise ValueError("no completed top-level test timings")
    return result


def update_profile(path: Path, measured: dict[str, float], run: int, head: str, write: bool) -> None:
    if not measured:
        raise ValueError(f"no completed timings for {path.name}")
    profile = json.loads(path.read_text())
    profile["source_run"] = run
    profile["source_head"] = head
    profile["description"] = (
        "Hosted completed top-level active seconds, including parallel children and excluding pauses. "
        "All pass/fail/skip timings are refreshed (0.001s resolution floor); "
        "unobserved and unfinished names retain their earlier bounds."
    )
    profile["costs"].update(measured)
    profile["costs"] = dict(sorted(profile["costs"].items()))
    print(f"{path.name}: {len(measured)} measured costs; test inventory unchanged; capacity is computed automatically")
    if write:
        path.write_text(json.dumps(profile, indent=2) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", type=Path, required=True)
    parser.add_argument("--run", type=int, required=True)
    parser.add_argument("--head", required=True)
    parser.add_argument("--scope", choices=("all", "runtime", "operator"), default="all")
    parser.add_argument("--write", action="store_true", help="write profiles; default only reports measured entries")
    args = parser.parse_args()
    if args.run <= 0 or len(args.head) != 40 or any(c not in "0123456789abcdef" for c in args.head):
        parser.error("positive hosted run and full lowercase source commit SHA required")
    measured = measured_costs(read_events(args.log.read_text().splitlines()))
    runtime: dict[str, float] = {}
    operator: dict[str, float] = {}
    for (package, name), seconds in measured.items():
        if package.startswith("sdo.dev/operator/runtime"):
            runtime[name] = max(runtime.get(name, 0.0), seconds)
        elif package.startswith("sdo.dev/operator/"):
            operator[f"{package}/{name}"] = seconds
    selected = {"runtime": runtime, "operator": operator}
    scopes = tuple(selected) if args.scope == "all" else (args.scope,)
    if any(not selected[scope] for scope in scopes):
        raise ValueError("completed timing evidence is required for every selected profile")
    for scope in scopes:
        update_profile(ROOT / f"scripts/{scope}_test_costs.json", selected[scope], args.run, args.head, args.write)


if __name__ == "__main__":
    main()
