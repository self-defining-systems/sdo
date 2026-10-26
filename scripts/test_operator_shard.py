#!/usr/bin/env python3
"""Balance real compiler tests across existing operator jobs, without omissions.

Discovery uses Go's compiled inventory. Measured expensive builder tests share
existing acceptance runners; every new builder/broker name keeps a default owner.
The JSON timing guard reports slow/running names before a budget failure escapes
as an unexplained aggregate timeout. No test, seed or subtest is removed.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

OPERATOR = Path(__file__).resolve().parents[1] / "sdo/operator"
PREFIX = "sdo.dev/operator/"
PACKAGES = ("builder", "broker", "memory/validate", "operatorcli", "worker_registry")
SHARDS = ("acceptance", "acceptance-second", "broker", "compiler", "compiler-replay", "remaining")
FLAGS = ("-race", "-parallel=2", "-count=1", "-timeout=4m", "-json")
# Hosted costs include parent active wall time, so parallel acceptance children
# cannot make an expensive test look cheap. Keep measured moves explicit: the
# Go package scheduler overlaps independent packages within these allocations.
COST_FILE = Path(__file__).with_name("operator_test_costs.json")
PROFILE = json.loads(COST_FILE.read_text())
MOVED_TESTS = PROFILE["placements"]
MOVED_BUILDER_TESTS = {
    name.removeprefix(PREFIX + "builder/"): shard
    for name, shard in MOVED_TESTS.items()
    if name.startswith(PREFIX + "builder/")
}
TEST_BUDGET_SECONDS = 180
SHARD_BUDGET_SECONDS = 5 * 60 * 70 // 100


def partition(inventory: dict[str, list[str]]) -> dict[str, dict[str, list[str]]]:
    missing = MOVED_BUILDER_TESTS.keys() - set(inventory.get(PREFIX + "builder", []))
    if missing:
        raise ValueError(f"missing measured builder tests (update partition after a rename): {sorted(missing)}")
    missing_moves = MOVED_TESTS.keys() - {
        package + "/" + name for package, names in inventory.items() for name in names
    }
    if missing_moves:
        raise ValueError(f"missing measured operator tests (update partition after a rename): {sorted(missing_moves)}")
    if set(MOVED_TESTS.values()) - set(SHARDS):
        raise ValueError("measured operator placement targets an unknown shard")
    groups: dict[str, dict[str, list[str]]] = {shard: {} for shard in SHARDS}
    name_owners: dict[str, str] = {}
    for package, names in inventory.items():
        for name in names:
            if package == PREFIX + "builder":
                shard = "compiler"
            elif package == PREFIX + "broker":
                shard = {
                    "TestCLIAndBrokerAgreeOnAcceptance": "acceptance",
                    "TestCLIAndBrokerAgreeOnAcceptanceSecondShard": "acceptance-second",
                }.get(name, "broker")
            elif package == PREFIX + "memory/validate":
                shard = "compiler"
            else:
                shard = "remaining"
            shard = MOVED_TESTS.get(package + "/" + name, shard)
            # A union -run regex is safe only when duplicate names share an owner.
            if name in name_owners and name_owners[name] != shard:
                raise ValueError(f"{name} in different shards across packages")
            name_owners[name] = shard
            groups[shard].setdefault(package, []).append(name)
    if not all(groups[shard] for shard in SHARDS if shard != "remaining"):
        raise ValueError("every operator name partition must contain tests")
    return groups


def check_budgets(
    events: list[dict], *, elapsed: float, enforce: bool = True, budget_seconds: float | None = None
) -> None:
    shard_budget = SHARD_BUDGET_SECONDS if budget_seconds is None else budget_seconds
    completed: list[dict] = []
    starts: dict[tuple[str, str], float] = {}
    paused: dict[tuple[str, str], float] = {}
    pause_seconds: dict[tuple[str, str], float] = {}
    for event in events:
        if not event.get("Test") or "/" in event["Test"]:
            continue
        key = (event["Package"], event["Test"])
        at = datetime.fromisoformat(event["Time"].replace("Z", "+00:00")).timestamp() if event.get("Time") else None
        action = event.get("Action")
        if action == "run" and at is not None:
            starts[key] = at
        elif action == "pause" and at is not None:
            paused[key] = at
        elif action == "cont" and at is not None and key in paused:
            pause_seconds[key] = pause_seconds.get(key, 0) + at - paused.pop(key)
        elif action in ("pass", "fail", "skip"):
            measured = dict(event)
            if at is not None and key in starts:
                measured["Elapsed"] = max(event.get("Elapsed", 0), at - starts[key] - pause_seconds.get(key, 0))
            completed.append(measured)
    slow = sorted(completed, key=lambda e: -e.get("Elapsed", 0))[:5]
    running = {(e["Package"], e["Test"]) for e in events if e.get("Action") == "run" and "/" not in e["Test"]}
    running -= {(e["Package"], e["Test"]) for e in completed}
    detail = "; ".join(f"{e['Package']}/{e['Test']}={e.get('Elapsed', 0):.2f}s" for e in slow)
    if running:
        detail += "; still running: " + ", ".join(f"{p}/{n}" for p, n in sorted(running))
    failures = [
        f"{e['Package']}/{e['Test']} {e.get('Elapsed', 0):.2f}s exceeds {TEST_BUDGET_SECONDS}s test budget"
        for e in completed
        if e.get("Elapsed", 0) > TEST_BUDGET_SECONDS
    ]
    if elapsed > shard_budget:
        failures.append(f"{elapsed:.2f}s exceeds {shard_budget}s shard budget")
    print(f"Operator timing: {elapsed:.2f}s; slowest: {detail}", flush=True)
    if enforce and failures:
        raise ValueError("; ".join(failures) + "; slowest: " + detail)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("shard", choices=SHARDS)
    args = parser.parse_args()
    started = time.monotonic()
    discovery = subprocess.run(
        ["go", "test", "-race", "-list", ".", "-json", *("./" + p for p in PACKAGES)],
        cwd=OPERATOR,
        text=True,
        capture_output=True,
        check=True,
    )
    inventory: dict[str, list[str]] = {}
    for line in discovery.stdout.splitlines():
        event = json.loads(line)
        name = event.get("Output", "").strip()
        if event.get("Action") == "output" and re.fullmatch(r"(?:Test|Fuzz|Example)\w*", name):
            inventory.setdefault(event["Package"], []).append(name)
    if set(inventory) != {PREFIX + p for p in PACKAGES}:
        raise ValueError("compiled discovery must contain every partitioned operator package")
    groups = partition(inventory)
    print(f"Coverage guard: {sum(map(len, inventory.values()))} compiled names assigned exactly once", flush=True)
    members = groups[args.shard]
    packages = sorted(members)
    names = [name for tests in members.values() for name in tests]
    if args.shard == "remaining":
        listed = subprocess.run(["go", "list", "./..."], cwd=OPERATOR, text=True, capture_output=True, check=True)
        unpartitioned = [p for p in listed.stdout.splitlines() if p not in {PREFIX + p for p in PACKAGES}]
        extra = subprocess.run(
            ["go", "test", "-race", "-list", ".", "-json", *unpartitioned],
            cwd=OPERATOR,
            text=True,
            capture_output=True,
            check=True,
        )
        for line in extra.stdout.splitlines():
            event = json.loads(line)
            name = event.get("Output", "").strip()
            if event.get("Action") == "output" and re.fullmatch(r"(?:Test|Fuzz|Example)\w*", name):
                inventory.setdefault(event["Package"], []).append(name)
        # Repeat the ownership check with the full inventory before selecting a
        # union regex, so a same-named moved test cannot leak into another job.
        members = partition(inventory)[args.shard]
        packages = sorted(set(members) | set(unpartitioned))
        names = [name for tests in members.values() for name in tests]
    selection = ["-run", "^(" + "|".join(re.escape(n) for n in sorted(set(names))) + ")$"]
    run_tests(
        ["go", "test", *FLAGS, *selection, *packages],
        started=started,
        enforce_budget=args.shard in ("compiler", "compiler-replay"),
    )


def run_tests(
    command: list[str],
    *,
    started: float,
    enforce_budget: bool = True,
    cwd: Path = OPERATOR,
    budget_seconds: float | None = None,
) -> None:
    """Bound the entire process group, including nested fixture compilers."""
    shard_budget = SHARD_BUDGET_SECONDS if budget_seconds is None else budget_seconds
    events: list[dict] = []
    parse_error: json.JSONDecodeError | None = None
    with subprocess.Popen(command, cwd=cwd, stdout=subprocess.PIPE, text=True, start_new_session=True) as process:

        def stop() -> None:
            if process.poll() is None:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass

        timer = threading.Timer(max(0, shard_budget - (time.monotonic() - started)), stop)
        if enforce_budget:
            timer.start()
        try:
            assert process.stdout is not None
            for line in process.stdout:
                print(line, end="", flush=True)
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError as exc:
                    parse_error = exc
            result = process.wait()
        finally:
            timer.cancel()
            if enforce_budget:
                timer.join()
    check_budgets(events, elapsed=time.monotonic() - started, enforce=enforce_budget, budget_seconds=shard_budget)
    if parse_error is not None:
        raise ValueError(f"invalid Go test JSON: {parse_error}")
    if result:
        raise subprocess.CalledProcessError(result, command)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, subprocess.CalledProcessError) as exc:
        print(f"Operator shard failed: {exc}", file=sys.stderr)
        sys.exit(1)
