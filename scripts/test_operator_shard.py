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
from pathlib import Path

OPERATOR = Path(__file__).resolve().parents[1] / "sdo/operator"
PREFIX = "sdo.dev/operator/"
PACKAGES = ("builder", "broker", "memory/validate")
SHARDS = ("acceptance", "acceptance-second", "broker", "compiler", "remaining")
FLAGS = ("-race", "-parallel=2", "-count=1", "-timeout=4m", "-json")
# Hosted/cap-2 profiles: ~40s and ~30s of builder work leave the compiler
# runner; the broker acceptance groups previously had this much spare capacity.
MOVED_BUILDER_TESTS = {
    "TestProgramDetectorCorpusChecksEveryLocatedBinding": "acceptance-second",
    "TestOperatorCorpusRequiresProductionClosedLoop": "acceptance-second",
    "TestProgramSaveReplayDoesNotRequireRenameInvariance": "acceptance",
    "TestTypedObservationProgramCompilesAndRejectsStaleEvidence": "acceptance",
}
TEST_BUDGET_SECONDS = 180
SHARD_BUDGET_SECONDS = 210


def partition(inventory: dict[str, list[str]]) -> dict[str, dict[str, list[str]]]:
    missing = MOVED_BUILDER_TESTS.keys() - set(inventory.get(PREFIX + "builder", []))
    if missing:
        raise ValueError(f"missing measured builder tests (update partition after a rename): {sorted(missing)}")
    groups: dict[str, dict[str, list[str]]] = {shard: {} for shard in SHARDS if shard != "remaining"}
    name_owners: dict[str, str] = {}
    for package, names in inventory.items():
        for name in names:
            if package == PREFIX + "builder":
                shard = MOVED_BUILDER_TESTS.get(name, "compiler")
            elif package == PREFIX + "broker":
                shard = {
                    "TestCLIAndBrokerAgreeOnAcceptance": "acceptance",
                    "TestCLIAndBrokerAgreeOnAcceptanceSecondShard": "acceptance-second",
                }.get(name, "broker")
            elif package == PREFIX + "memory/validate":
                shard = "compiler"
            else:
                raise ValueError(f"unexpected compiled package: {package}")
            # A union -run regex is safe only when duplicate names share an owner.
            if name in name_owners and name_owners[name] != shard:
                raise ValueError(f"{name} in different shards across packages")
            name_owners[name] = shard
            groups[shard].setdefault(package, []).append(name)
    if not all(groups.values()):
        raise ValueError("every operator name partition must contain tests")
    return groups


def check_budgets(events: list[dict], *, elapsed: float) -> None:
    completed = [
        e for e in events if e.get("Action") in ("pass", "fail", "skip") and e.get("Test") and "/" not in e["Test"]
    ]
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
    if elapsed > SHARD_BUDGET_SECONDS:
        failures.append(f"{elapsed:.2f}s exceeds {SHARD_BUDGET_SECONDS}s shard budget")
    print(f"Operator timing: {elapsed:.2f}s; slowest: {detail}", flush=True)
    if failures:
        raise ValueError("; ".join(failures) + "; slowest: " + detail)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("shard", choices=SHARDS)
    args = parser.parse_args()
    started = time.monotonic()
    if args.shard == "remaining":
        listed = subprocess.run(["go", "list", "./..."], cwd=OPERATOR, text=True, capture_output=True, check=True)
        packages = [p for p in listed.stdout.splitlines() if p not in {PREFIX + p for p in PACKAGES}]
        if not packages:
            raise ValueError("no remaining operator packages discovered")
        selection: list[str] = []
    else:
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
            raise ValueError("compiled discovery must contain builder, broker and memory/validate tests")
        groups = partition(inventory)
        print(f"Coverage guard: {sum(map(len, inventory.values()))} compiled names assigned exactly once", flush=True)
        members = groups[args.shard]
        packages = sorted(members)
        selection = ["-run", "^(" + "|".join(re.escape(n) for names in members.values() for n in names) + ")$"]
    run_tests(["go", "test", *FLAGS, *selection, *packages], started=started)


def run_tests(command: list[str], *, started: float) -> None:
    """Bound the entire process group, including nested fixture compilers."""
    events: list[dict] = []
    with subprocess.Popen(command, cwd=OPERATOR, stdout=subprocess.PIPE, text=True, start_new_session=True) as process:

        def stop() -> None:
            if process.poll() is None:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass

        timer = threading.Timer(max(0, SHARD_BUDGET_SECONDS - (time.monotonic() - started)), stop)
        timer.start()
        try:
            assert process.stdout is not None
            for line in process.stdout:
                print(line, end="", flush=True)
                events.append(json.loads(line))
            result = process.wait()
        finally:
            timer.cancel()
            timer.join()
    check_budgets(events, elapsed=time.monotonic() - started)
    if result:
        raise subprocess.CalledProcessError(result, command)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, subprocess.CalledProcessError) as exc:
        print(f"Operator shard failed: {exc}", file=sys.stderr)
        sys.exit(1)
