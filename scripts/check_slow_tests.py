#!/usr/bin/env python3
"""Fail when a PR-tier test is slower than the budget and is not allowlisted.

Reads `go test -json` output (--go) and/or pytest `--durations` output (--pytest).
Budget and allowlist live in .github/ci_budgets.json; see docs/testing-guide.md.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable

BUDGETS = Path(__file__).resolve().parents[1] / ".github" / "ci_budgets.json"
PYTEST_DURATION = re.compile(r"^\s*(?P<seconds>\d+(?:\.\d+)?)s\s+(?:call|setup|teardown)\s+(?P<node>\S+)")


def go_slow(lines: Iterable[str], limit: float) -> dict[str, float]:
    """Slowest elapsed per '<package> <TopLevelTest>'. Parallel subtests report time the parent does not."""
    slow: dict[str, float] = {}
    for line in lines:
        start = line.find('{"Time"')
        if start < 0:
            continue
        try:
            event = json.loads(line[start:])
        except json.JSONDecodeError:
            continue
        if event.get("Action") not in ("pass", "fail") or not event.get("Test"):
            continue
        elapsed = float(event.get("Elapsed", 0))
        if elapsed > limit:
            key = f"{event['Package']} {event['Test'].split('/')[0]}"
            slow[key] = max(slow.get(key, 0.0), elapsed)
    return slow


def pytest_slow(lines: Iterable[str], limit: float) -> dict[str, float]:
    slow: dict[str, float] = {}
    for line in lines:
        match = PYTEST_DURATION.match(line)
        if match and float(match["seconds"]) > limit:
            node = match["node"].split("[")[0]
            slow[node] = max(slow.get(node, 0.0), float(match["seconds"]))
    return slow


def offenders(slow: dict[str, float], allowed: dict[str, str]) -> dict[str, float]:
    return {key: seconds for key, seconds in slow.items() if key not in allowed}


def near_budget(args: argparse.Namespace, allowed: dict[str, dict[str, str]], limit: float) -> dict[str, float]:
    """Tests over half the budget: a duration that varies run to run (a loaded runner doubles it) fails a merge soon."""
    half = limit / 2
    found: dict[str, float] = {}
    for path in args.go:
        found.update(offenders(go_slow(path.read_text(errors="replace").splitlines(), half), allowed["go"]))
    for path in args.pytest:
        found.update(offenders(pytest_slow(path.read_text(errors="replace").splitlines(), half), allowed["python"]))
    return found


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go", type=Path, action="append", default=[])
    parser.add_argument("--pytest", type=Path, action="append", default=[])
    args = parser.parse_args()
    budgets = json.loads(BUDGETS.read_text())
    limit = float(budgets["slow_test_seconds"])
    allowed = budgets["slow_tests"]
    found: dict[str, float] = {}
    for path in args.go:
        found.update(offenders(go_slow(path.read_text(errors="replace").splitlines(), limit), allowed["go"]))
    for path in args.pytest:
        found.update(offenders(pytest_slow(path.read_text(errors="replace").splitlines(), limit), allowed["python"]))
    for key, seconds in sorted(found.items(), key=lambda item: -item[1]):
        print(f"::error::{key} took {seconds:.1f}s (PR budget {limit:.0f}s)")
    for key, seconds in sorted(near_budget(args, allowed, limit).items(), key=lambda item: -item[1]):
        if key not in found:
            print(
                f"::warning::{key} took {seconds:.1f}s, over half the {limit:.0f}s budget: its time varies run to run"
            )
    if found:
        print(
            "Move the test to the nightly/integration tier (testing.Short skip or the nightly ignore list), make it "
            "faster, or add it with a reason to slow_tests in .github/ci_budgets.json.",
            file=sys.stderr,
        )
        return 1
    print(f"no PR-tier test over {limit:.0f}s outside the allowlist")
    return 0


if __name__ == "__main__":
    sys.exit(main())
