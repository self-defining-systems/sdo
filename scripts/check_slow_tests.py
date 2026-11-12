#!/usr/bin/env python3
"""Fail when a PR-tier test is slower than the budget and is not allowlisted.

Reads `go test -json` output (--go) and/or pytest `--durations` output (--pytest).
Budget and allowlist live in .github/ci_budgets.json; see docs/testing-guide.md.
With --base <git ref>, a test that is new relative to that ref and takes over half
the budget also fails unless it is allowlisted with a reason: a new test is cheap to
fix now and a flake source once its time varies run to run.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable

BUDGETS = Path(__file__).resolve().parents[1] / ".github" / "ci_budgets.json"
PYTEST_DURATION = re.compile(r"^\s*(?P<seconds>\d+(?:\.\d+)?)s\s+(?:call|setup|teardown)\s+(?P<node>\S+)")


# A shared runner varies a test's wall time by tens of percent, so a test just over the budget is a warning and only
# one over NOISE_FACTOR times the budget fails an existing PR (a new test over half the budget still fails).
NOISE_FACTOR = 1.5


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


def test_name(key: str) -> str:
    """The function name of a Go key ('<package> <TopLevelTest>') or pytest node id."""
    return key.split("::")[-1] if "::" in key else key.rsplit(" ", 1)[-1]


def defined_at(base: str, key: str, root: Path) -> bool:
    """True when the base revision already defines the test (by name, in any test file)."""
    name = test_name(key)
    if "::" in key:
        spec, pattern = [f"{key.split('::')[0]}"], rf"def {name}\("
    else:
        spec, pattern = ["*_test.go"], rf"func {name}\("
    found = subprocess.run(
        ["git", "-C", str(root), "grep", "-qE", pattern, base, "--", *spec],
        check=False,
        capture_output=True,
    )
    if found.returncode > 1:
        raise RuntimeError(f"git grep failed against {base}: {found.stderr.decode().strip()}")
    return found.returncode == 0


def new_near_budget(
    near: dict[str, float], allowed: dict[str, dict[str, str]], base: str, root: Path = BUDGETS.parents[1]
) -> dict[str, float]:
    """Tests over half the budget that the base lacks and that carry no allowlist reason."""
    reasons = {**allowed["go"], **allowed["python"]}
    return {
        key: seconds
        for key, seconds in near.items()
        if not reasons.get(key, "").strip() and not defined_at(base, key, root)
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go", type=Path, action="append", default=[])
    parser.add_argument("--pytest", type=Path, action="append", default=[])
    parser.add_argument("--base", help="git ref of the PR base; makes new tests over half the budget fail")
    args = parser.parse_args()
    budgets = json.loads(BUDGETS.read_text())
    limit = float(budgets["slow_test_seconds"])
    allowed = budgets["slow_tests"]
    hard = limit * NOISE_FACTOR
    found: dict[str, float] = {}
    for path in args.go:
        found.update(offenders(go_slow(path.read_text(errors="replace").splitlines(), hard), allowed["go"]))
    for path in args.pytest:
        found.update(offenders(pytest_slow(path.read_text(errors="replace").splitlines(), hard), allowed["python"]))
    for key, seconds in sorted(found.items(), key=lambda item: -item[1]):
        print(f"::error::{key} took {seconds:.1f}s (PR budget {limit:.0f}s, failing above {hard:.0f}s)")
    near = {key: s for key, s in near_budget(args, allowed, limit).items() if key not in found}
    fresh = new_near_budget(near, allowed, args.base) if args.base else {}
    for key, seconds in sorted(fresh.items(), key=lambda item: -item[1]):
        print(f"::error::{key} is new and took {seconds:.1f}s, over half the {limit:.0f}s budget")
    found.update(fresh)
    for key, seconds in sorted(near.items(), key=lambda item: -item[1]):
        if key not in fresh:
            print(
                f"::warning::{key} took {seconds:.1f}s, over half the {limit:.0f}s budget: its time varies run to run"
            )
    if found:
        print(
            "Move the test to the nightly/integration tier (testing.Short skip or the nightly ignore list), make it "
            "faster, or add it with a reason to slow_tests in .github/ci_budgets.json "
            "(required for a new test over half the budget).",
            file=sys.stderr,
        )
        return 1
    print(f"no PR-tier test over {hard:.0f}s outside the allowlist")
    return 0


if __name__ == "__main__":
    sys.exit(main())
