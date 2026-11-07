#!/usr/bin/env python3
"""GitHub Actions cache budget: per-family limits, pruning of superseded keys, and the 80% alarm.

Every cache key belongs to a family (the key without its lockfile hash, commit sha and tool versions) and each
family has an explicit budget in .github/ci_budgets.json (`cache`): how many keys it may hold and how many MB.
`prune` runs on main after a cache save and deletes the keys a newer key of the same family supersedes (idle
for at least `min_idle_minutes`: a job that restored a key already has it; only a restore in flight needs it).
`check` runs in the hourly health job: it fails when the repository is at or over `alarm_fraction` of the GitHub
limit, when a superseded key stays unpruned for `stale_after_minutes`, when a family's newest keys exceed
its `max_mb`, or when a key matches no family. A second key beside the newest one for a few minutes is the
normal state while overlapping runs converge and is not an alarm.

usage: ci_cache_budget.py check | prune [family-prefix]
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from ci_github_api import GithubApiUnavailable, gh_json

BUDGETS = Path(__file__).resolve().parents[1] / ".github" / "ci_budgets.json"
HASHES = re.compile(r"-[0-9a-f]{64}(-[0-9a-f]{40})?$")
VERSION = re.compile(r"-\d+\.\d+\.\d+(?=-|$)")
MB = 1024 * 1024

Json = Mapping[str, Any]
Api = Callable[..., Any]


def family_of(key: str) -> str:
    """The key without its trailing lockfile hash and commit sha, and without dotted tool versions."""
    return VERSION.sub("", HASHES.sub("", key))


def budget_for(family: str, families: Mapping[str, Json]) -> tuple[str, Json] | None:
    """The budget whose name is the longest prefix of the family; None when no family budget covers it."""
    matches = [name for name in families if family.startswith(name)]
    return (max(matches, key=len), families[max(matches, key=len)]) if matches else None


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def grouped(caches: Iterable[Json], families: Mapping[str, Json]) -> tuple[dict[str, list[Json]], list[Json]]:
    by_budget: dict[str, list[Json]] = {name: [] for name in families}
    unknown: list[Json] = []
    for cache in caches:
        found = budget_for(family_of(cache["key"]), families)
        if found is None:
            unknown.append(cache)
        else:
            by_budget[found[0]].append(cache)
    return by_budget, unknown


def usage_failures(budget: Json, total_bytes: int, caches: list[Json], now: datetime) -> list[str]:
    """Alarms for conditions that persist or threaten the limit; never for the state a prune converges from.

    Overlapping pushes and PR runs leave a superseded key beside the newest one for a few minutes, so a family
    holding more than `keep` keys is not an alarm by itself. It alarms when a superseded key has sat unused for
    `stale_after_minutes` (the prune that should have removed it did not), when the newest `keep` keys exceed the
    family's `max_mb` (the budget no longer fits what the cache holds), when total usage is at the alarm fraction
    of the limit (counting the transient keys), or when a key is in no family. Every message names the keys and
    the action.
    """
    failures: list[str] = []
    limit = budget["repo_limit_mb"] * MB
    if total_bytes >= budget["alarm_fraction"] * limit:
        biggest_first = sorted(caches, key=lambda row: row["size_in_bytes"], reverse=True)
        biggest = ", ".join(f"{row['key']} ({row['size_in_bytes'] / MB:.0f} MB)" for row in biggest_first[:5])
        failures.append(
            f"Actions cache usage {total_bytes / MB:.0f} MB is at or over {budget['alarm_fraction']:.0%} "
            f"of the {budget['repo_limit_mb']} MB repository limit; GitHub evicts the least recently used at 100%. "
            f"Largest keys: {biggest}. Action: run `python3 scripts/ci_cache_budget.py prune` (needs actions: write) "
            f"or delete superseded keys; if only the newest keys remain, lower a family's max_mb in "
            f".github/ci_budgets.json or drop a cache"
        )
    stale = timedelta(minutes=budget["stale_after_minutes"])
    by_budget, unknown = grouped(caches, budget["families"])
    for name, rows in by_budget.items():
        allowed = budget["families"][name]
        newest = sorted(rows, key=lambda row: row["created_at"], reverse=True)[: allowed["keep"]]
        kept_mb = sum(row["size_in_bytes"] for row in newest) / MB
        if kept_mb > allowed["max_mb"]:
            keys = ", ".join(row["key"] for row in newest)
            failures.append(
                f"cache family {name}: the newest {allowed['keep']} key(s) hold {kept_mb:.0f} MB, over the "
                f"{allowed['max_mb']} MB budget ({keys}). Action: shrink what the cache stores, or raise max_mb in "
                f".github/ci_budgets.json with review"
            )
        lingering = superseded(rows, allowed["keep"], now, stale)
        if lingering:
            listed = ", ".join(
                f"{row['key']} (id {row['id']}, last used {row['last_accessed_at']})" for row in lingering
            )
            failures.append(
                f"cache family {name}: superseded key(s) unused for over {budget['stale_after_minutes']} min were "
                f"not pruned: {listed}. Action: `python3 scripts/ci_cache_budget.py prune {name}` with actions: "
                f"write, or `gh api -X DELETE repos/<repo>/actions/caches/<id>`; check that the main cache save "
                f"for this family is followed by its prune step"
            )
    failures.extend(f"cache key {row['key']} is in no family; add a budget under cache.families" for row in unknown)
    return failures


def superseded(rows: list[Json], keep: int, now: datetime, min_idle: timedelta) -> list[Json]:
    """Keys beyond the newest `keep` that nothing used in the last min_idle."""
    newest_first = sorted(rows, key=lambda row: row["created_at"], reverse=True)
    return [row for row in newest_first[keep:] if now - parse_time(row["last_accessed_at"]) >= min_idle]


def prune(
    budget: Json,
    caches: list[Json],
    delete: Callable[[int], None],
    now: datetime,
    prefix: str = "",
) -> list[Json]:
    by_budget, _ = grouped(caches, budget["families"])
    removed: list[Json] = []
    for name, rows in by_budget.items():
        if not name.startswith(prefix):
            continue
        for row in superseded(
            rows, budget["families"][name]["keep"], now, timedelta(minutes=budget["min_idle_minutes"])
        ):
            delete(row["id"])
            removed.append(row)
    return removed


def list_caches(api: Api) -> list[Json]:
    caches: list[Json] = []
    page = 1
    while True:
        rows = api(f"actions/caches?per_page=100&page={page}")["actions_caches"]
        caches.extend(rows)
        if len(rows) < 100:
            return caches
        page += 1


def delete_cache(cache_id: int) -> None:
    try:
        gh_json(f"actions/caches/{cache_id}", "DELETE")
    except subprocess.CalledProcessError as error:
        if "404" not in (error.stderr or ""):  # a concurrent prune already removed it
            raise


def render(budget: Json, total_bytes: int, caches: list[Json]) -> str:
    lines = [
        "### Actions cache budget",
        "",
        f"Repository usage {total_bytes / MB:.0f} MB of {budget['repo_limit_mb']} MB "
        f"(alarm at {budget['alarm_fraction']:.0%}), {len(caches)} keys.",
        "",
        "| family | keys | MB | budget keys | budget MB |",
        "|---|---|---|---|---|",
    ]
    by_budget, _ = grouped(caches, budget["families"])
    for name, rows in sorted(by_budget.items()):
        allowed = budget["families"][name]
        size = sum(r["size_in_bytes"] for r in rows) / MB
        lines.append(f"| {name} | {len(rows)} | {size:.0f} | {allowed['keep']} | {allowed['max_mb']} |")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["check", "prune"])
    parser.add_argument("prefix", nargs="?", default="", help="prune only families whose budget name starts with this")
    args = parser.parse_args()
    budget = json.loads(BUDGETS.read_text())["cache"]
    try:
        return run(args, budget)
    except GithubApiUnavailable as error:
        # Neither command has anything to act on without the listing; the next health run repeats it.
        print(f"::warning::GitHub API unavailable: {error}. Cache {args.command} skipped.")
        return 0


def run(args: argparse.Namespace, budget: Json) -> int:
    caches = list_caches(gh_json)
    if args.command == "prune":
        for row in prune(budget, caches, delete_cache, datetime.now(UTC), args.prefix):
            print(f"deleted {row['key']} ({row['size_in_bytes'] / MB:.0f} MB, last used {row['last_accessed_at']})")
        return 0
    # GitHub's usage counter lags deletions by up to an hour; the listing is current, so alarm on the listing
    # and show the counter for reference.
    total = sum(row["size_in_bytes"] for row in caches)
    reported = gh_json("actions/cache/usage")["active_caches_size_in_bytes"]
    print(f"GitHub's usage counter reports {reported / MB:.0f} MB (it lags deletions)")
    report = render(budget, total, caches)
    failures = usage_failures(budget, total, caches, datetime.now(UTC))
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with Path(summary).open("a") as handle:
            handle.write(report)
    print(report)
    for failure in failures:
        print(f"::error::{failure}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
