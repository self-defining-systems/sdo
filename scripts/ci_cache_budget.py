#!/usr/bin/env python3
"""GitHub Actions cache budget: per-family limits, pruning of superseded keys, and the 80% alarm.

Every cache key belongs to a family (the key without its lockfile hash, commit sha and tool versions) and each
family has an explicit budget in .github/ci_budgets.json (`cache`): how many keys it may hold and how many MB.
`prune` runs on main after a cache save and deletes the keys a newer key of the same family supersedes (idle
for at least `min_idle_minutes`, so a key a job is restoring right now survives). `check` runs in the hourly
health job: it fails when the repository is at or over `alarm_fraction` of the GitHub limit, when a family is
over its budget, or when a key matches no family.

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


def usage_failures(budget: Json, total_bytes: int, caches: list[Json]) -> list[str]:
    """Alarms: repository usage at the alarm fraction, a family over budget, a key in no family."""
    failures: list[str] = []
    limit = budget["repo_limit_mb"] * MB
    if total_bytes >= budget["alarm_fraction"] * limit:
        failures.append(
            f"Actions cache usage {total_bytes / MB:.0f} MB is at or over {budget['alarm_fraction']:.0%} "
            f"of the {budget['repo_limit_mb']} MB repository limit; GitHub evicts the least recently used at 100%"
        )
    by_budget, unknown = grouped(caches, budget["families"])
    for name, rows in by_budget.items():
        allowed = budget["families"][name]
        size = sum(row["size_in_bytes"] for row in rows) / MB
        if len(rows) > allowed["keep"] + budget["slack_keys"] or size > allowed["max_mb"]:
            failures.append(
                f"cache family {name}: {len(rows)} keys, {size:.0f} MB; "
                f"budget is {allowed['keep']} keys, {allowed['max_mb']} MB"
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


def gh_api(path: str, method: str = "GET") -> Any:
    repo = os.environ["GITHUB_REPOSITORY"]
    command = ["gh", "api", "-X", method, f"repos/{repo}/{path}"]
    out = subprocess.run(command, check=True, capture_output=True, text=True).stdout
    return json.loads(out) if out.strip() else None


def delete_cache(cache_id: int) -> None:
    try:
        gh_api(f"actions/caches/{cache_id}", "DELETE")
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
    caches = list_caches(gh_api)
    if args.command == "prune":
        for row in prune(budget, caches, delete_cache, datetime.now(UTC), args.prefix):
            print(f"deleted {row['key']} ({row['size_in_bytes'] / MB:.0f} MB, last used {row['last_accessed_at']})")
        return 0
    # GitHub's usage counter lags deletions by up to an hour; the listing is current, so alarm on the listing
    # and show the counter for reference.
    total = sum(row["size_in_bytes"] for row in caches)
    reported = gh_api("actions/cache/usage")["active_caches_size_in_bytes"]
    print(f"GitHub's usage counter reports {reported / MB:.0f} MB (it lags deletions)")
    report = render(budget, total, caches)
    failures = usage_failures(budget, total, caches)
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
