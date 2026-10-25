#!/usr/bin/env python3
"""Reject unexplained or mixed-purpose prompt-budget raises in every PR commit."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

if TYPE_CHECKING:
    from collections.abc import Sequence

BUDGET_PATH = "sdo/agents/prompt_budgets.toml"


@dataclass(frozen=True)
class Budget:
    max_chars: int
    reason: str


def parse_budgets(document: str) -> dict[str, Budget]:
    """Validate the manifest and flatten role.part entries without repository I/O."""
    entries: dict[str, Budget] = {}
    for role, parts in tomllib.loads(document).items():
        if not isinstance(parts, dict):
            raise ValueError(f"{role}: expected role table")
        for part, entry in parts.items():
            name = f"{role}.{part}"
            if not isinstance(entry, dict) or set(entry) != {"max_chars", "reason"}:
                raise ValueError(f"{name}: requires max_chars and reason")
            limit, reason = entry["max_chars"], entry["reason"]
            if type(limit) is not int or limit < 0:
                raise ValueError(f"{name}: max_chars must be a nonnegative integer")
            if not isinstance(reason, str) or not reason.strip():
                raise ValueError(f"{name}: reason must be nonempty")
            entries[name] = Budget(limit, reason)
    return entries


def check_raise(
    before: dict[str, Budget],
    after: dict[str, Budget],
    *,
    files: Sequence[str],
    trailers: str,
) -> list[str]:
    """New positive limits are raises too; decreases need no special commit."""
    raised = {
        name: entry
        for name, entry in after.items()
        if entry.max_chars > (before[name].max_chars if name in before else 0)
    }
    if not raised:
        return []
    errors = []
    if set(files) != {BUDGET_PATH}:
        errors.append("budget raises must change only " + BUDGET_PATH)
    if not re.search(r"^Budget-Reason:[ \t]*\S.*$", trailers, re.MULTILINE | re.IGNORECASE):
        errors.append("budget raises require a nonempty Budget-Reason: trailer")
    errors.extend(
        f"{name}: raised max_chars requires an updated reason"
        for name in raised
        if name in before and before[name].reason.strip() == after[name].reason.strip()
    )
    return errors


def git(repository: Path, *args: str, input_text: str | None = None) -> str:
    return subprocess.run(
        ["git", "-C", str(repository), *args], input=input_text, check=True, capture_output=True, text=True
    ).stdout


def at_commit(repository: Path, commit: str) -> dict[str, Budget]:
    exists = subprocess.run(
        ["git", "-C", str(repository), "cat-file", "-e", f"{commit}:{BUDGET_PATH}"], capture_output=True, check=False
    )
    return parse_budgets(git(repository, "show", f"{commit}:{BUDGET_PATH}")) if exists.returncode == 0 else {}


def check_range(repository: Path, base: str, head: str) -> list[str]:
    """Inspect individual commits, so an increase then decrease cannot hide a raise."""
    errors = []
    for commit in git(repository, "rev-list", "--reverse", "--topo-order", f"{base}..{head}").splitlines():
        parents = git(repository, "show", "-s", "--format=%P", commit).split()
        parent = parents[0] if parents else None
        files = (
            git(repository, "diff", "--name-only", "-z", parent, commit)
            if parent
            else git(repository, "diff-tree", "--root", "--no-commit-id", "--name-only", "-r", "-z", commit)
        )
        changed = files.rstrip("\0").split("\0")
        if BUDGET_PATH not in changed:
            continue
        try:
            before = at_commit(repository, parent) if parent else {}
            after = at_commit(repository, commit)
            message = git(repository, "show", "-s", "--format=%B", commit)
            trailers = git(repository, "interpret-trailers", "--parse", input_text=message)
            errors.extend(
                f"{commit[:12]}: {error}" for error in check_raise(before, after, files=changed, trailers=trailers)
            )
        except ValueError as exc:
            errors.append(f"{commit[:12]}: invalid budgets: {exc}")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True, help="PR base SHA (not a moving branch name)")
    parser.add_argument("--head", default="HEAD", help="PR head SHA")
    parser.add_argument("--repository", type=Path, default=Path.cwd())
    args = parser.parse_args()
    try:
        errors = check_range(args.repository, args.base, args.head)
    except (subprocess.CalledProcessError, ValueError) as exc:
        print(f"cannot validate prompt budget history: {exc}", file=sys.stderr)
        return 1
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print("prompt budget commit policy passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
