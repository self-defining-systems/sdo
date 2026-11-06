#!/usr/bin/env python3
"""Flaky-test quarantine: the data file's readers for CI and its deadline check.

Entries live in .github/ci_quarantine.json. PR, queue and main runs skip them
(`go-skip`, `pytest-deselect`); the nightly run still executes them
(`pytest-ids`, and the nightly Go run skips nothing). `check` fails when an
entry is malformed, past its deadline, or names a test that no longer exists.

    ci_quarantine.py check [--today YYYY-MM-DD]
    ci_quarantine.py go-skip         # one anchored regex for `go test -skip`, or nothing
    ci_quarantine.py pytest-deselect # `--deselect <id>` arguments, one per line
    ci_quarantine.py pytest-ids      # node ids, one per line
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / ".github" / "ci_quarantine.json"
KINDS = ("go", "pytest")


@dataclass(frozen=True)
class Entry:
    id: str
    kind: str
    owner: str
    reason: str
    deadline: date

    def __post_init__(self) -> None:
        if self.kind not in KINDS:
            raise ValueError(f"{self.id!r}: kind must be one of {KINDS}, got {self.kind!r}")
        for field in ("id", "owner", "reason"):
            if not getattr(self, field).strip():
                raise ValueError(f"{self.id!r}: {field} must not be empty")
        if self.kind == "go" and not re.fullmatch(r"\S+ Test\w+", self.id):
            raise ValueError(f"{self.id!r}: a go id is '<package import path> <TopLevelTest>'")
        if self.kind == "pytest" and "::" not in self.id:
            raise ValueError(f"{self.id!r}: a pytest id is a node id (path::test)")


def load(path: Path = DATA) -> list[Entry]:
    entries = [
        Entry(**{**raw, "deadline": date.fromisoformat(raw["deadline"])})
        for raw in json.loads(path.read_text())["tests"]
    ]
    ids = [e.id for e in entries]
    if len(set(ids)) != len(ids):
        raise ValueError("duplicate quarantine ids")
    return entries


def problems(entries: list[Entry], today: date, root: Path = ROOT) -> list[str]:
    found = []
    for e in entries:
        if e.deadline < today:
            found.append(
                f"{e.id}: quarantine deadline {e.deadline} passed (owner {e.owner}); fix the test or remove the entry"
            )
        if not exists(e, root):
            found.append(f"{e.id}: the test no longer exists; remove the entry")
    return found


def exists(entry: Entry, root: Path) -> bool:
    if entry.kind == "pytest":
        file, _, name = entry.id.partition("::")
        path = root / file
        return (
            path.is_file()
            and re.search(rf"def {re.escape(name.split('[')[0].split('::')[-1])}\(", path.read_text()) is not None
        )
    name = entry.id.split()[1]
    pattern = re.compile(rf"^func {re.escape(name)}\(", re.MULTILINE)
    return any(pattern.search(p.read_text(errors="replace")) for p in root.rglob("*_test.go") if ".git" not in p.parts)


def go_skip(entries: list[Entry]) -> str:
    names = sorted(e.id.split()[1] for e in entries if e.kind == "go")
    return f"^({'|'.join(names)})$" if names else ""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["check", "go-skip", "pytest-deselect", "pytest-ids"])
    parser.add_argument("--today", type=date.fromisoformat, default=None)
    parser.add_argument("--data", type=Path, default=DATA)
    args = parser.parse_args(argv)
    entries = load(args.data)
    if args.command == "check":
        found = problems(entries, args.today or date.today())
        for line in found:
            print(f"::error::{line}")
        print(f"{len(entries)} quarantined test(s); {len(found)} problem(s)")
        return 1 if found else 0
    if args.command == "go-skip":
        print(go_skip(entries))
    else:
        ids = [e.id for e in entries if e.kind == "pytest"]
        print("\n".join(f"--deselect\n{i}" if args.command == "pytest-deselect" else i for i in ids))
    return 0


if __name__ == "__main__":
    sys.exit(main())
