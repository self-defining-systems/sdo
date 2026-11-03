#!/usr/bin/env python3
"""Reject experiment output markers in tracked text; no per-file exemptions."""

import argparse
import re
import subprocess
from pathlib import Path

MARKERS = re.compile(rb"/mnt/data/shli/" + rb"sdo-exp|sdo-exp" + rb"/|\d{8}T\d{6}Z(?:-sdo)?")


def violations(root: Path) -> list[str]:
    names = subprocess.check_output(["git", "-C", str(root), "ls-files", "-z"]).decode().split("\0")
    findings: list[str] = []
    for name in filter(None, names):
        relative = Path(name)
        path = root / relative
        if not path.exists():  # A tracked deletion is safe to commit.
            continue
        if relative.parts[0] == "handoffs" or (len(relative.parts) == 1 and relative.match("*_REPORT.md")):
            findings.append(f"{name}: run report path")
        if not path.is_file() or path.is_symlink():
            continue  # Gitlinks and symlink targets are not tracked text files.
        content = path.read_bytes()
        if b"\0" in content:
            continue
        for number, line in enumerate(content.splitlines(), 1):
            if MARKERS.search(line):
                findings.append(f"{name}:{number}: run-output marker")
    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    findings = violations(args.root)
    for finding in findings:
        print(f"{finding}; keep run reports and measurements outside the repository")
    return int(bool(findings))


if __name__ == "__main__":
    raise SystemExit(main())
