"""Production SDO code, skills and prompts must not name specific applications.

The denylist is data: the directory names under ``apps/`` plus the extra names in
``scripts/app_name_rules.json``. Matching is case-insensitive and word-bounded;
words inside a name may be separated by a space, ``_`` or ``-`` or nothing, so
``hotel reservation``, ``HotelReservation`` and ``hotel_reservation`` all match.
Existing occurrences are a per-file ratchet in ``scripts/app_name_allowlist.json``:
a count may drop, never rise, and a file with no occurrences must not stay listed.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCAN_ROOTS = ("sdo",)
RULES = ROOT / "scripts/app_name_rules.json"
ALLOWLIST = ROOT / "scripts/app_name_allowlist.json"


def _name_pattern(name: str) -> str:
    tokens = [t for t in re.split(r"[\s_-]+", name.strip()) if t]
    return r"[ _-]?".join(re.escape(t) for t in tokens)


def denylist(root: Path, rules: dict) -> list[str]:
    """Return application names: apps/ directories plus the rule file's extras."""
    exempt = set(rules.get("exempt_apps", {}))
    apps = root / "apps"
    derived = sorted(p.name for p in apps.iterdir() if p.is_dir()) if apps.is_dir() else []
    names = {n.lower() for n in derived if n.lower() not in exempt}
    names.update(n.lower() for n in rules.get("extra_names", []))
    return sorted(names)


def compile_denylist(names: list[str]) -> re.Pattern[str]:
    alternatives = "|".join(_name_pattern(n) for n in sorted(names, key=len, reverse=True))
    return re.compile(rf"(?<![A-Za-z0-9])(?:{alternatives})(?![A-Za-z0-9])", re.IGNORECASE)


def tracked_files(root: Path, scan_roots: tuple[str, ...]) -> list[str]:
    out = subprocess.run(
        ["git", "-C", str(root), "ls-files", "-z", "--", *scan_roots],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return [p for p in out.split("\0") if p]


def count_matches(root: Path, files: list[str], pattern: re.Pattern[str]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for rel in files:
        try:
            text = (root / rel).read_text()
        except (UnicodeError, OSError):
            continue
        n = len(pattern.findall(text))
        if n:
            counts[rel] = n
    return counts


def violations(counts: Counter[str], allowlist: dict[str, dict]) -> list[str]:
    errors = []
    for rel, n in sorted(counts.items()):
        entry = allowlist.get(rel)
        if entry is None:
            errors.append(f"{rel}: {n} application-name occurrence(s); production SDO must stay application-generic")
        elif n > entry["count"]:
            errors.append(f"{rel}: {n} occurrences exceed the ratchet of {entry['count']}")
    for rel, entry in sorted(allowlist.items()):
        n = counts.get(rel, 0)
        if not entry.get("reason"):
            errors.append(f"{rel}: allowlist entry needs a reason")
        if n < entry["count"]:
            errors.append(
                f"{rel}: {n} occurrences, allowlist says {entry['count']}; lower the ratchet in {ALLOWLIST.name}"
            )
    return errors


def check(
    root: Path = ROOT,
    scan_roots: tuple[str, ...] = SCAN_ROOTS,
    rules_path: Path = RULES,
    allowlist_path: Path = ALLOWLIST,
) -> tuple[list[str], Counter[str]]:
    rules = json.loads(rules_path.read_text())
    allowlist = json.loads(allowlist_path.read_text()) if allowlist_path.exists() else {}
    pattern = compile_denylist(denylist(root, rules))
    counts = count_matches(root, tracked_files(root, scan_roots), pattern)
    return violations(counts, allowlist), counts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--counts", action="store_true", help="print current per-file counts as JSON and exit")
    args = parser.parse_args()
    errors, counts = check()
    if args.counts:
        print(json.dumps(dict(sorted(counts.items())), indent=1))
        return
    if errors:
        print("\n".join(errors), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
