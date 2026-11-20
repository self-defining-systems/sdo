"""Production SDO must stay cloud-native and platform-neutral.

``sdo/`` talks to the Kubernetes API, registries (images by digest, or BuildKit push to
one trusted registry) and Kubernetes volumes. It must not depend on kind, ``docker exec``,
node shell access, node-local volumes or any single cloud. Platform bindings (kind, EKS,
later GCP or Azure) live in ``benchmarks/`` and the lanes providers.

The denylist is the pattern list below. Existing uses are a per-file ratchet in
``scripts/platform_neutral_allowlist.json``: a count may drop, never rise, and a file
with no occurrences must not stay listed. Test files and generated code are not scanned.
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
ALLOWLIST = ROOT / "scripts/platform_neutral_allowlist.json"

PATTERNS = (
    r"docker (?:exec|run|build|load|cp)\b",  # container runtime commands
    r"docker, kind\b",  # tool lists naming the local cluster runtime
    r"\bkind (?:cluster|node|nodes|worker|lane|lanes|load|create|delete)\b",  # the kind local cluster tool
    r"\bkindnet\b",
    r"\bcrictl\b|\bnsenter\b|\bcontainerd\b|\bctr (?:images|run|task)\b",  # node shell and runtime access
    r"HostPathVolumeSource|HostPathDirectory|HostPathFile|\bhost_path\b|\bhostPath:",  # node-local volumes
    r"NFSVolumeSource",  # volumes that need a specific file server
    r"aws-sdk|aws-go|amazonaws\.com|\.ecr\.|cloud\.google\.com/go|google\.golang\.org/api|azure-sdk|gcr\.io|azurecr\.io|pkg\.dev",
    r"\b(?:EKS|GKE|AKS)\b",
)
PATTERN = re.compile("|".join(PATTERNS), re.IGNORECASE)
# Files that are not production code or not text worth scanning.
EXCLUDE = re.compile(r"(_test\.go$|/test_[^/]*\.py$|\.sum$|\.pyi$|_pb2\.py$|\.pb\.go$|/gen/|/testdata/|/_gen/)")


def tracked_files(root: Path, scan_roots: tuple[str, ...]) -> list[str]:
    out = subprocess.run(
        ["git", "-C", str(root), "ls-files", "-z", "--", *scan_roots], check=True, capture_output=True, text=True
    ).stdout
    return [p for p in out.split("\0") if p and not EXCLUDE.search(p)]


def count_matches(root: Path, files: list[str], pattern: re.Pattern[str]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for rel in files:
        try:
            text = (root / rel).read_text()
        except (UnicodeError, OSError):
            continue
        if n := len(pattern.findall(text)):
            counts[rel] = n
    return counts


def violations(counts: Counter[str], allowlist: dict[str, dict], allowlist_name: str = ALLOWLIST.name) -> list[str]:
    errors = []
    for rel, n in sorted(counts.items()):
        entry = allowlist.get(rel)
        if entry is None:
            errors.append(f"{rel}: {n} platform-specific reference(s); production SDO must stay platform-neutral")
        elif n > entry["count"]:
            errors.append(f"{rel}: {n} references exceed the ratchet of {entry['count']}")
    for rel, entry in sorted(allowlist.items()):
        n = counts.get(rel, 0)
        if not entry.get("reason"):
            errors.append(f"{rel}: allowlist entry needs a reason")
        if n < entry["count"]:
            errors.append(
                f"{rel}: {n} references, allowlist says {entry['count']}; lower the ratchet in {allowlist_name}"
            )
    return errors


def check(
    root: Path = ROOT,
    scan_roots: tuple[str, ...] = SCAN_ROOTS,
    allowlist_path: Path = ALLOWLIST,
) -> tuple[list[str], Counter[str]]:
    allowlist = json.loads(allowlist_path.read_text()) if allowlist_path.exists() else {}
    counts = count_matches(root, tracked_files(root, scan_roots), PATTERN)
    return violations(counts, allowlist, allowlist_path.name), counts


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
