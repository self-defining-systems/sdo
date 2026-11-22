"""Make an SREGym lanes memory directory (an ``operator.baseline``) from a commit of an application workspace.

``python -m benchmarks.sregym.lanes.baseline REPO COMMIT OUT``: ``OUT/memory.bundle`` (the repository up to COMMIT) and
``OUT/memory.json`` (``{"commit": ...}``), the format a run exports its learned memory in. A plan gives every arm of
an item the same baseline, the base memory p1 starts from; p2's ``operator.seed`` is p1's exported memory, which must
descend from it.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from benchmarks.sregym.adapter.budgets import WaitKind, wait_for
from benchmarks.sregym.lanes.workload import BUNDLE, MEMORY_FILE


def make(repo: Path, commit: str, out: Path) -> dict[str, str]:
    def git(*args: str) -> str:
        path = str(repo.resolve())
        return subprocess.run(
            ["git", "-c", f"safe.directory={path}", "-C", path, *args],
            capture_output=True,
            text=True,
            check=True,
            timeout=wait_for(WaitKind.OPERATION),
        ).stdout.strip()

    full = git("rev-parse", "--verify", f"{commit}^{{commit}}")
    out.mkdir(parents=True, exist_ok=True)
    ref = "refs/lanes-baseline/base"
    git("update-ref", ref, full)
    try:
        git("bundle", "create", str((out / BUNDLE).resolve()), ref)
    finally:
        git("update-ref", "-d", ref)
    meta = {"commit": full}
    (out / MEMORY_FILE).write_text(json.dumps(meta, indent=1) + "\n", encoding="utf-8")
    return meta


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 3:
        print("usage: python -m benchmarks.sregym.lanes.baseline REPO COMMIT OUT", file=sys.stderr)
        return 2
    print(json.dumps(make(Path(args[0]), args[1], Path(args[2]))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
