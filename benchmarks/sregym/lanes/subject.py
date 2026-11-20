"""The subject hooks of the SREGym lanes adapter: a shim, since the workload installs and drives SDO.

``python -m benchmarks.sregym.lanes.subject <verb>`` (``benchmarks/lanes/docs/contract.md``, subject verbs). On an
SREGym lane ``fastloop up`` installs SDO with the agent registrations and every ``fastloop run`` resets it to the
planned memory commit, so the hooks own nothing on the lane:

``check``, ``teardown``, ``collect``, ``transcripts``, ``snapshot``, ``reset``, ``reset-incidents``: nothing to do.
``prepare``, ``swap``: the attached pack and the SDO revision (``{"pack", "operator_version", "memory"}``); the
              operator file's seed (a memory directory) is passed to the workload's ``run`` through ``BENCH_OPERATOR``.
``idle``      idle (the workload's run waits for SDO's closure itself).
``describe``  no start-up period (``max_startup`` 0s), no model call of its own (the workload drives the agent).

Every other verb is unsupported (exit 64).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

UNSUPPORTED = 64
REPO_ROOT = Path(__file__).resolve().parents[3]
NOTHING = ("check", "teardown", "collect", "transcripts", "snapshot", "reset", "reset-incidents", "idle", "watch")


def revision(root: Path = REPO_ROOT) -> str:
    return subprocess.run(
        ["git", "-C", str(root), "rev-parse", "HEAD"], capture_output=True, text=True, check=True
    ).stdout.strip()


def seed_of(env: Mapping[str, str]) -> str:
    path = env.get("BENCH_OPERATOR")
    if not path:
        return ""
    return str(json.loads(Path(path).read_text(encoding="utf-8")).get("seed") or "")


def dispatch(verb: str, env: Mapping[str, str], version: Callable[[], str] = revision) -> tuple[int, dict[str, Any]]:
    if verb in NOTHING:
        return 0, {"ok": True}
    if verb == "describe":
        return 0, {"max_startup": "0s", "needs_credentials": False}
    if verb in ("prepare", "swap"):
        return 0, {"pack": env.get("BENCH_PACK", ""), "operator_version": version(), "memory": seed_of(env)}
    return UNSUPPORTED, {"unsupported": True}


def main(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    code, result = dispatch(args[0] if args else "", os.environ)
    print(json.dumps(result), flush=True)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
