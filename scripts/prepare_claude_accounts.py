#!/usr/bin/env python3
"""Sync cswap's Linux store into a private, refreshable account Secret bundle.

Only aliases, health, concurrency and lane assignments reach stdout. The bundle
must be outside the repository. Upload with kubectl --from-file, never literals.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path

from libs.agent_cli.claude_accounts import prepare_accounts, sync_cswap


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    default_store = Path(os.environ.get("XDG_DATA_HOME") or Path.home() / ".local/share") / "claude-swap"
    parser.add_argument("--store", type=Path, default=default_store)
    parser.add_argument("--cswap", default=str(Path.home() / ".local/bin/cswap"))
    parser.add_argument("--directory", type=Path, required=True, help="Private bundle, outside all source checkouts")
    parser.add_argument("--pool", type=Path, required=True, help="One canonical writable account pool per host")
    parser.add_argument("--alias", type=Path, action="append", default=[])
    parser.add_argument("--lane", action="append", default=[])
    parser.add_argument("--capacity", type=int, default=2)
    args = parser.parse_args()
    repository = Path(__file__).resolve().parents[1]
    if any(path.resolve().is_relative_to(repository) for path in (args.directory, args.pool, *args.alias)):
        parser.error("credential and runtime directories must be outside the source checkout")
    # cswap may emit account details; capture both streams and never echo errors.
    result = subprocess.run([args.cswap, "list", "--json"], capture_output=True, text=True, timeout=60, check=False)
    if result.returncode:
        parser.exit(1, "cswap inventory failed; output withheld\n")
    try:
        inventory = sync_cswap(
            args.store, args.directory, json.loads(result.stdout), lanes=args.lane, capacity=args.capacity
        )
        prepare_accounts(args.directory, args.pool, args.alias, capacity=args.capacity)
    except (OSError, ValueError, RuntimeError):
        parser.exit(1, "account preparation failed; credential-bearing diagnostics withheld\n")
    print(json.dumps(inventory, sort_keys=True))


if __name__ == "__main__":
    main()
