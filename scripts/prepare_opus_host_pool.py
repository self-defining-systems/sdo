#!/usr/bin/env python3
"""Provision one host's static Opus share through existing kind extraMounts."""

from __future__ import annotations

import argparse
from pathlib import Path

from libs.agent_cli.provider_capacity import DEFAULT_OPUS_HOST_SHARE, prepare_host_pool


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True, help="Canonical host pool outside source checkouts")
    parser.add_argument(
        "--alias", type=Path, action="append", default=[], help="Pool directory under an existing extraMount"
    )
    parser.add_argument("--capacity", type=int, default=DEFAULT_OPUS_HOST_SHARE)
    args = parser.parse_args()
    prepare_host_pool(args.directory, args.alias, capacity=args.capacity)
    print(f"Prepared {args.capacity} host tokens at {args.directory}; verified {len(args.alias)} inode aliases")


if __name__ == "__main__":
    main()
