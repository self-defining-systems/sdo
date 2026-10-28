"""Lease fence for shell credential keepers and fresh Python lane processes."""

from __future__ import annotations

import argparse
import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path


def guarded_module(command: list[str], *, cluster: str, kubeconfig: Path, lease: str, coord: Path) -> list[str]:
    if not lease:
        raise ValueError("pool: a lane process requires its coord lease")
    if command[1] != "-m":
        raise ValueError("pool: expected a Python module command")
    return [
        command[0],
        "-m",
        "benchmarks.sregym.fastloop.lane_guard",
        "--cluster",
        cluster,
        "--kubeconfig",
        str(kubeconfig),
        "--lease",
        lease,
        "--coord",
        str(coord),
        "--",
        *command[2:],
    ]


def keeper_environment(directory: Path, *, cluster: str, kubeconfig: Path, lease: str, coord: Path) -> dict[str, str]:
    if not lease:
        raise ValueError("pool: a credential keeper requires its coord lease")
    real = shutil.which("kubectl")
    if real is None:
        raise ValueError("pool: kubectl is required")
    directory.mkdir(parents=True, exist_ok=True)
    command = [
        sys.executable,
        "-m",
        "benchmarks.sregym.pool.guard",
        "--cluster",
        cluster,
        "--kubeconfig",
        str(kubeconfig),
        "--lease",
        lease,
        "--coord",
        str(coord),
        "--kubectl",
        real,
        "--",
    ]
    shim = directory / "kubectl"
    shim.write_text("#!/bin/sh\nexec " + shlex.join(command) + ' "$@"\n')
    shim.chmod(0o755)
    return {"PATH": str(directory) + os.pathsep + os.environ["PATH"], "KUBECONFIG": str(kubeconfig)}


def main() -> None:
    from benchmarks.sregym.fastloop.lane_guard import LaneGuard

    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("cluster", "kubeconfig", "lease", "coord", "kubectl"):
        parser.add_argument(f"--{name}", required=True)
    parser.add_argument("args", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    subprocess.run([args.coord, "check", args.cluster, "--lease", args.lease], check=True, stdout=subprocess.DEVNULL)
    remainder = args.args[1:] if args.args[:1] == ["--"] else args.args
    command = LaneGuard(args.cluster, Path(args.kubeconfig), args.lease, args.coord).command([args.kubectl, *remainder])
    os.execv(command[0], command)


if __name__ == "__main__":
    main()
