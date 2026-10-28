"""``python -m benchmarks.sregym.pool``: the pool owner and its host-side processes."""

from __future__ import annotations

import argparse
import datetime
import fcntl
import sys
from pathlib import Path

REQUESTS = Path("/mnt/data/shli/sdo-exp/mixed10/LE/REQUESTS_C5.md")


def poster(path: Path | None, sender: str):
    def post(message: str) -> None:
        print(message, flush=True)
        if path is None:
            return
        with path.open("a") as board:
            fcntl.flock(board, fcntl.LOCK_EX)
            board.write(
                f"{datetime.datetime.now(datetime.timezone.utc).isoformat()} {sender} -> orchestrator: {message}\n"
            )

    return post


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m benchmarks.sregym.pool", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="own one run: place the stream's pairs on registry lanes until done")
    run.add_argument("--config", type=Path, required=True)
    run.add_argument("--board", type=Path, default=None, help=f"append status lines here (e.g. {REQUESTS})")
    run.add_argument("--sender", default="POOL")
    status = commands.add_parser("lanes", help="print the lane registry")
    status.add_argument("--registry", type=Path, required=True)
    release = commands.add_parser("release", help="return an inspected held lane to ready")
    release.add_argument("--registry", type=Path, required=True)
    release.add_argument("--lane", required=True)
    release.add_argument("--note", required=True)
    for verb in ("pair", "prepare"):
        worker = commands.add_parser(verb, help=f"host-side {verb} process started by the pool owner")
        worker.add_argument("--spec", type=Path, required=True)
    args = parser.parse_args(argv)

    from benchmarks.sregym.pool.registry import LaneRegistry
    from benchmarks.sregym.pool.scheduler import DeploySpec, PairSpec, PoolConfig, Scheduler, release_hold

    if args.command == "run":
        from benchmarks.sregym.pool.hosts import ShellHost

        config = PoolConfig.model_validate_json(args.config.read_text())
        hosts = {name: ShellHost(name, host) for name, host in config.hosts.items()}
        post = poster(args.board, args.sender)
        with Scheduler(config, hosts, LaneRegistry(config.registry), post) as scheduler:
            summary = scheduler.run()
        print(summary, flush=True)
        return 0 if summary["held"] == 0 else 1
    if args.command == "lanes":
        sys.stdout.write(LaneRegistry(args.registry).read().model_dump_json(indent=2) + "\n")
        return 0
    if args.command == "release":
        lane = release_hold(LaneRegistry(args.registry), args.lane, args.note)
        print(f"released {lane.id}: {args.note}", flush=True)
        return 0
    from benchmarks.sregym.pool import worker

    if args.command == "pair":
        worker.pair(PairSpec.model_validate_json(args.spec.read_text()))
    else:
        worker.prepare(DeploySpec.model_validate_json(args.spec.read_text()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
