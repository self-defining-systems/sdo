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
    attach = commands.add_parser("attach-lane", help="deploy step: attach one prepared lane to its app's memory")
    attach.add_argument("--run-dir", type=Path, required=True)
    attach.add_argument("--lane", required=True)
    attach.add_argument("--problem", required=True)
    attach.add_argument("--accepted", type=Path, required=True, help="<app memory root>/accepted")
    attach.add_argument("--report", type=Path, required=True)
    remove = commands.add_parser("remove", help="forget a ready or held lane after its cluster was deleted")
    remove.add_argument("--registry", type=Path, required=True)
    remove.add_argument("--lane", required=True)
    remove.add_argument("--note", required=True)
    relay = commands.add_parser("relay", help="foreground TCP relay supervised by the pool's systemd unit")
    relay.add_argument("--spec", type=Path, required=True)
    relay.add_argument("--ready", type=Path)
    ensure = commands.add_parser("ensure-relay", help="install, check and restart the pool registry relay")
    for name in ("spec", "code-root", "state"):
        ensure.add_argument(f"--{name}", type=Path, required=True)
    real = commands.add_parser("prepare-real-app", help="deploy, credential, prepare and bind a real app lane")
    real.add_argument("--setup", type=Path, required=True)
    for name in ("lane", "problem", "lease", "repo", "branch"):
        real.add_argument(f"--{name}", required=True)
    real.add_argument("--run-dir", type=Path, required=True)
    real.add_argument("--agent-registrations", type=Path)
    real.add_argument("--provider", choices=("codex", "claude"), default="claude")
    real.add_argument("--model", default="claude-opus-5-5")
    real.add_argument("--reasoning-effort", choices=("low", "medium", "high", "xhigh", "max"), default="medium")
    seed = commands.add_parser("render-seed", help="render fresh memory with current Go policy and source inputs")
    for name in ("code-root", "source", "destination"):
        seed.add_argument(f"--{name}", type=Path, required=True)
    seed.add_argument("--application", required=True)
    seed.add_argument("--namespace", required=True)
    for verb in ("pair", "prepare"):
        worker = commands.add_parser(verb, help=f"host-side {verb} process started by the pool owner")
        worker.add_argument("--spec", type=Path, required=True)
    args = parser.parse_args(argv)

    if args.command == "ensure-relay":
        from benchmarks.sregym.pool.relay import RelayConfig, ensure_relay

        ensure_relay(RelayConfig.model_validate_json(args.spec.read_text()), args.code_root, args.state)
        return 0

    if args.command == "render-seed":
        from benchmarks.sregym.pool.seed import render_seed

        print(render_seed(args.code_root, args.source, args.destination, args.application, args.namespace))
        return 0

    if args.command == "relay":
        import asyncio

        from benchmarks.sregym.pool.relay import RelayConfig, serve

        asyncio.run(serve(RelayConfig.model_validate_json(args.spec.read_text()), args.ready))
        return 0
    if args.command == "prepare-real-app":
        from benchmarks.sregym.pool.real_apps import RealAppSetup, prepare_real_app

        prepare_real_app(
            RealAppSetup.model_validate_json(args.setup.read_text()),
            run_dir=args.run_dir,
            lane=args.lane,
            problem=args.problem,
            lease=args.lease,
            repo=args.repo,
            branch=args.branch,
            registrations=args.agent_registrations,
            provider=args.provider,
            model=args.model,
            reasoning_effort=args.reasoning_effort,
        )
        return 0

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
    if args.command == "remove":
        LaneRegistry(args.registry).remove(args.lane, expect={"ready", "held"})
        print(f"removed {args.lane}: {args.note}", flush=True)
        return 0
    if args.command == "release":
        lane = release_hold(LaneRegistry(args.registry), args.lane, args.note)
        print(f"released {lane.id}: {args.note}", flush=True)
        return 0
    from benchmarks.sregym.pool import worker

    if args.command == "attach-lane":
        worker.attach_lane(args.run_dir, args.lane, args.problem, args.accepted, args.report)
    elif args.command == "pair":
        worker.pair(PairSpec.model_validate_json(args.spec.read_text()))
    else:
        worker.prepare(DeploySpec.model_validate_json(args.spec.read_text()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
