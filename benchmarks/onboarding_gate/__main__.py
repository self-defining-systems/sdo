"""Onboarding fast gate: judge an SDO change against past onboarding sessions without a model or cluster.

    # once per recorded session (slow: re-runs the session's commands; cached)
    uv run python -m benchmarks.onboarding_gate replay --cache DIR TRIAL [TRIAL ...]
    # per candidate (a checkout's working tree): rejection classes gone, still and new
    uv run python -m benchmarks.onboarding_gate check --cache DIR --repo CHECKOUT [--json FILE]
    uv run python -m benchmarks.onboarding_gate first-read CHECKOUT
    # a real onboarding session on a synthetic app (kind, cheap model, $1 cap) or its scripted twin
    uv run python -m benchmarks.onboarding_gate e2e --repo CHECKOUT --out NEW_DIR [--model M] [--scripted]

A TRIAL directory holds ``session/transcript.jsonl``, ``session/start`` (UTC start time), a Git
bundle of the exported work (``*.bundle``) or the start checkout ``session/work``, and
``sdo-revision`` (the kit's source commit). The session's kit is ``KITS/sdo-<revision>/kit``
(``--kits``, default: the trial's grandparent). The start commit is the newest commit of the
recorded history committed before the session started.
"""

from __future__ import annotations

import argparse
import json
import sys
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from pathlib import Path

from benchmarks.onboarding_gate.check import Judgement, Kit, judge_all
from benchmarks.onboarding_gate.firstread import first_read
from benchmarks.onboarding_gate.replay import Replayed, ReplayError, Session, git, replay
from benchmarks.onboarding_gate.report import compare, text
from benchmarks.onboarding_gate.transcript import read_tool_calls


def discover(trial: Path, kits: Path | None = None) -> Session:
    """The Session of one trial directory (layout in the module docstring)."""
    transcript = trial / "session" / "transcript.jsonl"
    bundles = sorted(trial.glob("*.bundle"))
    start_repo = bundles[0] if bundles else trial / "session" / "work"
    if not start_repo.exists():
        raise ReplayError(f"{trial}: no *.bundle and no session/work")
    revision = (trial / "sdo-revision").read_text().strip()
    kit = (kits or trial.parent.parent) / f"sdo-{revision}" / "kit"
    started = datetime.fromisoformat((trial / "session" / "start").read_text().strip().replace("Z", "+00:00"))
    return Session(trial.name, transcript, start_repo, start_commit(start_repo, started), kit)


def start_commit(repo: Path, started: datetime) -> str:
    """The newest commit of repo's HEAD history committed before started."""
    log = git(repo, "log", "--format=%H %ct", "HEAD") if repo.is_dir() else _bundle_log(repo)
    for line in log.splitlines():
        commit, stamp = line.split()
        if int(stamp) < started.timestamp():
            return commit
    raise ReplayError(f"{repo}: no commit before the session start {started.isoformat()}")


def _bundle_log(bundle: Path) -> str:
    import subprocess
    import tempfile

    with tempfile.TemporaryDirectory() as temp:
        subprocess.run(["git", "init", "-q", "--bare", temp], check=True)
        subprocess.run(["git", "-C", temp, "fetch", "-q", str(bundle), "+HEAD:refs/heads/main"], check=True)
        return git(Path(temp), "log", "--format=%H %ct", "main")


def _replay_one(args: tuple[Path, Path | None, Path, bool]) -> str:
    trial, kits, cache, force = args
    target = cache / "sessions" / f"{trial.name}.json"
    if target.is_file() and not force:
        return f"{trial.name}: cached"
    session = discover(trial, kits)
    result = replay(session, cache / "replays" / trial.name)
    target.parent.mkdir(parents=True, exist_ok=True)
    (cache / "sessions" / f"{trial.name}.session.json").write_text(
        json.dumps({"transcript": str(session.transcript), "kit": str(session.kit)}) + "\n"
    )
    result.save(target)
    fidelity = f"{result.matched_commits}/{result.agent_commits}"
    return f"{trial.name}: {len(result.snapshots)} snapshots, fidelity {fidelity}, edit misses {result.edit_misses}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m benchmarks.onboarding_gate",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    commands = parser.add_subparsers(dest="command", required=True)
    rp = commands.add_parser("replay", help="rebuild recorded sessions' validator snapshots (cached)")
    rp.add_argument("--cache", type=Path, required=True)
    rp.add_argument("--kits", type=Path)
    rp.add_argument("--jobs", type=int, default=6)
    rp.add_argument("--force", action="store_true")
    rp.add_argument("trials", type=Path, nargs="+")
    ck = commands.add_parser("check", help="judge cached snapshots with a candidate checkout")
    ck.add_argument("--cache", type=Path, required=True)
    ck.add_argument("--repo", type=Path, required=True, help="SDO checkout whose working tree is the candidate")
    ck.add_argument("--only", default="", help="comma-separated session-name substrings")
    ck.add_argument("--jobs", type=int, default=8)
    ck.add_argument("--json", type=Path, help="also write the comparison as JSON")
    ck.add_argument("--all", action="store_true", help="judge every snapshot, not only rejected and final ones")
    ee = commands.add_parser("e2e", help="one onboarding session on the synthetic app (see e2e.py)")
    ee.add_argument("--repo", type=Path, required=True)
    ee.add_argument("--out", type=Path, required=True, help="a new directory for this run (outside the repository)")
    ee.add_argument("--model", default="claude-sonnet-5-5")
    ee.add_argument("--effort", default="medium")
    ee.add_argument("--budget-usd", type=float, default=1.0)
    ee.add_argument("--timeout", type=int, default=420, help="session wall-clock cap in seconds")
    ee.add_argument("--scripted", action="store_true", help="no model: a fixed script plays the agent")
    ee.add_argument("--reevaluate", action="store_true", help="re-read an existing --out run; start no session")
    fr = commands.add_parser("first-read", help="bytes of the onboarding skill's first read")
    fr.add_argument("repo", type=Path)
    args = parser.parse_args(argv)

    if args.command == "first-read":
        print(first_read(args.repo / "sdo" / "skills" / "sdo-onboarding" / "SKILL.md").text(), end="")
        return 0
    if args.command == "e2e":
        from benchmarks.onboarding_gate.e2e import E2EConfig, reevaluate, run

        result = (reevaluate if args.reevaluate else run)(
            E2EConfig(
                repo=args.repo.resolve(),
                out=args.out.resolve(),
                model=args.model,
                effort=args.effort,
                budget_usd=args.budget_usd,
                timeout_s=args.timeout,
                scripted=args.scripted,
            )
        )
        print(result.text(), end="")
        return 0 if result.validated and result.journeys_passed.startswith(("3 of 3",)) else 1
    cache = args.cache.resolve()
    if args.command == "replay":
        jobs = [(trial.resolve(), args.kits, cache, args.force) for trial in args.trials]
        with ProcessPoolExecutor(max_workers=args.jobs) as pool:
            for line in pool.map(_replay_one, jobs):
                print(line, flush=True)
        return 0

    sessions = [
        Replayed.load(path)
        for path in sorted((cache / "sessions").glob("*.json"))
        if not path.name.endswith(".session.json")
    ]
    if args.only:
        sessions = [s for s in sessions if any(part in s.session for part in args.only.split(","))]
    if not sessions:
        print("no replayed sessions in the cache: run replay first", file=sys.stderr)
        return 2
    # Each session's own kit judges all its snapshots once (cached for good).
    metas = {s.session: json.loads((cache / "sessions" / f"{s.session}.session.json").read_text()) for s in sessions}
    by_kit: dict[str, list[Replayed]] = {}
    for session in sessions:
        by_kit.setdefault(metas[session.session]["kit"], []).append(session)
    recorded: dict[str, Judgement] = {}
    for kit_dir, members in by_kit.items():
        recorded |= judge_all(Kit.from_directory(Path(kit_dir)), members, cache, args.jobs)
    # The candidate judges what can change the answer: every snapshot its recorded kit rejected and
    # each session's final state (--all: every snapshot).
    selected = (
        None
        if args.all
        else {tree for tree, judged in recorded.items() if judged.rejections or judged.error}
        | {session.snapshots[-1].tree for session in sessions}
    )
    candidate = Kit.build(args.repo.resolve(), cache)
    judged_candidate = judge_all(candidate, sessions, cache, args.jobs, selected)
    comparisons = [
        compare(session, read_tool_calls(Path(metas[session.session]["transcript"])), recorded, judged_candidate)
        for session in sessions
    ]
    print(text(comparisons, "each session's own", candidate.label), end="")
    print(first_read(args.repo / "sdo" / "skills" / "sdo-onboarding" / "SKILL.md").text(), end="")
    if args.json:
        args.json.write_text(json.dumps([item.to_json() for item in comparisons], indent=1) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
