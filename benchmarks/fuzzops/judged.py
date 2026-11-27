"""A baseline at its install's own health judgment, so later installs of it run no health judge.

An install whose accepted memory has no validated initial ``health_judge`` commit for its application inputs runs the
health judge (an agent turn of minutes) before the controller finishes starting, and commits its result on top of the
baseline. A curated baseline has none, so every row restored from it pays that turn again, and each row's judge may
write different files. Memory collected after one such install (a run's ``learned-baseline``, or ``hooks baseline``
of the lane) holds that judge commit right after the baseline's commit. This tool writes a new baseline directory at
that commit: the same render, namespace, objective and config, with the judge's commit as the accepted head.

``python -m benchmarks.fuzzops.judged --baseline DIR --from DIR --out DIR``; the last stdout line is
``{"baseline": <out>, "commit": <judge commit>, "judged_on": <baseline commit>}``.

Refused unless every commit between the baseline's commit and the judge commit (first parents) is a validated initial
``health_judge`` or ``upkeep`` publication with one parent that changes only ``.sdo/`` (the judge never changes the
application inputs), and the two baselines have the same render, namespace and control namespace.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
from dataclasses import replace
from pathlib import Path

from benchmarks.fuzzops.hooks import JUDGE_TRAILERS, Baseline, HookError, bundle_problem, sha256_file, write_baseline

MEMORY_PREFIX = ".sdo/"
_REQUIRED = JUDGE_TRAILERS[1:]
_ACTORS = ("health_judge", "upkeep")


def _git(repo: Path, *args: str) -> str:
    proc = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, timeout=120, check=False)
    if proc.returncode != 0:
        raise HookError(f"git {' '.join(args[:2])}: {proc.stderr.strip()[:300]}")
    return proc.stdout


def _trailers(repo: Path, commit: str) -> set[str]:
    body = _git(repo, "show", "-s", "--format=%B", commit)
    return {line.strip() for line in body.splitlines() if line.startswith("SDO-")}


def _actor(trailers: set[str]) -> str:
    for actor in _ACTORS:
        if f"SDO-Actor: {actor}" in trailers and all(t in trailers for t in _REQUIRED):
            return actor
    return ""


def judge_commit(repo: Path, base: str, head: str) -> str:
    """The newest validated initial health_judge commit on ``head``'s first-parent line whose commits since ``base``
    are all install-time publications (health_judge or upkeep, one parent, memory files only). Raises when there is
    none: the memory was not collected after an install of ``base``, or something else committed first."""
    if subprocess.run(["git", "-C", str(repo), "merge-base", "--is-ancestor", base, head], check=False).returncode:
        raise HookError(f"{base[:12]} is not an ancestor of {head[:12]}: the memory was not made from this baseline")
    line = _git(repo, "rev-list", "--first-parent", "--reverse", f"{base}..{head}").split()
    judged, parent = "", base
    for commit in line:
        parents = _git(repo, "show", "-s", "--format=%P", commit).split()
        actor = _actor(_trailers(repo, commit))
        if parents != [parent] or not actor:
            break
        changed = _git(repo, "diff", "--name-only", parent, commit).split()
        if outside := [path for path in changed if not path.startswith(MEMORY_PREFIX)]:
            raise HookError(f"install-time commit {commit[:12]} changes application inputs: {', '.join(outside[:5])}")
        if actor == "health_judge":
            judged = commit
        parent = commit
    if not judged:
        raise HookError(
            f"no validated initial health_judge commit right after {base[:12]} on {head[:12]}: "
            "collect the memory after one install of this baseline (a run's learned-baseline)"
        )
    return judged


def judged_baseline(baseline_dir: Path, collected_dir: Path, out: Path, branch: str = "judged") -> Baseline:
    """Write ``out``: ``baseline_dir`` with its accepted head moved to the health judgment its install made, read
    from ``collected_dir`` (a baseline-format directory of memory collected after that install)."""
    base, collected = Baseline.load(baseline_dir), Baseline.load(collected_dir)
    if (base.namespace, base.control) != (collected.namespace, collected.control):
        raise HookError(
            f"the collected memory is for {collected.namespace}/{collected.control}, the baseline for "
            f"{base.namespace}/{base.control}"
        )
    if sha256_file(base.path(base.render)) != sha256_file(collected.path(collected.render)):
        raise HookError("the collected memory was made on another render than the baseline's")
    if why := bundle_problem(collected.path(collected.bundle), collected.branch, collected.commit):
        raise HookError(why)
    out = out.resolve()
    if out.exists() and any(out.iterdir()):
        raise HookError(f"{out} is not empty")
    out.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as tmp:
        repo = Path(tmp) / "repo"
        _git(Path(tmp), "init", "-q", str(repo))
        for source in (base, collected):
            _git(repo, "fetch", "-q", str(source.path(source.bundle)), f"refs/heads/{source.branch}")
        commit = judge_commit(repo, base.commit, collected.commit)
        _git(repo, "branch", branch, commit)
        _git(repo, "bundle", "create", str(out / "memory.bundle"), branch)
    shutil.copy2(base.path(base.render), out / "render.yaml")
    return write_baseline(
        out, replace(base, bundle="memory.bundle", branch=branch, commit=commit, render="render.yaml", root=out)
    )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="benchmarks.fuzzops.judged", description=judged_baseline.__doc__)
    p.add_argument("--baseline", type=Path, required=True, help="the baseline whose installs run the health judge")
    p.add_argument("--from", dest="collected", type=Path, required=True, help="memory collected after one install")
    p.add_argument("--out", type=Path, required=True, help="new baseline directory (outside the repository)")
    p.add_argument("--branch", default="judged", help="branch name in the bundle")
    a = p.parse_args(argv)
    try:
        written = judged_baseline(a.baseline, a.collected, a.out, a.branch)
    except (HookError, ValueError) as exc:
        print(" ".join(str(exc).split()), file=sys.stderr)
        return 1
    judged_on = Baseline.load(a.baseline).commit
    print(json.dumps({"baseline": str(written.root), "commit": written.commit, "judged_on": judged_on}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
