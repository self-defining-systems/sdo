"""Generate, audit and optionally push one independent ablation branch."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

from benchmarks.sregym.ablation import ARMS, transform
from benchmarks.sregym.adapter import execute_operator
from benchmarks.sregym.memory_cli import artifact_ids, read_listing
from benchmarks.sregym.repository_git import host_git_command


def generate(repository: str, source_branch: str, arm: str, directory: Path, *, push: bool) -> dict[str, object]:
    """Clone the named remote revision; publish only after a clean accepted audit."""
    if arm not in ARMS:
        raise ValueError(f"unknown ablation arm: {arm}")
    if directory.exists():
        raise ValueError(f"destination already exists: {directory}")
    subprocess.run(
        ["git", "clone", "--single-branch", "--branch", source_branch, "--", repository, str(directory)],
        check=True,
        timeout=120,
    )

    def git(*args: str) -> str:
        return subprocess.check_output(host_git_command(directory, *args), text=True, timeout=120).strip()

    source = git("rev-parse", "HEAD")
    branch = f"ablate/20261103-{arm}"
    git("switch", "-c", branch)
    edits = transform(directory, arm)
    git("add", "--", ".sdo")
    if git("diff", "--cached", "--name-only"):
        git(
            "commit",
            "-m",
            f"bench: project {arm} from {source}\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>",
        )
    execute_operator("memory", ["check", "--accepted", "--root", str(directory), "--replay", "off"], timeout=900)
    listing = read_listing(directory)
    counts = {kind + "s": len(artifact_ids(listing, kind)) for kind in ("detector", "playbook", "program")}
    head = git("rev-parse", "HEAD")
    if push:
        git("push", "origin", f"HEAD:refs/heads/{branch}")
        remote = git("ls-remote", "origin", f"refs/heads/{branch}").split()[0]
        if remote != head:
            raise RuntimeError(f"remote head changed: {remote} != {head}")
    return {
        "repository": repository,
        "source": source,
        "arm": arm,
        "branch": branch,
        "head": head,
        "counts": counts,
        "metadata_edits": edits,
        "pushed": push,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--source-branch", required=True)
    parser.add_argument("--arm", choices=ARMS, required=True)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--push", action="store_true")
    args = parser.parse_args()
    report = generate(args.repository, args.source_branch, args.arm, args.directory.resolve(), push=args.push)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
