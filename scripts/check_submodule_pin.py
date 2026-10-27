#!/usr/bin/env python3
"""Fail when the SREGym gitlink moves backwards relative to the base branch.

A merge that resolves `third_party/sregym` to the branch's older side silently
reverts the base branch's pin. The pin may stay or move forward, so the base
pin must be an ancestor of (or equal to) the head pin in the SREGym history.
The check needs only commit history, so it works in a shallow CI checkout: it
fetches the base branch tip when absent and the two pinned commits (without
file contents) from the fork. It skips, saying why, when the base pin cannot
be fetched, and fails closed on every other uncertainty.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

PIN_PATH = "third_party/sregym"
DEFAULT_BASES = ("origin/vic-lsh/dev/sdo-improve", "origin/main")
FORK_HTTPS = "https://github.com/"
FORK_SSH = "git@github.com:"


class PinError(RuntimeError):
    """The head pin is not a descendant of the base pin, or cannot be judged."""


class Skip(RuntimeError):
    """The base pin cannot be obtained, so nothing can be compared."""


@dataclass(frozen=True)
class Verdict:
    base: str
    head: str
    detail: str


def git(repository: Path, *args: str, check: bool = False) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(repository), *args], check=check, capture_output=True, text=True, timeout=180
    )


def resolve(repository: Path, revision: str) -> str | None:
    result = git(repository, "rev-parse", "--verify", "--quiet", f"{revision}^{{commit}}")
    return result.stdout.strip() if result.returncode == 0 else None


def gitlink(repository: Path, revision: str, path: str) -> str | None:
    fields = git(repository, "ls-tree", revision, "--", path).stdout.split()
    return fields[2] if len(fields) == 4 and fields[:2] == ["160000", "commit"] else None


def fork_url(repository: Path, path: str) -> str:
    result = git(repository, "config", "-f", ".gitmodules", "--get", f"submodule.{path}.url")
    url = result.stdout.strip()
    if result.returncode or not url:
        raise PinError(f"no submodule.{path}.url in .gitmodules")
    return FORK_HTTPS + url.removeprefix(FORK_SSH) if url.startswith(FORK_SSH) else url


def base_commit(repository: Path, explicit: str | None) -> str:
    """The base branch tip: explicit, the pull request base, or a merged default."""
    pull_request_base = os.environ.get("GITHUB_BASE_REF", "")
    named = explicit or (f"origin/{pull_request_base}" if pull_request_base else None)
    if named:
        found = resolve(repository, named)
        if found is None and pull_request_base and explicit is None:
            # Shallow CI checkout: only the merge commit exists.
            fetched = git(repository, "fetch", "--depth=1", "--no-tags", "origin", pull_request_base)
            found = resolve(repository, "FETCH_HEAD") if fetched.returncode == 0 else None
        if found is None:
            raise Skip(f"base {named} is not available")
        return found
    for candidate in DEFAULT_BASES:
        found = resolve(repository, candidate)
        # Only a base this branch already contains can be regressed by it.
        if found and git(repository, "merge-base", "--is-ancestor", found, "HEAD").returncode == 0:
            return found
    raise Skip("no default base branch is contained in HEAD")


def history_contains(object_store: Path, base: str, head: str) -> bool | None:
    """True/False for the ancestry answer, or None when either commit is missing."""
    for sha in (base, head):
        if git(object_store, "cat-file", "-e", f"{sha}^{{commit}}").returncode:
            return None
    return git(object_store, "merge-base", "--is-ancestor", base, head).returncode == 0


def ancestry(repository: Path, path: str, remote: str, base: str, head: str) -> bool:
    local = repository / path
    toplevel = git(local, "rev-parse", "--show-toplevel") if local.is_dir() else None
    # An uninitialized submodule directory silently resolves to the parent repository.
    if toplevel is not None and toplevel.returncode == 0 and Path(toplevel.stdout.strip()).resolve() == local.resolve():
        answer = history_contains(local, base, head)
        if answer is not None:
            return answer
    with tempfile.TemporaryDirectory(prefix="sdo-pin-check-") as scratch:
        store = Path(scratch)
        git(store, "init", "--bare", "--quiet", check=True)
        fetch = ["fetch", "--quiet", "--no-tags", "--filter=blob:none", remote]
        if git(store, *fetch, base, head).returncode:
            if git(store, *fetch, base).returncode:
                raise Skip(f"base pin {base} is not fetchable from {remote}")
            raise PinError(
                f"head pin {head} is not fetchable from {remote}; the submodule pin points at a commit the fork lacks"
            )
        answer = history_contains(store, base, head)
        if answer is None:
            raise PinError(f"fetched {base} and {head} from {remote} but cannot read their history")
        return answer


def check(repository: Path, *, base: str | None = None, remote: str | None = None, path: str = PIN_PATH) -> Verdict:
    repository = repository.resolve()
    base_rev = base_commit(repository, base)
    base_pin = gitlink(repository, base_rev, path)
    if base_pin is None:
        raise Skip(f"base {base_rev[:12]} records no {path} gitlink")
    head_pin = gitlink(repository, "HEAD", path)
    if head_pin is None:
        raise PinError(f"HEAD records no {path} gitlink, but base {base_rev[:12]} pins {base_pin}")
    if head_pin == base_pin:
        return Verdict(base_pin, head_pin, "unchanged")
    if ancestry(repository, path, remote or fork_url(repository, path), base_pin, head_pin):
        return Verdict(base_pin, head_pin, "advanced")
    raise PinError(
        f"{path} moved off the base branch's history: base pins {base_pin}, this branch pins {head_pin}, "
        "and the base pin is not an ancestor of the branch pin. A merge likely kept this branch's older pin. "
        f"Restore it with: git update-index --cacheinfo 160000,{base_pin},{path} "
        "(or the newer pin your change needs, if it descends from the base pin)"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--base", help="base revision (default: origin/$GITHUB_BASE_REF, else a merged default branch)")
    parser.add_argument("--remote", help="SREGym fork URL (default: from .gitmodules)")
    args = parser.parse_args(argv)
    try:
        verdict = check(args.repository, base=args.base, remote=args.remote)
    except Skip as skip:
        print(f"SKIP submodule pin check: {skip}")
        return 0
    except PinError as error:
        print(f"FAIL submodule pin check: {error}", file=sys.stderr)
        return 1
    print(f"OK {PIN_PATH} {verdict.detail}: base {verdict.base[:12]} -> head {verdict.head[:12]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
