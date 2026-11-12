"""Replay upstream commits onto a run's branch as an engineer would: with ``git push``.

SDO commits and deploys from one configured branch; this adapter simulates engineers pushing to it. Each
upstream commit is applied (three-way) onto the branch head in a scratch clone, committed as ``bench-replayer``
with a ``Replayed-From`` trailer, and pushed without force. ``.sdo/`` is never written: diffs exclude it and
tree mode keeps the branch's own copy. A conflict with an earlier commit (typically SDO's) is recorded and
stops the replay (``on_conflict="stop"``, default) or skips that commit (``"skip"``).
"""

from __future__ import annotations

import os
import subprocess
import tempfile
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

RESERVED_BRANCHES = frozenset({"main", "master"})
BROKER_IDENTITY = "sdo-commit-broker@localhost"
TRAILER = "Replayed-From"
SDO_DIR = ".sdo"

GitRunner = Callable[
    [Sequence[str], "Path | None", "bytes | None", "dict[str, str] | None"], "subprocess.CompletedProcess[bytes]"
]


class ReplayError(RuntimeError):
    """A replay step failed for a reason other than a content conflict."""


def run_git(
    args: Sequence[str], cwd: Path | None = None, stdin: bytes | None = None, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[bytes]:
    """Default runner: a plain ``git`` subprocess; never raises on a non-zero exit."""
    return subprocess.run(
        ["git", *args], cwd=cwd, input=stdin, capture_output=True, check=False, env={**os.environ, **(env or {})}
    )


@dataclass(frozen=True)
class ReplayConfig:
    branch_remote: str
    branch: str
    upstream_repo: str
    commits: tuple[str, ...]
    schedule: tuple[float, ...] = ()
    base: str = ""
    mode: Literal["diff", "tree"] = "diff"
    on_conflict: Literal["stop", "skip"] = "stop"
    author_name: str = "bench-replayer"
    author_email: str = "bench-replayer@localhost"
    max_push_retries: int = 5

    def __post_init__(self) -> None:
        if not self.branch_remote or not self.upstream_repo:
            raise ValueError("branch_remote and upstream_repo are required")
        if self.branch.strip().lower() in RESERVED_BRANCHES or not self.branch:
            raise ValueError(f"refusing to replay onto reserved or empty branch {self.branch!r}")
        object.__setattr__(self, "commits", tuple(self.commits))
        object.__setattr__(self, "schedule", tuple(self.schedule))
        if not self.commits:
            raise ValueError("commits must not be empty")
        if self.schedule and len(self.schedule) != len(self.commits):
            raise ValueError("schedule must give one offset (seconds) per commit")
        if any(t < 0 for t in self.schedule) or list(self.schedule) != sorted(self.schedule):
            raise ValueError("schedule offsets must be non-negative and non-decreasing")
        if self.mode not in ("diff", "tree"):
            raise ValueError(f"mode must be 'diff' or 'tree', got {self.mode!r}")
        if self.mode == "diff" and not self.base:
            raise ValueError("diff mode needs base: the upstream commit before commits[0]")
        if self.on_conflict not in ("stop", "skip"):
            raise ValueError(f"on_conflict must be 'stop' or 'skip', got {self.on_conflict!r}")
        if self.author_email == BROKER_IDENTITY or "broker" in self.author_name.lower():
            raise ValueError("the replayer must not use the SDO commit broker identity")
        if self.max_push_retries < 1:
            raise ValueError("max_push_retries must be >= 1")


@dataclass(frozen=True)
class ReplayRecord:
    commit: str
    status: Literal["pushed", "conflict", "skipped", "empty"]
    paths: tuple[str, ...] = ()
    pushed_sha: str = ""
    attempts: int = 0


@dataclass
class ReplayResult:
    records: list[ReplayRecord] = field(default_factory=list)
    stopped_at: str = ""

    @property
    def conflicts(self) -> list[ReplayRecord]:
        """The ``replay_conflict{commit, paths}`` evidence, one record per conflicting commit."""
        return [r for r in self.records if r.status in ("conflict", "skipped")]

    @property
    def pushed(self) -> list[ReplayRecord]:
        return [r for r in self.records if r.status == "pushed"]


def _not_sdo(path: str) -> bool:
    return path != SDO_DIR and not path.startswith(SDO_DIR + "/")


class _Scratch:
    def __init__(self, config: ReplayConfig, git: GitRunner, directory: Path) -> None:
        self.config, self._git, self.dir = config, git, directory
        #: Commit messages for steps that have no upstream commit to read one from (a patch file).
        self.messages: dict[str, str] = {}
        self.env = {
            "GIT_AUTHOR_NAME": config.author_name,
            "GIT_AUTHOR_EMAIL": config.author_email,
            "GIT_COMMITTER_NAME": config.author_name,
            "GIT_COMMITTER_EMAIL": config.author_email,
        }

    def git(self, *args: str, stdin: bytes | None = None, ok: bool = False) -> subprocess.CompletedProcess[bytes]:
        proc = self._git(list(args), self.dir, stdin, self.env)
        if proc.returncode != 0 and not ok:
            raise ReplayError(f"git {' '.join(args)} failed: {proc.stderr.decode(errors='replace').strip()}")
        return proc

    def text(self, *args: str) -> str:
        return self.git(*args).stdout.decode().strip()

    def setup(self) -> None:
        c = self.config
        self.git("init", "-q", "-b", "replay-scratch")
        self.git("remote", "add", "origin", c.branch_remote)
        self.git("fetch", "-q", c.upstream_repo, "+refs/heads/*:refs/upstream/*", "+refs/tags/*:refs/upstream-tags/*")

    def sync_head(self) -> None:
        self.git("fetch", "-q", "origin", f"+refs/heads/{self.config.branch}:refs/remotes/origin/{self.config.branch}")
        self.git("checkout", "-q", "-f", "-B", "replay-scratch", f"origin/{self.config.branch}")
        self.git("clean", "-fdq")

    def message(self, commit: str) -> bytes:
        if commit in self.messages:
            return f"{self.messages[commit]}\n\n{TRAILER}: {commit}\n".encode()
        body = self.text("log", "-1", "--format=%B", commit)
        return f"{body}\n\n{TRAILER}: {commit}\n".encode()

    def stage_diff(self, prev: str, commit: str) -> tuple[bool, tuple[str, ...]]:
        """Apply diff(prev, commit) minus ``.sdo/`` three-way. Returns (clean, conflicting paths)."""
        patch = self.git("diff", "--binary", prev, commit, "--", ".", f":(exclude){SDO_DIR}").stdout
        if not patch:
            return True, ()
        applied = self.git("apply", "-3", "--index", stdin=patch, ok=True)
        if applied.returncode == 0:
            return True, ()
        unmerged = self.git("diff", "--name-only", "--diff-filter=U", ok=True).stdout.decode().split("\n")
        paths = tuple(p for p in unmerged if p)
        if not paths:  # a patch that did not apply at all leaves no unmerged entries
            stat = self.git("apply", "--numstat", stdin=patch, ok=True).stdout.decode().splitlines()
            paths = tuple(line.split("\t")[-1] for line in stat if line)
        self.git("reset", "-q", "--hard", "HEAD")
        return False, paths

    def stage_tree(self, commit: str) -> None:
        """Make the working tree ``commit``'s tree outside ``.sdo/``; the branch's ``.sdo/`` stays as is."""
        tracked = [p for p in self.text("ls-files", "-z").split("\0") if p and _not_sdo(p)]
        if tracked:
            self.git(
                "rm",
                "-q",
                "--cached",
                "--pathspec-from-file=-",
                "--pathspec-file-nul",
                stdin="\0".join(tracked).encode(),
            )
            for p in tracked:
                (self.dir / p).unlink(missing_ok=True)
        wanted = [p for p in self.text("ls-tree", "-r", "-z", "--name-only", commit).split("\0") if p and _not_sdo(p)]
        if wanted:
            self.git(
                "checkout", commit, "--pathspec-from-file=-", "--pathspec-file-nul", stdin="\0".join(wanted).encode()
            )

    def commit_and_push(self, commit: str) -> tuple[Literal["pushed", "empty", "raced"], str]:
        staged = [p for p in self.text("diff", "--cached", "--name-only", "-z").split("\0") if p]
        if not staged:
            return "empty", ""
        if any(not _not_sdo(p) for p in staged):
            raise ReplayError(f"refusing to commit {SDO_DIR}/ paths from {commit}: {staged}")
        self.git("commit", "-q", "-F", "-", stdin=self.message(commit))
        pushed = self.git("push", "-q", "origin", f"HEAD:refs/heads/{self.config.branch}", ok=True)
        if pushed.returncode == 0:
            return "pushed", self.text("rev-parse", "HEAD")
        return "raced", ""  # non-fast-forward: the caller refetches and re-applies


def replay(
    config: ReplayConfig,
    *,
    git: GitRunner = run_git,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> ReplayResult:
    """Replay ``config.commits`` in order at their scheduled offsets; see the module docstring."""
    result = ReplayResult()
    with tempfile.TemporaryDirectory(prefix="replay-") as tmp:
        scratch = _Scratch(config, git, Path(tmp))
        scratch.setup()
        start = clock()
        prev = config.base
        for i, commit in enumerate(config.commits):
            if config.schedule:
                sleep(max(0.0, start + config.schedule[i] - clock()))
            record = _replay_one(scratch, prev, commit)
            result.records.append(record)
            prev = commit
            if record.status == "conflict":
                if config.on_conflict == "stop":
                    result.stopped_at = commit
                    break
                result.records[-1] = ReplayRecord(commit, "skipped", record.paths, attempts=record.attempts)
    return result


def _replay_one(scratch: _Scratch, prev: str, commit: str) -> ReplayRecord:
    config = scratch.config
    for attempt in range(1, config.max_push_retries + 1):
        scratch.sync_head()
        if config.mode == "tree":
            scratch.stage_tree(commit)
        else:
            clean, paths = scratch.stage_diff(prev, commit)
            if not clean:
                return ReplayRecord(commit, "conflict", paths, attempts=attempt)
        outcome, sha = scratch.commit_and_push(commit)
        if outcome != "raced":
            return ReplayRecord(commit, outcome, pushed_sha=sha, attempts=attempt)
    raise ReplayError(f"push of {commit} lost the race {config.max_push_retries} times")


def replay_patch(
    branch_remote: str,
    branch: str,
    patch: Path,
    *,
    label: str,
    message: str = "",
    git: GitRunner = run_git,
    max_push_retries: int = 5,
) -> ReplayRecord:
    """Push one scripted change, a patch file, to ``branch`` the way an engineer would.

    The patch is applied three-way onto the branch's current head, excluding ``.sdo/``, committed as
    ``bench-replayer`` with a ``Replayed-From: <label>`` trailer and pushed without force; a lost push race
    refetches and re-applies. A patch that does not apply is a ``conflict`` record, nothing is pushed.
    """
    config = ReplayConfig(
        branch_remote=branch_remote,
        branch=branch,
        upstream_repo=branch_remote,
        commits=(label,),
        mode="tree",
        max_push_retries=max_push_retries,
    )
    data = Path(patch).read_bytes()
    with tempfile.TemporaryDirectory(prefix="replay-patch-") as tmp:
        scratch = _Scratch(config, git, Path(tmp))
        scratch.messages[label] = message or label
        scratch.setup()
        for attempt in range(1, max_push_retries + 1):
            scratch.sync_head()
            applied = scratch.git("apply", "-3", "--index", f"--exclude={SDO_DIR}/*", stdin=data, ok=True)
            if applied.returncode != 0:
                unmerged = scratch.git("diff", "--name-only", "--diff-filter=U", ok=True).stdout.decode().split("\n")
                paths = tuple(p for p in unmerged if p)
                scratch.git("reset", "-q", "--hard", "HEAD")
                return ReplayRecord(label, "conflict", paths, attempts=attempt)
            outcome, sha = scratch.commit_and_push(label)
            if outcome != "raced":
                return ReplayRecord(label, outcome, pushed_sha=sha, attempts=attempt)
    raise ReplayError(f"push of {label} lost the race {max_push_retries} times")
