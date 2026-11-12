"""Replay-as-an-engineer loop against real temp Git repositories with synthetic files."""

from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.fastloop import replay_upstream as rp

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

IDENT = ["-c", "user.name=eng", "-c", "user.email=eng@localhost"]


def git(path: Path, *args: str) -> str:
    done = subprocess.run(["git", *IDENT, "-C", str(path), *args], check=True, capture_output=True, text=True)
    return done.stdout.strip()


def write(repo: Path, name: str, text: str) -> None:
    target = repo / name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text)


def commit(repo: Path, message: str, **files: str) -> str:
    for name, text in files.items():
        write(repo, name.replace("__", "/").replace("_txt", ".txt"), text)
    git(repo, "add", "-A")
    git(repo, "commit", "-q", "-m", message)
    return git(repo, "rev-parse", "HEAD")


def body(middle: str) -> str:
    return f"top\n{middle}\nbottom\n"


class World:
    """An upstream history U0..U3 and a run branch seeded at U0 plus an ``.sdo/`` tree."""

    def __init__(self, tmp: Path, *, conflict: bool = True) -> None:
        self.up = tmp / "up"
        self.up.mkdir()
        git(self.up, "init", "-q", "-b", "trunk")
        self.u0 = commit(self.up, "u0", front_txt=body("f0"), api_txt=body("a0"), db_txt=body("d0"))
        self.u1 = commit(self.up, "u1: front", front_txt=body("f1"), **{".sdo__ignored.txt": "upstream sdo\n"})
        self.u2 = commit(self.up, "u2: api", api_txt=body("a2"))
        self.u3 = commit(self.up, "u3: db", db_txt=body("d3"))
        self.remote = tmp / "remote.git"
        git(tmp, "init", "-q", "--bare", "-b", "main", str(self.remote))
        self.work = tmp / "work"
        self.work.mkdir()
        git(self.work, "init", "-q", "-b", "run")
        for name, text in {"front_txt": body("f0"), "api_txt": body("a0"), "db_txt": body("d0")}.items():
            write(self.work, name.replace("_txt", ".txt"), text)
        commit(self.work, "seed", **{".sdo__goal.txt": "goal\n"})
        git(self.work, "remote", "add", "origin", str(self.remote))
        git(self.work, "push", "-q", "origin", "run")
        if conflict:
            self.sdo_commit(db_txt=body("dSDO"))

    def sdo_commit(self, **files: str) -> None:
        files = {k.replace("_txt", ".txt"): v for k, v in files.items()}
        git(
            self.work,
            "-c",
            f"user.email={rp.BROKER_IDENTITY}",
            "-c",
            "user.name=broker",
            "pull",
            "-q",
            "--rebase",
            "origin",
            "run",
        )
        for name, text in files.items():
            write(self.work, name, text)
        commit(self.work, "sdo: edit")
        git(self.work, "push", "-q", "origin", "run")

    def config(self, **kw: object) -> rp.ReplayConfig:
        args: dict[str, object] = {
            "branch_remote": str(self.remote),
            "branch": "run",
            "upstream_repo": str(self.up),
            "commits": (self.u1, self.u2, self.u3),
            "base": self.u0,
        }
        return rp.ReplayConfig(**{**args, **kw})  # type: ignore[arg-type]

    def show(self, rev: str, fmt: str) -> str:
        return git(self.remote, "log", "-1", f"--format={fmt}", rev)

    def log(self) -> list[str]:
        return git(self.remote, "log", "--format=%an|%ae|%cn|%ce|%s", "run").splitlines()


def test_clean_replay_pushes_three_engineer_commits_without_touching_sdo(tmp_path: Path) -> None:
    w = World(tmp_path, conflict=False)
    result = rp.replay(w.config())
    assert [r.status for r in result.records] == ["pushed"] * 3
    assert not result.conflicts
    for commit_id in (w.u1, w.u2, w.u3):
        assert any(f"Replayed-From: {commit_id}" in w.show(r.pushed_sha, "%B") for r in result.pushed)
    for r in result.pushed:
        assert (
            w.show(r.pushed_sha, "%an|%ae|%cn|%ce")
            == "bench-replayer|bench-replayer@localhost|bench-replayer|bench-replayer@localhost"
        )
        assert not [
            p
            for p in git(w.remote, "show", "--name-only", "--format=", r.pushed_sha).splitlines()
            if p.startswith(".sdo")
        ]
    assert git(w.remote, "show", "run:.sdo/goal.txt") == "goal"
    assert git(w.remote, "show", "run:db.txt") == body("d3").strip()


def test_conflict_with_sdo_commit_stops_replay_by_default(tmp_path: Path) -> None:
    w = World(tmp_path)
    result = rp.replay(w.config())
    assert [r.status for r in result.records] == ["pushed", "pushed", "conflict"]
    assert len(result.conflicts) == 1
    assert result.conflicts[0].commit == w.u3
    assert result.conflicts[0].paths == ("db.txt",)
    assert result.stopped_at == w.u3
    assert git(w.remote, "show", "run:db.txt") == body("dSDO").strip()


def test_skip_policy_continues_past_conflict(tmp_path: Path) -> None:
    w = World(tmp_path)
    w2 = commit(w.up, "u4: front again", front_txt=body("f4"))
    result = rp.replay(w.config(commits=(w.u1, w.u3, w2), on_conflict="skip"))
    assert [r.status for r in result.records] == ["pushed", "skipped", "pushed"]
    assert [r.commit for r in result.conflicts] == [w.u3]
    assert not result.stopped_at
    assert git(w.remote, "show", "run:front.txt") == body("f4").strip()


def test_push_race_is_refetched_and_retried(tmp_path: Path) -> None:
    w = World(tmp_path, conflict=False)
    raced: list[int] = []

    def racing(args: Sequence[str], cwd: Path | None, stdin: bytes | None, env: dict[str, str] | None):
        if args[0] == "push" and not raced:
            raced.append(1)
            w.sdo_commit(other_txt="x\n")
        return rp.run_git(args, cwd, stdin, env)

    result = rp.replay(w.config(commits=(w.u1,)), git=racing)
    assert [(r.status, r.attempts) for r in result.records] == [("pushed", 2)]
    assert git(w.remote, "show", "run:other.txt") == "x"
    assert git(w.remote, "show", "run:front.txt") == body("f1").strip()


@pytest.mark.parametrize("branch", ["main", "master", "MAIN"])
def test_reserved_branches_are_refused(tmp_path: Path, branch: str) -> None:
    with pytest.raises(ValueError, match="reserved"):
        World(tmp_path, conflict=False).config(branch=branch)


def test_broker_identity_is_refused(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="broker"):
        World(tmp_path, conflict=False).config(author_email=rp.BROKER_IDENTITY)


def test_tree_mode_commits_whole_tree_and_keeps_branch_sdo(tmp_path: Path) -> None:
    w = World(tmp_path)  # SDO already changed db.txt; a tree release overwrites app files only
    result = rp.replay(w.config(mode="tree", base="", commits=(w.u3,)))
    assert [r.status for r in result.records] == ["pushed"]
    assert git(w.remote, "show", "run:.sdo/goal.txt") == "goal"
    assert git(w.remote, "show", "run:db.txt") == body("d3").strip()
    assert git(w.remote, "ls-tree", "-r", "--name-only", "run").splitlines() == [
        ".sdo/goal.txt",
        "api.txt",
        "db.txt",
        "front.txt",
    ]
