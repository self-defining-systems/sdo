"""Memory snapshots and the offline merge of first-try deltas, against real git repositories."""

from __future__ import annotations

import json
import os
import subprocess
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.eval.snapshots import CheckResult, GitSnapshotStore

if TYPE_CHECKING:
    from pathlib import Path

ENV = {
    **os.environ,
    "GIT_AUTHOR_NAME": "t",
    "GIT_AUTHOR_EMAIL": "t@t",
    "GIT_COMMITTER_NAME": "t",
    "GIT_COMMITTER_EMAIL": "t@t",
}


def git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True, text=True, env=ENV
    ).stdout.strip()


def write(repo: Path, files: dict[str, str]) -> None:
    for path, text in files.items():
        full = repo / path
        full.parent.mkdir(parents=True, exist_ok=True)
        full.write_text(text)


def commit(repo: Path, files: dict[str, str], message: str = "c") -> str:
    write(repo, files)
    git(repo, "add", "-A")
    git(repo, "commit", "-q", "-m", message)
    return git(repo, "rev-parse", "HEAD")


@pytest.fixture
def base_repo(tmp_path: Path) -> tuple[Path, str]:
    repo = tmp_path / "onboarding"
    repo.mkdir()
    git(repo, "init", "-q")
    head = commit(repo, {".sdo/goal.md": "goal\n", ".sdo/playbooks/README.md": "index\n"}, "base")
    return repo, head


def lane(tmp_path: Path, name: str, base: tuple[Path, str], files: dict[str, str]) -> tuple[Path, str]:
    workspace = tmp_path / name
    subprocess.run(["git", "clone", "-q", str(base[0]), str(workspace)], check=True, env=ENV)
    return workspace, commit(workspace, files, f"first try {name}")


def store(tmp_path: Path, base: tuple[Path, str], **kwargs: object) -> GitSnapshotStore:
    return GitSnapshotStore(tmp_path / "store", {"hotel": (base[0], base[1])}, **kwargs)  # type: ignore[arg-type]


def tree(repo: Path, commit_id: str) -> set[str]:
    return set(git(repo, "ls-tree", "-r", "--name-only", commit_id).splitlines())


PLAYBOOK = ".sdo/playbooks/{id}/diagnose.go"


def test_base_resolves_to_the_onboarding_commit(tmp_path: Path, base_repo: tuple[Path, str]) -> None:
    assert store(tmp_path, base_repo).resolve("hotel", "base") == base_repo[1]


def test_own_snapshot_is_base_plus_only_that_problems_learning(tmp_path: Path, base_repo: tuple[Path, str]) -> None:
    s = store(tmp_path, base_repo)
    workspace, head = lane(
        tmp_path,
        "a",
        base_repo,
        {PLAYBOOK.format(id="fix-a"): "package a\n", ".sdo/outcomes/x.json": "{}", ".sdo/goal.md": "changed\n"},
    )
    s.import_first_try("hotel", "a", workspace, head)
    own = s.resolve("hotel", "own:a")
    files = tree(s.repositories()["hotel"], own)
    assert PLAYBOOK.format(id="fix-a") in files
    assert ".sdo/outcomes/x.json" not in files  # outcomes are not learning
    assert git(s.repositories()["hotel"], "show", f"{own}:.sdo/goal.md") == "goal"  # goal edits are not a delta
    assert git(s.repositories()["hotel"], "rev-parse", f"{own}^") == base_repo[1]


def test_a_problem_without_a_first_try_has_the_base_as_its_own_snapshot(
    tmp_path: Path, base_repo: tuple[Path, str]
) -> None:
    s = store(tmp_path, base_repo)
    s.record_first_try("hotel", "a", None)
    assert s.resolve("hotel", "own:a") == base_repo[1]


def test_merged_holds_every_delta_and_the_report_is_written(tmp_path: Path, base_repo: tuple[Path, str]) -> None:
    s = store(tmp_path, base_repo)
    for name in ("a", "b"):
        workspace, head = lane(tmp_path, name, base_repo, {PLAYBOOK.format(id=f"fix-{name}"): f"package {name}\n"})
        s.import_first_try("hotel", name, workspace, head)
    report = s.merge("hotel", ["a", "b"])
    files = tree(s.repositories()["hotel"], s.resolve("hotel", "merged"))
    assert {PLAYBOOK.format(id="fix-a"), PLAYBOOK.format(id="fix-b")} <= files
    assert report.included == ["a", "b"]
    assert json.loads((s.repositories()["hotel"] / "merge-report.json").read_text())["included"] == ["a", "b"]


def test_merged_is_not_available_before_the_merge(tmp_path: Path, base_repo: tuple[Path, str]) -> None:
    with pytest.raises(ValueError, match="has not been built"):
        store(tmp_path, base_repo).resolve("hotel", "merged")


def test_a_colliding_playbook_is_renamed_and_its_detector_follows(tmp_path: Path, base_repo: tuple[Path, str]) -> None:
    s = store(tmp_path, base_repo)
    detector = ".sdo/diagnostics/detectors/incidents/d-{n}/manifest.yaml"
    first = lane(
        tmp_path,
        "a",
        base_repo,
        {PLAYBOOK.format(id="restart"): "package a\n", detector.format(n="a"): "playbooks: [restart]\n"},
    )
    second = lane(
        tmp_path,
        "b",
        base_repo,
        {PLAYBOOK.format(id="restart"): "package b\n", detector.format(n="b"): "playbooks: [restart]\n"},
    )
    s.import_first_try("hotel", "a", *first)
    s.import_first_try("hotel", "b", *second)
    report = s.merge("hotel", ["a", "b"])
    repo = s.repositories()["hotel"]
    merged = s.resolve("hotel", "merged")
    new_id = report.renames["b"]["restart"]
    assert new_id.startswith("restart-")
    assert git(repo, "show", f"{merged}:{PLAYBOOK.format(id='restart')}") == "package a"
    assert git(repo, "show", f"{merged}:{PLAYBOOK.format(id=new_id)}") == "package b"
    assert git(repo, "show", f"{merged}:{detector.format(n='b')}") == f"playbooks: [{new_id}]"
    assert git(repo, "show", f"{merged}:{detector.format(n='a')}") == "playbooks: [restart]"


def test_an_identical_unit_is_a_duplicate_and_adds_nothing(tmp_path: Path, base_repo: tuple[Path, str]) -> None:
    s = store(tmp_path, base_repo)
    s.import_first_try("hotel", "a", *lane(tmp_path, "a", base_repo, {PLAYBOOK.format(id="same"): "package x // v1\n"}))
    s.import_first_try("hotel", "b", *lane(tmp_path, "b", base_repo, {PLAYBOOK.format(id="same"): "package x // v1\n"}))
    s.import_first_try(
        "hotel", "c", *lane(tmp_path, "c", base_repo, {PLAYBOOK.format(id="other"): "package x // v2\n"})
    )
    report = s.merge("hotel", ["a", "b", "c"])
    assert [d["problem"] for d in report.duplicates] == ["b"]
    assert report.renames == {}
    assert report.near_duplicates == []
    assert report.included == ["a", "b", "c"]


def test_same_content_under_different_ids_is_a_near_duplicate_and_both_are_kept(
    tmp_path: Path, base_repo: tuple[Path, str]
) -> None:
    s = store(tmp_path, base_repo)
    s.import_first_try("hotel", "e", *lane(tmp_path, "e", base_repo, {PLAYBOOK.format(id="one"): "package x // v1\n"}))
    s.import_first_try("hotel", "f", *lane(tmp_path, "f", base_repo, {PLAYBOOK.format(id="two"): "package x // v1\n"}))
    report = s.merge("hotel", ["e", "f"])
    assert len(report.near_duplicates) == 1
    assert report.near_duplicates[0]["of_problem"] == "e"
    assert report.included == ["e", "f"]
    files = tree(s.repositories()["hotel"], s.resolve("hotel", "merged"))
    assert {PLAYBOOK.format(id="one"), PLAYBOOK.format(id="two")} <= files


def test_a_delta_that_fails_the_memory_check_is_excluded(tmp_path: Path, base_repo: tuple[Path, str]) -> None:
    def check(repo: Path, commit_id: str) -> CheckResult:
        bad = PLAYBOOK.format(id="bad") in git(repo, "ls-tree", "-r", "--name-only", commit_id)
        return CheckResult(ok=not bad, output="does not compile" if bad else "")

    s = store(tmp_path, base_repo, check=check)
    s.import_first_try("hotel", "a", *lane(tmp_path, "a", base_repo, {PLAYBOOK.format(id="good"): "package a\n"}))
    s.import_first_try("hotel", "b", *lane(tmp_path, "b", base_repo, {PLAYBOOK.format(id="bad"): "package b\n"}))
    s.import_first_try("hotel", "c", *lane(tmp_path, "c", base_repo, {PLAYBOOK.format(id="fine"): "package c\n"}))
    report = s.merge("hotel", ["a", "b", "c"])
    files = tree(s.repositories()["hotel"], s.resolve("hotel", "merged"))
    assert report.included == ["a", "c"]
    assert report.excluded == [{"problem": "b", "reason": "memory check failed"}]
    assert PLAYBOOK.format(id="bad") not in files
    assert report.check["b"]["ok"] is False


def test_a_colliding_lib_file_is_kept_first_and_recorded_as_a_conflict(
    tmp_path: Path, base_repo: tuple[Path, str]
) -> None:
    s = store(tmp_path, base_repo)
    s.import_first_try("hotel", "a", *lane(tmp_path, "a", base_repo, {".sdo/lib/util.go": "package lib // a\n"}))
    s.import_first_try(
        "hotel",
        "b",
        *lane(
            tmp_path,
            "b",
            base_repo,
            {".sdo/lib/util.go": "package lib // b\n", PLAYBOOK.format(id="pb"): "package b\n"},
        ),
    )
    report = s.merge("hotel", ["a", "b"])
    repo = s.repositories()["hotel"]
    assert git(repo, "show", f"{s.resolve('hotel', 'merged')}:.sdo/lib/util.go") == "package lib // a"
    assert report.conflicts == [{"problem": "b", "unit": ".sdo/lib/util.go", "kind": "lib"}]
    assert PLAYBOOK.format(id="pb") in tree(repo, s.resolve("hotel", "merged"))


def test_missing_first_tries_and_empty_deltas_are_recorded(tmp_path: Path, base_repo: tuple[Path, str]) -> None:
    s = store(tmp_path, base_repo)
    s.record_first_try("hotel", "lost", None)
    s.import_first_try("hotel", "empty", *lane(tmp_path, "empty", base_repo, {".sdo/outcomes/o.json": "{}"}))
    report = s.merge("hotel", ["lost", "empty", "never-ran"])
    assert report.missing_first_try == ["lost", "never-ran"]
    assert report.empty_delta == ["empty"]
    assert report.merged == base_repo[1]


def test_from_config_builds_a_store(tmp_path: Path, base_repo: tuple[Path, str]) -> None:
    s = GitSnapshotStore.from_config(
        {"root": str(tmp_path / "s"), "bases": {"hotel": {"repo": str(base_repo[0]), "commit": base_repo[1]}}}
    )
    assert s.resolve("hotel", "base") == base_repo[1]
