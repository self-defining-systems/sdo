"""The branch remote's `up` says why its server did not roll out."""

from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING

from benchmarks.commit_series import remote

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def test_a_server_that_does_not_roll_out_is_described_in_the_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def fail(*_args: object) -> str:
        raise subprocess.CalledProcessError(1, ["kubectl", "rollout", "status"])

    seen: list[list[str]] = []

    def run(cmd: list[str], **_kw: object) -> subprocess.CompletedProcess[str]:
        seen.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, "Events:\n  Warning  Failed  ErrImagePull: pull access denied", "")

    monkeypatch.setattr(remote, "serve_upstream", fail)
    monkeypatch.setattr(remote.subprocess, "run", run)
    code = remote.main(
        [
            "up",
            "--kubeconfig",
            str(tmp_path / "k"),
            "--namespace",
            "fg1",
            "--image",
            "img:t",
            "--repository",
            str(tmp_path),
        ]
    )
    assert code == 1
    err = capsys.readouterr().err
    assert "ErrImagePull" in err
    assert any("describe" in c for c in seen)
    assert any("fg1-upstream" in c for c in seen)


def test_rewind_points_the_branch_at_a_commit_the_server_holds_and_refuses_one_it_lacks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    seen: list[list[str]] = []
    answers = iter([0, 0, 1])

    def run(cmd: list[str], **_kw: object) -> subprocess.CompletedProcess[str]:
        seen.append(cmd)
        return subprocess.CompletedProcess(cmd, next(answers), "", "fatal: Not a valid object name")

    monkeypatch.setattr(remote.subprocess, "run", run)
    args = ["rewind", "--kubeconfig", str(tmp_path / "k"), "--namespace", "fg1", "--head", "9d992b3"]
    assert remote.main(args) == 0
    assert [c[-3:] for c in seen] == [
        ["cat-file", "-e", "9d992b3^{commit}"],
        ["update-ref", "refs/heads/sdo-source", "9d992b3"],
    ]
    assert "fg1-upstream" in seen[0]
    assert remote.main(args) == 1  # the server lacks the commit: nothing moved
    assert len(seen) == 3
    assert "Not a valid object name" in capsys.readouterr().err
