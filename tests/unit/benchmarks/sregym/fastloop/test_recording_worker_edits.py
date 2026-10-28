"""Replay evidence is best effort: an oversized recording never fails a finished reflection."""

import subprocess
from pathlib import Path

from benchmarks.sregym.fastloop.recording_worker import record_reflection_edits


def _repository(root: Path) -> str:
    root.mkdir()
    for args in (("init", "-q"), ("config", "user.name", "Test"), ("config", "user.email", "test@example.invalid")):
        subprocess.run(["git", "-C", str(root), *args], check=True)
    (root / "base").write_text("base")
    subprocess.run(["git", "-C", str(root), "add", "."], check=True)
    subprocess.run(["git", "-C", str(root), "commit", "-qm", "base"], check=True)
    return subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()


def test_small_edits_are_recorded(tmp_path: Path) -> None:
    root = tmp_path / "checkout"
    base = _repository(root)
    (root / "new").write_text("content")

    record_reflection_edits(tmp_path / "checkpoint", root, base)

    assert (tmp_path / "checkpoint" / "file_edits.json").exists()
    assert not (tmp_path / "checkpoint" / "file_edits.unrecorded.txt").exists()


def test_oversized_edits_leave_a_marker_instead_of_failing(tmp_path: Path) -> None:
    root = tmp_path / "checkout"
    base = _repository(root)
    (root / "big").write_bytes(b"x" * ((32 << 20) + 1))

    record_reflection_edits(tmp_path / "checkpoint", root, base)

    assert not (tmp_path / "checkpoint" / "file_edits.json").exists()
    assert "at most 32 MiB" in (tmp_path / "checkpoint" / "file_edits.unrecorded.txt").read_text()
