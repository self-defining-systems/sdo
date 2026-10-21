"""Direct-edit checkpoints replay exact file bytes, deletions, and modes."""

import subprocess
from pathlib import Path

import pytest

from benchmarks.sregym.fastloop.file_edits import FileEdit, apply_file_edits, record_file_edits


def test_record_and_replay_direct_memory_tree(tmp_path: Path) -> None:
    root = tmp_path / "source"
    root.mkdir()
    for args in (("init", "-q"), ("config", "user.name", "Test"), ("config", "user.email", "test@example.invalid")):
        subprocess.run(["git", "-C", str(root), *args], check=True)
    (root / ".gitignore").write_text(".sdo/\n")
    (root / "deleted").write_text("before")
    subprocess.run(["git", "-C", str(root), "add", "."], check=True)
    subprocess.run(["git", "-C", str(root), "commit", "-qm", "base"], check=True)
    base = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
    destination = tmp_path / "replay"
    subprocess.run(["git", "clone", "-q", str(root), str(destination)], check=True)
    program = root / ".sdo/playbooks/fault"
    program.mkdir(parents=True)
    (program / "diagnose.go").write_bytes(b"package fault\n")
    script = program / "verify.sh"
    script.write_bytes(b"#!/bin/sh\ntrue\n")
    script.chmod(0o755)
    (root / "deleted").unlink()

    edits = record_file_edits(root, base)
    apply_file_edits(destination, edits)

    assert (destination / ".sdo/playbooks/fault/diagnose.go").read_bytes() == b"package fault\n"
    assert (destination / ".sdo/playbooks/fault/verify.sh").stat().st_mode & 0o777 == 0o755
    assert not (destination / "deleted").exists()


@pytest.mark.parametrize("path", ["../escape", "/absolute", ".git/config", "parent/../escape"])
def test_replay_rejects_unsafe_edit_paths(tmp_path: Path, path: str) -> None:
    with pytest.raises(ValueError, match="path"):
        apply_file_edits(tmp_path, [FileEdit(path, "eA==")])


def test_replay_rejects_symlink_parent(tmp_path: Path) -> None:
    (tmp_path / "link").symlink_to(tmp_path.parent, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        apply_file_edits(tmp_path, [FileEdit("link/escape", "eA==")])
