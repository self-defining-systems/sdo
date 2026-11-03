"""Run-output policy scans the Git index, including extensionless text."""

import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[3] / "scripts/check_experiment_outputs.py"


def tracked(root: Path, name: str, content: bytes) -> None:
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    subprocess.run(["git", "-C", str(root), "add", "--", name], check=True)


def scan(root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run([sys.executable, str(SCRIPT), "--root", str(root)], capture_output=True, text=True)


@pytest.mark.parametrize(
    ("name", "content"),
    [
        ("notes", b"/mnt/data/shli/" + b"sdo-exp"),
        (".agents/reference.md", b"sdo-exp" + b"/run"),
        ("README.md", b"202611" + b"01T022237Z-sdo"),
        ("source.py", b"202611" + b"01T022237Z"),
        ("EXAMPLE" + "_REPORT.md", b"report"),
        ("handoffs/result.md", b"report"),
    ],
)
def test_rejects_run_outputs(tmp_path: Path, name: str, content: bytes) -> None:
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    tracked(tmp_path, name, content)
    result = scan(tmp_path)
    assert result.returncode == 1, result.stderr
    assert name in result.stdout
    assert "outside the repository" in result.stdout


def test_ignores_untracked_binary_and_removed_files(tmp_path: Path) -> None:
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    tracked(tmp_path, "README.md", b"Generic limits and field definitions\n")
    tracked(tmp_path, "asset.bin", b"\0" + b"sdo-exp" + b"/run")
    tracked(tmp_path, "deleted.md", b"sdo-exp" + b"/run")
    (tmp_path / "deleted.md").unlink()
    (tmp_path / "untracked.md").write_bytes(b"sdo-exp" + b"/run")
    assert scan(tmp_path).returncode == 0


def test_git_failure_is_not_a_clean_scan(tmp_path: Path) -> None:
    assert scan(tmp_path).returncode != 0
