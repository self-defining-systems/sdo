"""The preflight uses the same package boundary that pytest prepend mode uses."""

import subprocess
import sys
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[3] / "scripts/check_pytest_imports.py"


def test_duplicate_flat_test_modules_fail_before_collection(tmp_path: Path) -> None:
    for group in ("agents", "benchmark"):
        directory = tmp_path / group
        directory.mkdir()
        (directory / "test_replay.py").write_text("def test_example(): pass\n")
    command = [sys.executable, str(SCRIPT), "--root", str(tmp_path)]
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    assert result.returncode == 1
    assert "pytest import collision test_replay" in result.stdout
    assert "agents" in result.stdout
    assert "benchmark" in result.stdout
    # Packaging one side gives it a distinct import identity.
    (tmp_path / "agents/__init__.py").touch()
    assert subprocess.run(command, capture_output=True, check=False).returncode == 0


def test_same_package_names_in_distinct_roots_still_collide(tmp_path: Path) -> None:
    for group in ("left", "right"):
        directory = tmp_path / group / "shared"
        directory.mkdir(parents=True)
        (directory / "__init__.py").touch()
        (directory / "test_replay.py").write_text("def test_example(): pass\n")
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--root", str(tmp_path)], capture_output=True, text=True, check=False
    )
    assert result.returncode == 1
    assert "shared.test_replay" in result.stdout
