"""Unit tests must fail before starting a compiler, including nested CLI calls."""

import os
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[4]


@pytest.mark.parametrize(
    "arguments", ["build ./...", "test ./...", "run main.go", "install ./...", "list -export ./..."]
)
def test_unit_go_guard_rejects_compilation(tmp_path: Path, arguments: str) -> None:
    real = tmp_path / "real-go"
    real.write_text("#!/bin/sh\nexit 0\n")
    real.chmod(0o755)
    result = subprocess.run(
        ["sh", "-c", f"go {arguments}"],
        env={
            **os.environ,
            "PATH": f"{ROOT / 'scripts/unit-tools'}:{os.environ['PATH']}",
            "SDO_UNIT_REAL_GO": str(real),
        },
        capture_output=True,
        text=True,
    )
    assert result.returncode == 97
    assert "pytest.mark.go_integration" in result.stderr


def test_memory_fixture_consumers_are_discovered_as_compiler_contracts(tmp_path: Path) -> None:
    import sys

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "--collect-only",
            "-q",
            "-p",
            "no:tach",
            "-m",
            "go_integration",
            "tests/unit/benchmarks/sregym/assurance/test_script_execution.py",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    names = result.stdout.splitlines()
    assert any(name.endswith("::test_resumed_reflection_accounts_its_tokens_and_edits_final_files") for name in names)
    assert any(
        name.endswith("::test_scripted_reflection_fixes_local_diagnostics_in_the_same_checkout") for name in names
    )


def test_guard_covers_module_fixture_setup(tmp_path: Path) -> None:
    import sys

    commands = tmp_path / "bin"
    commands.mkdir()
    go = commands / "go"
    go.write_text("#!/bin/sh\nexit 0\n")
    go.chmod(0o755)
    suite = tmp_path / "test_compiler.py"
    suite.write_text("""import subprocess
import pytest
@pytest.fixture(scope="module")
def compiled():
    subprocess.run(["go", "build", "./..."], check=True)
def test_compiler(compiled):
    pass
""")
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:tach",
            "-p",
            "tests.conftest",
            "--confcutdir",
            str(tmp_path),
            str(suite),
        ],
        cwd=ROOT,
        env={**os.environ, "PATH": f"{commands}:{os.environ['PATH']}"},
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1, result.stdout + result.stderr
    assert "Go compilation requires pytest.mark.go_integration" in result.stdout + result.stderr
