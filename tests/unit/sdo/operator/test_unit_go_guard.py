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
    import json
    import sys

    inventory = tmp_path / "inventory.json"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "--collect-only",
            "-q",
            "-p",
            "no:tach",
            "-p",
            "scripts.pytest_ci",
            "-m",
            "go_integration",
            "tests/unit/benchmarks/sregym/assurance/test_scripted_codex.py",
        ],
        cwd=ROOT,
        env={**os.environ, "SDO_PYTEST_INVENTORY": str(inventory)},
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    names = json.loads(inventory.read_text())
    assert any(name.endswith("::test_resumed_reflection_accounts_its_tokens_and_edits_final_files") for name in names)
    assert any(
        name.endswith("::test_scripted_reflection_fixes_local_diagnostics_in_the_same_checkout") for name in names
    )
