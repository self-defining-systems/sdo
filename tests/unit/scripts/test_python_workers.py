"""Python test worker defaults respect shared hosts and explicit overrides."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]


@pytest.mark.parametrize(
    ("cores", "override", "expected"),
    [(64, None, "4"), (2, None, "2"), (1, None, "1"), (64, "0", "0"), (64, "auto", "auto"), (64, "8", "8")],
)
def test_python_worker_selection(tmp_path: Path, cores: int, override: str | None, expected: str) -> None:
    for name, body in {
        "nproc": f"printf '%s\\n' {cores}",
        "uv": 'printf "%s\\n" "$@"',
    }.items():
        command = tmp_path / name
        command.write_text(f"#!/bin/sh\n{body}\n")
        command.chmod(0o755)
    env = {**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}"}
    env.pop("SDO_PYTEST_WORKERS", None)
    if override is not None:
        env["SDO_PYTEST_WORKERS"] = override
    result = subprocess.run(
        ["bash", str(ROOT / "scripts/test_python.sh"), "-m", "not go_integration"],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    args = result.stdout.splitlines()
    assert args[args.index("-n") + 1] == expected
    assert args[-3:] == ["-m", "not go_integration", "tests"]


def test_ci_explicitly_preserves_hosted_python_worker_count() -> None:
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    assert workflow["env"]["SDO_PYTEST_WORKERS"] == "2"
