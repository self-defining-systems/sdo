"""Ratchet: the SREGym worker runs in SREGym's own environment, without SDO's dependencies."""

from __future__ import annotations

import ast
import os
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[5]
SREGYM = ROOT / "third_party" / "sregym"
BRIDGE = ROOT / "benchmarks" / "sregym" / "fastloop" / "worker_bridge.py"


def worker_modules() -> list[str]:
    """Every first-party module the worker bridge can load, eagerly or on a request path."""
    tree = ast.parse(BRIDGE.read_text())
    return sorted(
        {
            node.module
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("benchmarks.")
        }
        | {"benchmarks.sregym.fastloop.worker_bridge"}
    )


@pytest.mark.skipif(not (SREGYM / "pyproject.toml").is_file(), reason="SREGym submodule is not initialized")
def test_worker_modules_import_in_the_sregym_environment() -> None:
    """h9.18 astronomy deploys failed: source deployment pulled sdo.contracts (protovalidate) into the worker."""
    modules = worker_modules()
    assert "benchmarks.sregym.fastloop.source_deployment" in modules
    script = "import importlib, sys\nfor name in sys.argv[1:]:\n    importlib.import_module(name)\n"
    result = subprocess.run(
        ["uv", "run", "--directory", str(SREGYM), "python", "-c", script, *modules],
        env={**os.environ, "PYTHONPATH": str(ROOT)},
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    assert result.returncode == 0, result.stderr[-4000:]
