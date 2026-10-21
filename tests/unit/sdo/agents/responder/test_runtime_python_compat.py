"""The production responder image uses Debian Bookworm's Python 3.11."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest


def test_responder_sources_parse_on_runtime_python() -> None:
    python311 = shutil.which("python3.11")
    if python311 is None:
        pytest.skip("Python 3.11 is unavailable outside the production image")
    repository = Path(__file__).resolve().parents[5]
    source_roots = (
        repository / "sdo" / "agents",
        repository / "sdo" / "contracts",
        repository / "libs" / "agent_cli",
    )
    sources = [str(source) for root in source_roots for source in root.rglob("*.py")]
    result = subprocess.run(
        [
            python311,
            "-c",
            "import ast, pathlib, sys; "
            "[(ast.parse(pathlib.Path(path).read_text(encoding='utf-8'), filename=path)) for path in sys.argv[1:]]",
            *sources,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
