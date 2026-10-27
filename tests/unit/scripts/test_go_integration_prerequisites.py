"""The external-process driver owns its pinned inputs before pytest workers start."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def test_go_integration_initializes_inputs_before_starting_pytest(tmp_path: Path) -> None:
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    driver = scripts / "test_go_integration.sh"
    shutil.copy2(ROOT / "scripts/test_go_integration.sh", driver)
    (tmp_path / "sdo/operator/runtime").mkdir(parents=True)
    tools = tmp_path / "bin"
    tools.mkdir()
    events = tmp_path / "events.jsonl"
    initialized = tmp_path / "initialized"
    for tool in ("uv", "go"):
        path = tools / tool
        path.write_text(
            f"#!{sys.executable}\n"
            "import json, os, pathlib, sys\n"
            "args = sys.argv[1:]\n"
            "with open(os.environ['EVENTS'], 'a') as out:\n"
            "    out.write(json.dumps([pathlib.Path(sys.argv[0]).name, *args]) + '\\n')\n"
            "ready = pathlib.Path(os.environ['INITIALIZED'])\n"
            "if args == ['run', 'python', 'scripts/init_test_prereqs.py']: ready.touch()\n"
            "if 'pytest' in args and not ready.exists(): sys.exit(62)\n"
        )
        path.chmod(0o755)
    result = subprocess.run(
        ["bash", str(driver)],
        env={
            **os.environ,
            "PATH": str(tools) + os.pathsep + os.defpath,
            "EVENTS": str(events),
            "INITIALIZED": str(initialized),
        },
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    calls = [json.loads(line) for line in events.read_text().splitlines()]
    assert calls[0] == ["uv", "run", "python", "scripts/init_test_prereqs.py"]
    assert any("pytest" in call for call in calls)
