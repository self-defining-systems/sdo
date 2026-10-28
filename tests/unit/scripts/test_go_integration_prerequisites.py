"""The external-process driver owns its pinned inputs before pytest workers start."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]


@pytest.mark.parametrize("shard", ["all", "0", "nightly"])
def test_go_integration_initializes_inputs_before_starting_pytest(tmp_path: Path, shard: str) -> None:
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
            "if ('pytest' in args or 'scripts/python_cost_plan.py' in args) and not ready.exists(): sys.exit(62)\n"
        )
        path.chmod(0o755)
    result = subprocess.run(
        ["bash", str(driver), shard],
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
    if shard == "all":
        pytest_call = next(call for call in calls if "pytest" in call)
        assert pytest_call[-1] == "tests"
    else:
        plan_call = next(call for call in calls if "scripts/python_cost_plan.py" in call)
        assert plan_call[plan_call.index("--bin") + 1] == shard
