"""Generate a task-specific script, then launch the ordinary production worker."""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

from benchmarks.sregym.agent_script.cli import launch
from benchmarks.sregym.fastloop.judge_script import judge_script
from benchmarks.sregym.fastloop.reflection_script import reflection_script
from benchmarks.sregym.fastloop.responder_script import responder_script
from sdo.contracts import AgentTask, HealthJudgeRequest, IncidentRequest, ReflectionRequest, parse_json


def _launch_task() -> int:
    role, *command = sys.argv[1:]
    task = parse_json(Path(os.environ.get("SDO_TASK_PATH", "/sdo/request/agent-task.json")).read_text(), AgentTask())
    tools = Path(os.environ.setdefault("SDO_TOOLS_DIR", "/sdo/session/tools"))
    exchange = tools / "requests" if (tools / "requests").is_dir() else tools
    exchange.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="generated-", dir=exchange) as temporary:
        os.environ["SDO_AGENT_SCRIPT_WORK_DIR"] = temporary
        if role == "responder":
            request = IncidentRequest()
            if not task.request.Unpack(request):
                raise ValueError("expected IncidentRequest")
            script = responder_script(request)
        elif role == "reflector":
            reflection = ReflectionRequest()
            if not task.request.Unpack(reflection):
                raise ValueError("expected ReflectionRequest")
            script = reflection_script(
                worktree=Path(task.workspace.checkout_path),
                incident_id=reflection.outcome.incident_id,
                closure=reflection.closure,
                capture=Path(os.environ.get("SDO_CAPTURED_CORPUS_DIR", "/sdo/session/corpus")),
            )
        elif role == "health-judge":
            judged = HealthJudgeRequest()
            if not task.request.Unpack(judged):
                raise ValueError("expected HealthJudgeRequest")
            script = judge_script(checkout=Path(task.workspace.checkout_path), request=judged)
        else:
            raise ValueError(f"unknown generated role: {role}")
        path = Path(temporary) / "script.json"
        path.write_text(script.model_dump_json())
        os.environ["SDO_AGENT_SCRIPT"] = str(path)
        return launch(command)


def main() -> int:
    keys = ("SDO_AGENT_SCRIPT", "SDO_AGENT_SCRIPT_WORK_DIR", "SDO_TOOLS_DIR")
    previous = {key: os.environ.get(key) for key in keys}
    try:
        return _launch_task()
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


if __name__ == "__main__":
    raise SystemExit(main())
