"""Benchmark worker entry points that record Codex CLI turns for stage replay."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from benchmarks.sregym.fastloop.model_recording import RecordingExecutor
from sdo.agents.responder import KubernetesResultPublisher, prepare_claude_home, prepare_codex_home
from sdo.agents.responder.codex import execute_incident
from sdo.agents.responder.reflection import CodexSessionBackend, SessionReflector
from sdo.agents.responder.reflection_worker import run_reflection_task
from sdo.agents.responder.worker import run_worker_task
from sdo.contracts import AgentTask, ReflectionRequest, parse_json

_REGISTRATIONS = Path(__file__).resolve().parents[3] / "sdo" / "agents" / "registrations"


def recording_registrations(results_dir: Path, image: str) -> Path:
    directory = results_dir / "recording_registrations"
    directory.mkdir(parents=True, exist_ok=True)
    for role, stage in (("responder", "responder"), ("reflector", "reflection")):
        registration = json.loads((_REGISTRATIONS / f"{role}.json").read_text(encoding="utf-8"))
        registration["kubernetes_job"] = {
            "image": image,
            "command": ["python3", "-m", "benchmarks.sregym.fastloop.recording_worker", stage],
        }
        (directory / f"{role}.json").write_text(json.dumps(registration, indent=2) + "\n", encoding="utf-8")
    return directory


def main(argv: list[str] | None = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    if len(args) != 1 or args[0] not in {"responder", "reflection"}:
        print("usage: recording_worker responder|reflection", file=sys.stderr)
        return 2
    namespace = os.environ.get("SDO_NAMESPACE", "")
    result_name = os.environ.get("SDO_RESULT_CONFIGMAP", "")
    if not namespace or not result_name:
        print("SDO_NAMESPACE and SDO_RESULT_CONFIGMAP are required", file=sys.stderr)
        return 2
    task_path = Path(os.environ.get("SDO_TASK_PATH", "/sdo/request/agent-task.json"))
    wire = task_path.read_text(encoding="utf-8")
    scratch = Path(os.environ.get("SDO_SESSION_SCRATCH", "/sdo/session/scratch"))
    recording = RecordingExecutor(scratch.parent / "model_io", request_json=wire, record=True)
    prepare_codex_home()
    prepare_claude_home()
    publisher = KubernetesResultPublisher(namespace=namespace)
    try:
        if args[0] == "responder":
            run_worker_task(
                task_path,
                result_name,
                publisher,
                executor=lambda request: execute_incident(
                    request, model="gpt-6-luna", provider="codex", executor=recording
                ),
            )
        else:
            task = parse_json(wire, AgentTask())
            request = ReflectionRequest()
            if not task.request.Unpack(request):
                raise ValueError("recording reflector requires a typed reflection request")
            reflector = SessionReflector(
                CodexSessionBackend(model="gpt-6-luna", executor=recording),
                guidance=request.guidance or "baseline",
            )
            run_reflection_task(task_path, result_name, publisher, reflector=reflector)
    except (OSError, ValueError, RuntimeError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
