"""Benchmark worker entry points that record Codex CLI turns for stage replay."""

from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path

from benchmarks.sregym.fastloop.file_edits import MAX_FILE_EDITS_BYTES, encode_file_edits, record_file_edits
from benchmarks.sregym.fastloop.model_recording import RecordingExecutor
from sdo.agents.responder import (
    AGENT_SKILLS,
    CodexSessionBackend,
    KubernetesResultPublisher,
    SessionReflector,
    execute_incident,
    prepare_claude_home,
    prepare_codex_home,
    run_reflection_task,
    run_worker_task,
)
from sdo.contracts import AgentTask, ReflectionRequest, parse_json

_REGISTRATIONS = Path(__file__).resolve().parents[3] / "sdo" / "agents" / "registrations"


def recording_registrations(results_dir: Path, image: str) -> Path:
    directory = results_dir / "recording_registrations"
    directory.mkdir(parents=True, exist_ok=True)
    recording_stages = {"responder": "responder", "reflector": "reflection"}
    for role in ("deployer", "health-judge", "responder", "reflector"):
        registration = json.loads((_REGISTRATIONS / f"{role}.json").read_text(encoding="utf-8"))
        registration["kubernetes_job"]["image"] = image
        if role in recording_stages:
            registration["kubernetes_job"]["command"] = [
                "python3",
                "-m",
                "benchmarks.sregym.fastloop.recording_worker",
                recording_stages[role],
            ]
        (directory / f"{role}.json").write_text(json.dumps(registration, indent=2) + "\n", encoding="utf-8")
    return directory


def _record_payload(path: Path, payload: str, *, limit: int = 1 << 20) -> None:
    if len(payload.encode()) > limit:
        raise ValueError(f"worker checkpoint {path.name} exceeds {limit} bytes")
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_suffix(".tmp")
    with os.fdopen(os.open(temporary, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600), "w", encoding="utf-8") as stream:
        stream.write(payload)
    temporary.replace(path)


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
    role = "reflector" if args[0] == "reflection" else "responder"
    prepare_codex_home(skills=AGENT_SKILLS[role])
    prepare_claude_home(skills=AGENT_SKILLS[role])
    directory = scratch.parent / "checkpoints" / hashlib.sha256(wire.encode()).hexdigest()
    _record_payload(directory / "request.json", wire)
    remote = KubernetesResultPublisher(namespace=namespace)

    class Publisher:
        def publish_payload(self, name: str, key: str, payload: str) -> None:
            if key != "agent-result.json":
                raise ValueError(f"unexpected worker result key {key!r}")
            if args[0] == "reflection" and json.loads(payload).get("status") == "completed":
                task = parse_json(wire, AgentTask())
                edits = record_file_edits(Path(task.workspace.checkout_path), task.workspace.base_commit)
                _record_payload(
                    directory / "file_edits.json", encode_file_edits(edits).decode(), limit=MAX_FILE_EDITS_BYTES
                )
            _record_payload(directory / "result.json", payload)
            remote.publish_payload(name, key, payload)

    publisher = Publisher()
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
