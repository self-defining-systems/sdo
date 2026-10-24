from __future__ import annotations

import json
from pathlib import Path

from benchmarks.sregym.fastloop.recording_worker import recording_registrations
from benchmarks.sregym.fastloop.sdo_agent import drift_tokens_from_state


def test_recording_registrations_route_workers_to_benchmark_boundary(tmp_path: Path) -> None:
    directory = recording_registrations(tmp_path, "worker:test")

    responder = json.loads((directory / "responder.json").read_text())
    reflector = json.loads((directory / "reflector.json").read_text())
    assert responder["kubernetes_job"] == {
        "image": "worker:test",
        "command": ["python3", "-m", "benchmarks.sregym.fastloop.recording_worker", "responder"],
    }
    assert reflector["kubernetes_job"]["command"][-1] == "reflection"


def test_recording_registrations_preserve_all_builtin_capabilities(tmp_path: Path) -> None:
    directory = recording_registrations(tmp_path, "worker:test")
    root = Path(__file__).resolve().parents[5]
    builtins = root / "sdo/agents/registrations"
    assert {path.name for path in directory.glob("*.json")} == {
        "deployer.json",
        "health-judge.json",
        "responder.json",
        "reflector.json",
    }
    for path in builtins.glob("*.json"):
        original = json.loads(path.read_text())
        recorded = json.loads((directory / path.name).read_text())
        assert recorded["agent_id"] == original["agent_id"]
        assert recorded["capabilities"] == original["capabilities"]
        assert recorded["kubernetes_job"]["image"] == "worker:test"
        if path.stem in {"deployer", "health-judge"}:
            assert recorded["kubernetes_job"]["command"] == original["kubernetes_job"]["command"]


def test_recording_roles_retain_scripted_incident_task_coverage(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[5]
    live = recording_registrations(tmp_path, "worker:test")
    scripted = root / "benchmarks/sregym/adapter/scripted_registrations"

    def task_types(directory: Path, roles: set[str] | None = None) -> set[str]:
        return {
            capability["task_type"]
            for path in directory.glob("*.json")
            if roles is None or path.stem in roles
            for capability in json.loads(path.read_text())["capabilities"]
        }

    assert task_types(live, {"responder", "reflector"}) == task_types(scripted)


def test_incident_drift_tokens_use_completed_investigations() -> None:
    from datetime import datetime, timezone

    started = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    finished = datetime(2026, 1, 1, 0, 2, 0, tzinfo=timezone.utc)
    state = {
        "drift_investigation": {
            "token_usage": [
                {"at": "2025-12-31T23:59:59Z", "tokens": 9, "input_tokens": 7, "output_tokens": 2},
                {"at": "2026-01-01T00:01:00Z", "tokens": 12, "input_tokens": 10, "output_tokens": 2},
                {"at": "2026-01-01T00:02:01Z", "tokens": 5, "input_tokens": 4, "output_tokens": 1},
            ]
        }
    }
    usage = drift_tokens_from_state(state, started, finished)
    assert usage.input_tokens == 10
    assert usage.output_tokens == 2


def test_recording_worker_persists_failed_reflection_checkpoint(tmp_path: Path, monkeypatch) -> None:
    from benchmarks.sregym.fastloop import recording_worker
    from sdo.contracts import AgentResult, AgentTask, ReflectionRequest, to_canonical_json

    task = AgentTask(task_id="reflection-1", task_type="outcome-reflection-direct-v1", workspace={"proposal_id": "one"})
    task.request.Pack(ReflectionRequest())
    wire = to_canonical_json(task)
    task_path = tmp_path / "task.json"
    task_path.write_text(wire)
    monkeypatch.setenv("SDO_NAMESPACE", "demo")
    monkeypatch.setenv("SDO_RESULT_CONFIGMAP", "result")
    monkeypatch.setenv("SDO_TASK_PATH", str(task_path))
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path / "scratch"))
    monkeypatch.setattr(recording_worker, "prepare_codex_home", lambda **_: None)
    monkeypatch.setattr(recording_worker, "prepare_claude_home", lambda **_: None)

    class Publisher:
        def publish_payload(self, *_):
            pass

    monkeypatch.setattr(recording_worker, "KubernetesResultPublisher", lambda **_: Publisher())

    def run(_path, _name, publisher, **_):
        publisher.publish_payload(
            "result",
            "agent-result.json",
            to_canonical_json(
                AgentResult(task_id="reflection-1", proposal_id="one", status="failed", error_summary="rejected")
            ),
        )

    monkeypatch.setattr(recording_worker, "run_reflection_task", run)
    assert recording_worker.main(["reflection"]) == 0
    checkpoints = list((tmp_path / "checkpoints").glob("*/request.json"))
    assert len(checkpoints) == 1
    assert checkpoints[0].read_text() == wire
    assert json.loads(checkpoints[0].with_name("result.json").read_text())["error_summary"] == "rejected"
