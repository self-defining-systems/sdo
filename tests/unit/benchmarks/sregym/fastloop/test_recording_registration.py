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


def test_recording_drift_registration_matches_production_contract(tmp_path: Path) -> None:
    directory = recording_registrations(tmp_path, "worker:test")
    drift = json.loads((directory / "drift_investigator.json").read_text())
    source = json.loads(
        (Path(__file__).resolve().parents[5] / "sdo/agents/registrations/drift_investigator.json").read_text()
    )
    assert {key: value for key, value in drift.items() if key != "kubernetes_job"} == {
        key: value for key, value in source.items() if key != "kubernetes_job"
    }
    assert drift["kubernetes_job"] == {
        "image": "worker:test",
        "command": ["python3", "-m", "benchmarks.sregym.fastloop.recording_worker", "drift"],
    }


def test_recording_and_scripted_registrations_cover_same_task_types(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[5]
    live = recording_registrations(tmp_path, "worker:test")
    scripted = root / "benchmarks/sregym/experiments/fastloop/scripted_registrations"

    def task_types(directory: Path) -> set[str]:
        return {
            capability["task_type"]
            for path in directory.glob("*.json")
            for capability in json.loads(path.read_text())["capabilities"]
        }

    assert task_types(live) == task_types(scripted)


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


def test_recording_drift_stage_passes_executor_to_real_worker(tmp_path: Path, monkeypatch) -> None:
    from benchmarks.sregym.fastloop import recording_worker

    task = tmp_path / "task.json"
    task.write_text("{}", encoding="utf-8")
    seen: list[object] = []
    monkeypatch.setenv("SDO_NAMESPACE", "demo")
    monkeypatch.setenv("SDO_RESULT_CONFIGMAP", "result")
    monkeypatch.setenv("SDO_TASK_PATH", str(task))
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path / "scratch"))
    monkeypatch.setattr(recording_worker, "prepare_codex_home", lambda: None)
    monkeypatch.setattr(recording_worker, "prepare_claude_home", lambda: None)
    monkeypatch.setattr(recording_worker, "KubernetesResultPublisher", lambda namespace: object())

    def investigate(_request: object, *, executor: object) -> object:
        seen.append(executor)
        return object()

    def run(_task: Path, _name: str, _publisher: object, *, investigator) -> None:
        investigator(object())

    monkeypatch.setattr(recording_worker.drift_worker, "investigate_drift", investigate)
    monkeypatch.setattr(recording_worker.drift_worker, "run_drift_task", run)
    assert recording_worker.main(["drift"]) == 0
    assert len(seen) == 1
    assert isinstance(seen[0], recording_worker.RecordingExecutor)
