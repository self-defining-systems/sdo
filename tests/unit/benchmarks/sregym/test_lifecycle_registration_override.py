"""A fresh install accepts a registration override only when its lifecycle workers are production's."""

import json
from pathlib import Path

from benchmarks.sregym.adapter.driver import lifecycle_workers_unchanged
from benchmarks.sregym.fastloop.recording_worker import recording_registrations


def test_live_recorder_keeps_the_production_lifecycle_workers(tmp_path: Path) -> None:
    assert lifecycle_workers_unchanged(recording_registrations(tmp_path, "responder:any"))


def test_a_changed_lifecycle_worker_is_not_a_production_override(tmp_path: Path) -> None:
    directory = recording_registrations(tmp_path, "responder:any")
    judge = json.loads((directory / "health-judge.json").read_text())
    judge["kubernetes_job"]["command"] = ["python3", "-m", "benchmarks.sregym.fastloop.generated_worker"]
    (directory / "health-judge.json").write_text(json.dumps(judge))
    assert not lifecycle_workers_unchanged(directory)
    (directory / "health-judge.json").unlink()
    assert not lifecycle_workers_unchanged(directory)
