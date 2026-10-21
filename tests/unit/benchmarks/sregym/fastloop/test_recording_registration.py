from __future__ import annotations

import json
from typing import TYPE_CHECKING

from benchmarks.sregym.fastloop.recording_worker import recording_registrations

if TYPE_CHECKING:
    from pathlib import Path


def test_recording_registrations_route_both_workers_to_benchmark_boundary(tmp_path: Path) -> None:
    directory = recording_registrations(tmp_path, "worker:test")

    responder = json.loads((directory / "responder.json").read_text())
    reflector = json.loads((directory / "reflector.json").read_text())
    assert responder["kubernetes_job"] == {
        "image": "worker:test",
        "command": ["python3", "-m", "benchmarks.sregym.fastloop.recording_worker", "responder"],
    }
    assert reflector["kubernetes_job"]["command"][-1] == "reflection"
