from pathlib import Path

import pytest

from sdo.agents.responder.drift_worker import run_drift_task
from sdo.contracts import AgentResult, DriftInvestigationResult, parse_json

FIXTURE = (
    Path(__file__).resolve().parents[4] / "fixtures" / "sdo" / "contracts" / "go" / "drift_investigation_task.json"
)


class Publisher:
    def __init__(self) -> None:
        self.payload = ""

    def publish_payload(self, _name: str, _key: str, payload: str) -> None:
        self.payload = payload


def test_drift_worker_publishes_typed_result(monkeypatch, tmp_path: Path) -> None:
    task = tmp_path / "task.json"
    task.write_bytes(FIXTURE.read_bytes())
    publisher = Publisher()
    monkeypatch.setattr("sdo.agents.responder.drift_worker.write_application_kubeconfig", lambda _: None)

    run_drift_task(
        task,
        "result",
        publisher,
        investigator=lambda _: DriftInvestigationResult(
            events=[{"event_index": 0, "verdict": "unsure", "reason": "no relation evidence"}]
        ),
    )

    envelope = parse_json(publisher.payload, AgentResult())
    result = DriftInvestigationResult()
    assert envelope.response.Unpack(result)
    assert result.events[0].verdict == "unsure"


def test_drift_worker_rejects_unbound_memory(tmp_path: Path) -> None:
    task = tmp_path / "task.json"
    task.write_text(FIXTURE.read_text().replace('"accepted_commit": "source"', '"accepted_commit": "other"', 1))

    with pytest.raises(ValueError, match="bound to accepted commit"):
        run_drift_task(task, "result", Publisher())
