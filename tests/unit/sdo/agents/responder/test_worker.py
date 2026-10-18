import json
from pathlib import Path

import pytest
from google.protobuf import json_format
from google.protobuf.any_pb2 import Any

from sdo.agents.responder.worker import run_worker_task
from sdo.agents.views import (
    IncidentRequest as ModelRequest,
)
from sdo.agents.views import (
    IncidentResult as ModelResult,
)
from sdo.contracts import AgentResult, AgentTask
from sdo.contracts.proto import IncidentRequest as ProtoRequest
from sdo.contracts.proto import IncidentResult as ProtoResult
from sdo.contracts.proto import parse_json, to_canonical_json

FIXTURES = Path(__file__).resolve().parents[4] / "fixtures" / "sdo" / "contracts"


class RecordingPublisher:
    def __init__(self) -> None:
        self.published: tuple[str, str, str] | None = None

    def publish_payload(self, name: str, key: str, payload: str) -> None:
        self.published = (name, key, payload)


def _task(path: Path, *, task_type: str = "incident-response") -> AgentTask:
    logical = ModelRequest.model_validate_json((FIXTURES / "incident_request.json").read_text())
    raw = logical.model_dump(mode="json")
    raw["memory_snapshot"] = {
        "accepted_commit": logical.repository_base_commit,
        "goal_markdown": "Example objective",
        "architecture_markdown": "Example architecture",
    }
    for finding in raw["findings"]:
        finding["playbook_ids"] = ["missing-configmap"]
    for playbook in raw["surfaced_playbooks"]:
        playbook["playbook_id"] = "missing-configmap"
    request = json_format.ParseDict(raw, ProtoRequest())
    packed = Any()
    packed.Pack(request)
    task = AgentTask(
        schema_version="sdo.dev/v1alpha1",
        task_id="task-1",
        agent_id="sdo-responder",
        task_type=task_type,
        request=packed,
        workspace={"proposal_id": "proposal-1", "checkout_path": request.repository_worktree},
    )
    path.write_text(to_canonical_json(task), encoding="utf-8")
    return task


def test_worker_uses_typed_envelope_and_returns_typed_response(tmp_path: Path) -> None:
    path = tmp_path / "agent-task.json"
    _task(path)
    legacy = ModelResult.model_validate_json((FIXTURES / "incident_result.json").read_text())
    expected = legacy.model_copy(
        update={
            "applied_playbooks": [
                item.model_copy(
                    update={
                        "playbook_id": "missing-configmap",
                        "script_names": ["verify.sh"],
                    }
                )
                for item in legacy.applied_playbooks
            ],
            "proposed_artifact_ids": [],
        }
    )
    publisher = RecordingPublisher()

    run_worker_task(path, "result-map", publisher, executor=lambda _: expected)

    assert publisher.published is not None
    name, key, payload = publisher.published
    assert (name, key) == ("result-map", "agent-result.json")
    envelope = parse_json(payload, AgentResult())
    assert (envelope.task_id, envelope.proposal_id, envelope.status) == ("task-1", "proposal-1", "completed")
    result = ProtoResult()
    assert envelope.response.Unpack(result)
    assert result.incident_id == expected.incident_id


def test_worker_rejects_unregistered_task_type(tmp_path: Path) -> None:
    path = tmp_path / "agent-task.json"
    _task(path, task_type="source-deployment")
    publisher = RecordingPublisher()

    with pytest.raises(ValueError, match="task type"):
        run_worker_task(path, "result-map", publisher, executor=lambda _: None)

    assert publisher.published is None


def test_native_worker_rejects_physical_memory_paths_in_result(tmp_path: Path) -> None:
    path = tmp_path / "agent-task.json"
    _task(path)
    legacy = ModelResult.model_validate_json((FIXTURES / "incident_result.json").read_text())
    legacy = legacy.model_copy(
        update={
            "applied_playbooks": [legacy.applied_playbooks[0].model_copy(update={"playbook_id": ".sdo/playbooks/old"})]
        }
    )
    publisher = RecordingPublisher()

    with pytest.raises(RuntimeError, match="logical playbook"):
        run_worker_task(path, "result-map", publisher, executor=lambda _: legacy)

    assert publisher.published is None


def test_native_worker_requires_commit_bound_logical_memory(tmp_path: Path) -> None:
    path = tmp_path / "agent-task.json"
    task = _task(path)
    request = ProtoRequest()
    assert task.request.Unpack(request)
    request.ClearField("memory_snapshot")
    task.request.Pack(request)
    path.write_text(to_canonical_json(task), encoding="utf-8")
    publisher = RecordingPublisher()

    with pytest.raises(ValueError, match="logical memory snapshot"):
        run_worker_task(path, "result-map", publisher, executor=lambda _: None)

    assert publisher.published is None


def test_worker_folds_go_late_findings_receipts_into_typed_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "agent-task.json"
    _task(path)
    result = ModelResult.model_validate_json((FIXTURES / "incident_result.json").read_text())
    receipts = tmp_path / "late-findings.jsonl"
    receipts.write_text(
        "\n".join(
            json.dumps(row)
            for row in (
                {
                    "incident_id": result.incident_id,
                    "detector_ids": ["detector-a"],
                    "playbook_ids": ["playbook-a"],
                    "report_digest": "a" * 64,
                },
                {
                    "incident_id": result.incident_id,
                    "detector_ids": ["detector-b", "detector-a"],
                    "playbook_ids": ["playbook-b"],
                    "report_digest": "b" * 64,
                },
            )
        )
        + "\n"
    )
    monkeypatch.setenv("SDO_LATE_FINDINGS_RECEIPTS_PATH", str(receipts))
    publisher = RecordingPublisher()

    run_worker_task(path, "result-map", publisher, executor=lambda _: result)

    assert publisher.published is not None
    envelope = parse_json(publisher.published[2], AgentResult())
    typed = ProtoResult()
    assert envelope.response.Unpack(typed)
    assert typed.late_findings_pull.incident_id == result.incident_id
    assert typed.late_findings_pull.pull_count == 2
    assert list(typed.late_findings_pull.detector_ids) == ["detector-a", "detector-b"]
    assert list(typed.late_findings_pull.playbook_ids) == ["playbook-a", "playbook-b"]
    assert typed.late_findings_pull.report_digest == "b" * 64


def test_worker_rejects_invalid_go_late_findings_receipt(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "agent-task.json"
    _task(path)
    result = ModelResult.model_validate_json((FIXTURES / "incident_result.json").read_text())
    receipts = tmp_path / "late-findings.jsonl"
    receipts.write_text(
        json.dumps({"incident_id": result.incident_id, "detector_ids": [], "playbook_ids": [], "report_digest": "bad"})
        + "\n"
    )
    monkeypatch.setenv("SDO_LATE_FINDINGS_RECEIPTS_PATH", str(receipts))
    publisher = RecordingPublisher()

    with pytest.raises(ValueError, match="late-findings receipt"):
        run_worker_task(path, "result-map", publisher, executor=lambda _: result)

    assert publisher.published is None
