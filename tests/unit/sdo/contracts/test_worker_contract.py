"""The worker envelope is independent of any particular agent implementation."""

from google.protobuf.any_pb2 import Any

from sdo.contracts import AgentRegistration, AgentResult, AgentTask
from sdo.contracts.proto import IncidentRequest, parse_json, to_canonical_json


def test_worker_task_roundtrip_carries_typed_request() -> None:
    request = IncidentRequest(schema_version="sdo.dev/v1alpha1", incident_id="i-1")
    payload = Any()
    payload.Pack(request)
    task = AgentTask(
        schema_version="sdo.dev/v1alpha1",
        task_id="task-1",
        agent_id="responder-a",
        task_type="incident-response",
        request=payload,
    )

    decoded = parse_json(to_canonical_json(task), AgentTask())

    unpacked = IncidentRequest()
    assert decoded.request.Unpack(unpacked)
    assert unpacked.incident_id == "i-1"


def test_registration_and_result_do_not_encode_agent_language() -> None:
    registration = AgentRegistration(
        schema_version="sdo.dev/v1alpha1",
        agent_id="external-responder",
        capabilities=[
            {
                "task_type": "incident-response",
                "request_type_url": "type.googleapis.com/sdodev.contracts.v1alpha1.IncidentRequest",
                "result_type_url": "type.googleapis.com/sdodev.contracts.v1alpha1.IncidentResult",
            }
        ],
        kubernetes_job={"image": "example/worker:1", "command": ["/worker"]},
    )
    result = AgentResult(schema_version="sdo.dev/v1alpha1", task_id="task-1", status="completed")

    assert parse_json(to_canonical_json(registration), AgentRegistration()).agent_id == "external-responder"
    assert parse_json(to_canonical_json(result), AgentResult()).status == "completed"
