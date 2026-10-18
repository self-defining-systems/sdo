"""Typed worker edge for the built-in incident responder.

The operator sees only AgentTask/AgentResult ProtoJSON. This module adapts
their typed incident payloads to this agent's existing Python implementation.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, cast

from google.protobuf import json_format

from sdo.agents.responder.codex import ResponderExecutionError, execute_incident
from sdo.agents.responder.credentials import prepare_claude_home, prepare_codex_home
from sdo.agents.responder.job import KubernetesResultPublisher, write_application_kubeconfig
from sdo.contracts import (
    AgentResult,
    AgentTask,
    IncidentRequest,
    IncidentResult,
    LateFindingsPull,
    ProtoIncidentRequest,
    ProtoIncidentResult,
    parse_json,
    to_canonical_json,
)

if TYPE_CHECKING:
    from collections.abc import Callable

TASK_TYPE = "incident-response"
AGENT_ID = "sdo-responder"
DEFAULT_TASK_PATH = Path("/sdo/request/agent-task.json")
DEFAULT_LATE_FINDINGS_RECEIPTS = Path("/tmp/sdo-late-findings-pulls.jsonl")


class ResultPublisher(Protocol):
    def publish_payload(self, name: str, key: str, payload: str) -> None: ...


def _late_findings_pull(incident_id: str) -> LateFindingsPull | None:
    path = Path(os.environ.get("SDO_LATE_FINDINGS_RECEIPTS_PATH", str(DEFAULT_LATE_FINDINGS_RECEIPTS)))
    try:
        size = path.stat().st_size
    except FileNotFoundError:
        return None
    if size > 256_000:
        raise ValueError("late-findings receipt file exceeds the worker limit")
    try:
        rows = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError) as exc:
        raise ValueError(f"late-findings receipt could not be read: {exc}") from exc
    if len(rows) > 32:
        raise ValueError("late-findings receipt count exceeds the worker limit")
    detector_ids: set[str] = set()
    playbook_ids: set[str] = set()
    last_digest = ""
    for line in rows:
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"late-findings receipt is malformed JSON: {exc}") from exc
        if not isinstance(row, dict) or set(row) != {"incident_id", "detector_ids", "playbook_ids", "report_digest"}:
            raise ValueError("late-findings receipt has an invalid shape")
        if row["incident_id"] != incident_id:
            raise ValueError("late-findings receipt names another incident")
        for field, collected in (("detector_ids", detector_ids), ("playbook_ids", playbook_ids)):
            values = row[field]
            if not isinstance(values, list) or any(
                not isinstance(value, str) or not value or "/" in value for value in values
            ):
                raise ValueError(f"late-findings receipt has invalid {field}")
            collected.update(values)
        digest = row["report_digest"]
        if not isinstance(digest, str) or len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest):
            raise ValueError("late-findings receipt has invalid report digest")
        last_digest = digest
    if not rows:
        return None
    return LateFindingsPull(
        incident_id=incident_id,
        pull_count=len(rows),
        detector_ids=sorted(detector_ids),
        playbook_ids=sorted(playbook_ids),
        report_digest=last_digest,
    )


def run_worker_task(
    task_path: Path,
    result_name: str,
    publisher: ResultPublisher,
    *,
    executor: Callable[[IncidentRequest], IncidentResult] = execute_incident,
) -> AgentResult:
    task = cast("AgentTask", parse_json(task_path.read_text(encoding="utf-8"), AgentTask()))
    if task.schema_version != "sdo.dev/v1alpha1" or not task.task_id:
        raise ValueError("worker task has invalid version or task ID")
    if task.agent_id != AGENT_ID or task.task_type != TASK_TYPE:
        raise ValueError(f"worker cannot handle agent {task.agent_id!r} task type {task.task_type!r}")
    typed_request = ProtoIncidentRequest()
    if not task.request.Unpack(typed_request):
        raise ValueError("worker task request is not an IncidentRequest")
    # The existing responder model is the agent's internal representation.
    request = IncidentRequest.model_validate_json(
        json_format.MessageToJson(
            typed_request, preserving_proto_field_name=True, always_print_fields_with_no_presence=True
        )
    )
    if request.memory_snapshot is None:
        raise ValueError("incident worker requires a logical memory snapshot")
    if request.memory_snapshot.accepted_commit != request.repository_base_commit:
        raise ValueError("incident memory snapshot is not bound to accepted base commit")
    if task.workspace.proposal_id and task.workspace.checkout_path != request.repository_worktree:
        raise ValueError("worker task checkout does not match incident request")
    kubeconfig = write_application_kubeconfig(request.namespace)
    if kubeconfig is not None:
        os.environ["KUBECONFIG"] = str(kubeconfig)
    result = executor(request)
    if result.incident_id != request.incident_id:
        raise ResponderExecutionError("responder result incident_id does not match request")
    for playbook in result.applied_playbooks:
        if "/" in playbook.playbook_id or any("/" in script for script in playbook.script_names):
            raise ResponderExecutionError("native responder must return logical playbook and script names")
    if any("/" in artifact_id for artifact_id in result.proposed_artifact_ids):
        raise ResponderExecutionError("native responder must return logical artifact IDs")
    late_findings_pull = _late_findings_pull(request.incident_id)
    if late_findings_pull is not None:
        result = result.model_copy(update={"late_findings_pull": late_findings_pull})
    typed_result = json_format.Parse(result.model_dump_json(exclude_none=True), ProtoIncidentResult())
    envelope = AgentResult(
        schema_version="sdo.dev/v1alpha1",
        task_id=task.task_id,
        status=result.status.value,
        proposal_id=task.workspace.proposal_id,
    )
    envelope.response.Pack(typed_result)
    publisher.publish_payload(result_name, "agent-result.json", to_canonical_json(envelope))
    return envelope


def main() -> int:
    namespace = os.environ.get("SDO_NAMESPACE", "")
    result_name = os.environ.get("SDO_RESULT_CONFIGMAP", "")
    if not namespace or not result_name:
        print("SDO_NAMESPACE and SDO_RESULT_CONFIGMAP are required", file=sys.stderr)
        return 2
    try:
        prepare_codex_home()
        prepare_claude_home()
        run_worker_task(
            Path(os.environ.get("SDO_TASK_PATH", str(DEFAULT_TASK_PATH))),
            result_name,
            KubernetesResultPublisher(namespace=namespace),
        )
    except (OSError, ValueError, RuntimeError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
