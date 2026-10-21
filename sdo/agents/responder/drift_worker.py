"""Read-only responder capability for unexplained live drift."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, cast

from google.protobuf import json_format

from libs.agent_cli.structured import AGENT_PROVIDERS, StructuredTurnError, run_structured_turn, turn_usage
from sdo.agents.responder.credentials import prepare_claude_home, prepare_codex_home
from sdo.agents.responder.job import KubernetesResultPublisher, write_application_kubeconfig
from sdo.contracts import (
    AgentResult,
    AgentTask,
    DriftInvestigationResult,
    DriftInvestigationTask,
    parse_json,
    to_canonical_json,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from agentshim import CommandExecutor

AGENT_ID = "sdo-drift-investigator"
TASK_TYPE = "drift-investigation"
DEFAULT_TASK_PATH = Path("/sdo/request/agent-task.json")


class ResultPublisher(Protocol):
    def publish_payload(self, name: str, key: str, payload: str) -> None: ...


def _result_schema() -> dict[str, object]:
    return {
        "type": "object",
        "properties": {
            "events": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "event_index": {"type": "integer", "minimum": 0},
                        "verdict": {"type": "string", "enum": ["bug", "benign", "unsure"]},
                        "reason": {"type": "string"},
                        "evidence": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "kind": {
                                        "type": "string",
                                        "enum": ["source_contradiction", "violated_relation", "observed_impact"],
                                    },
                                    "observation": {"type": "string"},
                                },
                                "required": ["kind", "observation"],
                                "additionalProperties": False,
                            },
                        },
                        "mitigation_request": {"type": "string"},
                    },
                    "required": ["event_index", "verdict", "reason", "evidence", "mitigation_request"],
                    "additionalProperties": False,
                },
            }
        },
        "required": ["events"],
        "additionalProperties": False,
    }


def investigate_drift(
    request: DriftInvestigationTask, *, executor: CommandExecutor | None = None
) -> DriftInvestigationResult:
    provider = os.getenv("SDO_AGENT_PROVIDER", "codex").strip().lower()
    if provider not in AGENT_PROVIDERS:
        raise ValueError(f"unsupported drift investigator provider {provider!r}")
    evidence = [json_format.MessageToDict(event, preserving_proto_field_name=True) for event in request.events]
    memory = json_format.MessageToDict(request.memory_snapshot, preserving_proto_field_name=True)
    prompt = (
        "Investigate only these specific live changes. Return one verdict per event: bug, benign, or unsure. "
        "A bug needs both a contradiction with the accepted source and either a violated resource relation "
        "or observed impact. Give a concrete observation for each evidence item. If evidence is missing, "
        "return unsure. Do not change resources, files, or source. Use only read-only commands: "
        "sdo status, sdo diagnose, kubectl get, and reading the source checkout. "
        "Do not run sdo apply, kubectl patch/apply/delete, or any other write command. "
        f"Accepted commit: {request.accepted_commit}; namespace: {request.namespace}; "
        f"capture reference: {request.capture_id} ({request.capture_sha256}) at /sdo/session/corpus/open@detect.json. "
        f"Logical memory snapshot: {json.dumps(memory, sort_keys=True)[:12000]}. "
        f"Events: {json.dumps(evidence, sort_keys=True)}"
    )
    try:
        turn = run_structured_turn(
            provider,
            prompt,
            output_schema=_result_schema(),
            cwd=request.repository_worktree,
            access="read-only",
            model=os.getenv("SDO_RESPONDER_MODEL") or None,
            executor=executor,
        )
        result = json_format.ParseDict(json.loads(turn.output_json), DriftInvestigationResult())
    except (StructuredTurnError, ValueError) as exc:
        raise ValueError(f"invalid drift investigation result: {exc}") from exc
    usage = turn_usage(turn)
    result.input_tokens = int(usage["input_tokens"])
    result.output_tokens = int(usage["output_tokens"])
    return result


def run_drift_task(
    task_path: Path,
    result_name: str,
    publisher: ResultPublisher,
    *,
    investigator: Callable[[DriftInvestigationTask], DriftInvestigationResult] = investigate_drift,
) -> AgentResult:
    task = cast("AgentTask", parse_json(task_path.read_text(encoding="utf-8"), AgentTask()))
    if task.schema_version != "sdo.dev/v1alpha1" or task.agent_id != AGENT_ID or task.task_type != TASK_TYPE:
        raise ValueError("drift worker task has wrong version, agent, or task type")
    request = DriftInvestigationTask()
    if not task.request.Unpack(request) or request.batch_id != task.task_id:
        raise ValueError("drift worker requires a matching DriftInvestigationTask")
    if not request.HasField("memory_snapshot") or request.memory_snapshot.accepted_commit != request.accepted_commit:
        raise ValueError("drift worker memory is not bound to accepted commit")
    if (
        task.workspace.checkout_path != request.repository_worktree
        or task.workspace.base_commit != request.accepted_commit
    ):
        raise ValueError("drift worker checkout differs from request")
    kubeconfig = write_application_kubeconfig(request.namespace)
    if kubeconfig is not None:
        os.environ["KUBECONFIG"] = str(kubeconfig)
    result = investigator(request)
    envelope = AgentResult(
        schema_version="sdo.dev/v1alpha1",
        task_id=task.task_id,
        status="completed",
        proposal_id=task.workspace.proposal_id,
    )
    envelope.response.Pack(result)
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
        run_drift_task(
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
