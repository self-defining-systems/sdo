from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from google.protobuf import json_format

from libs.agent_cli.structured import AGENT_PROVIDERS, StructuredTurnError, run_structured_turn, turn_usage
from libs.agent_cli.usage_checkpoint import UsageCheckpoint
from sdo.agents.code_paths import agent_guidance
from sdo.agents.prompt_templates import RenderedPrompt, named_prompt, render_prompt
from sdo.agents.provider import MODEL_ENV, resolve_agent
from sdo.agents.responder import recorded_actions
from sdo.agents.responder.continuation import continuation_prompt
from sdo.agents.responder.credentials import SKILLS_ROOT
from sdo.agents.responder.incident_context import prepared_incident_data
from sdo.agents.responder.reflection import INCIDENT_REASONING_EFFORT
from sdo.agents.responder.result_output import parse_incident_result
from sdo.agents.responder.session_recovery import SessionRecoveryError, open_recovery
from sdo.agents.responder.session_turn import (
    WrongBranchError,
    branch_correction,
    branch_instruction,
    session_turn_options,
    unwrap_branch,
)
from sdo.contracts import (
    IncidentRequest,
    IncidentResult,
    model_output_schema,
)

if TYPE_CHECKING:
    from agentshim import CommandExecutor


class ResponderExecutionError(RuntimeError):
    pass


def main() -> int:
    try:
        request = json_format.Parse(sys.stdin.read(), IncidentRequest())
    except (ValueError, json_format.ParseError) as exc:
        print(f"invalid incident request: {exc}", file=sys.stderr)
        return 2

    try:
        result = execute_incident(request)
    except ResponderExecutionError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json_format.MessageToJson(result, preserving_proto_field_name=True))
    return 0


def execute_incident(
    request: IncidentRequest,
    *,
    model: str | None = None,
    provider: str | None = None,
    executor: CommandExecutor | None = None,
) -> IncidentResult:
    with prepared_incident_data(request) as data:
        return _execute_incident(request, incident_data=data, model=model, provider=provider, executor=executor)


def _execute_incident(
    request: IncidentRequest,
    *,
    incident_data: str,
    model: str | None = None,
    provider: str | None = None,
    executor: CommandExecutor | None = None,
) -> IncidentResult:
    if provider is None:
        try:
            resolved = resolve_agent("responder", "responder")
        except ValueError as exc:
            raise ResponderExecutionError(str(exc)) from exc
        selected_provider, model = resolved.provider, model or resolved.model
    else:
        selected_provider = provider.strip().lower()
        if selected_provider not in AGENT_PROVIDERS:
            raise ResponderExecutionError(f"unsupported agent provider {selected_provider!r}")
        model = model or os.getenv(MODEL_ENV["responder"]) or None
    prompt = _responder_prompt(request, incident_data=incident_data)
    try:
        recovery = open_recovery(request.incident_id, selected_provider)
        resume_session_id = recovery.resume_session_id() if recovery is not None else None
    except SessionRecoveryError as exc:
        raise ResponderExecutionError(str(exc)) from exc
    if resume_session_id is None:
        recorded_actions.reset()
    if recovery is not None:
        prompt += recovery.recovered_prompt()
    continuation = os.environ.get("SDO_RESPONSE_CONTINUATION", "")
    if continuation:
        if recovery is None or resume_session_id is None:
            raise ResponderExecutionError("controller continuation requires the existing provider session")
        try:
            prompt = continuation_prompt(continuation)
        except ValueError as exc:
            raise ResponderExecutionError(f"invalid controller continuation: {exc}") from exc
    corrected_result = False
    usage: dict[str, int | float] = {}
    options = session_turn_options(selected_provider, Path(request.repository_worktree))
    options["environment"] = {**(options.get("environment") or {}), "SDO_INCIDENT_ID": request.incident_id}
    while True:
        checkpoint_directory = os.environ.get("SDO_RESPONDER_USAGE_DIR", "")
        checkpoint = (
            UsageCheckpoint(
                Path(checkpoint_directory),
                request.incident_id,
                os.environ.get("SDO_RESPONDER_TASK_ID", request.incident_id),
                "responder",
            )
            if checkpoint_directory
            else None
        )
        try:
            completed = run_structured_turn(
                selected_provider,  # type: ignore[arg-type]
                prompt,
                cwd=request.repository_worktree,
                access="danger-full-access",
                model=model,
                reasoning_effort=INCIDENT_REASONING_EFFORT,
                timeout_seconds=recovery.remaining_seconds() if recovery is not None else None,
                resume_session_id=resume_session_id,
                executor=executor,
                on_session_started=recovery.checkpoint if recovery is not None else None,
                on_usage=checkpoint.publish if checkpoint is not None else None,
                **options,
            )
        except SessionRecoveryError as exc:
            raise ResponderExecutionError(str(exc)) from exc
        except StructuredTurnError as exc:
            raise ResponderExecutionError(f"{selected_provider} responder failed: {exc}") from exc
        for name, value in turn_usage(completed).items():
            usage[name] = usage.get(name, 0) + value
        try:
            result = parse_incident_result(
                unwrap_branch(completed.output_json, "incident_result"),
                usage,
                incident_id=request.incident_id,
            )
            break
        except WrongBranchError as exc:
            if corrected_result:
                raise ResponderExecutionError(f"invalid {selected_provider} incident result: {exc}") from exc
            corrected_result = True
            resume_session_id = completed.session_id
            prompt = branch_correction("incident_result")
        except ValueError as exc:
            if not corrected_result:
                corrected_result = True
                resume_session_id = completed.session_id
                prompt = render_prompt(
                    "responder",
                    "result_correction",
                    part="session.instructions",
                    result_type=type(exc).__name__,
                    diagnostic=str(exc)[:1000],
                )
                continue
            raise ResponderExecutionError(f"invalid {selected_provider} incident result: {exc}") from exc
    try:
        if recovery is not None:
            recovery.acknowledge_reports()
    except SessionRecoveryError as exc:
        raise ResponderExecutionError(str(exc)) from exc
    try:
        recorded_actions.merge_into(result)
    except recorded_actions.RecordedActionsError as exc:
        raise ResponderExecutionError(f"invalid recorded repair actions: {exc}") from exc
    if not result.HasField("responder_session_id"):
        result.responder_session_id = completed.session_id
    return result


#: Proto fields the model does not supply: worker accounting and session, the controller's
#: program-loop state, and the detector states and artifact IDs the responder never reports.
_MODEL_OMITS: dict[str, tuple[str, ...]] = {
    "sdodev.contracts.v1alpha1.IncidentResult": (
        "schema_version",
        "proposed_artifact_ids",
        "usage",
        "responder_session_id",
        "error",
        "late_findings_pull",
        "repair_actions",
    ),
    "sdodev.contracts.v1alpha1.AppliedPlaybook": ("parameter_bindings", "script_names"),
}


def _incident_result_schema() -> dict[str, object]:
    """The model-facing schema of ``IncidentResult``, generated from the proto the Go loader reads."""

    return model_output_schema(IncidentResult.DESCRIPTOR, omit=_MODEL_OMITS)


LATE_FINDINGS_ENV = "SDO_LATE_FINDINGS"
LATE_FINDINGS_MODES = ("off", "pull")
LATE_FINDINGS_COMMAND = "sdo-operator late-findings"


def late_findings_guidance(request: IncidentRequest, *, mode: str | None = None) -> str:
    """Pull-before-act paragraph; empty unless the ``pull`` mode is selected (default: the environment)."""

    selected = (os.getenv(LATE_FINDINGS_ENV, "") if mode is None else mode).strip() or "off"
    if selected not in LATE_FINDINGS_MODES:
        raise ValueError(f"late-findings mode must be one of {', '.join(LATE_FINDINGS_MODES)}: {selected!r}")
    if selected == "off":
        return ""
    command = render_prompt(
        "responder",
        "late_findings_guidance_01",
        part="responder.instructions",
        late_findings_command=LATE_FINDINGS_COMMAND,
        incident_id=request.incident_id,
    )
    return render_prompt("responder", "late_findings_guidance_02", part="responder.instructions", command=command)


def _responder_prompt(request: IncidentRequest, *, incident_data: str) -> RenderedPrompt:
    if not request.HasField("memory_snapshot"):
        raise ValueError("incident responder requires a logical memory snapshot")
    prompt = render_prompt(
        "responder",
        "incident_shell" if agent_guidance() == "shell" else "incident_response_instructions",
        part="responder.instructions",
        incident_id=request.incident_id,
        skills_root=SKILLS_ROOT,
        repair_policy=request.repair_policy,
        branch_instruction=branch_instruction("incident_result"),
    )
    prompt += late_findings_guidance(request)
    extra = os.getenv("SDO_RESPONDER_EXTRA_INSTRUCTIONS", "").strip()
    if extra:
        prompt += named_prompt(
            "responder.additional_context",
            render_prompt("responder", "incident_01", part="responder.additional_context", extra_instructions=extra),
        )
    return prompt + named_prompt("responder.incident_data", incident_data)


if __name__ == "__main__":
    raise SystemExit(main())
