from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from google.protobuf import json_format

from libs.agent_cli.structured import AGENT_PROVIDERS, StructuredTurnError, turn_usage
from libs.agent_cli.usage_checkpoint import UsageCheckpoint
from sdo.agents.code_paths import agent_guidance
from sdo.agents.prompt_templates import RenderedPrompt, named_prompt, render_prompt
from sdo.agents.prompt_templates import run_agent_turn as run_structured_turn
from sdo.agents.provider import MODEL_ENV, agent_effort, resolve_agent
from sdo.agents.replay import selected_skills_root
from sdo.agents.responder.credentials import SKILLS_ROOT
from sdo.agents.responder.incident_opening import onset, opened_on
from sdo.agents.responder.result_output import parse_incident_result
from sdo.agents.responder.session_recovery import SessionRecoveryError, open_recovery
from sdo.agents.responder.session_turn import (
    WrongBranchError,
    branch_correction,
    branch_instruction,
    session_turn_options,
    unwrap_branch,
)
from sdo.agents.telemetry_context import telemetry_context
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
    try:
        if provider is None:
            resolved = resolve_agent("responder", "responder")
            selected_provider, model, effort = resolved.provider, model or resolved.model, resolved.effort
        else:
            selected_provider = provider.strip().lower()
            if selected_provider not in AGENT_PROVIDERS:
                raise ResponderExecutionError(f"unsupported agent provider {selected_provider!r}")
            model, effort = model or os.getenv(MODEL_ENV["responder"]) or None, agent_effort()
    except ValueError as exc:
        raise ResponderExecutionError(str(exc)) from exc
    prompt = _responder_prompt(request)
    try:
        recovery = open_recovery(request.incident_id, selected_provider)
        resume_session_id = recovery.resume_session_id() if recovery is not None else None
    except SessionRecoveryError as exc:
        raise ResponderExecutionError(str(exc)) from exc
    if recovery is not None:
        prompt += recovery.recovered_prompt()
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
                reasoning_effort=effort,
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
                    diagnostic=str(exc),
                )
                continue
            raise ResponderExecutionError(f"invalid {selected_provider} incident result: {exc}") from exc
    try:
        if recovery is not None:
            recovery.acknowledge_reports()
    except SessionRecoveryError as exc:
        raise ResponderExecutionError(str(exc)) from exc
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
        "repair_actions",
    ),
    "sdodev.contracts.v1alpha1.AppliedPlaybook": ("parameter_bindings", "script_names"),
}


def _incident_result_schema() -> dict[str, object]:
    """The model-facing schema of ``IncidentResult``, generated from the proto the Go loader reads."""

    return model_output_schema(IncidentResult.DESCRIPTOR, omit=_MODEL_OMITS)


def _responder_prompt(request: IncidentRequest) -> RenderedPrompt:
    if not request.HasField("memory_snapshot"):
        raise ValueError("incident responder requires a logical memory snapshot")
    prompt = render_prompt(
        "responder",
        "incident_shell" if agent_guidance() == "shell" else "incident_prompt",
        part="responder.instructions",
        incident_id=request.incident_id,
        namespace=request.namespace,
        opened_on=opened_on(request),
        onset=onset(request),
        skills_root=selected_skills_root(SKILLS_ROOT),
        branch_instruction=branch_instruction("incident_result"),
    )
    extra = "\n".join(
        part
        for part in (
            telemetry_context(request.telemetry, "responder"),
            os.getenv("SDO_RESPONDER_EXTRA_INSTRUCTIONS", "").strip(),
        )
        if part
    )
    if extra:
        prompt += named_prompt(
            "responder.additional_context",
            render_prompt(
                "responder",
                "incident_01",
                part="responder.additional_context",
                extra_instructions=named_prompt("responder.additional_context", extra, required=False),
            ),
        )
    return prompt


render_incident_prompt = _responder_prompt


if __name__ == "__main__":
    raise SystemExit(main())
