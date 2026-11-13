"""The investigator session: one read-only hypothesis test, started by SDO beside the responder.

An investigation is the responder's own task with ``investigator_brief`` set. It reuses the worker, Job and
session machinery, but it runs its own role (skill set, session directory, tool allow-list in Go) and its
result carries only report text in ``IncidentResult.investigation_report``. Go keeps that text for display and
parses only its first line; nothing here is read by closure, escalation or learning.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from google.protobuf import json_format

from libs.agent_cli.provider_capacity import ProviderCapacity, RateLimitPolicy
from libs.agent_cli.structured import AGENT_PROVIDERS, StructuredTurnError, turn_usage
from libs.agent_cli.usage_checkpoint import UsageCheckpoint
from sdo.agents.prompt_templates import named_prompt
from sdo.agents.prompt_templates import run_agent_turn as run_structured_turn
from sdo.agents.provider import agent_effort, resolve_agent
from sdo.agents.replay import selected_skills_root
from sdo.agents.responder.credentials import SKILLS_ROOT
from sdo.agents.responder.result_output import validate_incident_result
from sdo.agents.responder.session_turn import session_turn_options
from sdo.contracts import SCHEMA_VERSION, IncidentResult

if TYPE_CHECKING:
    from sdo.contracts import IncidentRequest

#: An investigator never waits long for provider capacity: it fails soft rather than queue ahead of a responder.
_CAPACITY_WAIT_SECONDS = 20.0
_REPORT_SCHEMA: dict[str, object] = {
    "type": "object",
    "properties": {"report": {"type": "string"}},
    "required": ["report"],
    "additionalProperties": False,
}


class InvestigationError(RuntimeError):
    pass


def investigation_prompt(request: IncidentRequest) -> str:
    skills = selected_skills_root(SKILLS_ROOT)
    return named_prompt(
        "investigator.instructions",
        f"{request.investigator_brief}\nSkill file: {skills}/sdo-investigator/SKILL.md",
    )


def execute_investigation(request: IncidentRequest) -> IncidentResult:
    """Run the brief to one report; the report is the whole result."""
    if not request.investigator_brief:
        raise InvestigationError("not an investigation")
    try:
        resolved = resolve_agent("responder", "responder")
    except ValueError as exc:
        raise InvestigationError(str(exc)) from exc
    provider = resolved.provider
    if provider not in AGENT_PROVIDERS:
        raise InvestigationError(f"unsupported agent provider {provider!r}")
    options = session_turn_options(provider, Path(request.repository_worktree))
    options["output_schema"] = _REPORT_SCHEMA
    options["environment"] = {**(options.get("environment") or {}), "SDO_INCIDENT_ID": request.incident_id}
    options["provider_capacity"] = ProviderCapacity(
        environment={**os.environ, **options["environment"]},
        policy=RateLimitPolicy(
            initial_delay_seconds=5.0, max_delay_seconds=10.0, max_wait_seconds=_CAPACITY_WAIT_SECONDS
        ),
    )
    started = datetime.now(UTC).isoformat()
    usage_directory = os.environ.get("SDO_RESPONDER_USAGE_DIR", "")
    checkpoint = (
        UsageCheckpoint(
            Path(usage_directory),
            request.incident_id,
            os.environ.get("SDO_RESPONDER_TASK_ID", request.incident_id),
            "responder",
        )
        if usage_directory
        else None
    )
    try:
        completed = run_structured_turn(
            provider,  # type: ignore[arg-type]
            investigation_prompt(request),  # type: ignore[arg-type]
            cwd=request.repository_worktree,
            access="danger-full-access",
            model=resolved.model,
            reasoning_effort=agent_effort(),
            on_usage=checkpoint.publish if checkpoint is not None else None,
            **options,
        )
    except StructuredTurnError as exc:
        raise InvestigationError(f"{provider} investigator failed: {exc}") from exc
    document = json.loads(completed.output_json)
    report = str(document.get("report", "")).strip() if isinstance(document, dict) else ""
    if not report:
        raise InvestigationError("investigator returned no report")
    finished = datetime.now(UTC).isoformat()
    result = json_format.ParseDict(
        {
            "schema_version": SCHEMA_VERSION,
            "incident_id": request.incident_id,
            "status": "completed",
            "investigation_report": report[:16000],
            "responder_session_id": completed.session_id,
            "usage": turn_usage(completed),
            "timing": {"started_at": started, "completed_at": finished},
        },
        IncidentResult(),
    )
    validate_incident_result(result)
    return result
