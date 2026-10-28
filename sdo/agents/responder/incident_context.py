"""Lean, complete responder facts with bounded inline delivery."""

from __future__ import annotations

import json
from contextlib import ExitStack, contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any

from google.protobuf import json_format

from sdo.agents.context_output import budget_for
from sdo.agents.prompt_templates import render_prompt
from sdo.agents.responder.reflection_evidence import playbook_description
from sdo.agents.responder.tool_output import write_json
from sdo.agents.responder.tools_cli import command_surface

MAX_INCIDENT_SUMMARY_CHARS = budget_for("responder.incident_data")

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sdo.contracts import IncidentRequest, StageReport


def incident_data_view(request: IncidentRequest) -> dict[str, Any]:
    """Retain actionable evidence once, excluding transport state and memory bodies."""
    document = json_format.MessageToDict(request, preserving_proto_field_name=True)
    for name in ("schema_version", "memory_snapshot", "detector_history", "cancellation_token", "prepared_agent_task"):
        document.pop(name, None)
    snapshot = request.memory_snapshot
    document["intent"] = {"goal": snapshot.goal_markdown, "architecture": snapshot.architecture_markdown}
    document["available_playbook_ids"] = sorted({item.id for item in snapshot.playbooks})
    latest = {}
    for evaluation in sorted(request.detector_history, key=lambda item: item.evaluated_at.ToNanoseconds()):
        latest[evaluation.detector_id] = evaluation
    document["latest_detector_evaluations"] = [
        json_format.MessageToDict(latest[detector_id], preserving_proto_field_name=True)
        for detector_id in sorted(latest)
    ]
    runs = document.get("initial_program_runs", [])
    actionable = []
    for run in runs:
        if run.get("verdict", {}).get("result") == "RESULT_NO_MATCH":
            continue
        capability = run.get("can_mitigate")
        run["capability"] = (
            "unknown" if capability is None else "diagnosis-and-mitigation" if capability else "diagnosis-only"
        )
        actionable.append(run)
    if runs:
        document["initial_program_runs"] = actionable
        document["omitted_no_match_program_runs"] = len(runs) - len(actionable)
    if request.HasField("playbook_coverage"):
        document["playbook_coverage"] = json_format.MessageToDict(
            request.playbook_coverage,
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=True,
        )
    else:
        document.setdefault(
            "playbook_coverage",
            {"matches": [], "unexplained_finding_fingerprints": [item.fingerprint for item in request.findings]},
        )
    return document


def prior_stage_summary(reports: list[StageReport]) -> list[dict[str, Any]]:
    """Project typed controller evidence without repeating executable plan bodies."""
    summaries = []
    for report in reports:
        stage = json_format.MessageToDict(report, preserving_proto_field_name=True)
        mitigations = []
        for attempt in report.mitigations:
            item = json_format.MessageToDict(attempt, preserving_proto_field_name=True)
            receipt = item.get("apply_receipt")
            if receipt is not None:
                item["apply_receipt"] = {
                    key: receipt[key]
                    for key in ("application_id", "applied", "rolled_back", "error", "rollout_pending")
                    if key in receipt
                }
            diagnosis = item.get("post_apply_diagnose")
            if diagnosis is not None:
                item["post_apply_diagnose"] = {
                    key: diagnosis[key]
                    for key in ("program_id", "program_revision", "verdict", "error")
                    if key in diagnosis
                }
            mitigations.append(item)
        stage["mitigations"] = mitigations
        summaries.append(stage)
    return summaries


def prior_response_summary(request: IncidentRequest) -> dict[str, Any]:
    """Keep bounded prior work visible; complete history remains in durable outcomes."""
    episodes = list(request.prior_response_progress)
    return {
        "episodes": [
            {
                "incident_id": item.incident_id[:100],
                "truncated": item.truncated,
                "successful_actions": [
                    {"action_id": action.action_id[:80], "summary": action.summary[:120], "success": True}
                    for action in item.successful_actions[:2]
                ],
                "actions_omitted": max(0, len(item.successful_actions) - 2),
                "blockers": [
                    {"reason_code": blocker.reason_code, "reason": blocker.reason[:200]}
                    for blocker in item.blockers[:1]
                ],
                "blockers_omitted": max(0, len(item.blockers) - 1),
                "error": item.error[:120],
            }
            for item in reversed(episodes[-2:])
        ],
        "episodes_omitted": sum(item.omitted_episodes for item in episodes) + max(0, len(episodes) - 2),
    }


def render_incident_data(request: IncidentRequest) -> str:
    """Render the complete projection; production uses prepared_incident_data for its cap."""
    return _render_data(incident_data_view(request))


def _render_data(document: dict[str, Any]) -> str:
    encoded = json.dumps(document, ensure_ascii=False, separators=(",", ":"))
    return render_prompt("responder", "incident_data", part="responder.incident_data", request_json=encoded)


@contextmanager
def prepared_incident_data(request: IncidentRequest) -> Iterator[str]:
    """Keep actionable facts inline and complete supplements private for the whole turn."""
    document = incident_data_view(request)
    document["incident_tools"] = command_surface()
    with ExitStack() as stack:
        if request.memory_snapshot.playbooks:
            directory = Path(stack.enter_context(TemporaryDirectory(prefix="sdo-playbooks-")))
            documents = {}
            for index, playbook in enumerate(sorted(request.memory_snapshot.playbooks, key=lambda item: item.id)):
                path = directory / f"{index}.md"
                path.touch(mode=0o600)
                path.write_text(playbook.markdown, encoding="utf-8")
                documents[playbook.id] = str(path)
            document["playbook_documents"] = documents
            document["playbook_index"] = [
                {
                    "id": playbook.id,
                    "description": playbook_description(playbook.markdown),
                    "path": documents[playbook.id],
                }
                for playbook in sorted(request.memory_snapshot.playbooks, key=lambda item: item.id)
            ]
            document["memory_accepted_commit"] = request.memory_snapshot.accepted_commit
        facts = write_json(document, command="incident", incident_id=request.incident_id)
        stack.callback(facts.unlink, missing_ok=True)
        inline = {**document, "details_file": str(facts)}
        if request.prior_response_progress:
            inline["prior_response_progress"] = prior_response_summary(request)
        if request.prior_stages:
            inline["prior_stages"] = prior_stage_summary(list(request.prior_stages))
        yield _render_data(inline)
