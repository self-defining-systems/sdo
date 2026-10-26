"""Lean, complete responder facts with bounded inline delivery."""

from __future__ import annotations

import json
from contextlib import ExitStack, contextmanager
from pathlib import Path
from string import Template
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any

from google.protobuf import json_format

from sdo.agents.responder.tool_output import write_json
from sdo.agents.responder.tools_cli import command_surface

MAX_INCIDENT_SUMMARY_CHARS = 5000

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


def render_incident_data(request: IncidentRequest) -> str:
    """Render the complete projection; production uses prepared_incident_data for its cap."""
    return _render_data(incident_data_view(request))


def _render_data(document: dict[str, Any]) -> str:
    encoded = json.dumps(document, ensure_ascii=False, separators=(",", ":"))
    template = Path(__file__).with_name("prompts") / "incident_data.md"
    return Template(template.read_text(encoding="utf-8")).substitute(request_json=encoded)


def _render_summary(document: dict[str, Any], details_file: str) -> str:
    template = Path(__file__).with_name("prompts") / "incident_data_overflow.md"
    return Template(template.read_text(encoding="utf-8")).substitute(
        summary_json=json.dumps(document, ensure_ascii=False, separators=(",", ":")), details_file=details_file
    )


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
            document["memory_accepted_commit"] = request.memory_snapshot.accepted_commit
        facts = write_json(document, command="incident", incident_id=request.incident_id)
        stack.callback(facts.unlink, missing_ok=True)
        inline = {**document, "details_file": str(facts)}
        if request.prior_stages:
            inline["prior_stages"] = prior_stage_summary(list(request.prior_stages))
        rendered = _render_data(inline)
        if len(rendered) <= MAX_INCIDENT_SUMMARY_CHARS:
            yield rendered
            return
        summary = dict(inline)
        summary["supplemental_fields"] = []
        # Large live evidence remains complete in facts. Its presence must not
        # hide finding identities, matched repairs or the local command surface.
        findings = document.get("findings", [])
        summary["findings"] = [
            {
                key: (value[:160] + "…" if len(value) > 160 else value) if isinstance(value, str) else value
                for key, value in finding.items()
                if key != "evidence"
            }
            for finding in findings[:8]
        ]
        summary["finding_count"] = len(findings)
        summary["finding_details_file"] = str(facts)
        if len(findings) > 8:
            summary["findings_omitted"] = len(findings) - 8
        for name in (
            "intent",
            "initial_program_runs",
            "relevant_outcomes",
            "state_changes",
            "latest_detector_evaluations",
            "observation_generation",
            "playbook_documents",
            "namespace_onboarding",
        ):
            if name in summary:
                summary.pop(name)
                summary["supplemental_fields"].append(name)
            rendered = _render_summary(summary, str(facts))
            if len(rendered) <= MAX_INCIDENT_SUMMARY_CHARS:
                yield rendered
                return
        # Exceptionally large findings or coverage still retain every byte in
        # facts; never present a truncated finding as complete evidence.
        summary = {
            "incident_id": request.incident_id,
            "application": request.application,
            "namespace": request.namespace,
            "finding_count": len(request.findings),
            "supplemental_fields": sorted(document),
            "findings": [
                {"fingerprint": item.fingerprint[:160], "summary": item.summary[:160]} for item in request.findings[:4]
            ],
            "findings_omitted": max(0, len(request.findings) - 4),
            "playbook_coverage": {
                "matches": [
                    {
                        "playbook_id": match.playbook_id[:100],
                        "can_mitigate": match.can_mitigate,
                        "mitigation_selection": match.mitigation_selection[:100],
                    }
                    for match in request.playbook_coverage.matches[:4]
                ],
                "matches_omitted": max(0, len(request.playbook_coverage.matches) - 4),
                "unexplained_finding_fingerprints": [
                    item[:160] for item in document["playbook_coverage"]["unexplained_finding_fingerprints"][:4]
                ],
                "unexplained_findings_omitted": max(
                    0, len(document["playbook_coverage"]["unexplained_finding_fingerprints"]) - 4
                ),
            },
            "incident_tools": document["incident_tools"],
        }
        if request.prior_stages:
            summary["prior_stages"] = [
                {
                    "stage": report.stage[:100],
                    "mitigations": [
                        {
                            "playbook_id": attempt.playbook_id[:100],
                            "result": attempt.result,
                            "skip_reason": attempt.skip_reason,
                            "error": attempt.error[:160],
                        }
                        for attempt in report.mitigations[:4]
                    ],
                    "mitigations_omitted": max(0, len(report.mitigations) - 4),
                    "remaining_finding_fingerprints": [
                        item[:160] for item in report.remaining_finding_fingerprints[:4]
                    ],
                    "remaining_findings_omitted": max(0, len(report.remaining_finding_fingerprints) - 4),
                }
                for report in request.prior_stages[:2]
            ]
        rendered = _render_summary(summary, str(facts))
        # Keep a visible action/result from each stage alongside current work.
        # Omitted counts point to complete facts instead of hiding truncation.
        lists = [
            (stage, key, omitted)
            for stage in summary.get("prior_stages", [])
            for key, omitted in (
                ("mitigations", "mitigations_omitted"),
                ("remaining_finding_fingerprints", "remaining_findings_omitted"),
            )
        ]
        lists += [
            (summary, "findings", "findings_omitted"),
            (summary["playbook_coverage"], "matches", "matches_omitted"),
            (summary["playbook_coverage"], "unexplained_finding_fingerprints", "unexplained_findings_omitted"),
        ]
        while len(rendered) > MAX_INCIDENT_SUMMARY_CHARS:
            candidate = next(((owner, key, omitted) for owner, key, omitted in lists if len(owner[key]) > 1), None)
            if candidate is None:
                break
            owner, key, omitted = candidate
            owner[key].pop()
            owner[omitted] += 1
            rendered = _render_summary(summary, str(facts))
        yield rendered
