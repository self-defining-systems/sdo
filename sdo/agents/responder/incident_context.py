"""Lean, complete responder facts with bounded inline delivery."""

from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path
from string import Template
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Any

from google.protobuf import json_format

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sdo.contracts import IncidentRequest


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
    if "playbook_coverage" not in request.DESCRIPTOR.fields_by_name:
        # D1 owns this typed field; remove this named integration placeholder
        # after the coverage contract is merged into the production base.
        document["playbook_coverage"] = {"availability": "pending-controller-coverage"}
    else:
        document.setdefault(
            "playbook_coverage",
            {"matches": [], "unexplained_finding_fingerprints": [item.fingerprint for item in request.findings]},
        )
    return document


def render_incident_data(request: IncidentRequest) -> str:
    """Render the complete projection; production uses prepared_incident_data for its cap."""
    document = json.dumps(incident_data_view(request), ensure_ascii=False, separators=(",", ":"))
    template = Path(__file__).with_name("prompts") / "incident_data.md"
    return Template(template.read_text(encoding="utf-8")).substitute(request_json=document)


@contextmanager
def prepared_incident_data(request: IncidentRequest) -> Iterator[str]:
    """Keep inline context below 12k; retain every overflow fact for the whole turn."""
    rendered = render_incident_data(request)
    if len(rendered) <= 12000:
        yield rendered
        return
    with NamedTemporaryFile(mode="w", encoding="utf-8", prefix="sdo-incident-", suffix=".json") as facts:
        json.dump(incident_data_view(request), facts, ensure_ascii=False)
        facts.flush()
        summary = {
            "incident_id": request.incident_id,
            "application": request.application,
            "namespace": request.namespace,
            "finding_count": len(request.findings),
        }
        template = Path(__file__).with_name("prompts") / "incident_data_overflow.md"
        yield Template(template.read_text(encoding="utf-8")).substitute(
            summary_json=json.dumps(summary, ensure_ascii=False, separators=(",", ":")), details_file=facts.name
        )
