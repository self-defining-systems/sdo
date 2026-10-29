"""Controller evidence for proactive detector learning; no benchmark inputs."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from sdo.agents.prompt_templates import named_prompt, render_prompt
from sdo.contracts import timestamp_datetime

if TYPE_CHECKING:
    from sdo.contracts import IncidentClosure, ObjectRef

# A learning hint, not admission policy or a change to generic health persistence.
NOTICEABLE_DELAY_SECONDS = 10


def _key(ref: ObjectRef) -> tuple[str, str, str, str]:
    return ref.api_version, ref.kind, ref.namespace, ref.name


def _evidence(closure: IncidentClosure) -> tuple[dict[str, Any], bool] | None:
    """Expose measured timing and linked detector candidates, leaving unknowns explicit.

    Resource watch observations establish visibility, not injector completion or
    exact write time. Playbook links identify candidates, never prove predicate
    equivalence; the author must compare the accepted detector source.
    """
    timeline = [entry for entry in closure.detector_timeline if entry.HasField("first_activated_at")]
    if not timeline:
        return None
    first_at = min(timestamp_datetime(entry.first_activated_at) for entry in timeline)
    first = [entry for entry in timeline if timestamp_datetime(entry.first_activated_at) == first_at]
    resources = {_key(ref) for cause in closure.result.confirmed_root_causes for ref in cause.resources}
    observations = [
        event
        for event in closure.request.drift_events
        if _key(event.ref) in resources and event.HasField("observed_at")
    ]
    visible = min((timestamp_datetime(event.observed_at) for event in observations), default=None)
    linked: set[str] = set()
    playbooks: set[str] = set()
    for finding in closure.request.findings:
        if _key(finding.primary_resource) in resources or any(
            _key(ref) in resources for ref in finding.related_resources
        ):
            playbooks.update(finding.playbook_ids)
            linked.add(finding.detector_id)
    candidates = [
        detector.id
        for detector in closure.request.memory_snapshot.detectors
        if detector.detector_class == "incident"
        and (detector.id in linked or playbooks.intersection(detector.possible_playbook_ids))
    ]
    delay = (first_at - visible).total_seconds() if visible is not None and visible <= first_at else None
    data: dict[str, Any] = {
        "first_detectors": [entry.detector_id for entry in first],
        "activated_at": first_at.isoformat(),
        "cause_observed_at": visible.isoformat() if visible is not None else None,
        "activation_delay_seconds": delay,
        "visibility_basis": "controller resource observation" if visible is not None else "unknown",
        "existing_cause_detectors": candidates,
        "coverage_basis": "linked candidates; compare predicates"
        if candidates
        else "no linked detector; inspect accepted memory",
    }
    late_generic = (
        delay is not None
        and delay >= NOTICEABLE_DELAY_SECONDS
        and all(entry.detector_class == "health" for entry in first)
    )
    return data, late_generic


def detector_learning_needed(closure: IncidentClosure) -> bool:
    """A validation no_change pass does not settle a measured detection gap."""
    evidence = _evidence(closure)
    return evidence is not None and evidence[1]


def detection_learning(closure: IncidentClosure) -> str:
    """Render the same facts that decide whether native no-change may omit learning."""
    evidence = _evidence(closure)
    if evidence is None:
        return ""
    data, late_generic = evidence
    return render_prompt(
        "responder",
        "detection_learning",
        part="reflection_brief.findings",
        facts=named_prompt("reflection_brief.findings", json.dumps(data, indent=2), required=False),
        request=render_prompt("responder", "detection_learning_request", part="reflection_brief.findings")
        if late_generic
        else "",
    )
