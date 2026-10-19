"""Compact incident brief for a fresh-session first reflection attempt.

A resumed reflection re-sends the responder's whole transcript on every model
request. The fresh mode instead hands a new session this brief: the closure's
live evidence (verbatim where it matters), the responder's structured result,
its shell commands when the per-turn usage log recorded them, and short
excerpts of the operational memory the incident touched. Every section has a
character budget so the brief stays under about 8K tokens.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from google.protobuf import json_format

from sdo.contracts import timestamp_datetime

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from google.protobuf.timestamp_pb2 import Timestamp

    from sdo.contracts import DetectorEvaluation, Finding, IncidentClosure, IncidentResult, ObjectRef

#: Upper bound on the whole brief: about 4 characters per token, under 8K tokens.
BRIEF_MAX_CHARS = 30_000
_FINDINGS_BUDGET = 12_000
_RESULT_BUDGET = 6_000
_COMMANDS_BUDGET = 6_000
_MEMORY_BUDGET = 9_000
_MAX_FULL_FINDINGS = 4
_EVIDENCE_MAX_CHARS = 3_000
_METADATA_MAX_CHARS = 1_200
_DETAILS_MAX_CHARS = 700
_COMMAND_MAX_CHARS = 300
_HEAD_COMMANDS = 10
_PLAYBOOK_EXCERPT_CHARS = 700
_MAX_PLAYBOOK_EXCERPTS = 4
_INDEX_EXCERPT_CHARS = 1_000
_DETECTOR_DESCRIPTION_CHARS = 500
_DETECTOR_PREDICATE_CHARS = 1_400


def _clip(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    omitted = len(text) - limit
    return f"{text[:limit]}\n[... {omitted} characters omitted ...]"


def _iso(value: Timestamp) -> str:
    return timestamp_datetime(value).isoformat()


def _plain(value: Any) -> Any:
    """JSON-native form of a ``Struct`` value: protobuf numbers are doubles, so whole ones read as integers."""

    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, dict):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_plain(item) for item in value]
    return value


def _ref(ref: ObjectRef) -> str:
    location = f"{ref.namespace}/{ref.name}" if ref.namespace else ref.name
    version = f" ({ref.api_version})" if ref.api_version else ""
    return f"{ref.kind} {location}{version}"


def _fenced(text: str) -> str:
    fence = "~~~~" if "```" in text else "```"
    return f"{fence}\n{text}\n{fence}"


def _finding(finding: Finding) -> str:
    lines = [
        f"- [{finding.severity}] {finding.detector_id} / {finding.rule_id} ({finding.status}): {finding.summary}",
        f"  primary: {_ref(finding.primary_resource)}",
    ]
    if finding.related_resources:
        lines.append("  related: " + "; ".join(_ref(ref) for ref in finding.related_resources[:10]))
    if finding.parameter_bindings:
        bindings = "; ".join(f"{role}={_ref(ref)}" for role, ref in sorted(finding.parameter_bindings.items()))
        lines.append(f"  parameter bindings: {bindings}")
    if finding.fingerprint:
        lines.append(f"  fingerprint: {finding.fingerprint}")
    if finding.playbook_ids:
        lines.append("  playbooks: " + ", ".join(finding.playbook_ids))
    lines.append("  evidence (verbatim):")
    lines.append(_fenced(_clip(finding.evidence, _EVIDENCE_MAX_CHARS)))
    if finding.metadata.fields:
        metadata = json.dumps(_plain(json_format.MessageToDict(finding.metadata)), sort_keys=True, default=str)
        lines.append("  metadata (object shapes, verbatim JSON):")
        lines.append(_fenced(_clip(metadata, _METADATA_MAX_CHARS)))
    return "\n".join(lines)


def _history(evaluations: Sequence[DetectorEvaluation]) -> list[str]:
    by_detector: dict[str, list[DetectorEvaluation]] = {}
    for evaluation in evaluations:
        by_detector.setdefault(evaluation.detector_id, []).append(evaluation)
    lines = []
    for detector_id, entries in sorted(by_detector.items()):
        statuses = ", ".join(entry.status for entry in entries[-6:])
        lines.append(
            f"- {detector_id}: {len(entries)} evaluation(s) {_iso(entries[0].evaluated_at)} .. "
            f"{_iso(entries[-1].evaluated_at)}; latest statuses: {statuses}"
        )
    return lines


def _states(label: str, evaluations: Sequence[DetectorEvaluation]) -> list[str]:
    if not evaluations:
        return [f"{label}: none recorded"]
    rendered = []
    for state in evaluations:
        detail = f" fingerprints={state.fingerprints}" if state.fingerprints else ""
        error = f" error={state.error}" if state.error else ""
        rendered.append(f"{state.detector_id}={state.status}{detail}{error}")
    return [f"{label}: " + "; ".join(rendered)]


def _findings_section(closure: IncidentClosure) -> str:
    request = closure.request
    lines = [
        "## Live evidence at dispatch (closure request)",
        f"Application {request.application} in namespace {request.namespace}; source commit "
        f"{request.source_commit}, deployed commit {request.deployed_commit}.",
        f"Detected {_iso(closure.detected_at)}, dispatched {_iso(closure.dispatched_at)}, responder "
        f"completed {_iso(closure.responder_completed_at)}, health verified {_iso(closure.verified_at)}.",
        "Findings:",
    ]
    lines.extend(_finding(finding) for finding in request.findings[:_MAX_FULL_FINDINGS])
    lines.extend(
        f"- [{finding.severity}] {finding.detector_id} / {finding.rule_id}: {finding.summary} "
        f"({_ref(finding.primary_resource)}; evidence omitted)"
        for finding in request.findings[_MAX_FULL_FINDINGS:]
    )
    lines.append("Detector history before dispatch:")
    lines.extend(_history(request.detector_history))
    if request.surfaced_playbooks:
        lines.append(
            "Surfaced playbooks: " + ", ".join(playbook.playbook_id for playbook in request.surfaced_playbooks)
        )
    lines.extend(
        f"Prior outcome {prior.incident_id} ({prior.match_reason}): root causes {list(prior.root_cause_summaries)}; "
        f"repairs {list(prior.repair_action_summaries)}; applied {list(prior.applied_playbook_ids)}"
        for prior in request.relevant_outcomes
    )
    lines.extend(_states("Post-response health detector states", closure.final_detector_states))
    lines.extend(_states("Post-response incident detector states", closure.incident_detector_states))
    return _clip("\n".join(lines), _FINDINGS_BUDGET)


def _result_section(result: IncidentResult | None, dispatch_error: str) -> str:
    lines = ["## Responder structured result"]
    if result is None:
        lines.append(f"No responder result was recorded (dispatch error: {dispatch_error or 'none'}).")
        return "\n".join(lines)
    lines.append(f"Status: {result.status}" + (f"; error: {result.error}" if result.error else ""))
    lines.append("Confirmed root causes:")
    lines.extend(
        f"- {cause.summary} [{'; '.join(_ref(ref) for ref in cause.resources)}]"
        for cause in result.confirmed_root_causes
    )
    if not result.confirmed_root_causes:
        lines.append("- none")
    lines.append("Repair actions:")
    lines.extend(
        f"- [{'ok' if action.success else 'FAILED'}] {action.kind} {action.target}: {action.summary} -- "
        f"{_clip(action.details, _DETAILS_MAX_CHARS)}"
        for action in result.repair_actions
    )
    if not result.repair_actions:
        lines.append("- none recorded")
    if result.repair_changes:
        lines.append("Source changes: " + ", ".join(result.repair_changes))
    lines.append("Verification:")
    lines.extend(
        f"- [{'passed' if evidence.passed else 'FAILED'}] {evidence.name}: "
        f"{_clip(evidence.details, _DETAILS_MAX_CHARS)}"
        for evidence in result.verification_evidence
    )
    if not result.verification_evidence:
        lines.append("- none recorded")
    for playbook in result.applied_playbooks:
        scripts = f" (scripts: {', '.join(playbook.script_names)})" if playbook.script_names else ""
        lines.append(f"Applied playbook: {playbook.playbook_id}{scripts}")
    return _clip("\n".join(lines), _RESULT_BUDGET)


def responder_shell_commands(log: Path | None, session_id: str | None) -> list[str] | None:
    """Shell commands the responder session ran, from the per-turn usage log; ``None`` when unavailable."""

    if log is None or not session_id:
        return None
    commands: list[str] = []
    found = False
    try:
        with log.open(encoding="utf-8", errors="replace") as handle:
            for line in handle:
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(record, dict) or record.get("session_id") != session_id:
                    continue
                found = True
                lines = record.get("shell_command_lines")
                if isinstance(lines, list):
                    commands.extend(str(command) for command in lines)
    except OSError:
        return None
    return commands if found else None


def _commands_section(commands: list[str] | None) -> str:
    header = "## Responder shell commands"
    if commands is None:
        return f"{header}\nThe responder's shell commands are not available for this session."
    if not commands:
        return f"{header}\nThe responder session recorded no shell commands."
    rendered = [f"{index}. {_clip(command, _COMMAND_MAX_CHARS)}" for index, command in enumerate(commands, 1)]
    if sum(len(line) + 1 for line in rendered) <= _COMMANDS_BUDGET:
        return "\n".join([f"{header} ({len(commands)}, in order)", *rendered])
    # Keep the first diagnostic commands and as many of the last (repair and
    # verification) commands as fit.
    head = rendered[:_HEAD_COMMANDS]
    used = sum(len(line) + 1 for line in head)
    tail: list[str] = []
    for line in reversed(rendered[_HEAD_COMMANDS:]):
        if used + len(line) + 1 > _COMMANDS_BUDGET - 80:
            break
        tail.insert(0, line)
        used += len(line) + 1
    omitted = len(rendered) - len(head) - len(tail)
    return "\n".join(
        [
            f"{header} ({len(commands)}, in order)",
            *head,
            f"[... {omitted} commands omitted ...]",
            *tail,
        ]
    )


def _relevant_playbooks(
    closure: IncidentClosure, fired_detectors: set[str], possible: dict[str, list[str]]
) -> list[str]:
    paths: list[str] = []
    candidates = [playbook.playbook_id for playbook in closure.request.surfaced_playbooks]
    for finding in closure.request.findings:
        candidates.extend(finding.playbook_ids)
    if closure.HasField("result"):
        candidates.extend(playbook.playbook_id for playbook in closure.result.applied_playbooks)
    for detector_id in sorted(fired_detectors):
        candidates.extend(possible.get(detector_id, []))
    for path in candidates:
        if path not in paths:
            paths.append(path)
    return paths


def _logical_memory_section(closure: IncidentClosure, *, detector_detail: bool, event_dependence: bool) -> str:
    assert closure.request.HasField("memory_snapshot")
    snapshot = closure.request.memory_snapshot
    fired = {finding.detector_id for finding in closure.request.findings}
    lines = ["## Existing operational memory (trusted logical snapshot)"]
    health = [item.id for item in snapshot.detectors if item.detector_class == "health"]
    lines.append("Health detectors (judge-owned, never edit): " + (", ".join(health) or "none"))
    possible = {item.id: list(item.possible_playbook_ids) for item in snapshot.detectors}
    for detector in snapshot.detectors:
        if detector.detector_class != "incident":
            continue
        watches = ", ".join(f"{watch.api_version}/{watch.kind}" for watch in detector.watches) or "none"
        marker = " [raised a finding in this incident]" if detector.id in fired else ""
        lines.append(
            f"- {detector.id} (watches: {watches}, firing: {detector.firing}, "
            f"playbooks: {list(detector.possible_playbook_ids)}, "
            f"originatingIncident: {detector.originating_incident}){marker}"
        )
        if detector_detail:
            if detector.description:
                lines.append("  Description: " + _clip(detector.description, _DETECTOR_DESCRIPTION_CHARS))
            if detector.predicate_excerpt:
                lines.append(_fenced(_clip(detector.predicate_excerpt, _DETECTOR_PREDICATE_CHARS)))
            if event_dependence:
                lines.append("  Reads Kubernetes Events: " + ("yes" if detector.reads_events else "no"))
    if snapshot.playbook_index_markdown:
        lines.append("Playbook index:")
        lines.append(_fenced(_clip(snapshot.playbook_index_markdown, _INDEX_EXCERPT_CHARS)))
    available = {item.id: item for item in snapshot.playbooks}
    lines.append("Existing playbooks: " + (", ".join(sorted(available)) or "none"))
    relevant = [item for item in _relevant_playbooks(closure, fired, possible) if item in available]
    if not relevant:
        relevant = sorted(available)
    for playbook_id in relevant[:_MAX_PLAYBOOK_EXCERPTS]:
        lines.append(f"Excerpt of playbook {playbook_id}:")
        lines.append(_fenced(_clip(available[playbook_id].markdown.strip(), _PLAYBOOK_EXCERPT_CHARS)))
    return _clip("\n".join(lines), _MEMORY_BUDGET)


def incident_brief(
    closure: IncidentClosure,
    *,
    worktree: Path,
    responder_turn_log: Path | None = None,
    detector_detail: bool = False,
    event_dependence: bool = False,
) -> str:
    """Render the bounded incident brief for a fresh first reflection attempt.

    ``detector_detail`` adds, for every existing incident detector, its Spec
    description and a bounded excerpt of its match predicate, so the reader can
    compare the confirmed signature with what each detector already matches.
    ``event_dependence`` (with ``detector_detail``) also states whether the detector
    source reads Event objects, which may not exist when a fault is introduced.
    """

    result = closure.result if closure.HasField("result") else None
    session_id = None if result is None else result.responder_session_id
    sections = [
        _findings_section(closure),
        _result_section(result, closure.dispatch_error),
        _logical_memory_section(closure, detector_detail=detector_detail, event_dependence=event_dependence),
        _commands_section(responder_shell_commands(responder_turn_log, session_id)),
    ]
    body = _clip("\n\n".join(sections), BRIEF_MAX_CHARS)
    return (
        f"Incident brief (built by the broker from the verified closure; about {len(body) // 4} tokens):\n"
        f"{body}\nEnd of incident brief.\n"
    )
