"""Lean, complete responder facts with bounded inline delivery."""

from __future__ import annotations

import json
from contextlib import ExitStack, contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any

from google.protobuf import json_format

from sdo.agents.context_output import budget_for
from sdo.agents.prompt_templates import count_chars, render_prompt
from sdo.agents.responder.reflection_evidence import playbook_description
from sdo.agents.responder.rollout_release import release_lines
from sdo.agents.responder.tool_output import write_json
from sdo.agents.responder.tools_cli import command_purposes, command_surface
from sdo.contracts import Verdict

MAX_INCIDENT_SUMMARY_CHARS = budget_for("responder.incident_data")

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from sdo.contracts import Finding, IncidentRequest, ObjectRef, ProgramRun, StageReport


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


def prior_response_summary(request: IncidentRequest) -> dict[str, Any]:
    """Keep bounded prior work visible; complete history remains in durable outcomes."""
    episodes = list(request.prior_response_progress)
    return {
        "episodes": [
            {
                "incident_id": item.incident_id,
                "truncated": item.truncated,
                "successful_actions": [
                    {"action_id": action.action_id, "summary": _clip(action.summary), "success": True}
                    for action in item.successful_actions[:2]
                ],
                "actions_omitted": max(0, len(item.successful_actions) - 2),
                "blockers": [
                    {"reason_code": blocker.reason_code, "reason": _clip(blocker.reason)}
                    for blocker in item.blockers[:1]
                ],
                "blockers_omitted": max(0, len(item.blockers) - 1),
                "error": _clip(item.error),
            }
            for item in reversed(episodes[-2:])
        ],
        "episodes_omitted": sum(item.omitted_episodes for item in episodes) + max(0, len(episodes) - 2),
    }


def render_incident_data(request: IncidentRequest) -> str:
    """Render the complete projection; production uses prepared_incident_data for its cap."""
    return _render_data(incident_data_view(request))


def _encode(document: dict[str, Any]) -> str:
    return json.dumps(document, ensure_ascii=False, separators=(",", ":"))


def _render_data(document: dict[str, Any]) -> str:
    return render_prompt("responder", "incident_data", part="responder.incident_data", request_json=_encode(document))


_SEVERITY_ORDER = {"critical": 0, "warn": 1, "info": 2, "latent": 3}
_LINE_CHARS = 160
#: The rest stay in the facts file; the brief says how many.
MAX_INLINE_FINDINGS = 10


def _clip(text: str, limit: int = _LINE_CHARS) -> str:
    text = " ".join(text.split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _ref(ref: ObjectRef) -> str:
    return "/".join(part for part in (ref.kind, ref.namespace, ref.name) if part)


def finding_fingerprint(finding: Finding) -> str:
    """The controller's finding identity (Go ``FindingFingerprint``)."""
    if finding.fingerprint:
        return finding.fingerprint
    ref = finding.primary_resource
    return f"{finding.detector_id}/{finding.rule_id}/{ref.namespace}/{ref.kind}/{ref.name}"


def finding_line(finding: Finding) -> str:
    """One line: identity for ``--finding`` plus the detector's own summary."""
    line = f"{finding_fingerprint(finding)}: {_clip(finding.summary, 120)}"
    ref = _ref(finding.primary_resource)
    return line if finding.primary_resource.name in finding.summary else f"{line} [{ref}]"


def _stage_disposition(result: str, kept_reason: str) -> str:
    """What an applied program left behind: cleared, reverted-ineffective or kept-<why>."""
    if result == "applied_cleared":
        return "cleared"
    if result == "reverted_ineffective" or (result == "applied_still_matching" and kept_reason):
        return (f"kept-{kept_reason}" if kept_reason else result).replace("_", "-")
    return result


def stage_action_lines(reports: Iterable[StageReport]) -> list[str]:
    """What each automatic action ran, what it left behind and the objects it wrote, one line each.

    Attempts that one application settled with the same verdict are one action.
    """
    lines = []
    for report in reports:
        actions: dict[tuple[str, str | int, str, str, bool, str], dict[str, Any]] = {}
        for index, attempt in enumerate(report.mitigations):
            receipt = attempt.apply_receipt
            error = attempt.error or receipt.error or receipt.rollback_error
            key = (
                attempt.playbook_id,
                receipt.application_id or index,
                _stage_disposition(attempt.result, attempt.kept_reason),
                attempt.skip_reason,
                receipt.rolled_back and attempt.result != "reverted_ineffective",
                error,
            )
            action = actions.setdefault(key, {"targets": [], "writes": 0, "cleared": set()})
            touched = attempt.touched_objects or [binding.object for binding in attempt.bindings]
            action["targets"] += [_ref(ref) for ref in touched]
            action["writes"] += attempt.plan_action_count
            action["cleared"] |= set(attempt.cleared_finding_fingerprints)
        for (playbook, _, result, skip_reason, rolled_back, error), action in actions.items():
            targets = list(dict.fromkeys(action["targets"]))
            line = f"{report.stage}: {playbook}"
            if targets:
                line += " on " + ",".join(targets[:3]) + (f" +{len(targets) - 3}" if len(targets) > 3 else "")
            line += f" -> {result}"
            if skip_reason:
                line += f" ({skip_reason})"
            if action["writes"]:
                line += f"; {action['writes']} write{'s' if action['writes'] != 1 else ''}"
            if rolled_back:
                line += "; rolled back"
            if action["cleared"]:
                line += f"; cleared {len(action['cleared'])}"
            if error:
                line += f"; error: {_clip(error)}"
            lines.append(line)
        if report.error:
            lines.append(f"{report.stage}: error: {_clip(report.error)}")
    return lines


def program_run_lines(runs: Iterable[ProgramRun]) -> list[str]:
    """Diagnoses at incident open that matched or could not decide, one line each."""
    lines = []
    for run in runs:
        if run.verdict.result == Verdict.RESULT_NO_MATCH:
            continue
        targets = [_ref(binding.object) for binding in run.bindings]
        line = run.program_id + (" on " + ",".join(targets[:3]) if targets else "")
        line += f" -> {Verdict.Result.Name(run.verdict.result)}"
        if run.HasField("can_mitigate"):
            line += "; mitigation" if run.can_mitigate else "; diagnosis only"
        if run.verdict.evidence:
            evidence = run.verdict.evidence[0]
            line += f"; {evidence.fact or 'observed'}={_clip(evidence.observed, 60)}"
            if evidence.expected:
                line += f" (expected {_clip(evidence.expected, 60)})"
        if run.verdict.question:
            line += f"; question: {_clip(run.verdict.question)}"
        if run.error:
            line += f"; error: {_clip(run.error)}"
        lines.append(line)
    return lines


def _coverage_brief(request: IncidentRequest) -> dict[str, Any]:
    """Matches that explain an open finding; settled matches stay in the facts file."""
    open_findings = {finding_fingerprint(finding) for finding in request.findings}
    matches = [
        {
            "playbook_id": match.playbook_id,
            "can_mitigate": match.can_mitigate,
            "bindings": [_ref(binding.object) for binding in match.bindings],
            "explained_finding_fingerprints": explained,
            "verified": f"{match.verified_successes} succeeded, {match.verified_failures} failed",
        }
        for match in request.playbook_coverage.matches
        if (explained := [item for item in match.explained_finding_fingerprints if item in open_findings])
    ]
    explained = {fingerprint for match in matches for fingerprint in match["explained_finding_fingerprints"]}
    return {"matches": matches, "unexplained_findings": len(open_findings - explained)}


def _rollout_pending(request: IncidentRequest) -> list[dict[str, Any]]:
    """Stalls the stage handed off and stalls left by earlier failed episodes."""
    pending = [
        attempt.apply_receipt.rollout_pending for report in request.prior_stages for attempt in report.mitigations
    ]
    pending += [blocker for item in request.prior_response_progress for blocker in item.blockers]
    return [json_format.MessageToDict(item, preserving_proto_field_name=True) for item in pending]


def responder_brief(request: IncidentRequest, *, details_file: str, full: dict[str, Any]) -> dict[str, Any]:
    """The first message: open findings, one line per automatic action, pointers to the rest."""
    findings = sorted(request.findings, key=lambda item: _SEVERITY_ORDER.get(item.severity, 1))
    brief: dict[str, Any] = {
        "incident_id": request.incident_id,
        "application": request.application,
        "namespace": request.namespace,
        "findings": [finding_line(finding) for finding in findings[:MAX_INLINE_FINDINGS]],
        "playbook_coverage": _coverage_brief(request),
    }
    if len(findings) > MAX_INLINE_FINDINGS:
        brief["findings_omitted"] = len(findings) - MAX_INLINE_FINDINGS
    if runs := program_run_lines(request.initial_program_runs):
        brief["program_runs"] = runs
    if request.prior_stages:
        brief["stage_actions"] = stage_action_lines(request.prior_stages)
        brief["stage_cleared_findings"] = len(
            {fingerprint for report in request.prior_stages for fingerprint in report.cleared_finding_fingerprints}
        )
    if request.prior_response_progress:
        brief["prior_response_progress"] = prior_response_summary(request)
    if releases := release_lines(_rollout_pending(request)):
        brief["rollout_releases"] = releases
    if "playbook_documents" in full:
        brief["playbook_documents"] = dict(full["playbook_documents"])
        brief["playbook_index"] = {item["id"]: _clip(item["description"], 120) for item in full["playbook_index"]}
    surface = full["incident_tools"]
    brief["incident_tools"] = {
        "commands": command_purposes(),
        "help": "sdo COMMAND --help lists arguments",
        "reference": surface["reference"],
        "output": surface["output"],
    }
    brief["details_file"] = details_file
    return _fit(brief)


def _fit(brief: dict[str, Any]) -> dict[str, Any]:
    """Trim the longest list first, counting omissions, until the message fits its budget."""

    def lists() -> dict[str, list[Any]]:
        return {
            "findings": brief["findings"],
            "stage_actions": brief.get("stage_actions", []),
            "rollout_releases": brief.get("rollout_releases", []),
            "program_runs": brief.get("program_runs", []),
            "playbook_coverage.matches": brief["playbook_coverage"]["matches"],
            "playbook_documents": list(brief.get("playbook_documents", {})),
        }

    # Measure before the renderer's generic cap, which must never see this message.
    framing = count_chars(_render_data({})) - count_chars(_encode({}))
    while framing + count_chars(_encode(brief)) > MAX_INCIDENT_SUMMARY_CHARS:
        name, items = max(lists().items(), key=lambda item: len(item[1]))
        if not items:
            raise ValueError("responder brief exceeds its budget without any list to trim")
        if name == "playbook_documents":
            brief["playbook_documents"].pop(items[-1])
            brief["playbook_index"].pop(items[-1], None)
        else:
            items.pop()
        owner, key = (brief["playbook_coverage"], "matches") if "." in name else (brief, name)
        owner[f"{key}_omitted"] = owner.get(f"{key}_omitted", 0) + 1
    return brief


@contextmanager
def prepared_incident_data(request: IncidentRequest) -> Iterator[str]:
    """Inline only what the responder must act on; complete facts stay private for the whole turn."""
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
        yield _render_data(responder_brief(request, details_file=str(facts), full=document))
