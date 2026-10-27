"""Bounded outcome views for the reflection request.

Reflection is told about the incident it just closed and about earlier
incidents in the same memory. Serializing every ``OutcomeRecord`` whole costs
tokens in proportion to the operator's incident count: each record carries a
per-timestamp ``detector_history``, full finding evidence, and usage. That
text is re-read on every model request of the reflection session. These views
keep what learning is grounded in (findings, root causes, repairs, playbooks,
detector transitions) and drop repeated evaluations and, for past incidents,
raw evidence.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from typing import TYPE_CHECKING, Any

from google.protobuf import json_format

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from sdo.contracts import DetectorEvaluation, ObjectRef, OutcomeRecord


def _timestamp(evaluation: DetectorEvaluation) -> str:
    return evaluation.evaluated_at.ToJsonString()


def collapse_detector_history(evaluations: Sequence[DetectorEvaluation]) -> list[dict[str, Any]]:
    """Run-length encode each detector's evaluations by (status, fingerprints, error, reason).

    Consecutive identical evaluations of one detector become a single run with
    its first and last timestamps and count; every status or fingerprint
    transition is kept, in first-seen detector order.
    """

    runs: dict[str, list[dict[str, Any]]] = {}
    for evaluation in evaluations:
        detector_runs = runs.setdefault(evaluation.detector_id, [])
        error = evaluation.error if evaluation.HasField("error") else None
        signature = (evaluation.status, tuple(evaluation.fingerprints), error, evaluation.reason)
        if detector_runs and detector_runs[-1]["_signature"] == signature:
            detector_runs[-1]["last_evaluated_at"] = _timestamp(evaluation)
            detector_runs[-1]["evaluations"] += 1
            continue
        detector_runs.append(
            {
                "_signature": signature,
                "detector_id": evaluation.detector_id,
                "status": evaluation.status,
                "fingerprints": list(evaluation.fingerprints),
                "error": error,
                **({"reason": evaluation.reason} if evaluation.reason else {}),
                "first_evaluated_at": _timestamp(evaluation),
                "last_evaluated_at": _timestamp(evaluation),
                "evaluations": 1,
            }
        )
    return [
        {key: value for key, value in run.items() if key != "_signature"}
        for detector_runs in runs.values()
        for run in detector_runs
    ]


def current_outcome_view(outcome: OutcomeRecord) -> dict[str, Any]:
    """Learning evidence with repeated sweeps and large tool transcripts compacted.

    This is a model-facing projection only. The committed OutcomeRecord remains
    authoritative, including timings and full tool responses. Distinct program
    verdict/evidence/error transitions and all action receipts remain here.
    """

    view = json_format.MessageToDict(
        outcome, preserving_proto_field_name=True, always_print_fields_with_no_presence=True
    )
    if not view.get("disabled_setup_detectors"):
        view.pop("disabled_setup_detectors", None)
    view.pop("reflection_eligibility", None)  # supplied once as controller-owned work
    if not view.get("source_check_notes"):
        view.pop("source_check_notes", None)  # information only; shown when the closure recorded any
    view["detector_history"] = collapse_detector_history(outcome.detector_history)
    compaction = {}
    if view["program_runs"]:
        original = view["program_runs"]
        collapsed = _collapse_program_runs(original)
        compaction["program_runs_omitted_duplicates"] = len(original) - len(collapsed)
        view["program_runs"], view["program_run_summary"] = split_quiet_program_runs(collapsed)
    if view["program_tool_calls"]:
        original = view["program_tool_calls"]
        calls = _collapse_tool_calls(original)
        compaction["program_tool_calls_omitted_duplicates"] = len(original) - len(calls)
        view["program_tool_calls"] = calls
    if compaction:
        view["reflection_compaction"] = compaction
    return view


# Keep head and tail so command failures and final status remain visible. Full
# responses can be retrieved from the committed outcome, rather than replayed
# into every reflection model request.
_TOOL_RESPONSE_MAX_CHARS = 4000


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _collapse_program_runs(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    retained: list[dict[str, Any]] = []
    latest: dict[str, tuple[str, dict[str, Any]]] = {}
    for run in runs:
        identity = _canonical([run["program_id"], sorted(run["bindings"], key=_canonical)])
        # Duration is sweep noise; everything else (including revision, evidence,
        # fingerprint and binding source) can distinguish a learning signal.
        signature = _canonical(
            {
                **{key: value for key, value in run.items() if key != "duration"},
                "bindings": sorted(run["bindings"], key=_canonical),
            }
        )
        previous = latest.get(identity)
        if previous is not None and previous[0] == signature:
            previous[1]["evaluations"] += 1
            continue
        entry = {**run, "evaluations": 1}
        retained.append(entry)
        latest[identity] = (signature, entry)
    return retained


#: Verdicts that carry no per-binding signal worth replaying: the program did not fire, or could not tell.
_QUIET_RESULTS = {"RESULT_NO_MATCH": "no_match", "RESULT_UNKNOWN": "unknown"}
_SUMMARY_MAX_PROGRAMS = 30
_SUMMARY_MAX_REASONS = 3
_SUMMARY_REASON_MAX_CHARS = 160
_SUMMARY_MAX_BINDINGS = 5


def split_quiet_program_runs(runs: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Keep runs with a signal; reduce NO_MATCH and UNKNOWN runs to one summary per program revision.

    ``runs`` are collapsed runs, each weighted by its ``evaluations`` count. The
    summary names the program, its revision, the evaluation count per status, the
    distinct reasons its UNKNOWN runs gave and the objects (up to five) they were
    bound to, so a reflection still sees which programs ran and why some could not decide.
    """

    kept: list[dict[str, Any]] = []
    summaries: dict[tuple[str, str], dict[str, Any]] = {}
    reasons: dict[tuple[str, str], list[str]] = {}
    bindings: dict[tuple[str, str], list[str]] = {}
    for run in runs:
        status = _QUIET_RESULTS.get(run["verdict"]["result"])
        if status is None:
            kept.append(run)
            continue
        key = (run["program_id"], run.get("program_revision", ""))
        summary = summaries.setdefault(
            key, {"program_id": key[0], "program_revision": key[1], "no_match": 0, "unknown": 0}
        )
        summary[status] += run.get("evaluations", 1)
        if status == "unknown" and run.get("error") and run["error"] not in reasons.setdefault(key, []):
            reasons[key].append(run["error"])
        if status == "unknown":
            names = bindings.setdefault(key, [])
            for binding in run.get("bindings", []):
                name = _binding_name(binding)
                if name and name not in names and len(names) < _SUMMARY_MAX_BINDINGS:
                    names.append(name)
    ordered = list(summaries.items())
    result = []
    for key, summary in ordered[:_SUMMARY_MAX_PROGRAMS]:
        distinct = reasons.get(key, [])
        if distinct:
            summary["unknown_reasons"] = [text[:_SUMMARY_REASON_MAX_CHARS] for text in distinct[:_SUMMARY_MAX_REASONS]]
            if len(distinct) > _SUMMARY_MAX_REASONS:
                summary["unknown_reasons_omitted"] = len(distinct) - _SUMMARY_MAX_REASONS
        if bindings.get(key):
            summary["unknown_bindings"] = bindings[key]
        result.append(summary)
    if len(ordered) > _SUMMARY_MAX_PROGRAMS:
        result.append({"programs_omitted": len(ordered) - _SUMMARY_MAX_PROGRAMS})
    return kept, result


def _binding_name(binding: dict[str, Any]) -> str:
    """The bound object as ``Kind/name``; real UNKNOWN runs carry no error text, so this says where they ran."""

    target = binding.get("object", {})
    return f"{target['kind']}/{target['name']}" if target.get("kind") and target.get("name") else ""


def _corpus_refs(value: Any) -> list[str]:
    refs: set[str] = set()

    def visit(item: Any, key: str = "") -> None:
        if isinstance(item, dict):
            for name, child in item.items():
                visit(child, name)
        elif isinstance(item, list):
            for child in item:
                visit(child, key)
        elif isinstance(item, str):
            if ("corpus" in key or "capture" in key) and len(item) <= 512:
                refs.add(item)
            refs.update(re.findall(r"(?:[.\w/-]*/)?corpus/[^\s\"'<>]+", item))

    visit(value)
    return sorted(refs)


def _collapse_tool_calls(calls: list[dict[str, Any]]) -> list[dict[str, Any]]:
    retained: list[dict[str, Any]] = []
    latest: dict[str, tuple[str, dict[str, Any]]] = {}
    for index, call in enumerate(calls):
        request, response = call["request_json"], call["response_json"]
        try:
            request = _canonical(json.loads(request))
        except ValueError:
            pass
        try:
            parsed_response = json.loads(response)
            response = _canonical(parsed_response)
        except ValueError:
            parsed_response = None
        identity = _canonical([call["command"], request])
        signature = _canonical([response, call["error"], call["summary"]])
        previous = latest.get(identity)
        if previous is not None and previous[0] == signature:
            previous[1]["evaluations"] += 1
            previous[1]["last_at"] = call.get("at")
            continue
        entry = {**call, "evaluations": 1, "last_at": call.get("at"), "outcome_call_index": index}
        if len(response) > _TOOL_RESPONSE_MAX_CHARS:
            half = _TOOL_RESPONSE_MAX_CHARS // 2
            entry["response_json"] = response[:half] + "\n[... omitted ...]\n" + response[-half:]
            entry["response_json_omitted_chars"] = len(response) - _TOOL_RESPONSE_MAX_CHARS
            entry["response_json_sha256"] = hashlib.sha256(call["response_json"].encode()).hexdigest()
            entry["corpus_refs"] = _corpus_refs(parsed_response if parsed_response is not None else response)
            if isinstance(parsed_response, dict):
                entry["response_status"] = {
                    key: parsed_response[key]
                    for key in ("error", "summary", "application_id", "plan_id", "healthy", "unresolved")
                    if key in parsed_response
                }
        retained.append(entry)
        latest[identity] = (signature, entry)
    return retained


def _resource(ref: ObjectRef) -> str:
    return f"{ref.kind}/{ref.name}"


# Prior history is context, not the current repair's evidence. Bound both the
# number of entries and each narrative so a single long incident stays small.
_HISTORY_ITEMS = 2
_HISTORY_TEXT_CHARS = 160


def _history_text(value: str) -> str:
    if len(value) <= _HISTORY_TEXT_CHARS:
        return value
    return value[:_HISTORY_TEXT_CHARS] + " [omitted]"


def prior_outcome_view(record: OutcomeRecord) -> dict[str, Any]:
    """A bounded orientation summary; the full prior record is available by reference."""

    view: dict[str, Any] = {
        "incident_id": _history_text(record.incident_id),
        "classification": _history_text(record.classification),
        "findings": [
            {
                "detector_id": _history_text(finding.detector_id),
                "rule_id": _history_text(finding.rule_id),
                "summary": _history_text(finding.summary),
                "fingerprint": _history_text(finding.fingerprint),
                "primary_resource": _history_text(_resource(finding.primary_resource)),
            }
            for finding in record.findings[:_HISTORY_ITEMS]
        ],
        "confirmed_root_causes": [
            {
                "summary": _history_text(cause.summary),
                "resources": [_history_text(_resource(ref)) for ref in cause.resources[:_HISTORY_ITEMS]],
            }
            for cause in record.confirmed_root_causes[:_HISTORY_ITEMS]
        ],
        "repair_actions": [
            {
                "kind": _history_text(action.kind),
                "target": _history_text(action.target),
                "summary": _history_text(action.summary),
                "success": action.success,
            }
            for action in record.repair_actions[:_HISTORY_ITEMS]
        ],
        "applied_playbooks": [_history_text(value) for value in record.applied_playbooks[:_HISTORY_ITEMS]],
        "confirmed_playbooks": [_history_text(value) for value in record.confirmed_playbooks[:_HISTORY_ITEMS]],
        "rejected_playbooks": [_history_text(value) for value in record.rejected_playbooks[:_HISTORY_ITEMS]],
        "detector_evaluations": len(record.detector_history),
        "memory_commit": _history_text(record.memory_commit) if record.HasField("memory_commit") else None,
    }
    for field in (
        "findings",
        "confirmed_root_causes",
        "repair_actions",
        "applied_playbooks",
        "confirmed_playbooks",
        "rejected_playbooks",
    ):
        omitted = len(getattr(record, field)) - len(view[field])
        if omitted:
            view[field + "_omitted"] = omitted
    return {key: value for key, value in view.items() if value is not None and value != [] and value != 0}


def history_view(
    history: Sequence[OutcomeRecord], *, current_incident_id: str, evidence_directory: Path | None = None
) -> list[dict[str, Any]]:
    """Every prior incident the request carries (Go keeps the latest few); the current one is shown separately."""

    prior = [record for record in history if record.incident_id != current_incident_id]
    views = []
    for record in prior:
        view = prior_outcome_view(record)
        if evidence_directory is not None:
            # IDs are data, never paths. These private copies live outside the
            # proposal tree and survive the backend's in-session corrections.
            name = f"{len(views)}.json"
            path = evidence_directory / name
            with open(path, "w", encoding="utf-8", opener=lambda p, flags: os.open(p, flags, 0o600)) as stream:
                stream.write(json_format.MessageToJson(record, preserving_proto_field_name=True))
            view["full_outcome_path"] = str(path)
        views.append(view)
    return views
