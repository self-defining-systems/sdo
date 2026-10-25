"""Compact action output and complete, typed per-invocation report files."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

from sdo.agents.responder import tool_output

MAX_MUTATION_OUTPUT_BYTES = 2048


def command_report_schema() -> dict[str, str]:
    """Generated from runtime.CommandReport and its Go/proto field types."""
    return json.loads(Path(__file__).with_name("command_report_schema.json").read_text())


def finding_progress(
    before: Sequence[Mapping[str, Any]],
    after: Sequence[Mapping[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    """Compare controller snapshots; changed evidence is visible as a possible regression."""
    prior = {str(item.get("fingerprint", "")): item for item in before}
    current = {str(item.get("fingerprint", "")): item for item in after}
    return {
        "fixed": [dict(item) for key, item in prior.items() if key not in current],
        "new": [
            dict(item)
            for key, item in current.items()
            if key not in prior or item.get("evidence") != prior[key].get("evidence")
        ],
        "unchanged": [
            dict(item)
            for key, item in current.items()
            if key in prior and item.get("evidence") == prior[key].get("evidence")
        ],
    }


def _line(finding: Mapping[str, Any]) -> str:
    ref = finding.get("primary_resource") or {}
    resource = "/".join(str(ref.get(key, "")) for key in ("kind", "namespace", "name"))
    detector = tool_output._brief(finding.get("detector_id", "unknown"), 32)
    resource = tool_output._brief(resource, 40)
    reason = tool_output._brief(finding.get("summary", "finding remains active"), 48)
    return f"- {detector} {resource}: {reason}"


def format_mutation_response(
    before: Sequence[Mapping[str, Any]],
    after: Sequence[Mapping[str, Any]],
    action_results: Sequence[dict[str, Any]],
) -> tuple[str, Path]:
    """Return bounded summary text and its complete JSON file; supports multiple actions."""
    actions = list(action_results)
    progress = finding_progress(before, after)
    report = {"actions": actions, "findings": list(after), **progress}
    # Workers set SDO_SESSION_SCRATCH to their private session work directory.
    # Standalone invocations retain the existing private tool-output directory.
    command = str(actions[-1].get("command", "mitigate")) if actions else "mitigate"
    directory = os.environ.get("SDO_SESSION_SCRATCH")
    path = tool_output.write_json(report, command=command, root=Path(directory) / "sdo-reports" if directory else None)
    warning = next(
        (action.get("incident_verified") for action in reversed(actions) if action.get("incident_verified")), None
    )
    if len(actions) == 1:
        action = actions[0]
        if action.get("error") or action.get("success") is False or action.get("exit_code"):
            result = "failed: " + tool_output._brief(action.get("error") or action.get("summary", "see receipt"), 100)
        elif action.get("targeted_healthy"):
            result = "target finding cleared"
        elif action.get("success"):
            result = "mutation completed; verification follows the controller"
        else:
            result = tool_output._brief(action.get("summary", "completed; see report"), 100)
        lines = [f"This action: {result}."]
    else:
        failed = sum(bool(action.get("error") or action.get("success") is False) for action in actions)
        lines = [f"Actions: {len(actions) - failed} completed; {failed} failed (see individual receipts)."]
    lines += [
        f"Incident: {'verified' if warning else 'NOT verified'}, still broken: {len(after)}",
        f"fixed by/since this action: {len(progress['fixed'])}",
    ]
    schema = command_report_schema()
    tail = [
        f"incident_verified: {warning['message'] if warning else 'false'}",
        f"Full JSON: {path}",
        "Schema (top-level keys):",
    ]
    tail.extend(f"{key}[]: {description.removeprefix('array of ')}" for key, description in schema.items())
    rows = []
    for key, label in (("new", "new since this action"), ("unchanged", "still broken, unchanged")):
        rows.append(f"{label}: {len(progress[key])}" + (" (possible regression)" if key == "new" else ""))
        rows.extend(_line(finding) for finding in progress[key])
    # The verdict, counts, warning, file and schema are never removed. When the
    # finding list itself exceeds the display budget, point explicitly to its
    # complete typed arrays instead of silently losing findings in truncation.
    for row in rows:
        candidate = "\n".join([*lines, row, *tail])
        if len(candidate.encode()) > MAX_MUTATION_OUTPUT_BYTES - 100:
            lines.append(f"Additional finding lines: {path} (new[], unchanged[])")
            break
        lines.append(row)
    text = "\n".join([*lines, *tail])
    return text, path
