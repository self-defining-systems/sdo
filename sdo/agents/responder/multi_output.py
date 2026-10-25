"""Adapt a selection-set response to the shared mutation summary formatter."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from pathlib import Path


def option_action_results(response: dict[str, Any]) -> list[dict[str, Any]]:
    """Keep every option's typed report and execution together in the full file."""
    reports = response.get("option_reports")
    if not isinstance(reports, list) or not reports:
        return [response]
    executions = response.get("report", {}).get("execution", [])
    actions = []
    for index, report in enumerate(reports):
        execution = executions[index] if index < len(executions) else {}
        action = dict(response)
        action.pop("option_reports", None)
        action.pop("error", None)
        action.pop("success", None)
        status = report.get("status", "STATUS_UNRESOLVED")
        action["report"] = {**report, "execution": [execution] if execution else []}
        action["option_id"] = report.get("option_id", execution.get("option_id", ""))
        action["application_id"] = report.get("application_id", execution.get("application_id", ""))
        action["pending"] = bool(execution.get("pending"))
        action["summary"] = f"{action['option_id']}: {status}"
        action["targeted_healthy"] = status == "STATUS_SUCCESS"
        if not action["pending"]:
            action.pop("rollout_pending", None)
        if execution.get("error") and not action["pending"]:
            action["error"] = execution["error"]
        elif status in ("STATUS_EXECUTION_ERROR", "STATUS_REFUSED"):
            action["error"] = report.get("reason_code", status)
        actions.append(action)
    return actions


def format_selection_response(
    before: Sequence[Mapping[str, Any]] | None,
    after: Sequence[Mapping[str, Any]] | None,
    response: dict[str, Any],
    *,
    json_output: bool = False,
    controller_progress: Mapping[str, Any] | None = None,
) -> tuple[str, Path]:
    """Use the shared summary and retain the complete controller response once."""
    from sdo.agents.responder.mutation_output import (
        MAX_MUTATION_OUTPUT_BYTES,
        _bounded_text,
        clamp_mutation_summary,
        compact_mutation_envelope,
        format_mutation_response,
    )
    from sdo.agents.responder.tool_output import _brief

    text, path = format_mutation_response(
        before, after, [response], json_output=json_output, controller_progress=controller_progress
    )

    def emitted_bytes(lines: list[str]) -> int:
        rendered = "\n".join(lines)
        if json_output:
            rendered = json.dumps(
                compact_mutation_envelope(rendered, path, response), ensure_ascii=False, separators=(",", ":")
            )
        return len(rendered.encode()) + 1

    if not response.get("option_reports"):
        if response.get("rollout_pending"):
            lines = text.splitlines()
            application = _bounded_text(response.get("application_id", "unknown"), 80)
            lines[0] = f"This action: rollout pending; application={application}."
            return clamp_mutation_summary("\n".join(lines), path, response, json_output=json_output), path
        return text, path
    actions = option_action_results(response)
    pending = sum(action["pending"] for action in actions)
    failed = sum(bool(action.get("error")) for action in actions if not action["pending"])
    succeeded = sum(action["targeted_healthy"] for action in actions)
    unresolved = len(actions) - pending - failed - succeeded
    lines = text.splitlines()
    lines[0] = f"Options: {succeeded} succeeded; {pending} pending; {failed} failed; {unresolved} unresolved."
    rows = []
    for action in actions:
        status = (
            "pending"
            if action["pending"]
            else action["report"].get("status", "STATUS_UNRESOLVED").removeprefix("STATUS_").lower()
        )
        rows.append(
            f"option: {_brief(action['option_id'], 48)} {status} application={_brief(action['application_id'], 64)}"
        )
    # Keep verdicts/counts/schema intact. Make room for option identities by
    # moving only displayed finding details to the already complete report.
    lines = [
        "Additional finding details: Full JSON new[]/unchanged[]."
        if line.startswith("Additional finding lines:")
        else line
        for line in lines
    ]
    reserve = sum(len(row.encode()) + 1 for row in rows[:2]) + 70
    removed_details = False
    while emitted_bytes(lines) + reserve > MAX_MUTATION_OUTPUT_BYTES - 1:
        candidates = [index for index, line in enumerate(lines) if line.startswith(("new: -", "unchanged: -"))]
        if not candidates:
            break
        lines.pop(candidates[-1])
        if not removed_details:
            lines.insert(1, "Finding details omitted: see Full JSON findings/new/unchanged[].")
            removed_details = True
    shown = []
    for row in rows:
        omitted = len(rows) - len(shown) - 1
        tail = [f"Options omitted: {omitted}; see option_reports[] in Full JSON."] if omitted else []
        if emitted_bytes([*lines[:1], *shown, row, *tail, *lines[1:]]) > MAX_MUTATION_OUTPUT_BYTES - 1:
            break
        shown.append(row)
    omitted = len(rows) - len(shown)
    if omitted:
        shown.append(f"Options omitted: {omitted}; see option_reports[] in Full JSON.")
    return clamp_mutation_summary(
        "\n".join([*lines[:1], *shown, *lines[1:]]), path, response, json_output=json_output
    ), path
