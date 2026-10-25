"""Adapt a selection-set response to the shared mutation summary formatter."""

from __future__ import annotations

from typing import Any


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
