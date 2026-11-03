"""Pre-injection responder accounting from immutable worker task/result records."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from benchmarks.sregym.fastloop.records import TokenCounts


def responder_setup_usage(configmaps: list[dict[str, Any]], started: datetime) -> TokenCounts:
    """Count each completed task once; missing result usage is an accounting error."""
    requests: set[str] = set()
    for item in configmaps:
        wire = item.get("data", {}).get("agent-task.json")
        if not wire:
            continue
        task = json.loads(wire)
        created = item.get("metadata", {}).get("creationTimestamp")
        if not created or datetime.fromisoformat(created.replace("Z", "+00:00")) < started:
            continue
        request = task.get("request", {})
        if request.get("@type", "").endswith("IncidentRequest"):
            requests.add(task.get("task_id", task.get("taskId")))
    results: dict[str, TokenCounts] = {}
    for item in configmaps:
        wire = item.get("data", {}).get("agent-result.json")
        if not wire:
            continue
        result = json.loads(wire)
        task_id = result.get("task_id", result.get("taskId"))
        if task_id not in requests:
            continue
        response = result.get("response", {})
        if not isinstance(response.get("usage"), dict):
            raise ValueError(f"pre-injection responder task {task_id} has no usage")
        results[task_id] = TokenCounts.from_usage(response["usage"])
    if requests - results.keys():
        raise ValueError("pre-injection responder tasks have no terminal usage")
    return sum(results.values(), TokenCounts())
