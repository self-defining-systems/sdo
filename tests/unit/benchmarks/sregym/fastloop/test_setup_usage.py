"""Setup usage must not mix old lane tasks or count duplicate results."""

import json
from datetime import datetime, timezone

import pytest

from benchmarks.sregym.fastloop.setup_usage import responder_setup_usage


def test_setup_usage_scopes_deduplicates_and_keeps_cached_tokens():
    start = datetime(2030, 1, 1, tzinfo=timezone.utc)

    def task(name, created):
        return {
            "metadata": {"creationTimestamp": created},
            "data": {
                "agent-task.json": json.dumps(
                    {
                        "task_id": name,
                        "request": {"@type": "type.googleapis.com/sdodev.contracts.v1alpha1.IncidentRequest"},
                    }
                )
            },
        }

    def result(name):
        return {
            "data": {
                "agent-result.json": json.dumps(
                    {
                        "task_id": name,
                        "response": {
                            "usage": {"input_tokens": "100", "cached_input_tokens": "80", "output_tokens": "20"}
                        },
                    }
                )
            }
        }

    rows = [
        task("old", "2029-01-01T00:00:00Z"),
        task("new", "2030-01-01T00:01:00Z"),
        result("old"),
        result("new"),
        result("new"),
    ]
    usage = responder_setup_usage(rows, start)
    assert usage.total_tokens == 120
    assert usage.cached_input_tokens == 80
    with pytest.raises(ValueError, match="no terminal usage"):
        responder_setup_usage(rows[:2], start)
