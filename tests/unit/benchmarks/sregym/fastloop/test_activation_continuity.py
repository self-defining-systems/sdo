import json
from datetime import datetime, timezone

import pytest

from benchmarks.sregym.fastloop.continuity import check_activations


def trace(gap=0.2, skip=False, paused=False):
    base = datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp()
    rows = [(i * gap, {"controller_iteration": i + 11 + (1 if skip and i >= 2 else 0)}) for i in range(-10, 16)]
    rows.append((gap / 2, {"memory_host_activation": "abc", "activation_latency_ms": 40}))
    rows.sort(key=lambda row: row[0])
    if paused:
        rows.insert(1, (0.01, {"maintenance_generation": "paused", "controller_maintenance": "paused"}))
    return "\n".join(f"{datetime.fromtimestamp(base + t, timezone.utc).isoformat()} {json.dumps(d)}" for t, d in rows)


def test_continuity_records_activation_and_identity():
    rows, error = check_activations(trace(), {"uid": "one"}, {"uid": "one"}, max_gap=1)
    assert error is None
    assert rows[0].evaluation_count >= 10
    assert rows[0].max_evaluation_gap_seconds == 0.2
    assert rows[0].controller_unchanged
    assert rows[0].passed


@pytest.mark.parametrize(
    ("log", "after"),
    [
        (trace(2), {"uid": "one"}),
        (trace(skip=True), {"uid": "one"}),
        (trace(), {"uid": "two"}),
        (trace(paused=True), {"uid": "one"}),
        ("", {"uid": "one"}),
    ],
)
def test_continuity_fails_closed(log, after):
    _, error = check_activations(log, {"uid": "one"}, after, max_gap=1, expected_commits={"abc"})
    assert error


@pytest.mark.parametrize("field", ["uid", "start_time", "containers"])
def test_each_controller_identity_field_is_enforced(field):
    before = {
        "uid": "one",
        "start_time": "2026-01-01T00:00:00Z",
        "containers": [{"name": "controller", "restart_count": 0, "started_at": "2026-01-01T00:00:01Z"}],
    }
    after = {**before, field: "changed"}
    rows, error = check_activations(trace(), before, after, max_gap=1)
    assert error
    assert not rows[0].controller_unchanged


def test_missing_post_activation_evaluations_fail():
    logs = "\n".join(line for line in trace().splitlines() if "memory_host_activation" in line or "00:00:00+" in line)
    rows, error = check_activations(logs, {"uid": "one"}, {"uid": "one"}, max_gap=1)
    assert error
    assert rows[0].evaluations_after == 0
