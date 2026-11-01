"""Fail-closed checks of timestamped controller evaluations around host activation."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from pydantic import AwareDatetime, BaseModel


class ActivationContinuity(BaseModel):
    accepted_commit: str
    activated_at: AwareDatetime
    activation_latency_ms: int
    evaluation_count: int
    evaluations_before: int
    evaluations_after: int
    max_evaluation_gap_seconds: float | None
    allowed_gap_seconds: float
    controller_before: dict[str, Any]
    controller_after: dict[str, Any]
    controller_unchanged: bool
    passed: bool


def check_activations(
    logs: str,
    before: dict[str, Any],
    after: dict[str, Any],
    *,
    max_gap: float,
    expected_commits: set[str] | None = None,
) -> tuple[list[ActivationContinuity], str | None]:
    events = []
    for line in logs.splitlines():
        try:
            stamp, payload = line.split(" ", 1)
            events.append((datetime.fromisoformat(stamp.replace("Z", "+00:00")), json.loads(payload)))
        except (ValueError, TypeError):
            continue
    rows = []
    errors = []
    for at, event in events:
        if "memory_host_activation" not in event:
            continue
        # Cover the complete build/catch-up interval, plus a cadence margin on each side.
        start = at.timestamp() - event["activation_latency_ms"] / 1000 - 2 * max_gap
        end = at.timestamp() + 2 * max_gap
        window = [(t, d) for t, d in events if start <= t.timestamp() <= end]
        evaluations = [(t, d["controller_iteration"]) for t, d in window if "controller_iteration" in d]
        gaps = [(b[0] - a[0]).total_seconds() for a, b in zip(evaluations, evaluations[1:], strict=False)]
        n_before = sum(t < at for t, _ in evaluations)
        n_after = sum(t > at for t, _ in evaluations)
        unchanged = bool(before) and before == after
        passed = (
            unchanged
            and n_before > 0
            and n_after > 0
            and bool(gaps)
            and max(gaps) <= max_gap
            and all(b[1] == a[1] + 1 for a, b in zip(evaluations, evaluations[1:], strict=False))
            and not any(d.get("controller_maintenance") == "paused" for _, d in window)
            and (evaluations[0][0].timestamp() - start <= max_gap)
            and (end - evaluations[-1][0].timestamp() <= max_gap)
        )
        rows.append(
            ActivationContinuity(
                accepted_commit=event["memory_host_activation"],
                activated_at=at,
                activation_latency_ms=event["activation_latency_ms"],
                evaluation_count=len(evaluations),
                evaluations_before=n_before,
                evaluations_after=n_after,
                max_evaluation_gap_seconds=max(gaps) if gaps else None,
                allowed_gap_seconds=max_gap,
                controller_before=before,
                controller_after=after,
                controller_unchanged=unchanged,
                passed=passed,
            )
        )
        if not passed:
            errors.append(f"detection continuity failed at {event['memory_host_activation']}")
    missing = (expected_commits or set()) - {row.accepted_commit for row in rows}
    if missing:
        errors.append(f"missing activation evidence: {sorted(missing)}")
    return rows, "; ".join(errors) or None
