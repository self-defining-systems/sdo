"""Synthetic ``sdo-operability`` documents in the shape SDO exposes (built at test time)."""

from __future__ import annotations

import json

from benchmarks.sregym.fastloop.toy_operability import COMPLETE_ITEMS, OperabilityRead, parse_operability

COMMIT = "h0"


def configmap(
    items: dict[str, str] | None = None, *, commit: str = COMMIT, stage: str = "complete", drop: tuple[str, ...] = ()
) -> dict[str, object]:
    status = dict.fromkeys(COMPLETE_ITEMS, "pass") | (items or {})
    summary_items = []
    report_items = []
    for item_id in COMPLETE_ITEMS:
        if item_id in drop:
            continue
        failing = status[item_id] in ("fail", "pending")
        summary_items.append(
            {"id": item_id, "status": status[item_id]}
            | ({"first_failed_check": f"{item_id}.x", "cause": "coverage", "owner": "designer"} if failing else {})
        )
        report_items.append({"id": item_id, "checks": [{"id": f"{item_id}.x", "status": status[item_id]}]})
    summary = {"accepted_commit": commit, "stage": stage, "digest": "sha256:0", "items": summary_items}
    report = {"stage": stage, "subject": {"accepted_commit": commit}, "items": report_items}
    return {"data": {"summary.json": json.dumps(summary), "report.json": json.dumps(report)}}


def green_read(items: dict[str, str] | None = None, **kwargs: object) -> OperabilityRead:
    return parse_operability(configmap(items, **kwargs))  # type: ignore[arg-type]
