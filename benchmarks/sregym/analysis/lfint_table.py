"""Per-incident detector-firing table for a late-firing integration pipeline.

Joins, for each stage of an SDO persistent-controller pipeline, the strict
receipt (detector timeline), the result CSV (fault injection time, TTD/TTM),
the receipt's native broker ledger (request findings, surfaced playbooks, reflection decision), the
durable detector-firing stream, and the learned incident detectors in the
chained workspace. Output is JSON on stdout so it can be tabulated.

    uv run python -m benchmarks.sregym.analysis.lfint_table third_party/sregym/logs/<pipeline>
"""

from __future__ import annotations

import csv
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from benchmarks.sregym.analysis.incident_cost import RECEIPT_NAME, pipeline_stage_dirs

# Same predicate as the reflection brief's "reads Event objects" flag (a Watches entry is not a read).
EVENT_PATTERN = re.compile(r"\.(?:Events|RecentEventsFor)\(|\bcorev1\.Event\b")


def _ts(value: str | None) -> float | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _stage_files(stage_dir: Path) -> dict[str, Path]:
    found: dict[str, Path] = {}
    for name, pattern in (
        ("receipt", RECEIPT_NAME),
        ("csv", "*_results.csv"),
        ("resolution", "sdo_incident_resolution.json"),
    ):
        matches = sorted((stage_dir / "runs").rglob(pattern))
        if matches:
            found[name] = matches[0]
    return found


def _firings(stage_dirs: dict[int, Path]) -> list[dict[str, Any]]:
    events: dict[str, dict[str, Any]] = {}
    for stage_dir in stage_dirs.values():
        for path in (stage_dir / "runs").rglob("detector-firings.jsonl"):
            for line in path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    record = json.loads(line)
                    events[str(record.get("event_id"))] = record
    return sorted(events.values(), key=lambda e: e["recorded_at"])


def _detector_sources(workspace: Path) -> dict[str, dict[str, Any]]:
    root = workspace / ".sdo" / "diagnostics" / "detectors" / "incidents"
    out: dict[str, dict[str, Any]] = {}
    if not root.is_dir():
        return out
    for directory in sorted(p for p in root.iterdir() if p.is_dir()):
        source = directory / "detector.go"
        text = source.read_text(encoding="utf-8") if source.exists() else ""
        out[directory.name] = {"reads_events": bool(EVENT_PATTERN.search(text)), "bytes": len(text)}
    return out


def build(pipeline: Path) -> dict[str, Any]:
    stage_dirs = {index: path for index, _name, path in pipeline_stage_dirs(pipeline)}
    firings = _firings(stage_dirs)
    rows: list[dict[str, Any]] = []
    windows: list[tuple[float, float, int]] = []
    for index, stage_dir in sorted(stage_dirs.items()):
        files = _stage_files(stage_dir)
        row: dict[str, Any] = {"stage": index, "name": stage_dir.name}
        if "csv" in files:
            record = next(csv.DictReader(files["csv"].open(encoding="utf-8")))
            row.update(
                ttd_s=float(record["TTL"]) if record.get("TTL") else None,
                ttm_s=float(record["TTM"]) if record.get("TTM") else None,
                diagnosis_ok=record.get("Diagnosis.success"),
                mitigation_ok=record.get("Mitigation.success"),
                fault_injected_at=float(record["fault_injected_at"]),
            )
        resolution = _load(files["resolution"]) if "resolution" in files else {}
        receipt = _load(files["receipt"]) if "receipt" in files else {}
        incident_id = str(receipt.get("incident_id") or resolution.get("incident_id") or "")
        row["incident_id"] = incident_id
        row["strict_receipt"] = bool(receipt)
        ledger = receipt.get("native_ledger") or {}
        closure = ledger.get("closure") or {}
        request = closure.get("request") or {}
        dispatched = _ts(closure.get("dispatched_at")) or _ts(closure.get("detected_at"))
        row["dispatched_at"] = dispatched
        if row.get("fault_injected_at") and dispatched:
            row["dispatch_minus_fault_injected_s"] = round(dispatched - row["fault_injected_at"], 2)
        timeline = receipt.get("detector_timeline") or closure.get("detector_timeline") or []
        row["detector_timeline"] = [
            {
                "detector": e["detector_id"],
                "rule": e.get("rule_id"),
                "class": e.get("detector_class"),
                "relation": e.get("relation"),
                "activated_minus_dispatch_s": round(_ts(e["first_activated_at"]) - dispatched, 2)
                if dispatched
                else None,
                "activated_minus_fault_injected_s": round(_ts(e["first_activated_at"]) - row["fault_injected_at"], 2)
                if row.get("fault_injected_at")
                else None,
                "bindings": {k: v.get("name") for k, v in (e.get("parameter_bindings") or {}).items()},
            }
            for e in timeline
        ]
        for key in (
            "incident_detector_fired_before_dispatch",
            "incident_detector_fired_after_dispatch",
            "no_incident_detector_fired",
        ):
            row[key] = receipt.get(key, closure.get(key))
        row["request_finding_detectors"] = sorted({f["detector_id"] for f in request.get("findings") or []})
        row["request_playbooks"] = [p["playbook_id"] for p in request.get("surfaced_playbooks") or []]
        row["closure_incident_detector_states"] = [
            [s["detector_id"], s.get("status")] for s in closure.get("incident_detector_states") or []
        ]
        row["reflection_decision"] = (ledger.get("reflection_decision") or {}).get("learning_decision")
        rusage = receipt.get("usage") or {}
        row["responder_usage"] = {k: rusage.get(k) for k in ("input_tokens", "output_tokens") if k in rusage}
        nxt = stage_dirs.get(index + 1)
        workspace = (nxt or stage_dir) / "application_workspace"
        row["incident_detectors_after"] = _detector_sources(workspace)
        row["playbook_dirs_after"] = sorted(p.name for p in (workspace / ".sdo" / "playbooks").glob("*") if p.is_dir())
        inject = row.get("fault_injected_at")
        if inject and closure.get("verified_at"):
            windows.append(
                (
                    inject - float(resolution.get("fault_gate_timings_seconds", {}).get("fault_injection_request", 0)),
                    _ts(closure["verified_at"]) or 0.0,
                    index,
                )
            )
        rows.append(row)
    # False-positive control (b): non-health firings outside every incident window.
    healthy_hits = []
    for event in firings:
        if event.get("detector_class") == "health":
            continue
        at = _ts(event["recorded_at"]) or 0.0
        if not any(start <= at <= end for start, end, _ in windows):
            healthy_hits.append(
                {k: event.get(k) for k in ("recorded_at", "event", "detector_id", "rule_id", "fingerprint")}
            )
    return {
        "rows": rows,
        "incident_windows": windows,
        "non_health_events": [
            {
                k: e.get(k)
                for k in (
                    "recorded_at",
                    "event",
                    "detector_id",
                    "rule_id",
                    "fingerprint",
                    "dispatch_relation",
                    "incident_id",
                )
            }
            for e in firings
            if e.get("detector_class") != "health"
        ],
        "non_health_events_outside_incident_windows": healthy_hits,
    }


def main(argv: list[str] | None = None) -> int:
    print(json.dumps(build(Path((argv or sys.argv[1:])[0])), indent=1, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
