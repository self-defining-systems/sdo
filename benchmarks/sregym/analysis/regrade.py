"""Regrade a saved fastloop batch without modifying run outputs or contacting clusters.

Usage: python -m benchmarks.sregym.analysis.regrade --batch PATH --output PATH
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from datetime import datetime
from pathlib import Path
from typing import Any

from benchmarks.sregym.adapter import grade_faults
from benchmarks.sregym.protocol.injection import FaultComponent


def _seconds(start: Any, end: Any) -> float | None:
    if not start or not end:
        return None
    return (
        datetime.fromisoformat(end.replace("Z", "+00:00")) - datetime.fromisoformat(start.replace("Z", "+00:00"))
    ).total_seconds()


def regrade_row(row: dict[str, Any], receipt: dict[str, Any]) -> dict[str, Any]:
    if receipt and receipt.get("incident_id") != row.get("incident_id"):
        raise ValueError("saved receipt does not belong to row incident")
    ledger = receipt.get("native_ledger") or {}
    document = ledger.get("outcome_document") or {}
    components = [FaultComponent.model_validate(item) for item in row.get("fault_components", [])]
    reversions = row.get("fault_reversions") or [item.model_dump() for item in grade_faults(components, None, None)]
    result = {
        "run_id": row.get("run_id"),
        "index": row.get("index"),
        "arm": row.get("ablation_arm"),
        "problem_id": row.get("problem_id"),
        "injection_state": (row.get("injection") or {}).get("state"),
        "infra_error": row.get("infra_error"),
        "oracle_success": (row.get("oracle") or {}).get("success"),
        "resolution_path": row.get("resolution_path")
        or document.get("resolution_path")
        or receipt.get("resolution_path"),
        "faults_reverted": sum(item["status"] == "reverted" for item in reversions),
        "faults_total": len(components),
        "faults_unknown": sum(item["status"] == "unknown" for item in reversions),
        "fault_reversions": reversions,
        "reversion_evidence": "grade-time footprint projections"
        if row.get("fault_reversions")
        else "unknown: no saved grade-time footprint inventory; oracle/repair claims are insufficient",
    }
    injection = row.get("injection") or {}
    for origin, start in [
        ("injection", injection.get("started_at") if injection.get("state") == "injected" else None),
        ("detection", row.get("detected_at")),
    ]:
        for endpoint, key in [("mitigation", "mitigation_applied_at"), ("resolution", "resolved_at")]:
            result[f"{origin}_to_{endpoint}_s"] = _seconds(start, row.get(key))
    return result


def regrade_batch(batch: Path, output: Path) -> None:
    rows = []
    for path in sorted(batch.glob("lanes/*/results/*/incidents.jsonl")):
        for line in path.read_text().splitlines():
            row = json.loads(line)
            receipt = {}
            artifacts = row.get("artifacts_dir")
            if artifacts:
                receipt_path = Path(artifacts) / "sdo_production_receipt_strict.json"
                if receipt_path.is_file():
                    receipt = json.loads(receipt_path.read_text())
            result = regrade_row(row, receipt)
            result["source"] = str(path)
            if not result["arm"]:
                result["arm"] = next(
                    (arm for arm in ["playbooks-only", "detectors-only", "full", "neither"] if arm in path.parent.name),
                    "unknown",
                )
            rows.append(result)
    if not rows:
        raise ValueError(f"no incident rows below {batch}")
    output.mkdir(parents=True, exist_ok=True)
    with (output / "regraded.csv").open("w") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        for row in rows:
            writer.writerow({**row, "fault_reversions": json.dumps(row["fault_reversions"], sort_keys=True)})
    report = [
        "# Saved-evidence regrading",
        "",
        f"Snapshot: {len(rows)} rows. All source outputs are unchanged.",
        "",
        "Exact restoration is independent of SREGym success. Missing grade-time state remains unknown; "
        "zero proven reversions with unknowns does not mean zero repairs. Current lane state and "
        "pre-reset captures taken after cleanup cannot establish historical grade-time restoration.",
        "",
        "Medians below are wall seconds to repair/verified resolution, excluding reflection. "
        "Only injected rows without infra_error contribute to pass rates and timing medians. "
        "Injection origin is injection start. Detection origin uses the recorded first detection "
        "(including the end-of-injection alert in playbooks-only). Missing endpoints are omitted.",
        "",
        "| Arm | eligible / rows | SREGym pass | proven reverted / total (unknown) | "
        "median injection→mitigation | detection→mitigation | injection→resolution | detection→resolution |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for arm in sorted({str(row["arm"]) for row in rows}):
        group = [row for row in rows if row["arm"] == arm]
        eligible = [row for row in group if row["injection_state"] == "injected" and not row["infra_error"]]

        def median(key: str, eligible: list[dict[str, Any]] = eligible) -> str:
            values = [row[key] for row in eligible if row[key] is not None]
            return f"{statistics.median(values):.1f} (n={len(values)})" if values else "unknown (n=0)"

        graded = [row for row in eligible if row["oracle_success"] is not None]
        report.append(
            f"| {arm} | {len(eligible)} / {len(group)} | "
            f"{sum(row['oracle_success'] is True for row in graded)}/{len(graded)} | "
            f"{sum(row['faults_reverted'] for row in eligible)} / "
            f"{sum(row['faults_total'] for row in eligible)} "
            f"({sum(row['faults_unknown'] for row in eligible)} unknown) | "
            + " | ".join(
                median(key)
                for key in [
                    "injection_to_mitigation_s",
                    "detection_to_mitigation_s",
                    "injection_to_resolution_s",
                    "detection_to_resolution_s",
                ]
            )
            + " |"
        )
    (output / "regraded-summary.md").write_text("\n".join(report) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    regrade_batch(args.batch, args.output)


if __name__ == "__main__":
    main()
