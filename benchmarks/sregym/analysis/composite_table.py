"""Per-run table for composite (multi-fault) fast-loop results.

Reads ``composite_<index>_<problem>.json`` (written by ``benchmarks.sregym.fastloop.composite``)
and ``incidents.jsonl`` from one or more fast-loop results directories.

    uv run python -m benchmarks.sregym.analysis.composite_table <results-dir>...
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _load_incidents(directory: Path) -> dict[int, dict[str, Any]]:
    path = directory / "incidents.jsonl"
    if not path.is_file():
        return {}
    records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return {int(record["index"]): record for record in records}


def _normalize(report: dict[str, Any]) -> dict[str, Any]:
    """Reports written before the tracker restarted at injection end measured from agent start."""

    if report.get("origin") == "injection_end":
        return report
    offset = float(report.get("poll_origin_offset_s") or 0.0)

    def shift(value: Any) -> Any:
        return None if value is None else max(0.0, float(value) - offset)

    report = dict(report)
    report["resolved_s"] = {name: shift(value) for name, value in report["resolved_s"].items()}
    report["first_green_s"] = {name: shift(value) for name, value in report.get("first_green_s", {}).items()}
    report["all_resolved_s"] = shift(report.get("all_resolved_s"))
    report["timeline"] = [{**sample, "t": shift(sample["t"])} for sample in report.get("timeline", [])]
    report["origin"] = "injection_end"
    return report


def _final_green_runs(report: dict[str, Any]) -> dict[str, float | None]:
    """Start of each fault's final unbroken green run, with no minimum duration.

    The tracker's own ``resolved_s`` needs the run to last ``stable_seconds``; an agent that fixes a
    fault and returns within that window would otherwise count as unresolved, so tables use this.
    """

    result: dict[str, float | None] = dict.fromkeys(report["resolved_s"])
    for sample in report.get("timeline", []):
        for name, green in sample["states"].items():
            if not green:
                result[name] = None
            elif result.get(name) is None:
                result[name] = float(sample["t"])
    return result


def rows(directories: list[Path]) -> list[dict[str, Any]]:
    table: list[dict[str, Any]] = []
    for directory in directories:
        incidents = _load_incidents(directory)
        for report_path in sorted(directory.glob("composite_*.json")):
            report = _normalize(json.loads(report_path.read_text(encoding="utf-8")))
            if report.get("timeline"):
                report["resolved_s"] = _final_green_runs(report)
                report["faults_resolved"] = sum(1 for value in report["resolved_s"].values() if value is not None)
                report["all_resolved_s"] = (
                    None
                    if any(value is None for value in report["resolved_s"].values())
                    else max(report["resolved_s"].values())
                )
            index = int(report_path.name.split("_")[1])
            record = incidents.get(index, {})
            usage = record.get("responder_tokens") or {}
            reflection = record.get("reflection_tokens") or {}
            oracle = (record.get("oracle") or {}).get("details", {})
            table.append(
                {
                    "run": directory.name,
                    "index": index,
                    "agent": report.get("agent"),
                    "problem": report.get("problem_id"),
                    "resolved": (
                        f"{report['faults_resolved']}/{report['faults_total']}"
                        if report.get("tracking_supported", True)
                        else "untracked"
                    ),
                    "tracking_supported": report.get("tracking_supported", True),
                    "faults_total": oracle.get("faults_total", report.get("faults_total")),
                    "faults_fixed": oracle.get("faults_fixed"),
                    "oracle_invalid": oracle.get("oracle_invalid", 0),
                    "faults_diagnosed": (record.get("diagnosis_grade") or {})
                    .get("details", {})
                    .get("faults_diagnosed"),
                    "composite_valid": oracle.get("composite_valid"),
                    "per_fault_details": {key: value for key, value in oracle.items() if key.startswith("fault_")},
                    "all_resolved_s": report.get("all_resolved_s"),
                    "per_fault_s": report.get("resolved_s"),
                    "inert_faults": [name for name, red in (report.get("ever_red") or {}).items() if not red],
                    "oracle_accuracy": oracle.get("accuracy"),
                    "oracle_success": (record.get("oracle") or {}).get("success"),
                    "stop_reason": report.get("stop_reason"),
                    "incidents": len(report.get("incidents", [])) or None,
                    "tokens": (usage.get("total_tokens") or 0) + (reflection.get("total_tokens") or 0),
                    "responder_tokens": usage.get("total_tokens"),
                    "error": (record.get("error") or "")[:80] or None,
                }
            )
    return table


def format_table(table: list[dict[str, Any]]) -> str:
    if not table:
        return "no composite reports found"
    header = [
        "run",
        "#",
        "agent",
        "resolved",
        "all_s",
        "oracle",
        "stop",
        "incidents",
        "tokens",
        "per-fault resolution (s)",
        "inert",
    ]
    lines = [header]
    for row in table:
        per_fault = ", ".join(
            f"{name.split(':')[0]}:{'-' if value is None else f'{value:.0f}'}"
            for name, value in row["per_fault_s"].items()
        )
        lines.append(
            [
                str(row["run"]),
                str(row["index"]),
                str(row["agent"]),
                row["resolved"],
                "-" if row["all_resolved_s"] is None else f"{row['all_resolved_s']:.0f}",
                str(row["oracle_success"]),
                str(row["stop_reason"] or "-"),
                str(row["incidents"] or "-"),
                str(row["tokens"]),
                per_fault,
                ",".join(name.split(":")[0] for name in row["inert_faults"]) or "-",
            ]
        )
    widths = [max(len(line[column]) for line in lines) for column in range(len(header))]
    return "\n".join("  ".join(cell.ljust(width) for cell, width in zip(line, widths, strict=True)) for line in lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directories", type=Path, nargs="+")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    table = rows([path for directory in args.directories for path in ([directory, *sorted(directory.glob("*/"))])])
    print(json.dumps(table, indent=2) if args.json else format_table(table))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
