from __future__ import annotations

import json
from typing import TYPE_CHECKING

from benchmarks.sregym.analysis.composite_table import format_table, rows

if TYPE_CHECKING:
    from pathlib import Path


def test_table_joins_the_composite_report_with_the_incident_record(tmp_path: Path) -> None:
    (tmp_path / "composite_000_composite3_x.json").write_text(
        json.dumps(
            {
                "agent": "codex",
                "problem_id": "composite3_x",
                "faults_resolved": 2,
                "faults_total": 3,
                "all_resolved_s": None,
                "resolved_s": {
                    "readiness:geo": 40.0,
                    "configmap:mongodb-rate": 90.0,
                    "network_policy:recommendation": None,
                },
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "incidents.jsonl").write_text(
        json.dumps(
            {
                "index": 0,
                "oracle": {"success": False, "details": {"accuracy": 66.67}},
                "responder_tokens": {"total_tokens": 100},
                "reflection_tokens": {"total_tokens": 5},
            }
        )
        + "\n",
        encoding="utf-8",
    )

    table = rows([tmp_path])

    assert table[0]["resolved"] == "2/3"
    assert table[0]["tokens"] == 105
    assert table[0]["oracle_success"] is False
    text = format_table(table)
    assert "readiness:40" in text
    assert "network_policy:-" in text


def test_reports_from_before_the_injection_end_origin_are_shifted(tmp_path: Path) -> None:
    (tmp_path / "composite_000_c.json").write_text(
        json.dumps(
            {
                "agent": "sdo",
                "problem_id": "c",
                "faults_resolved": 1,
                "faults_total": 1,
                "all_resolved_s": 400.0,
                "poll_origin_offset_s": 350.0,
                "resolved_s": {"readiness:geo": 400.0},
            }
        ),
        encoding="utf-8",
    )

    table = rows([tmp_path])

    assert table[0]["per_fault_s"] == {"readiness:geo": 50.0}
    assert table[0]["all_resolved_s"] == 50.0


def test_an_agent_that_returns_right_after_a_fix_still_counts_the_fault_resolved(tmp_path: Path) -> None:
    timeline = [
        {"t": 0.0, "states": {"a": False, "b": False}},
        {"t": 5.0, "states": {"a": True, "b": False}},
        {"t": 10.0, "states": {"a": True, "b": True}},
    ]
    (tmp_path / "composite_000_c.json").write_text(
        json.dumps(
            {
                "agent": "codex",
                "problem_id": "c",
                "origin": "injection_end",
                "faults_resolved": 1,
                "faults_total": 2,
                "all_resolved_s": None,
                "resolved_s": {"a": 5.0, "b": None},
                "timeline": timeline,
            }
        ),
        encoding="utf-8",
    )

    table = rows([tmp_path])

    assert table[0]["resolved"] == "2/2"
    assert table[0]["all_resolved_s"] == 10.0


def test_invalid_oracle_count_is_reported_separately_from_fixed_faults(tmp_path: Path) -> None:
    (tmp_path / "composite_000_synthetic.json").write_text(
        json.dumps(
            {
                "agent": "sdo",
                "problem_id": "synthetic",
                "faults_total": 2,
                "faults_resolved": 1,
                "origin": "injection_end",
                "resolved_s": {"synthetic": 1.0},
            }
        )
    )
    (tmp_path / "incidents.jsonl").write_text(
        json.dumps(
            {
                "index": 0,
                "oracle": {
                    "success": True,
                    "details": {
                        "faults_total": 1,
                        "faults_fixed": 1,
                        "oracle_invalid": 1,
                    },
                },
            }
        )
        + "\n"
    )
    row = rows([tmp_path])[0]
    assert (row["faults_total"], row["faults_fixed"], row["oracle_invalid"]) == (1, 1, 1)
