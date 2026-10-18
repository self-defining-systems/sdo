from __future__ import annotations

import json
from typing import TYPE_CHECKING

from benchmarks.sregym.analysis.lfint_table import build

if TYPE_CHECKING:
    from pathlib import Path


def test_rows_read_the_native_ledger_carried_by_the_strict_receipt(tmp_path: Path) -> None:
    stage = tmp_path / "stage_0_example"
    run = stage / "runs" / "one"
    run.mkdir(parents=True)
    (stage / "application_workspace").mkdir()
    ledger = {
        "incident_id": "incident-1",
        "closure": {
            "dispatched_at": "2026-01-01T00:00:10Z",
            "request": {
                "findings": [{"detector_id": "health-objective"}],
                "surfaced_playbooks": [{"playbook_id": "fix-cache"}],
            },
        },
        "reflection_decision": {"learning_decision": "updated"},
    }
    receipt = {"schema_version": "sdo.production-receipt/v2", "incident_id": "incident-1", "native_ledger": ledger}
    (run / "sdo_production_receipt_strict.json").write_text(json.dumps(receipt), encoding="utf-8")

    row = build(tmp_path)["rows"][0]

    assert row["request_finding_detectors"] == ["health-objective"]
    assert row["request_playbooks"] == ["fix-cache"]
    assert row["reflection_decision"] == "updated"
    assert row["dispatched_at"] is not None
