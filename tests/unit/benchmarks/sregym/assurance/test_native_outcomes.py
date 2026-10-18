"""Assurance reads the native, incident-keyed outcome format."""

from __future__ import annotations

import json
from hashlib import sha256
from typing import TYPE_CHECKING

from benchmarks.sregym.assurance.harness import _evidenced_outcome, _outcome_file, _read_outcome, _read_outcome_text

if TYPE_CHECKING:
    from pathlib import Path


def test_reads_only_the_requested_native_outcome(tmp_path: Path) -> None:
    outcomes = tmp_path / ".sdo" / "outcomes"
    outcomes.mkdir(parents=True)
    requested = outcomes / f"{sha256(b'incident-1').hexdigest()}.json"
    requested.write_text(json.dumps({"schema_version": 1, "incident_id": "incident-1", "classification": "success"}))
    (tmp_path / ".sdo" / "outcomes.jsonl").write_text(
        json.dumps({"incident_id": "incident-1", "classification": "external_recovery"}) + "\n"
    )

    assert _outcome_file("incident-1") == f".sdo/outcomes/{requested.name}"
    assert _read_outcome(tmp_path, "incident-1")["classification"] == "success"
    assert _read_outcome(tmp_path, "incident-2") is None


def test_rejects_wrong_incident_or_malformed_native_outcome() -> None:
    assert _read_outcome_text('{"incident_id":"other"}', "incident-1") is None
    assert _read_outcome_text('{"incident_id":', "incident-1") is None


def test_prefers_correlated_go_evidence_when_host_checkout_has_not_synced(tmp_path: Path) -> None:
    evidence = {
        "native_ledger": {
            "outcome_document": {"schema_version": 1, "incident_id": "incident-1", "classification": "success"}
        }
    }
    assert _evidenced_outcome(evidence, tmp_path, "incident-1")["classification"] == "success"
    assert _evidenced_outcome(evidence, tmp_path, "incident-2") is None
