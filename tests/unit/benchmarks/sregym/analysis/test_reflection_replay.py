from __future__ import annotations

import json
import subprocess
from hashlib import sha256
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.analysis.reflection_replay import find_ledger, memory_delta, read_outcomes

if TYPE_CHECKING:
    from pathlib import Path


def _git(cwd: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(cwd), *args], check=True, capture_output=True)


def test_find_ledger_matches_an_incident_by_suffix_in_stage_receipts(tmp_path: Path) -> None:
    for name, incident in (("a", "app-111"), ("b", "app-222")):
        run = tmp_path / "runs" / name
        run.mkdir(parents=True)
        receipt = {"schema_version": "sdo.production-receipt/v2", "native_ledger": {"incident_id": incident}}
        (run / "sdo_production_receipt_strict.json").write_text(json.dumps(receipt), encoding="utf-8")

    assert find_ledger(tmp_path, "222")["incident_id"] == "app-222"
    with pytest.raises(SystemExit):
        find_ledger(tmp_path, "333")


def test_read_outcomes_loads_the_incident_keyed_native_store(tmp_path: Path) -> None:
    store = tmp_path / ".sdo" / "outcomes"
    store.mkdir(parents=True)
    for incident in ("app-111", "app-222"):
        document = {"schema_version": 1, "incident_id": incident, "classification": "success"}
        (store / f"{sha256(incident.encode()).hexdigest()}.json").write_text(json.dumps(document), encoding="utf-8")

    assert sorted(record.incident_id for record in read_outcomes(tmp_path)) == ["app-111", "app-222"]


def test_memory_delta_separates_new_from_widened_detectors_and_playbooks(tmp_path: Path) -> None:
    _git(tmp_path, "init", "-q")
    _git(tmp_path, "config", "user.email", "t@example.com")
    _git(tmp_path, "config", "user.name", "t")
    old = tmp_path / ".sdo/diagnostics/detectors/incidents/old_one"
    old.mkdir(parents=True)
    (old / "detector.go").write_text("package old_one\n", encoding="utf-8")
    playbook = tmp_path / ".sdo/playbooks/old-pb"
    playbook.mkdir(parents=True)
    (playbook / "README.md").write_text("x\n", encoding="utf-8")
    _git(tmp_path, "add", "-A")
    _git(tmp_path, "commit", "-q", "-m", "base")

    (old / "detector.go").write_text("package old_one\n// widened\n", encoding="utf-8")
    sibling = tmp_path / ".sdo/diagnostics/detectors/incidents/sibling"
    sibling.mkdir()
    (sibling / "detector.go").write_text("package sibling\n", encoding="utf-8")
    (playbook / "README.md").write_text("y\n", encoding="utf-8")

    delta = memory_delta(tmp_path)

    assert delta["modified_detectors"] == ["old_one"]
    assert delta["new_detectors"] == ["sibling"]
    assert delta["modified_playbooks"] == ["old-pb"]
    assert delta["new_playbooks"] == []
