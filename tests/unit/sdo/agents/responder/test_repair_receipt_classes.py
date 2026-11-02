"""Every mutating responder command class records a receipt reflection can encode.

The Go half (sdo/operator/memory/validate/repair_receipt_classes_test.go) saves
each listed kind as a RunScript program step through the shared validator.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from google.protobuf import json_format

from sdo.agents.responder import recorded_actions
from sdo.contracts import RepairActionReceipt

ROOT = Path(__file__).resolve().parents[5]
CLASSES = json.loads((ROOT / "tests/fixtures/sdo/repair_receipt_classes.json").read_text())


@pytest.mark.parametrize("entry", CLASSES["sdo_act"], ids=lambda entry: entry["kind"])
def test_sdo_act_records_a_linkable_receipt(
    entry: dict[str, object], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    command = [str(part) for part in entry["command"]]  # type: ignore[union-attr]
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    # A synthetic CLI that succeeds and reports nothing, so deletes read as gone.
    fake = bin_dir / command[0]
    fake.write_text("#!/bin/sh\nexit 0\n")
    fake.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}:/usr/bin:/bin")
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path / "scratch"))
    monkeypatch.delenv("SDO_API_HISTORY_DIR", raising=False)
    resource = {"api_version": "v1", "kind": "Pod", "namespace": "app", "name": "web-1"}

    code = recorded_actions.run_and_record(command, [resource], on_completed=lambda *_: None)

    assert code == 0
    lines = recorded_actions.actions_path().read_text().splitlines()
    receipt = json_format.ParseDict(json.loads(lines[-1])["receipt"], RepairActionReceipt())
    assert receipt.kind == entry["kind"]
    assert receipt.success
    assert receipt.started_at.seconds
    assert receipt.completed_at.seconds
    assert receipt.action_id.startswith("sdo-act-")
    assert list(receipt.resources)
    # No script bytes are claimed; reflection links the step by receipt identity.
    assert not receipt.script_digest
