"""Social slot 2 retry 7 retained no reason for the missing rendered lock.

The adapter's lock-settlement failure must carry the controller's own
reconciliation error, so a live retry can be diagnosed without another hour.
"""

from __future__ import annotations

import json
import subprocess
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.adapter.persistent import PersistentControllerError, _collect_settled_receipt
from tests.unit.benchmarks.sregym.adapter.test_closeout_receipt_race import _clock
from tests.unit.benchmarks.sregym.adapter.test_persistent import FakeOps, _config

if TYPE_CHECKING:
    from pathlib import Path

REASON = "reconcile rendered lock: Deployment/user-service rollout is incomplete"


def test_lock_settlement_failure_surfaces_the_controller_reconciliation_error(tmp_path: Path) -> None:
    config = _config(tmp_path)
    repository = config.repository
    repository.mkdir(parents=True, exist_ok=True)

    def git(*args: str) -> str:
        return subprocess.check_output(["git", "-C", str(repository), *args], text=True).strip()

    git("init", "-q")
    (repository / "app.yaml").write_text("seed\n")
    git("add", ".")
    git("-c", "user.name=test", "-c", "user.email=test@example.invalid", "commit", "-qm", "seed")
    seed = git("rev-parse", "HEAD")
    (repository / "app.yaml").write_text("repaired\n")
    git("add", ".")
    git("-c", "user.name=test", "-c", "user.email=test@example.invalid", "commit", "-qm", "repair")
    lock = repository / ".sdo/rendered/lock.json"
    lock.parent.mkdir(parents=True)
    lock.write_text(json.dumps({"source_commit": seed}))

    class StalledOps(FakeOps):
        def collect_receipt(self, config, incident_id, artifacts_dir):
            return {"schema_version": "sdo.production-receipt/v2", "native_ledger": {"acknowledged": True}}

    ops = StalledOps()
    ops.logs[config.control_namespace] = [REASON]
    with pytest.raises(PersistentControllerError) as raised:
        _collect_settled_receipt(ops, config, "one", tmp_path / "receipt", _clock(ops), timeout=5)
    assert REASON in str(raised.value)
