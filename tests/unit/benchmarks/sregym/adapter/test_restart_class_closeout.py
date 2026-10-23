"""Drive adapter closeout from a killed real Go controller and native broker.

Only maintenance transport is fake. Receipt, source commits, outcome, and lock
publication come from the runtime generator, with no fabricated lock writes.
"""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import replace
from pathlib import Path
from typing import Any

from benchmarks.sregym.adapter.persistent import _collect_settled_receipt, _rendered_lock_settled
from benchmarks.sregym.protocol.native_receipt import native_production_receipt
from tests.unit.benchmarks.sregym.adapter.test_persistent import FakeOps, _clock, _config

ROOT = Path(__file__).resolve().parents[5]


def test_restart_class_ack_to_publication_uses_broker_owned_lock(tmp_path: Path) -> None:
    fixture = tmp_path / "native"
    fixture.mkdir()
    command = [
        "go",
        "test",
        "-run",
        "^TestRestartClassHermetic$/seed=1/after-ack-before-publication$",
        "-count=1",
        "-timeout=90s",
        ".",
    ]
    prepared = subprocess.run(
        command,
        cwd=ROOT / "sdo/operator/runtime",
        env={**os.environ, "RT1_CLOSEOUT_FIXTURE": str(fixture)},
        capture_output=True,
        text=True,
        timeout=100,
        check=False,
    )
    assert prepared.returncode == 0, prepared.stdout + prepared.stderr
    environment = dict(item.split("=", 1) for item in json.loads((fixture / "environment.json").read_text()))
    config = replace(_config(tmp_path), repository=fixture / "accepted")
    assert not _rendered_lock_settled(config.repository), "fixture must stop after ACK and before publication"

    class NativeOps(FakeOps):
        def collect_receipt(self, config: Any, incident_id: str, artifacts_dir: Path) -> dict[str, Any]:
            raw = json.loads((fixture / "controller.json.ack-receipt").read_text())
            return native_production_receipt(
                raw["native_ledger"],
                namespace=raw["namespace"],
                control_namespace=raw["control_namespace"],
                controller_image=raw["controller_image"],
                responder_image=raw["responder_image"],
                validator_image=raw["validator_image"],
                controller_pod_uid=raw["controller_pod_uid"],
                responder_job_evidence=raw["responder_job_evidence"],
            )

        def set_maintenance(
            self, control_namespace: str, *, paused: bool, generation: str, preserve_baseline: bool = False
        ) -> None:
            super().set_maintenance(
                control_namespace, paused=paused, generation=generation, preserve_baseline=preserve_baseline
            )
            if paused:
                return
            resumed = subprocess.run(
                ["go", "test", "-run", "^TestRestartClassProcess$", "-count=1", "-timeout=45s", "."],
                cwd=ROOT / "sdo/operator/runtime",
                env={**environment, "RT1_CUT": ""},
                capture_output=True,
                text=True,
                timeout=50,
                check=False,
            )
            assert resumed.returncode == 0, resumed.stdout + resumed.stderr

    ops = NativeOps()
    ops.logs[config.control_namespace] = []
    receipt = _collect_settled_receipt(
        ops, config, environment["RT1_INCIDENT"], tmp_path / "receipt", _clock(ops), timeout=5
    )
    assert _rendered_lock_settled(config.repository), "ACK returned a strict receipt while source lock remained stale"
    assert receipt["native_ledger"]["acknowledged"] is True
    assert receipt["native_ledger"]["outcome_document"]["classification"] == "success"
    assert [event[0] for event in ops.events] == ["active", "paused"]
