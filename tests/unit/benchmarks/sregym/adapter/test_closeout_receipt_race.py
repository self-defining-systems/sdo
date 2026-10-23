"""Reproduction for the intermittent strict-receipt rejection on solved composites.

The luna variance run surfaced a close-out race: on multi-fault COMPOSITE
incidents the strict-receipt gate rejected a receipt whose incident the
mitigation oracle had SOLVED. Two facets were observed, both only on composites
(single-fault incidents settle in one closure, so the window never opens):

* Seed A / i07: ``completed=false`` while every detector was clear and the
  outcome was committed -- the gate sampled the durable responder result before
  it reached a terminal ``completed`` status.
* Seed B / i10: ``remaining_worktrees != []`` -- the gate sampled the shared
  worktree directory before the controller's asynchronous close-out had drained
  the incident's worktree.

Both are timing races: ``drain_pending_incident`` samples the receipt exactly
once, immediately after ``_wait_for_reflection_drain`` returns, and that drain
predicate (acknowledged + one relaunch-after-closure) does not guarantee the two
volatile close-out signals have settled. Composites open the window because they
chain follow-up responders and closure retries, so the predicate can fire during
an intermediate relaunch while the final close-out is still settling.

These tests drive the public ``drain_pending_incident`` with the in-memory
cluster fake and a seeded clock (no Kubernetes), reusing the chaos-pattern seam:
model a close-out signal that is unsettled at the instant the drain predicate
fires and settles a few polls later. The fix makes the drain wait for the
close-out to settle before it samples, so a solved incident is accepted.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.sregym.adapter.persistent import (
    STRICT_RECEIPT_FILENAME,
    PersistentState,
    drain_pending_incident,
)
from tests.unit.benchmarks.sregym.adapter.test_persistent import FakeOps, _clock, _run

if TYPE_CHECKING:
    from pathlib import Path

    from benchmarks.sregym.adapter.runtime import RuntimeConfig


class _LateCompletingOps(FakeOps):
    """A responder result that reaches ``completed`` a few polls after the relaunch.

    The reflection drain predicate (acknowledged + relaunch) fires immediately,
    but the durable incident-result ConfigMap still reports a non-terminal status
    for ``completed_settle`` more close-out samples. This is the ``completed``
    race from seed A, expressed deterministically.
    """

    completed_settle: dict[str, int]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.completed_settle = {}

    def collect_receipt(self, config: RuntimeConfig, incident_id: str, artifacts_dir: Path) -> dict[str, Any]:
        receipt = super().collect_receipt(config, incident_id, artifacts_dir)
        remaining = self.completed_settle.get(incident_id, 0)
        if remaining > 0:
            self.completed_settle[incident_id] = remaining - 1
            receipt["completed"] = False
        return receipt


class _LateDrainingWorktreeOps(FakeOps):
    """A worktree the controller's close-out drains a few polls after the relaunch.

    The shared ``/workspace/worktrees`` directory still lists the incident's
    worktree for ``worktree_settle`` more close-out samples. This is the
    ``remaining_worktrees`` race from seed B, expressed deterministically as a
    transient drain lag.
    """

    worktree_settle: dict[str, int]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.worktree_settle = {}

    def collect_receipt(self, config: RuntimeConfig, incident_id: str, artifacts_dir: Path) -> dict[str, Any]:
        receipt = super().collect_receipt(config, incident_id, artifacts_dir)
        remaining = self.worktree_settle.get(incident_id, 0)
        if remaining > 0:
            self.worktree_settle[incident_id] = remaining - 1
            receipt["remaining_worktrees"] = [f"/workspace/worktrees/{incident_id}-draining"]
        return receipt


def _drained_record(tmp_path: Path, ops: FakeOps):
    """Run one stage that leaves a pending, reflectable incident ready to drain."""

    _run(tmp_path, ops, "s0", [])
    ops.reflectable.add("incident-1")
    state = PersistentState.load(tmp_path / "sdo_persistent_controller.json")
    return state.controllers["hotel"]


def test_drain_waits_for_the_responder_result_to_report_completed(tmp_path: Path) -> None:
    ops = _LateCompletingOps()
    ops.completed_settle["incident-1"] = 2
    record = _drained_record(tmp_path, ops)

    # The incident was solved; the receipt must be accepted, not rejected for a
    # transiently non-terminal responder result.
    drain_pending_incident(
        record,
        ops=ops,
        repository=tmp_path / "s0" / "application_workspace",
        drained_by="next-stage",
        clock=_clock(ops),
    )

    assert (record.pending.receipt_dir / STRICT_RECEIPT_FILENAME).is_file()


def test_drain_waits_for_the_worktree_to_drain(tmp_path: Path) -> None:
    ops = _LateDrainingWorktreeOps()
    ops.worktree_settle["incident-1"] = 2
    record = _drained_record(tmp_path, ops)

    drain_pending_incident(
        record,
        ops=ops,
        repository=tmp_path / "s0" / "application_workspace",
        drained_by="next-stage",
        clock=_clock(ops),
    )

    assert (record.pending.receipt_dir / STRICT_RECEIPT_FILENAME).is_file()


@pytest.mark.parametrize("publish", [True, False])
def test_closeout_waits_for_rendered_lock_after_acknowledged_source_repair(tmp_path: Path, publish: bool) -> None:
    import json
    import subprocess

    from benchmarks.sregym.adapter.persistent import PersistentControllerError, _collect_settled_receipt
    from tests.unit.benchmarks.sregym.adapter.test_persistent import _config

    config = _config(tmp_path)
    repository = config.repository
    repository.mkdir(parents=True, exist_ok=True)

    def git(*args: str) -> str:
        return subprocess.check_output(["git", "-C", str(repository), *args], text=True).strip()

    git("init", "-q")
    source = repository / "app.yaml"
    source.write_text("seed\n")
    git("add", ".")
    git("-c", "user.name=test", "-c", "user.email=test@example.invalid", "commit", "-qm", "seed")
    seed = git("rev-parse", "HEAD")
    source.write_text("repaired\n")
    git("add", ".")
    git("-c", "user.name=test", "-c", "user.email=test@example.invalid", "commit", "-qm", "repair")
    repair = git("rev-parse", "HEAD")
    lock = repository / ".sdo/rendered/lock.json"
    lock.parent.mkdir(parents=True)
    lock.write_text(json.dumps({"source_commit": seed}))

    class PublishingOps(FakeOps):
        samples = 0

        def collect_receipt(self, config, incident_id, artifacts_dir):
            self.samples += 1
            if publish and self.samples == 3:
                lock.write_text(json.dumps({"source_commit": repair}))
            return {"schema_version": "sdo.production-receipt/v2", "native_ledger": {"acknowledged": True}}

    ops = PublishingOps()
    ops.logs[config.control_namespace] = []
    if not publish:
        with pytest.raises(PersistentControllerError, match="current rendered source lock"):
            _collect_settled_receipt(ops, config, "one", tmp_path / "receipt", _clock(ops), timeout=5)
        assert ops.samples == 6
        assert [event[0] for event in ops.events] == ["active", "paused"]
        return
    receipt = _collect_settled_receipt(ops, config, "one", tmp_path / "receipt", _clock(ops), timeout=5)
    assert receipt["native_ledger"]["acknowledged"]
    assert ops.samples == 3
    assert [event[0] for event in ops.events] == ["active", "paused"]
