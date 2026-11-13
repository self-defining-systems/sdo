"""Pausing SDO is refused for incident work, never for a restarted controller's start-up hold."""

from __future__ import annotations

from typing import Any

import pytest

from benchmarks.sregym.adapter import persistent
from benchmarks.sregym.adapter.eval_guards import incident_work_pending

BOUND = "2099-01-01T00:00:00Z"


def _startup_state(**extra: Any) -> dict[str, Any]:
    return {
        "busy_until": BOUND,
        "startup": {"began_at": "2098-12-31T00:00:00Z", "bound_at": BOUND},
        **extra,
    }


@pytest.mark.parametrize(
    ("state", "pending"),
    [
        (_startup_state(), False),  # a restarted controller re-entering its persisted start-up period
        (_startup_state(incident_open=True), True),
        (_startup_state(pending_closure={"request": {}}), True),
        (_startup_state(queued_reflection_incident_ids=["i"]), True),
        (_startup_state(startup={"began_at": "a", "bound_at": BOUND, "established_at": "b"}), True),
        ({"busy_until": "2000-01-01T00:00:00Z", "pending_closure": {"request": {}}}, True),
        ({}, False),
    ],
)
def test_pause_guard_ignores_only_a_startup_hold(state: dict[str, Any], pending: bool) -> None:
    assert incident_work_pending(state) is pending


def test_pause_is_not_refused_by_a_startup_hold(monkeypatch: pytest.MonkeyPatch) -> None:
    ops = persistent.KubectlClusterOps()
    calls: list[bool] = []
    monkeypatch.setattr(persistent, "go_set_maintenance", lambda *_a, paused, **_k: calls.append(paused))
    monkeypatch.setattr(ops, "runtime_state", lambda _control: _startup_state())
    ops.set_maintenance("c-sdo", paused=True, generation="g")
    assert calls == [True]

    monkeypatch.setattr(ops, "runtime_state", lambda _control: _startup_state(incident_open=True))
    with pytest.raises(persistent.PersistentControllerError, match="cannot pause"):
        ops.set_maintenance("c-sdo", paused=True, generation="g2")
