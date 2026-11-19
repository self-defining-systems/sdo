"""A resident SDO install may start a row only from state a fresh install would also have at injection.

Every carried-over item is named, so the warm transaction falls back to the reinstall instead of starting the row.
"""

import pytest

from benchmarks.sregym.fastloop.resident_start import ResidentStart, resident_start_violations

PLANNED = "a" * 40
BOUNDARY = "2026-01-01T00:10:00Z"
AFTER = "2026-01-01T00:11:00.5Z"
BEFORE = "2026-01-01T00:05:00Z"


def fresh_state():
    """What a fresh install's controller holds when its admission passed, before injection."""
    return {
        "setup_evaluated_revision": PLANNED,
        "dispatch_state": "DISPATCH_STATE_IDLE",
        "pending_batch": {},
        "finding_states": {},
        "drift_events": [],
        "history": [{"detector_id": "workload-health", "evaluated_at": AFTER, "status": "clear"}],
        "detector_clear_since": {"workload-health": AFTER},
        "startup": {"began_at": AFTER, "established_at": AFTER},
        "namespace_onboarding": {"accepted_commit": PLANNED, "objects": ["Deployment/web"]},
        "ownership_adoption": {"source_commit": PLANNED},
    }


def start(**changes):
    values = {
        "state": fresh_state(),
        "planned_commit": PLANNED,
        "accepted_head": PLANNED,
        "boundary": BOUNDARY,
        "control_configmaps": ("sdo-config", "sdo-controller-state", "sdo-namespace-onboarding"),
        "config_digest": "config",
        "expected_config_digest": "config",
        "onboarding_reference": {"accepted_commit": PLANNED, "objects": ["Deployment/web"]},
    }
    values.update(changes)
    return ResidentStart(**values)


def test_a_state_equal_to_a_fresh_install_at_injection_may_start_the_row():
    assert resident_start_violations(start()) == []


def mutated(path, value):
    state = fresh_state()
    state[path] = value
    return start(state=state)


@pytest.mark.parametrize(
    ("case", "reason"),
    [
        (start(accepted_head="b" * 40), "planned memory"),
        (mutated("setup_evaluated_revision", "b" * 40), "evaluated"),
        (mutated("dispatch_state", "DISPATCH_STATE_DISPATCHED"), "dispatch"),
        (mutated("last_acknowledged_incident_id", "app-1"), "last_acknowledged_incident_id"),
        (mutated("pending_closure", {"incident_id": "app-1"}), "pending_closure"),
        (mutated("pending_batch", {"k": 1}), "pending_batch"),
        (mutated("finding_states", {"drift": {}}), "finding_states"),
        (mutated("drift_events", [{"classification": "unexplained"}]), "drift_events"),
        (mutated("failed_closures", ["app-1"]), "failed_closures"),
        (mutated("history", [{"detector_id": "x", "evaluated_at": BEFORE, "status": "clear"}]), "history"),
        (mutated("detector_clear_since", {"x": BEFORE}), "detector_clear_since"),
        (mutated("startup", {"began_at": BEFORE, "established_at": BEFORE}), "startup"),
        (mutated("startup", {"began_at": AFTER}), "startup"),
        (start(onboarding_reference=None), "onboarding"),
        (start(onboarding_reference={"accepted_commit": "b" * 40, "objects": []}), "onboarding"),
        (start(control_configmaps=("sdo-config", "sdo-app-1-abc-request")), "session"),
        (start(control_configmaps=("sdo-reflection-1-result",)), "session"),
        (start(config_digest="other-model"), "config"),
    ],
)
def test_each_carried_over_item_is_named(case, reason):
    violations = resident_start_violations(case)
    assert violations
    assert any(reason in row for row in violations), violations


def test_the_warm_transaction_falls_back_when_the_resident_state_differs(tmp_path, monkeypatch):
    """A warm proof whose state carries over an earlier row raises, so prepare_lane runs the full reset."""
    from datetime import datetime, timezone
    from types import SimpleNamespace

    from benchmarks.sregym.fastloop import warm_environment

    state = fresh_state()
    state["last_acknowledged_incident_id"] = "app-1"
    monkeypatch.setattr(warm_environment, "kubectl", lambda *a, **k: SimpleNamespace(stdout="configmap/sdo-config\n"))
    monkeypatch.setattr(
        warm_environment.PersistentState, "load", lambda path: SimpleNamespace(controllers={"app": None})
    )
    monkeypatch.setattr(warm_environment, "controller_settings", lambda payload: {"model": "m"})
    monkeypatch.setattr(warm_environment, "runtime_config_payload", lambda config: {})
    lane = object.__new__(warm_environment.WarmEnvironment)
    lane.environment = SimpleNamespace(
        kubeconfig=tmp_path / "kube", namespace="app", state_path=tmp_path, run_dir=tmp_path
    )
    lane.control, lane.config, lane.planned_commit = "app-sdo", None, PLANNED
    with pytest.raises(ValueError, match="last_acknowledged_incident_id"):
        lane._require_fresh_equivalent(state, datetime(2026, 1, 1, 0, 10, tzinfo=timezone.utc), PLANNED)
