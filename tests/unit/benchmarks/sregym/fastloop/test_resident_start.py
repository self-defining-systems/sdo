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


def test_onboarding_is_a_fresh_installs_when_it_is_for_the_planned_commit_and_names_the_same_objects():
    """A reseeded controller onboards on the planned memory; the reference was recorded on another commit (the base)."""
    reference = {
        "acceptedCommit": "b" * 40,
        "objects": [{"apiVersion": "apps/v1", "kind": "Deployment", "namespace": "app", "name": "web"}],
    }
    state = fresh_state()
    state["namespace_onboarding"] = {
        "accepted_commit": PLANNED,
        "established_at": AFTER,
        "objects": [{"api_version": "apps/v1", "kind": "Deployment", "namespace": "app", "name": "web"}],
        "environment_objects": [{"digest": "sha256:" + "0" * 64}],
    }
    assert resident_start_violations(start(state=state, onboarding_reference=reference)) == []
    state["namespace_onboarding"]["objects"].append(
        {"api_version": "v1", "kind": "Service", "namespace": "app", "name": "x"}
    )
    assert any(
        "onboarding" in row for row in resident_start_violations(start(state=state, onboarding_reference=reference))
    )
    state["namespace_onboarding"] = {**state["namespace_onboarding"], "accepted_commit": "c" * 40}
    assert any(
        "onboarding" in row for row in resident_start_violations(start(state=state, onboarding_reference=reference))
    )


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
        (mutated("startup", {"began_at": BEFORE}), "startup"),
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
    with pytest.raises(ValueError, match="last_acknowledged_incident_id") as raised:
        lane._require_fresh_equivalent(state, datetime(2026, 1, 1, 0, 10, tzinfo=timezone.utc), PLANNED)
    from benchmarks.sregym.fastloop.resident_start import ResidentStartMismatch

    assert isinstance(raised.value, ResidentStartMismatch)
    assert any("last_acknowledged_incident_id" in row for row in raised.value.violations)


def test_a_restart_from_memory_continues_the_installs_established_period():
    """A reseeded row restarts the same install: its start-up period was established before the boundary and is
    continued (a restart starts no new period); the evaluation state after the boundary is what this row proves."""
    assert resident_start_violations(mutated("startup", {"began_at": BEFORE, "established_at": BEFORE})) == []


def test_pods_and_replicasets_renamed_by_a_rollout_do_not_make_onboarding_differ():
    """Recorded on EKS (fast-eks7 lane s1): three fresh installs of the same application named different
    ``ReplicaSet``/``Pod`` objects (pod-template hash, random suffix), so every reseeded row was refused with
    'onboarding differs from a fresh install's'. Configuration objects still have to match."""
    from benchmarks.sregym.fastloop.resident_start import onboarding_matches

    commit = "a" * 40
    reference = {
        "accepted_commit": commit,
        "objects": [
            "v1/ConfigMap/app/settings",
            "v1/ServiceAccount/app/web",
            "apps/v1/ReplicaSet/app/web-6b7668b4cb",
            "v1/Pod/app/web-6b7668b4cb-qwqxp",
        ],
    }
    record = {
        "acceptedCommit": commit,
        "objects": [
            {"apiVersion": "v1", "kind": "ConfigMap", "namespace": "app", "name": "settings"},
            {"apiVersion": "v1", "kind": "ServiceAccount", "namespace": "app", "name": "web"},
            {"apiVersion": "apps/v1", "kind": "ReplicaSet", "namespace": "app", "name": "web-79d7cd79b7"},
            {"apiVersion": "v1", "kind": "Pod", "namespace": "app", "name": "web-79d7cd79b7-lx6nz"},
        ],
    }
    assert onboarding_matches(record, reference, commit)
    record["objects"] = record["objects"][1:]  # a configuration object is missing
    assert not onboarding_matches(record, reference, commit)
