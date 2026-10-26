from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import pytest

from benchmarks.sregym.fastloop.fault_tracker import (
    FaultTracker,
    composite_faults,
    describe_sregym_faults,
    probes_for,
    stream_faults,
)
from benchmarks.sregym.fastloop.stream import load_stream


def test_fault_description_uses_the_deployed_application_source(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from benchmarks.sregym.fastloop import fault_tracker

    source = tmp_path / "application_workspace"

    def run(_command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        assert kwargs["env"]["SREGYM_APP_SOURCE_DIR"] == str(source)
        return subprocess.CompletedProcess(
            [],
            0,
            stdout=json.dumps(
                {
                    "paired": {
                        "schema_version": "sdo.sregym-fault-description/v1",
                        "problem_id": "paired",
                        "faults": [{"kind": "target_port", "component": "geo"}],
                        "injection_order": [0],
                    }
                }
            ),
        )

    monkeypatch.setattr(fault_tracker.subprocess, "run", run)
    assert describe_sregym_faults(("paired",), sregym_dir=tmp_path, app_source_dir=source) == {
        "paired": (fault_tracker.FaultSpec("target_port", "geo"),)
    }


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def _tracker(states: dict[str, list[bool]], clock: _Clock, stable: float = 20.0) -> FaultTracker:
    iterators = {name: iter(values) for name, values in states.items()}
    probes = {name: (lambda it=it: next(it)) for name, it in iterators.items()}
    return FaultTracker(probes, stable_seconds=stable, clock=clock)


def _run(tracker: FaultTracker, clock: _Clock, steps: int, interval: float = 10.0) -> None:
    for _ in range(steps):
        tracker.poll()
        clock.now += interval


def test_resolution_is_the_start_of_the_final_unbroken_green_run() -> None:
    clock = _Clock()
    tracker = _tracker(
        {"a": [False, True, True, True, True], "b": [False, False, False, True, True]}, clock, stable=10.0
    )
    _run(tracker, clock, 5)
    times = tracker.resolution_times()
    assert times == {"a": 10.0, "b": 30.0}
    assert tracker.all_resolved_at() == 30.0


def test_a_flap_after_green_invalidates_the_earlier_resolution() -> None:
    clock = _Clock()
    tracker = _tracker({"a": [True, True, True, False, True, True, True]}, clock)
    _run(tracker, clock, 7)
    assert tracker.resolution_times() == {"a": 40.0}
    assert tracker.first_green() == {"a": 0.0}


def test_green_shorter_than_the_stability_window_is_not_resolved() -> None:
    clock = _Clock()
    tracker = _tracker({"a": [False, True]}, clock, stable=20.0)
    _run(tracker, clock, 2)
    assert tracker.resolution_times() == {"a": None}
    assert tracker.all_resolved_at() is None
    assert tracker.all_green_now() is True


def test_unresolved_fault_blocks_all_resolved() -> None:
    clock = _Clock()
    tracker = _tracker({"a": [True, True, True], "b": [False, False, False]}, clock)
    _run(tracker, clock, 3)
    assert tracker.resolution_times() == {"a": 0.0, "b": None}
    assert tracker.all_resolved_at() is None
    assert tracker.all_green_now() is False


def test_probe_errors_count_as_unresolved_and_are_recorded() -> None:
    clock = _Clock()

    def broken() -> bool:
        raise RuntimeError("api down")

    tracker = FaultTracker({"a": broken}, stable_seconds=0.0, clock=clock)
    tracker.poll()
    assert tracker.samples[-1].states == {"a": False}
    assert "api down" in tracker.samples[-1].errors["a"]


def test_registered_composites_declare_faults_in_injection_order() -> None:
    assert [fault.kind for fault in composite_faults("composite3_hotel_geo_rate_recommendation")] == [
        "readiness",
        "configmap",
        "network_policy",
    ]
    five = composite_faults("composite5_hotel_geo_rate_recommendation_frontend_user")
    assert [(fault.kind, fault.component) for fault in five] == [
        ("readiness", "geo"),
        ("configmap", "mongodb-rate"),
        ("network_policy", "recommendation"),
        ("wrong_selector", "frontend"),
        ("resource_request", "user"),
    ]
    assert len({fault.component for fault in five}) == 5
    variant = composite_faults("composite3b_hotel_profile_mongodb_geo_recommendation")
    assert [(fault.kind, fault.component) for fault in variant] == [
        ("readiness", "profile"),
        ("configmap", "mongodb-geo"),
        ("network_policy", "recommendation"),
    ]
    assert composite_faults("missing_configmap_hotel_reservation") == ()


def _deployment(ready: int, replicas: int = 1, *, updated: int | None = None) -> dict[str, Any]:
    return {
        "metadata": {"generation": 2},
        "spec": {"replicas": replicas},
        "status": {
            "observedGeneration": 2,
            "readyReplicas": ready,
            "updatedReplicas": replicas if updated is None else updated,
            "replicas": replicas,
        },
    }


def _reader(objects: dict[tuple[str, str], dict[str, Any] | None]):
    return lambda kind, name: objects.get((kind, name))


def test_probes_report_per_fault_state_from_cluster_objects() -> None:
    faults = composite_faults("composite5_hotel_geo_rate_recommendation_frontend_user")
    blocking = {
        "spec": {"podSelector": {"matchLabels": {"io.kompose.service": "recommendation"}}, "ingress": [], "egress": []}
    }
    cluster = {
        ("deployment", "geo"): _deployment(0),
        ("deployment", "mongodb-rate"): _deployment(1),
        ("networkpolicy", "deny-all-recommendation"): blocking,
        ("endpoints", "frontend"): {"subsets": None},
        ("deployment", "user"): _deployment(1, updated=0),
        ("deployment", "recommendation"): _deployment(1),
    }
    probes = probes_for(faults, _reader(cluster))
    assert {name: probe() for name, probe in probes.items()} == {
        "readiness:geo": False,
        "configmap:mongodb-rate": True,
        "network_policy:recommendation": False,
        "wrong_selector:frontend": False,
        "resource_request:user": False,
    }
    cluster[("deployment", "geo")] = _deployment(1)
    cluster[("networkpolicy", "deny-all-recommendation")] = None
    cluster[("endpoints", "frontend")] = {"subsets": [{"addresses": [{"ip": "10.0.0.1"}]}]}
    cluster[("deployment", "user")] = _deployment(1)
    assert all(probe() for probe in probes.values())


def test_a_network_policy_that_allows_traffic_no_longer_blocks() -> None:
    faults = composite_faults("composite3_hotel_geo_rate_recommendation")
    relaxed = {"spec": {"podSelector": {"matchLabels": {}}, "ingress": [{}], "egress": [{}]}}
    cluster: dict[tuple[str, str], dict[str, Any] | None] = {
        ("networkpolicy", "deny-all-recommendation"): relaxed,
        ("deployment", "recommendation"): _deployment(1),
    }
    probe = probes_for(faults, _reader(cluster))["network_policy:recommendation"]
    assert probe() is True


def test_unreadable_deployment_is_unresolved() -> None:
    faults = composite_faults("composite3_hotel_geo_rate_recommendation")
    probe = probes_for(faults, _reader({}))["readiness:geo"]
    assert probe() is False


def test_unknown_fault_kind_is_rejected() -> None:
    from benchmarks.sregym.fastloop.fault_tracker import FaultSpec

    with pytest.raises(ValueError, match="no probe"):
        probes_for((FaultSpec("mystery", "x"),), _reader({}))


def test_all_playbook_stream_ids_resolve_to_registered_fault_probes_without_a_cluster() -> None:
    root = Path(__file__).resolve().parents[5]
    chart = (
        root / "third_party" / "sregym" / "SREGym-applications" / "hotelReservation" / "helm-chart" / "hotelreservation"
    )
    if not chart.is_dir():
        pytest.skip("SREGym application chart is not initialized in this worktree")
    streams = [
        load_stream(root / "tests" / "fixtures" / "sregym" / "fastloop" / f"pbprog_{name}.json")
        for name in ("smoke", "explicit", "repeat", "compose")
    ]
    problem_ids = tuple(dict.fromkeys(problem_id for stream in streams for ids in stream for problem_id in ids))
    catalog = describe_sregym_faults(problem_ids, sregym_dir=root / "third_party" / "sregym")

    for stream in streams:
        for ids in stream:
            faults = stream_faults(ids, catalog=catalog)
            assert faults
            if all(fault.tracking_supported for fault in faults):
                # Field probes capture a healthy API reference before injection.
                def baseline_reader(kind, name):
                    if kind == "deployment":
                        return {
                            **_deployment(1),
                            "spec": {
                                "replicas": 1,
                                "template": {"spec": {"containers": [{"name": "app", "image": "good"}]}},
                            },
                        }
                    return None

                assert len(probes_for(faults, baseline_reader)) == len(faults)
            else:
                with pytest.raises(ValueError, match="tracking unsupported"):
                    probes_for(faults, _reader({}))
            if ids[0].startswith("mf__"):
                assert len(faults) == 2

    policy = catalog["network_policy_block__v_hotel_reservation_search_both_descriptive"][0]
    assert policy.policy_name == "deny-all-search"
    objects = {
        ("deployment", "search"): _deployment(1),
        ("networkpolicy", policy.policy_name): {
            "spec": {"podSelector": {"matchLabels": {policy.pod_label_key: "search"}}, "ingress": [], "egress": []}
        },
    }
    probe = probes_for((policy,), _reader(objects))[policy.name]
    assert probe() is False
    objects[("networkpolicy", policy.policy_name)] = None
    assert probe() is True


def test_restart_makes_now_time_zero_and_drops_earlier_samples() -> None:
    clock = _Clock()
    tracker = _tracker({"a": [False, True, True, True]}, clock, stable=10.0)
    _run(tracker, clock, 1)
    tracker.restart()
    clock.now += 5.0
    _run(tracker, clock, 3)
    assert tracker.resolution_times() == {"a": 5.0}


def test_ever_red_flags_a_fault_whose_injection_had_no_effect() -> None:
    clock = _Clock()
    tracker = _tracker({"a": [True, True, True], "b": [True, False, True]}, clock, stable=1.0)
    _run(tracker, clock, 3)
    assert tracker.ever_red() == {"a": False, "b": True}
