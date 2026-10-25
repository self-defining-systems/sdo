"""Admission fixtures reproduce unbounded Jaeger and a leftover foreign app."""

import json
import subprocess
from datetime import datetime, timezone

import pytest

from benchmarks.sregym.adapter import lane_resources as lanes

NOW = datetime(2026, 10, 25, 20, tzinfo=timezone.utc)
Gi = 1024**3


@pytest.fixture
def snapshot():
    return {
        "namespaces": {
            "items": [
                {"metadata": {"name": name, "creationTimestamp": "2026-10-24T22:00:00Z"}}
                for name in [
                    "default",
                    "kube-system",
                    "kube-public",
                    "kube-node-lease",
                    "observe",
                    "sregym",
                    "local-path-storage",
                    "openebs",
                    "calico-system",
                    "tigera-operator",
                    "hotel-reservation",
                    "hotel-reservation-sdo",
                ]
            ]
        },
        "pods": {
            "items": [
                {
                    "metadata": {"namespace": "observe", "name": "jaeger-agent-abc", "uid": "uid-jaeger"},
                    "spec": {
                        "nodeName": "lane-control-plane",
                        "containers": [{"name": "jaeger-agent", "image": "jaegertracing/all-in-one:1.57"}],
                    },
                    "status": {
                        "phase": "Running",
                        "containerStatuses": [
                            {"name": "jaeger-agent", "state": {"running": {"startedAt": "2026-10-24T22:00:00Z"}}}
                        ],
                    },
                }
            ]
        },
        "nodes": {"items": [{"metadata": {"name": "lane-control-plane"}}]},
        "summary": {
            "node": {"nodeName": "lane-control-plane"},
            "pods": [
                {
                    "podRef": {"namespace": "observe", "name": "jaeger-agent-abc", "uid": "uid-jaeger"},
                    "containers": [
                        {
                            "name": "jaeger-agent",
                            "memory": {"time": "2026-10-25T19:59:50Z", "workingSetBytes": 256 * 1024**2},
                        }
                    ],
                }
            ],
        },
    }


def evaluate(snapshot, *, host=32 * Gi, policy=None):
    return lanes.evaluate_lane_resources(
        namespaces=snapshot["namespaces"],
        pods=snapshot["pods"],
        summaries=[snapshot["summary"]],
        app_namespaces={"hotel-reservation"},
        host_available_bytes=host,
        now=NOW,
        policy=policy or lanes.LaneResourcePolicy(),
    )


@pytest.mark.parametrize("gib", [74, 83])
def test_unbounded_observer_names_the_component(snapshot, gib):
    snapshot["summary"]["pods"][0]["containers"][0]["memory"]["workingSetBytes"] = gib * Gi
    with pytest.raises(ValueError, match=r"eval:.*observe/jaeger-agent-abc/jaeger-agent.*budget"):
        evaluate(snapshot)


def test_every_container_has_a_budget(snapshot):
    snapshot["pods"]["items"][0]["spec"]["containers"][0]["image"] = "registry/app:latest"
    snapshot["summary"]["pods"][0]["containers"][0]["memory"]["workingSetBytes"] = 9 * Gi
    with pytest.raises(ValueError, match="budget"):
        evaluate(snapshot)


def test_host_floor_rejects_memory_pressure(snapshot):
    with pytest.raises(ValueError, match=r"eval:.*host.*available.*floor"):
        evaluate(snapshot, host=15 * Gi)


def test_foreign_app_namespace_reports_name_and_age(snapshot):
    snapshot["namespaces"]["items"].append(
        {"metadata": {"name": "social-network", "creationTimestamp": "2026-10-24T22:00:00Z"}}
    )
    with pytest.raises(ValueError, match=r"eval:.*foreign.*social-network.*22"):
        evaluate(snapshot)


def test_unknown_workload_namespace_also_fails(snapshot):
    snapshot["namespaces"]["items"].append(
        {"metadata": {"name": "forgotten-app", "creationTimestamp": "2026-10-24T22:00:00Z"}}
    )
    with pytest.raises(ValueError, match="forgotten-app"):
        evaluate(snapshot)


def test_missing_configured_app_fails(snapshot):
    snapshot["namespaces"]["items"] = [
        r for r in snapshot["namespaces"]["items"] if r["metadata"]["name"] != "hotel-reservation"
    ]
    with pytest.raises(ValueError, match="missing.*hotel-reservation"):
        evaluate(snapshot)


@pytest.mark.parametrize("bad", ["missing", "stale", "future", "wrong_uid", "negative"])
def test_unmeasurable_running_container_fails_closed(snapshot, bad):
    memory = snapshot["summary"]["pods"][0]["containers"][0]["memory"]
    if bad == "missing":
        del memory["workingSetBytes"]
    elif bad == "stale":
        memory["time"] = "2026-10-25T19:00:00Z"
    elif bad == "future":
        memory["time"] = "2026-10-25T21:00:00Z"
    elif bad == "wrong_uid":
        snapshot["summary"]["pods"][0]["podRef"]["uid"] = "retired-pod"
    else:
        memory["workingSetBytes"] = -1
    with pytest.raises(ValueError, match="eval:.*memory"):
        evaluate(snapshot)


def test_healthy_lane_has_auditable_evidence(snapshot):
    result = evaluate(snapshot)
    assert result["passed"] is True
    assert result["host_available_bytes"] == 32 * Gi
    assert result["containers"][0]["component"] == "observe/jaeger-agent-abc/jaeger-agent"
    assert result["containers"][0]["budget_bytes"] == 2 * Gi
    assert result["app_namespaces"] == ["hotel-reservation"]


def test_host_floor_and_container_budget_are_inclusive(snapshot):
    snapshot["summary"]["pods"][0]["containers"][0]["memory"]["workingSetBytes"] = 2 * Gi
    assert evaluate(snapshot, host=16 * Gi)["passed"]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"host_floor_bytes": 0},
        {"default_container_budget_bytes": -1},
        {"max_sample_age_seconds": 0},
        {"component_budgets": {"x": 0}},
    ],
)
def test_invalid_configuration_fails_at_construction(kwargs):
    with pytest.raises((TypeError, ValueError)):
        lanes.LaneResourcePolicy(**kwargs)


def test_shell_uses_only_bounded_get_queries(snapshot, monkeypatch, tmp_path):
    calls = []
    responses = [snapshot["namespaces"], snapshot["pods"], snapshot["nodes"], snapshot["summary"]]

    def run(command, **kwargs):
        calls.append(command)
        assert command[:3] == ["kubectl", "--kubeconfig", str(tmp_path / "kubeconfig")]
        assert command[3] == "get"
        assert kwargs["timeout"] <= 5
        assert "--request-timeout=4s" in command
        return subprocess.CompletedProcess(command, 0, json.dumps(responses.pop(0)), "")

    monkeypatch.setattr(lanes.subprocess, "run", run)
    monkeypatch.setattr(lanes, "host_available_memory", lambda: 32 * Gi)
    result = lanes.assert_lane_resources(tmp_path / "kubeconfig", app_namespaces={"hotel-reservation"}, now=NOW)
    assert result["passed"]
    assert len(calls) == 4
    assert "secrets" not in " ".join(" ".join(row) for row in calls)


def test_shell_propagates_unavailable_measurements(snapshot, monkeypatch, tmp_path):
    def unavailable(*args, **kwargs):
        raise subprocess.TimeoutExpired(args[0], 5)

    monkeypatch.setattr(lanes.subprocess, "run", unavailable)
    with pytest.raises(ValueError, match=r"eval:.*unavailable"):
        lanes.assert_lane_resources(tmp_path / "kubeconfig", app_namespaces={"hotel-reservation"}, now=NOW)


def test_host_available_uses_memavailable(tmp_path):
    path = tmp_path / "meminfo"
    path.write_text("MemTotal: 263000000 kB\nMemFree: 100 kB\nMemAvailable: 20000000 kB\n")
    assert lanes.host_available_memory(path) == 20000000 * 1024
    path.write_text("MemTotal: 263000000 kB\nMemFree: 100 kB\n")
    with pytest.raises(ValueError, match="MemAvailable"):
        lanes.host_available_memory(path)


def test_creation_host_guard_rejects_before_any_cluster_query(monkeypatch):
    monkeypatch.setattr(lanes, "host_available_memory", lambda: 15 * Gi)
    with pytest.raises(ValueError, match="host.*floor"):
        lanes.assert_host_headroom()
    monkeypatch.setattr(lanes, "host_available_memory", lambda: 32 * Gi)
    assert lanes.assert_host_headroom()["host_available_bytes"] == 32 * Gi


def test_admission_helpers_are_exported_through_the_adapter_facade():
    from benchmarks.sregym.adapter import LaneResourcePolicy, assert_host_headroom, assert_lane_resources

    assert LaneResourcePolicy is lanes.LaneResourcePolicy
    assert assert_host_headroom is lanes.assert_host_headroom
    assert assert_lane_resources is lanes.assert_lane_resources


def test_pinned_benchmark_infrastructure_is_not_a_foreign_app(snapshot):
    for name in ("policy-system", "khaos", "chaos-mesh"):
        snapshot["namespaces"]["items"].append(
            {"metadata": {"name": name, "creationTimestamp": "2026-10-24T22:00:00Z"}}
        )
    assert evaluate(snapshot)["passed"]


def test_static_mirror_pod_uses_kubelet_hash_identity(snapshot):
    pod = snapshot["pods"]["items"][0]
    pod["metadata"]["annotations"] = {
        "kubernetes.io/config.source": "file",
        "kubernetes.io/config.hash": "static-hash",
        "kubernetes.io/config.mirror": "static-hash",
    }
    pod["metadata"]["ownerReferences"] = [{"kind": "Node", "name": "lane-control-plane", "uid": "node-uid"}]
    snapshot["summary"]["pods"][0]["podRef"]["uid"] = "static-hash"
    assert evaluate(snapshot)["passed"]


def test_unbound_mirror_annotation_cannot_bypass_pod_uid(snapshot):
    pod = snapshot["pods"]["items"][0]
    pod["metadata"]["annotations"] = {"kubernetes.io/config.mirror": "retired-pod"}
    snapshot["summary"]["pods"][0]["podRef"]["uid"] = "retired-pod"
    with pytest.raises(ValueError, match="memory.*identity"):
        evaluate(snapshot)


def test_component_override_is_exact_and_pod_override_wins(snapshot):
    snapshot["summary"]["pods"][0]["containers"][0]["memory"]["workingSetBytes"] = 3 * Gi
    policy = lanes.LaneResourcePolicy(component_budgets={"observe/jaeger-agent": 4 * Gi})
    assert evaluate(snapshot, policy=policy)["containers"][0]["budget_bytes"] == 4 * Gi
    policy = lanes.LaneResourcePolicy(
        component_budgets={"observe/jaeger-agent": 4 * Gi, "observe/jaeger-agent-abc/jaeger-agent": 2 * Gi}
    )
    with pytest.raises(ValueError, match="budget"):
        evaluate(snapshot, policy=policy)


def test_malformed_memory_reports_component_unavailability(snapshot):
    snapshot["summary"]["pods"][0]["containers"][0]["memory"] = None
    with pytest.raises(ValueError, match="observe/jaeger-agent-abc/jaeger-agent.*memory"):
        evaluate(snapshot)


@pytest.mark.parametrize("kwargs", [{"component_budgets": "not-a-map"}, {"infrastructure_namespaces": "observe"}])
def test_wrong_configuration_shape_raises_type_error(kwargs):
    with pytest.raises(TypeError):
        lanes.LaneResourcePolicy(**kwargs)
