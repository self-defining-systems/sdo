"""Resource admission must fence setup and the real pre-injection proof."""

import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from benchmarks.sregym.fastloop import admission, cli, rbac


@pytest.fixture
def lane(tmp_path, monkeypatch):
    traffic = tmp_path / ".sdo/diagnostics/traffic/workloads"
    traffic.mkdir(parents=True)
    (traffic / "health.yaml").write_text("purpose: health-probe\nscenarios:\n- id: frontend\n")
    environment = SimpleNamespace(
        workspace=tmp_path, namespace="hotel-reservation", kubeconfig=tmp_path / "kubeconfig", deploy_problem="fixture"
    )
    calls = []
    monkeypatch.setattr(admission, "assert_host_disk_headroom", lambda: {"passed": True, "mount": "/fixture"})

    def request(*args, **kwargs):
        calls.append("traffic")
        return {"success": True}

    def state(_control):
        return {
            "history": [
                {"detector_id": key, "status": "clear", "evaluated_at": datetime.now(timezone.utc).isoformat()}
                for key in ("workload-health", "service-endpoints", "service-reachability", "traffic-health")
            ]
        }

    gate = admission.LaneAdmission(
        environment, SimpleNamespace(request=request), SimpleNamespace(runtime_state=state), tmp_path / "admission"
    )
    monkeypatch.setattr(rbac, "verify_rbac", lambda *args: {"passed": True})
    monkeypatch.setattr(admission, "workload_identity", lambda *args: {"Deployment/app": {"generation": 1}})
    monkeypatch.setattr(admission, "assert_clean_baseline", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        gate,
        "_windows",
        lambda *args: {
            "windows": {"health": {"scenarios": [{"scenario": {"id": "frontend"}, "qualified": True, "samples": [{}]}]}}
        },
    )
    return gate, calls


@pytest.mark.parametrize("failing_check", [1, 2])
def test_resources_checked_before_and_after_stabilization(lane, monkeypatch, failing_check):
    gate, calls = lane

    def resources(kubeconfig, *, app_namespaces):
        assert kubeconfig == gate.environment.kubeconfig
        assert app_namespaces == {"hotel-reservation"}
        calls.append("resources")
        if calls.count("resources") == failing_check:
            raise ValueError("eval: hotel-reservation/jaeger/container exceeds budget")
        return {"passed": True, "containers": []}

    monkeypatch.setattr(admission, "assert_lane_resources", resources, raising=False)
    with pytest.raises(ValueError, match="hotel-reservation/jaeger/container"):
        gate.prove("hotel-reservation-sdo")
    assert calls == (["resources"] if failing_check == 1 else ["resources", "traffic", "resources"])
    if failing_check == 2:
        assert json.loads((gate.directory / "lane-resources.json").read_text())["passed"]


def test_host_memory_floor_precedes_lane_setup_and_docker_queries(monkeypatch, tmp_path):
    def reject():
        raise ValueError("eval: host available memory below floor")

    monkeypatch.setattr(cli, "assert_host_headroom", reject, raising=False)
    monkeypatch.setattr(cli.subprocess, "run", lambda *args, **kwargs: pytest.fail("Docker query before memory gate"))
    with pytest.raises(ValueError, match="host available memory"):
        cli.check_host_capacity(SimpleNamespace(run_dir=tmp_path))


@pytest.mark.parametrize("failing_check", [1, 2])
def test_disk_pressure_blocks_entry_and_final_injection_proof(lane, monkeypatch, failing_check):
    gate, calls = lane
    monkeypatch.setattr(admission, "assert_lane_resources", lambda *args, **kwargs: {"passed": True})

    def disk():
        calls.append("disk")
        if calls.count("disk") == failing_check:
            raise ValueError("eval: host disk mount /mnt/data 97.0% used; free space")
        return {"passed": True, "mount": "/mnt/data"}

    monkeypatch.setattr(admission, "assert_host_disk_headroom", disk)
    with pytest.raises(ValueError, match=r"disk mount /mnt/data"):
        gate.prove("hotel-reservation-sdo")
    assert calls == (["disk"] if failing_check == 1 else ["disk", "traffic", "disk"])


def test_host_disk_floor_precedes_lane_setup(monkeypatch, tmp_path):
    monkeypatch.setattr(cli, "assert_host_headroom", lambda: {"passed": True})

    def reject():
        raise ValueError("eval: host disk mount /mnt/data 97.0% used; free space")

    monkeypatch.setattr(cli, "assert_host_disk_headroom", reject)
    monkeypatch.setattr(cli.subprocess, "run", lambda *args, **kwargs: pytest.fail("setup before disk gate"))
    with pytest.raises(ValueError, match=r"disk mount /mnt/data"):
        cli.check_host_capacity(SimpleNamespace(run_dir=tmp_path))
