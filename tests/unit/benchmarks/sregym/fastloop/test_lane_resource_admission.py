"""Resource admission must fence setup and the real pre-injection proof."""

import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from benchmarks.sregym.fastloop import admission, cli, hotel_helpers, rbac


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
    monkeypatch.setattr(hotel_helpers, "admit_install", lambda *args: {"passed": True, "profile": "ABSENT"})
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


def test_oracle_uses_remaining_admission_budget_instead_of_shorter_poll_timeout(lane, monkeypatch):
    gate, _ = lane
    monkeypatch.setattr(admission, "assert_lane_resources", lambda *args, **kwargs: {"passed": True})
    now = [0.0]
    monkeypatch.setattr(admission.time, "monotonic", lambda: now[0])

    def request(op, **kwargs):
        assert op == "baseline_oracle"
        assert kwargs["timeout"] == pytest.approx(280)
        now[0] += 200
        return {"success": True}

    def windows(_control):
        now[0] += 20
        return {
            "windows": {"health": {"scenarios": [{"scenario": {"id": "frontend"}, "qualified": True, "samples": [{}]}]}}
        }

    gate.worker.request = request
    monkeypatch.setattr(gate, "_windows", windows)
    assert gate.prove("hotel-reservation-sdo")["passed"] is True


def test_onboarding_has_its_own_budget_before_steady_admission(lane, monkeypatch):
    gate, _ = lane
    monkeypatch.setattr(admission, "assert_lane_resources", lambda *args, **kwargs: {"passed": True})
    now = [0.0]
    monkeypatch.setattr(admission.time, "monotonic", lambda: now[0])
    gate.before_proof = lambda *args: now.__setitem__(0, now[0] + 345)

    def request(op, **kwargs):
        assert kwargs["timeout"] == pytest.approx(300)
        return {"success": True}

    gate.worker.request = request
    assert gate.prove("hotel-reservation-sdo")["passed"]


def test_admission_uses_the_requested_installer_configuration(lane, monkeypatch):
    from benchmarks.sregym.adapter.native_operator import NativeInstallConfig

    original, calls = lane
    env = original.environment
    requested = NativeInstallConfig(
        repository=env.workspace,
        namespace=env.namespace,
        control_namespace=env.namespace + "-sdo",
        application=env.namespace,
        controller_image="controller:test",
        responder_image="responder:test",
        validator_image="validator:test",
        repository_pvc="repository",
        credentials_secret="unused",
        model="scripted",
        kubeconfig=env.kubeconfig,
        permissive_responder=True,
        responder_namespaces=("policy-system",),
    )
    seen = []

    class ReachedRBAC(Exception):
        pass

    def verify(kubeconfig, namespace, config):
        seen.append(config)
        raise ReachedRBAC

    monkeypatch.setattr(rbac, "verify_rbac", verify)
    monkeypatch.setattr(admission, "assert_lane_resources", lambda *args, **kwargs: {"passed": True})
    gate = admission.LaneAdmission(env, original.worker, original.ops, original.directory, install_config=requested)
    with pytest.raises(ReachedRBAC):
        gate.prove(env.namespace + "-sdo")
    assert seen == [requested]


def observe_only(gate, monkeypatch) -> list[float]:
    """Give admission a fake clock and fail if it ever restores the application."""
    from functools import partial

    from benchmarks.sregym.fastloop import baseline_recovery, reset

    gate.timeout_seconds = 10
    now = [0.0]

    def sleep(seconds: float) -> None:
        now[0] += seconds

    monkeypatch.setattr(admission.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(admission, "wait_for_proof", partial(admission.wait_for_proof, sleep=sleep))
    monkeypatch.setattr(admission, "assert_lane_resources", lambda *args, **kwargs: {"passed": True})
    monkeypatch.setattr(reset, "RestoreChecks", lambda *args: SimpleNamespace(prepare=lambda **kwargs: None))
    monkeypatch.setattr(baseline_recovery, "KubectlApplicationCluster", lambda *args: None)
    monkeypatch.setattr(reset, "restore_application", lambda *args, **kwargs: pytest.fail("admission restarted app"))
    return now


@pytest.mark.parametrize("waiting_for", ["controller", "traffic", "reflection", "independent_traffic"])
def test_admission_waits_without_restarting_the_healthy_application(lane, monkeypatch, waiting_for):
    gate, calls = lane
    now = observe_only(gate, monkeypatch)
    state = gate.ops.runtime_state
    windows = gate._windows

    def runtime(control):
        value = state(control)
        if now[0] < 6:
            if waiting_for == "controller":
                value["history"] = []
            if waiting_for == "reflection":
                value["pending_closure"] = {"incident_id": "previous", "reflection": "running"}
        return value

    def traffic(*args, **kwargs):
        assert args == ("baseline_oracle",), "admission must only observe application traffic"
        calls.append("traffic")
        return {"success": waiting_for != "independent_traffic" or now[0] >= 6}

    monkeypatch.setattr(gate.ops, "runtime_state", runtime)
    monkeypatch.setattr(gate.worker, "request", traffic)
    monkeypatch.setattr(
        gate, "_windows", lambda control: {} if waiting_for == "traffic" and now[0] < 6 else windows(control)
    )
    proof = gate.prove("hotel-reservation-sdo")
    assert proof["passed"] is True
    assert 6 <= now[0] < 10
    assert proof["incident_work_pending"] is False


def test_admission_never_injects_while_reflection_remains_pending(lane, monkeypatch):
    from benchmarks.sregym.adapter.eval_guards import StabilizationError

    gate, calls = lane
    now = observe_only(gate, monkeypatch)
    state = gate.ops.runtime_state
    monkeypatch.setattr(gate.ops, "runtime_state", lambda control: dict(state(control), pending_closure={"id": "busy"}))
    inner = SimpleNamespace(
        set_maintenance=lambda *args, **kwargs: None,
        inject_after_resume=lambda *args: pytest.fail("injected during reflection"),
    )
    with pytest.raises(StabilizationError):
        admission.EvalAdmissionOps(inner, gate).inject_after_resume("hotel-reservation-sdo", "g1", lambda: None)
    assert now[0] == 10
    assert calls == []
    records = [json.loads(line) for line in (gate.directory / "stabilization-001.jsonl").read_text().splitlines()]
    assert all(row["incident_work_pending"] and not row["passed"] for row in records)
