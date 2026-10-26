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
    complete = {"complete": {"acceptedCommit": "a" * 40, "objects": [{}]}, "pending": None}
    monkeypatch.setattr(admission.LaneAdmission, "_onboarding", lambda self, control: complete)
    # No SDO worker Job from a previous row is still running.
    monkeypatch.setattr(admission.LaneAdmission, "_active_workers", lambda self, control: [])
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
    monkeypatch.setattr(admission.time, "sleep", sleep)
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
    gate, calls = lane
    now = observe_only(gate, monkeypatch)
    gate.prior_closure_timeout_seconds = 30
    state = gate.ops.runtime_state
    monkeypatch.setattr(gate.ops, "runtime_state", lambda control: dict(state(control), pending_closure={"id": "busy"}))
    inner = SimpleNamespace(
        set_maintenance=lambda *args, **kwargs: None,
        inject_after_resume=lambda *args: pytest.fail("injected during reflection"),
    )
    with pytest.raises(admission.PriorClosureIncompleteError):
        admission.EvalAdmissionOps(inner, gate).inject_after_resume("hotel-reservation-sdo", "g1", lambda: None)
    assert now[0] == 30
    assert calls == []
    assert not (gate.directory / "stabilization-001.jsonl").exists(), "the stabilization clock started"
    assert json.loads((gate.directory / "prior-closure.json").read_text())["passed"] is False


def test_workload_refusal_names_the_sdo_incident_active_before_injection(lane, monkeypatch):
    gate, _calls = lane
    observe_only(gate, monkeypatch)
    state = gate.ops.runtime_state
    # SDO opens the incident after admission captured the workload identity.
    opened = []
    monkeypatch.setattr(
        gate.ops,
        "runtime_state",
        lambda control: (
            dict(state(control), incident_open=True, incident_request={"incident_id": "astronomy-1793"})
            if opened
            else state(control)
        ),
    )
    identities = iter([{"Deployment/currency": {"pods": ["old"]}}])

    def identity(*_args):
        for row in identities:
            opened.append(True)
            return row
        raise ValueError("restore invariant: workload not Ready: Deployment/currency (ready 0/1)")

    monkeypatch.setattr(admission, "workload_identity", identity)
    with pytest.raises(ValueError, match="workload not Ready: Deployment/currency") as refused:
        gate.prove("astronomy-shop-sdo")
    assert "SDO incident work active before injection: astronomy-1793" in str(refused.value)


def first_adoption(gate, monkeypatch, finishes_at: float) -> list[float]:
    """Model a fresh seed: install-existing and the first health refresh run until ``finishes_at``."""
    now = [0.0]
    monkeypatch.setattr(admission.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(admission.time, "sleep", lambda seconds: now.__setitem__(0, now[0] + seconds))
    monkeypatch.setattr(admission, "assert_lane_resources", lambda *args, **kwargs: {"passed": True})
    state = gate.ops.runtime_state

    def runtime(control):
        return state(control) if now[0] >= finishes_at else {"history": []}

    def onboarding(control):
        if now[0] < finishes_at:
            return {"complete": None, "pending": {"acceptedCommit": "a" * 40}}
        return {"complete": {"acceptedCommit": "b" * 40, "objects": [{"kind": "Deployment"}]}, "pending": None}

    monkeypatch.setattr(gate.ops, "runtime_state", runtime)
    monkeypatch.setattr(gate, "_onboarding", onboarding)
    return now


def test_stabilization_clock_starts_only_after_first_adoption_onboarding(lane, monkeypatch):
    gate, _ = lane
    now = first_adoption(gate, monkeypatch, finishes_at=290)

    def request(op, **kwargs):
        assert kwargs["timeout"] == pytest.approx(300), "onboarding consumed the stabilization window"
        return {"success": True}

    gate.worker.request = request
    assert gate.prove("hotel-reservation-sdo")["passed"] is True
    evidence = json.loads((gate.directory / "onboarding.json").read_text())
    assert evidence["passed"] is True
    assert evidence["accepted_commit"] == "b" * 40
    assert 290 <= evidence["seconds"] < 300
    assert now[0] >= 290


def test_unfinished_onboarding_fails_on_its_own_budget_without_sampling_health(lane, monkeypatch):
    gate, calls = lane
    gate.onboarding_timeout_seconds = 120
    first_adoption(gate, monkeypatch, finishes_at=10_000)
    with pytest.raises(ValueError, match=r"onboarding did not finish within 120s"):
        gate.prove("hotel-reservation-sdo")
    assert "traffic" not in calls
    evidence = json.loads((gate.directory / "onboarding.json").read_text())
    assert evidence["passed"] is False
    assert evidence["pending"] is True


def previous_reflection(gate, monkeypatch, *, finishes_at: float):
    """A lane whose previous incident is still reflecting until *finishes_at*."""
    now = [0.0]
    monkeypatch.setattr(admission.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(admission.time, "sleep", lambda seconds: now.__setitem__(0, now[0] + seconds))
    monkeypatch.setattr(admission, "assert_lane_resources", lambda *args, **kwargs: {"passed": True})
    healthy = gate.ops.runtime_state

    def state(control):
        if now[0] < finishes_at:
            return {"closure_state": "CLOSURE_STATE_COMMITTED", "last_acknowledged_incident_id": "row-003"}
        return {**healthy(control), "last_acknowledged_incident_id": "row-004"}

    def workers(control):
        return ["sdo-reflect-row-004"] if now[0] < finishes_at else []

    gate.ops = SimpleNamespace(runtime_state=state)
    monkeypatch.setattr(gate, "_active_workers", workers)
    return now


def test_admission_waits_for_the_previous_reflection_before_the_stabilization_clock(lane, monkeypatch):
    gate, _ = lane
    now = previous_reflection(gate, monkeypatch, finishes_at=420)

    def request(op, **kwargs):
        assert now[0] >= 420, "health was sampled while the previous reflection was running"
        assert kwargs["timeout"] == pytest.approx(300), "the previous reflection consumed the stabilization window"
        return {"success": True}

    gate.worker.request = request
    assert gate.prove("hotel-reservation-sdo")["passed"] is True
    evidence = json.loads((gate.directory / "prior-closure.json").read_text())
    assert evidence["passed"] is True
    assert evidence["last_acknowledged_incident_id"] == "row-004"
    assert evidence["active_workers"] == []
    assert 420 <= evidence["seconds"] < 430
    assert "accepted_before" in evidence


def test_stuck_previous_reflection_is_a_typed_not_measured_row(lane, monkeypatch):
    gate, calls = lane
    gate.prior_closure_timeout_seconds = 600
    previous_reflection(gate, monkeypatch, finishes_at=10_000)
    with pytest.raises(admission.PriorClosureIncompleteError, match=r"previous incident closure did not finish") as info:
        gate.prove("hotel-reservation-sdo")
    assert info.value.reason == "prior_closure_incomplete"
    assert "traffic" not in calls
    evidence = json.loads((gate.directory / "prior-closure.json").read_text())
    assert evidence["passed"] is False
    assert evidence["active_workers"] == ["sdo-reflect-row-004"]
    assert evidence["closure_state"] == "CLOSURE_STATE_COMMITTED"
    assert evidence["budget_seconds"] == 600
