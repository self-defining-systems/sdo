"""Resource admission must fence setup and the real pre-injection proof."""

import json
from datetime import datetime, timedelta, timezone
from functools import partial
from pathlib import Path
from types import SimpleNamespace

import pytest

from benchmarks.sregym.adapter import WaitKind, persistent, wait_for
from benchmarks.sregym.fastloop import admission, cli, hotel_helpers, rbac

EPOCH = datetime(2030, 1, 1, tzinfo=timezone.utc)
OVERDUE = "2000-01-01T00:00:00Z"  # a busy_until the controller itself has already let lapse
CLOSEOUT_MARGIN = wait_for(WaitKind.CLOSEOUT_SETTLE)


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

    def state(_control):
        return {
            "history": [
                {"detector_id": key, "status": "clear", "evaluated_at": datetime.now(timezone.utc).isoformat()}
                for key in ("workload-health", "service-endpoints", "service-reachability", "traffic-health")
            ]
        }

    gate = admission.LaneAdmission(environment, SimpleNamespace(runtime_state=state), tmp_path / "admission")
    complete = {"complete": {"acceptedCommit": "a" * 40, "objects": [{}]}, "pending": None, "rejected": None}
    monkeypatch.setattr(admission.LaneAdmission, "_onboarding", lambda self, control: complete)
    monkeypatch.setattr(rbac, "verify_rbac", lambda *args: {"passed": True})
    monkeypatch.setattr(hotel_helpers, "admit_install", lambda *args: {"passed": True, "profile": "ABSENT"})
    monkeypatch.setattr(admission, "workload_identity", lambda *args: {"Deployment/app": {"generation": 1}})
    monkeypatch.setattr(admission, "assert_clean_baseline", lambda *args, **kwargs: None)

    def windows(*args):
        calls.append("sample")
        return {
            "windows": {"health": {"scenarios": [{"scenario": {"id": "frontend"}, "qualified": True, "samples": [{}]}]}}
        }

    monkeypatch.setattr(gate, "_windows", windows)
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
    assert calls == (["resources"] if failing_check == 1 else ["resources", "sample", "resources"])
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
    assert calls == (["disk"] if failing_check == 1 else ["disk", "sample", "disk"])


def test_host_disk_floor_precedes_lane_setup(monkeypatch, tmp_path):
    monkeypatch.setattr(cli, "assert_host_headroom", lambda: {"passed": True})

    def reject():
        raise ValueError("eval: host disk mount /mnt/data 97.0% used; free space")

    monkeypatch.setattr(cli, "assert_host_disk_headroom", reject)
    monkeypatch.setattr(cli.subprocess, "run", lambda *args, **kwargs: pytest.fail("setup before disk gate"))
    with pytest.raises(ValueError, match=r"disk mount /mnt/data"):
        cli.check_host_capacity(SimpleNamespace(run_dir=tmp_path))


def test_onboarding_has_its_own_budget_before_steady_admission(lane, monkeypatch):
    gate, _ = lane
    monkeypatch.setattr(admission, "assert_lane_resources", lambda *args, **kwargs: {"passed": True})
    now = [0.0]
    monkeypatch.setattr(admission.time, "monotonic", lambda: now[0])
    gate.before_proof = lambda *args: now.__setitem__(0, now[0] + 345)
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
    gate = admission.LaneAdmission(env, original.ops, original.directory, install_config=requested)
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


@pytest.mark.parametrize("waiting_for", ["controller", "traffic", "reflection"])
def test_admission_waits_without_restarting_the_healthy_application(lane, monkeypatch, waiting_for):
    gate, _calls = lane
    now = observe_only(gate, monkeypatch)
    state = gate.ops.runtime_state
    windows = gate._windows

    def runtime(control):
        value = state(control)
        if now[0] < 6:
            if waiting_for == "controller":
                value["history"] = []
            if waiting_for == "reflection":
                value.update(pending_closure={"incident_id": "previous"}, busy_until=OVERDUE)
        return value

    monkeypatch.setattr(gate.ops, "runtime_state", runtime)
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
    state = gate.ops.runtime_state
    monkeypatch.setattr(
        gate.ops,
        "runtime_state",
        lambda control: dict(state(control), pending_closure={"id": "busy"}, busy_until=OVERDUE),
    )
    inner = SimpleNamespace(
        set_maintenance=lambda *args, **kwargs: None,
        inject_after_resume=lambda *args: pytest.fail("injected during reflection"),
    )
    with pytest.raises(admission.PriorClosureIncompleteError):
        admission.EvalAdmissionOps(inner, gate).inject_after_resume("hotel-reservation-sdo", "g1", lambda: None)
    assert now[0] == CLOSEOUT_MARGIN
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
            dict(
                state(control),
                incident_open=True,
                incident_request={"incident_id": "astronomy-1793"},
                busy_until=OVERDUE,
            )
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
            return {"complete": None, "pending": {"acceptedCommit": "a" * 40}, "rejected": None}
        return {"complete": {"acceptedCommit": "b" * 40, "objects": [{"kind": "Deployment"}]}, "pending": None, "rejected": None}

    monkeypatch.setattr(gate.ops, "runtime_state", runtime)
    monkeypatch.setattr(gate, "_onboarding", onboarding)
    return now


def test_stabilization_clock_starts_only_after_first_adoption_onboarding(lane, monkeypatch):
    gate, _ = lane
    now = first_adoption(gate, monkeypatch, finishes_at=290)

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
    assert "sample" not in calls
    evidence = json.loads((gate.directory / "onboarding.json").read_text())
    assert evidence["passed"] is False
    assert evidence["pending"] is True


def test_rejected_onboarding_fails_at_once_with_the_controller_reason(lane, monkeypatch):
    """The controller's rejection is final (one evaluation); waiting out the budget cannot change it."""
    gate, calls = lane
    now = first_adoption(gate, monkeypatch, finishes_at=10_000)
    rejected = {
        "complete": None,
        "pending": {"acceptedCommit": "a" * 40},
        "rejected": {"reason": "install window contradicted or expired"},
    }
    monkeypatch.setattr(gate, "_onboarding", lambda control: rejected if now[0] >= 30 else {**rejected, "rejected": None})
    monkeypatch.setattr(admission, "kubectl", lambda *args, **kwargs: SimpleNamespace(stdout="controller line\n"))
    with pytest.raises(ValueError, match=r"rejected onboarding after 30s .*install window contradicted or expired"):
        gate.prove("hotel-reservation-sdo")
    assert "sample" not in calls
    assert now[0] < 100
    evidence = json.loads((gate.directory / "onboarding.json").read_text())
    assert evidence["passed"] is False
    assert evidence["rejected_reason"] == "install window contradicted or expired"
    assert (gate.directory / "onboarding-controller.log").read_text() == "controller line\n"


def test_unfinished_onboarding_keeps_the_controller_log(lane, monkeypatch):
    gate, _ = lane
    gate.onboarding_timeout_seconds = 120
    first_adoption(gate, monkeypatch, finishes_at=10_000)
    monkeypatch.setattr(admission, "kubectl", lambda *args, **kwargs: SimpleNamespace(stdout="window never started\n"))
    with pytest.raises(ValueError, match=r"did not finish within 120s"):
        gate.prove("hotel-reservation-sdo")
    assert (gate.directory / "onboarding-controller.log").read_text() == "window never started\n"


def wall_clock(gate, monkeypatch, now):
    """Wall time that follows the faked monotonic clock, so ``busy_until`` is seconds after the start."""
    monkeypatch.setattr(gate, "_wall_now", lambda: EPOCH + timedelta(seconds=now[0]))


def busy_at(seconds: float) -> str:
    return (EPOCH + timedelta(seconds=seconds)).isoformat()


def fake_time(gate, monkeypatch):
    now = [0.0]
    monkeypatch.setattr(admission.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(admission.time, "sleep", lambda seconds: now.__setitem__(0, now[0] + seconds))
    wall_clock(gate, monkeypatch, now)
    return now


def previous_reflection(gate, monkeypatch, *, finishes_at: float, controller_bound: float):
    """A lane whose previous incident is still reflecting until *finishes_at*; the controller bounds it itself."""
    now = fake_time(gate, monkeypatch)
    monkeypatch.setattr(admission, "assert_lane_resources", lambda *args, **kwargs: {"passed": True})
    healthy = gate.ops.runtime_state

    def state(control):
        if now[0] < finishes_at:
            return {
                "closure_state": "CLOSURE_STATE_COMMITTED",
                "last_acknowledged_incident_id": "row-003",
                "busy_until": busy_at(controller_bound),
            }
        return {**healthy(control), "last_acknowledged_incident_id": "row-004"}

    gate.ops = SimpleNamespace(runtime_state=state)
    return now


def test_admission_waits_for_the_previous_reflection_before_the_stabilization_clock(lane, monkeypatch):
    gate, _ = lane
    now = previous_reflection(gate, monkeypatch, finishes_at=420, controller_bound=900)

    windows = gate._windows

    def sampled(control):
        assert now[0] >= 420, "health was sampled while the previous reflection was running"
        return windows(control)

    gate._windows = sampled
    assert gate.prove("hotel-reservation-sdo")["passed"] is True
    evidence = json.loads((gate.directory / "prior-closure.json").read_text())
    assert evidence["passed"] is True
    assert evidence["last_acknowledged_incident_id"] == "row-004"
    assert 420 <= evidence["seconds"] < 430
    assert "accepted_before" in evidence


def test_prior_closure_wait_reads_only_the_controller_state(lane, monkeypatch):
    """No Job list, label or ConfigMap read: the controller's one published fact is the whole input."""
    gate, _ = lane
    now = previous_reflection(gate, monkeypatch, finishes_at=60, controller_bound=900)
    monkeypatch.setattr(admission, "kubectl", lambda *args, **kwargs: pytest.fail("prior-closure wait read the API"))
    evidence = gate.await_prior_closure("hotel-reservation-sdo")
    assert evidence["passed"] is True
    assert now[0] >= 60


def test_stuck_previous_reflection_ends_one_margin_after_the_controllers_own_bound(lane, monkeypatch):
    gate, calls = lane
    previous_reflection(gate, monkeypatch, finishes_at=10_000, controller_bound=600)
    with pytest.raises(
        admission.PriorClosureIncompleteError, match=r"previous incident closure did not finish"
    ) as info:
        gate.prove("hotel-reservation-sdo")
    assert info.value.reason == "prior_closure_incomplete"
    assert "sample" not in calls
    evidence = json.loads((gate.directory / "prior-closure.json").read_text())
    assert evidence["passed"] is False
    assert evidence["closure_state"] == "CLOSURE_STATE_COMMITTED"
    assert evidence["busy_until"] == busy_at(600)
    assert evidence["budget_seconds"] == 600 + CLOSEOUT_MARGIN


def closure_record(gate, state):
    gate.prior_closure_timeout_seconds = 0.01
    gate.ops = SimpleNamespace(runtime_state=lambda _control: state)


def test_admission_after_a_permanently_failed_closure_passes(lane):
    gate, _ = lane
    # The controller keeps the failed closure pending until a new incident begins; it publishes no busy_until.
    closure_record(
        gate,
        {
            "closure_state": "CLOSURE_STATE_FAILED",
            "pending_closure": {"request": {"incident_id": "row-008"}},
            "closure_failure": {"incident_id": "row-008", "permanent": True},
        },
    )
    assert gate.await_prior_closure("hotel-reservation-sdo")["passed"] is True


@pytest.mark.parametrize(
    "state",
    [
        {"closure_state": "CLOSURE_STATE_PENDING", "pending_closure": {"request": {"incident_id": "row-008"}}},
        {"closure_state": "CLOSURE_STATE_ACKNOWLEDGING", "closure_receipt": {"incident_id": "row-008"}},
        {"incident_open": True, "last_acknowledged_incident_id": "row-007"},
        # A failed attempt's acknowledged closure whose reflection is queued, or whose queue is unreadable.
        {"last_acknowledged_incident_id": "row-008", "queued_reflection_incident_ids": ["row-008"]},
        {"last_acknowledged_incident_id": "row-008", "queued_reflections_unknown": True},
    ],
    ids=["pending", "acknowledging", "open-incident", "queued-reflection", "reflection-queue-unknown"],
)
def test_admission_waits_while_the_controller_publishes_busy_until(lane, monkeypatch, state):
    gate, calls = lane
    fake_time(gate, monkeypatch)
    closure_record(gate, {**state, "busy_until": OVERDUE})
    with pytest.raises(admission.PriorClosureIncompleteError):
        gate.await_prior_closure("hotel-reservation-sdo")
    assert "sample" not in calls


def test_a_queue_of_two_reflections_with_a_failing_head_is_waited_out(lane, monkeypatch):
    """P50: two queued reflections (one backing off 182 s) are 2 x 2102 s of SDO work, not a fixed budget."""
    gate, _ = lane
    now = fake_time(gate, monkeypatch)
    reflection_attempt = 1800 + 182 + 120
    finishes_at = 2 * reflection_attempt - 300

    def state(_control):
        if now[0] >= finishes_at:
            return {"last_acknowledged_incident_id": "row-008"}
        return {
            "last_acknowledged_incident_id": "row-008",
            "queued_reflection_incident_ids": ["row-007", "row-008"],
            "busy_until": busy_at(2 * reflection_attempt),
        }

    gate.ops = SimpleNamespace(runtime_state=state)
    evidence = gate.await_prior_closure("hotel-reservation-sdo")
    assert evidence["passed"] is True
    assert evidence["seconds"] >= finishes_at
    assert evidence["budget_seconds"] >= 2 * reflection_attempt


def test_new_work_during_stabilization_waits_on_the_controllers_bound_then_restarts(lane, monkeypatch):
    """P41: a reflection that starts during stabilization outlives the fixed 300 s stabilization budget."""
    gate, calls = lane
    now = observe_only(gate, monkeypatch)
    wall_clock(gate, monkeypatch, now)
    healthy = gate.ops.runtime_state
    reads, opened = [0], []  # the reflection opens at the first stabilization sample and runs 450 s

    def running():
        return bool(opened) and now[0] < opened[0] + 450

    def state(control):
        reads[0] += 1
        if reads[0] == 2:
            opened.append(now[0])
        value = healthy(control)
        if running():
            value.update(
                incident_open=True,
                incident_request={"incident_id": "onboarding-incident"},
                busy_until=busy_at(opened[0] + 1800),
            )
        return value

    monkeypatch.setattr(gate.ops, "runtime_state", state)

    proof = gate.prove("hotel-reservation-sdo")

    assert proof["passed"] is True
    assert now[0] >= opened[0] + 450, "injection was admitted while the new reflection was running"
    suspended = [json.loads(line) for line in (gate.directory / "stabilization-001.jsonl").read_text().splitlines()]
    assert suspended[-1]["suspended"] == "incident_work_started"
    assert (gate.directory / "stabilization-002.jsonl").is_file(), "the stabilization clock did not restart"
    evidence = json.loads((gate.directory / "prior-closure.json").read_text())
    assert evidence["passed"] is True
    assert evidence["budget_seconds"] >= 1800


def test_an_unreadable_controller_state_is_typed_not_a_closure_timeout(lane):
    gate, _ = lane
    closure_record(gate, {})
    with pytest.raises(admission.ControllerStateUnavailableError) as info:
        gate.await_prior_closure("hotel-reservation-sdo")
    assert info.value.reason == "controller_state_unavailable"
    # One type for "state unreadable", shared with the recovery drain.
    assert admission.ControllerStateUnavailableError is persistent.ControllerStateUnavailableError


GOLDEN_STATES = Path(__file__).parents[5] / "sdo/operator/runtime/testdata/eval_wait/chained_reflections.json"


def test_eval_wait_outlasts_chained_work_in_the_real_controllers_exported_states(lane, monkeypatch):
    """REV25 P1 #3: the wait reads states the Go controller actually exported (recorded with a fixed clock).

    Incident 2 opens after incident 1's whole window lapsed and the controller never goes idle. The wait must
    end at the idle export, not one margin after the stale bound.
    """
    samples = json.loads(GOLDEN_STATES.read_text())
    start = datetime.fromisoformat(samples[0]["at"].replace("Z", "+00:00"))
    idle_at = (datetime.fromisoformat(samples[-1]["at"].replace("Z", "+00:00")) - start).total_seconds()
    assert "busy_until" not in samples[-1]["state"], "the last recorded export must be idle"
    lapsed = (datetime.fromisoformat(samples[1]["at"].replace("Z", "+00:00")) - start).total_seconds()
    assert lapsed + CLOSEOUT_MARGIN < idle_at, "the recorded timeline must outlast a stale bound plus one margin"

    gate, _ = lane
    now = fake_time(gate, monkeypatch)
    monkeypatch.setattr(gate, "_wall_now", lambda: start + timedelta(seconds=now[0]))

    def exported(_control):
        current = [
            s
            for s in samples
            if (datetime.fromisoformat(s["at"].replace("Z", "+00:00")) - start).total_seconds() <= now[0]
        ]
        return current[-1]["state"]

    gate.ops = SimpleNamespace(runtime_state=exported)
    evidence = gate.await_prior_closure("hotel-reservation-sdo")
    assert evidence["passed"] is True
    assert evidence["seconds"] >= idle_at


# --- ADMP1: the readiness rule is "controller idle, every required detector clear since the resume" ---


def frozen_wall_clock(monkeypatch, now: list[float]) -> None:
    """Make admission's wall clock follow the fake monotonic clock, so evidence times are exact."""

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return EPOCH + timedelta(seconds=now[0])

    monkeypatch.setattr(admission, "datetime", Clock)


def test_clear_evidence_gathered_while_the_previous_closure_ran_is_not_re_awaited(lane, monkeypatch):
    """Admission is ready when the controller is idle and its detectors are clear since the resume.

    Detectors evaluate while the previous row's closure is awaited; those evaluations after the resume count.
    The proof must not wait for a second round of evaluations after the closure ends.
    """
    gate, calls = lane
    now = previous_reflection(gate, monkeypatch, finishes_at=60, controller_bound=900)
    frozen_wall_clock(monkeypatch, now)
    gate.timeout_seconds = 10
    monkeypatch.setattr(
        admission,
        "wait_for_proof",
        partial(admission.wait_for_proof, sleep=lambda seconds: now.__setitem__(0, now[0] + seconds)),
    )
    healthy = gate.ops.runtime_state

    def state(control):
        value = healthy(control)
        if "history" in value:
            evaluated = (EPOCH + timedelta(seconds=30)).isoformat()  # during the wait, after the resume
            value["history"] = [dict(row, evaluated_at=evaluated) for row in value["history"]]
        return value

    gate.ops = SimpleNamespace(runtime_state=state)
    proof = gate.prove("hotel-reservation-sdo")
    assert proof["passed"] is True
    assert calls.count("sample") == 1, "admission waited for a re-evaluation that had already happened"
    assert now[0] < 70


def test_evidence_from_before_the_resume_is_not_ready(lane, monkeypatch):
    gate, _ = lane
    now = observe_only(gate, monkeypatch)
    frozen_wall_clock(monkeypatch, now)
    stale = (EPOCH - timedelta(seconds=1)).isoformat()
    healthy = gate.ops.runtime_state
    gate.ops = SimpleNamespace(
        runtime_state=lambda control: dict(
            healthy(control), history=[dict(row, evaluated_at=stale) for row in healthy(control)["history"]]
        )
    )
    with pytest.raises(admission.StabilizationError, match="did not settle"):
        gate.prove("hotel-reservation-sdo")


def test_admission_proof_is_controller_and_prober_evidence_only(lane, monkeypatch):
    """Traffic is proven once, by the restore that precedes admission; admission records no traffic call."""
    gate, calls = lane
    monkeypatch.setattr(admission, "assert_lane_resources", lambda *args, **kwargs: {"passed": True})
    proof = gate.prove("hotel-reservation-sdo")
    assert proof["passed"] is True
    assert "independent_traffic" not in proof
    assert not hasattr(gate, "worker")


# --- the three kept checks, each refusing a polluted lane before injection ---


def test_foreign_namespace_residue_refuses_the_row(lane, monkeypatch):
    gate, calls = lane

    def residue(*args, **kwargs):
        raise ValueError("eval: injected objects remain: Deployment/other-app/leftover")

    monkeypatch.setattr(admission, "assert_lane_resources", lambda *args, **kwargs: {"passed": True})
    monkeypatch.setattr(admission, "assert_clean_baseline", residue)
    with pytest.raises(ValueError, match="injected objects remain"):
        gate.prove("hotel-reservation-sdo")
    assert "sample" not in calls


def test_a_workload_that_rolls_after_ready_refuses_the_row(lane, monkeypatch):
    gate, _ = lane
    monkeypatch.setattr(admission, "assert_lane_resources", lambda *args, **kwargs: {"passed": True})
    identities = iter([{"Deployment/app": {"pods": ["uid-1"]}}, {"Deployment/app": {"pods": ["uid-2"]}}])
    monkeypatch.setattr(admission, "workload_identity", lambda *args: next(identities))
    with pytest.raises(ValueError, match=r"restore invariant: workloads rolled after Ready: \['Deployment/app'\]"):
        gate.prove("hotel-reservation-sdo")


def test_open_incident_work_refuses_injection_and_names_the_incident(lane, monkeypatch):
    gate, calls = lane
    now = fake_time(gate, monkeypatch)
    monkeypatch.setattr(admission, "assert_lane_resources", lambda *args, **kwargs: {"passed": True})
    gate.ops = SimpleNamespace(
        runtime_state=lambda _control: {
            "incident_open": True,
            "incident_request": {"incident_id": "row-009"},
            "busy_until": busy_at(120),
        }
    )
    with pytest.raises(admission.PriorClosureIncompleteError, match=r"incident work: row-009"):
        gate.prove("hotel-reservation-sdo")
    assert "sample" not in calls
    assert now[0] == 120 + CLOSEOUT_MARGIN
