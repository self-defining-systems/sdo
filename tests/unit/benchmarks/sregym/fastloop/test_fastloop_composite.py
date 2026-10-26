from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.sregym.adapter.driver import DeployedLifecycle, DeployedLifecycleContext
from benchmarks.sregym.adapter.persistent import (
    Clock,
    ClosureFailedError,
    ControllerPod,
    DetectionMissError,
    PersistentState,
    control_namespace_for,
)
from benchmarks.sregym.adapter.runtime import RuntimeConfig
from benchmarks.sregym.fastloop.composite import CompositeSdoAgent, CompositeSettings, TrackedAgent
from benchmarks.sregym.fastloop.fault_tracker import FaultSpec
from benchmarks.sregym.fastloop.loop import AgentOutcome, InjectionWindow, LoopConfig, run_incidents
from benchmarks.sregym.fastloop.records import OracleVerdict, TokenCounts
from benchmarks.sregym.fastloop.sdo_agent import SdoAgentSettings

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

INJECTED = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
PROBLEM = "composite3_hotel_geo_rate_recommendation"
FAST = CompositeSettings(deadline_seconds=60, idle_seconds=5, stable_seconds=0.001, poll_seconds=0.01)


@pytest.fixture(autouse=True)
def registered_faults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "benchmarks.sregym.fastloop.composite.composite_faults",
        lambda problem: (
            (
                FaultSpec("readiness", "geo"),
                FaultSpec("configmap", "mongodb-rate"),
                FaultSpec("network_policy", "recommendation"),
            )
            if problem == PROBLEM
            else ()
        ),
    )


def _closure(incident_id: str, offset: int) -> dict[str, Any]:
    at = INJECTED + timedelta(seconds=offset)
    return {
        "request": {"incident_id": incident_id},
        "result": {
            "confirmed_root_causes": [{"summary": f"cause of {incident_id}"}],
            "repair_actions": [{"summary": f"fix {incident_id}", "success": True, "completed_at": at.isoformat()}],
        },
        "detected_at": at.isoformat(),
        "dispatched_at": at.isoformat(),
        "responder_completed_at": (at + timedelta(seconds=20)).isoformat(),
        "verified_at": (at + timedelta(seconds=30)).isoformat(),
        "final_detector_states": [{"detector_id": "health", "status": "clear"}],
    }


@dataclass
class FakeOps:
    """One controller; incident-1 closes on injection, incident-2 appears after incident-1 is reflected."""

    second_incident: bool = True
    wedge: bool = False
    fixed: set[str] = field(default_factory=set)
    pods: dict[str, ControllerPod] = field(default_factory=dict)
    states: dict[str, dict[str, Any]] = field(default_factory=dict)
    logs: dict[str, list[str]] = field(default_factory=dict)
    maintenance: list[str] = field(default_factory=list)
    receipts: list[str] = field(default_factory=list)
    receipt_configs: list[RuntimeConfig] = field(default_factory=list)
    seen_closures: set[tuple[str, str]] = field(default_factory=set)
    #: Worktree directory names under the shared /workspace/worktrees root. Empty by
    #: default, so there is nothing for the quiescent-drain reap to remove.
    worktrees: set[str] = field(default_factory=set)

    def controller_pod(self, control_namespace: str) -> ControllerPod | None:
        return self.pods.get(control_namespace)

    def namespace_exists(self, namespace: str) -> bool:
        return namespace in self.pods

    def delete_controller(self, control_namespace: str) -> None:
        self.pods.pop(control_namespace, None)

    def install(self, config: RuntimeConfig) -> bool:
        control = config.control_namespace
        assert control is not None
        self.pods[control] = ControllerPod(name="pod", uid="uid")
        self.states[control] = {}
        self.seen_closures = {key for key in self.seen_closures if key[0] != control}
        self.logs[control] = []
        return False

    def set_maintenance(
        self, control_namespace: str, *, paused: bool, generation: str, preserve_baseline: bool = False
    ) -> None:
        self.maintenance.append("paused" if paused else "active")
        self.logs[control_namespace].append(
            json.dumps(
                {"controller_maintenance": "paused" if paused else "active", "maintenance_generation": generation}
            )
        )

    def runtime_state(self, control_namespace: str) -> dict[str, Any]:
        if threading.current_thread().name == "controller-state":
            return self.states.get(control_namespace, {})
        time.sleep(0.03)  # real time passes so the background fault poller samples between controller polls
        state = self.states[control_namespace]
        closure = state.get("pending_closure")
        if isinstance(closure, dict):
            incident_id = closure["request"]["incident_id"]
            if (control_namespace, incident_id) in self.seen_closures:
                state.pop("pending_closure")
                state["last_acknowledged_incident_id"] = incident_id
                self.logs[control_namespace].extend(
                    [
                        json.dumps({"controller_closure_restart": incident_id}),
                        json.dumps({"controller_supervisor": "relaunch", "launches": 1}),
                        json.dumps({"controller_maintenance": "active", "maintenance_generation": "relaunched"}),
                    ]
                )
                if incident_id == "incident-1" and self.second_incident:
                    state["pending_closure"] = _closure("incident-2", 300)
                if incident_id == "incident-2":
                    self.fixed.add("network_policy:recommendation")
            else:
                self.seen_closures.add((control_namespace, incident_id))
        return state

    def controller_logs(self, control_namespace: str) -> str:
        return "\n".join(self.logs.get(control_namespace, []))

    def inject_after_resume(
        self, control_namespace: str, generation: str, inject: Callable[[], None]
    ) -> dict[str, float]:
        inject()
        self.fixed |= {"readiness:geo", "configmap:mongodb-rate"}
        if self.wedge:
            self.states[control_namespace].update(
                {
                    "incident_open": True,
                    "responder_done": True,
                    "incident_request": {"incident_id": "incident-1"},
                    "incident_detected_at": INJECTED.isoformat(),
                    "incident_result": {
                        "confirmed_root_causes": [{"summary": "geo and mongodb-rate"}],
                        "repair_actions": [
                            {"summary": "fixed two", "success": True, "completed_at": INJECTED.isoformat()}
                        ],
                        "usage": {"input_tokens": 500, "cached_input_tokens": 400, "output_tokens": 50},
                    },
                }
            )
            return {"controller_baseline_wait": 1.0}
        self.states[control_namespace]["pending_closure"] = _closure("incident-1", 9)
        return {"controller_baseline_wait": 1.0}

    def collect_receipt(self, config: RuntimeConfig, incident_id: str, artifacts_dir: Path) -> dict[str, Any]:
        self.receipts.append(incident_id)
        self.receipt_configs.append(config)
        return {
            "schema_version": "sdo.production-receipt/v1",
            "pre_cutover": False,
            "validator_mode": "kubernetes-job",
            "lifecycle_provenance": True,
            "production_job_dispatch": True,
            "completed": True,
            "repair_policy": "recorded-actions",
            "repair_actions": [
                {
                    "action_id": "a1",
                    "kind": "kubectl",
                    "target": "configmap/x",
                    "summary": "restored",
                    "details": "restored",
                    "started_at": "2026-09-27T12:00:10+00:00",
                    "completed_at": "2026-09-27T12:00:11+00:00",
                    "success": True,
                    "reversible": True,
                }
            ],
            "outcome_commit": "outcome",
            "reflection_commit": "reflection",
            "validator_evidence_commit": "reflection",
            "validator_execution_required": False,
            "validator_skipped_reason": "unchanged-diagnostics",
            "validator_network_policy_canaries": [],
            "same_session_reflection": True,
            "detector_clear": [{"status": "clear", "fingerprints": []}],
            "independent_verification": [{"passed": True}],
            "acknowledged": True,
            "cleaned": True,
            "remaining_worktrees": [],
            "responder_jobs": [f"sdo-{incident_id}"],
            "incident_id": incident_id,
            "usage": {"input_tokens": 100, "cached_input_tokens": 50, "output_tokens": 10},
            "reflection_usage": {"input_tokens": 40, "cached_input_tokens": 0, "output_tokens": 4},
            "memory_reuse": {"warm_path": incident_id == "incident-2", "match_reasons": []},
            "phase_timings_seconds": {"operational_recovery": 30.0, "post_recovery_learning_and_receipt": 60.0},
            "recorded_at": (INJECTED + timedelta(seconds=100)).isoformat(),
        }

    def reap_orphan_worktrees(self, config: RuntimeConfig, keep_dirnames: set[str]) -> list[str]:
        self.worktrees = {name for name in self.worktrees if name in keep_dirnames}
        return [f"/workspace/worktrees/{name}" for name in sorted(self.worktrees)]

    def export_runtime_artifacts(self, config: Any, artifacts_dir: Path) -> dict[str, str | None]:
        return {"directory": None, "error": None}

    def export_controller_logs(self, control_namespace: str, artifacts_dir: Path) -> None:
        pass


def _clock() -> Clock:
    now = [0.0]

    def sleep(seconds: float) -> None:
        now[0] += seconds

    return Clock(monotonic=lambda: now[0], sleep=sleep, now=lambda: INJECTED)


def _agent(
    tmp_path: Path,
    ops: FakeOps,
    *,
    agent_registrations: Path | None = None,
    detection_timeout_seconds: float | None = None,
) -> CompositeSdoAgent:
    repository = tmp_path / "application_workspace"
    repository.mkdir(exist_ok=True)
    namespace = "hotel-reservation"
    settings = SdoAgentSettings(
        repository=repository,
        namespace=namespace,
        application="Hotel Reservation",
        runtime_config=RuntimeConfig(
            repository=repository,
            namespace=namespace,
            application="Hotel Reservation",
            controller_image="controller:fastloop",
            responder_image="responder:fastloop",
            repository_pvc="sdo-application-repository",
            credentials_secret="sdo-agent-credentials",
            model="gpt-6-luna",
            timeout_seconds=3600,
            repair_policy="recorded-actions",
            controller_namespace=control_namespace_for(namespace),
            agent_registrations=agent_registrations,
        ),
        state_path=tmp_path / "state.json",
        results_dir=tmp_path / "incidents",
        verification_timeout_seconds=600,
        detection_timeout_seconds=detection_timeout_seconds,
    )
    context = DeployedLifecycleContext(health_objective="objective", active_resources=[])

    def reader(kind: str, name: str) -> dict[str, Any] | None:
        green = {
            "readiness:geo": ("deployment", "geo"),
            "configmap:mongodb-rate": ("deployment", "mongodb-rate"),
            "network_policy:recommendation": ("deployment", "recommendation"),
        }
        for fault, key in green.items():
            if (kind, name) == key and fault in ops.fixed:
                return {
                    "metadata": {"generation": 1},
                    "spec": {"replicas": 1},
                    "status": {"observedGeneration": 1, "readyReplicas": 1, "updatedReplicas": 1, "replicas": 1},
                }
        if kind == "networkpolicy":
            return None
        return {"metadata": {"generation": 1}, "spec": {"replicas": 1}, "status": {"observedGeneration": 1}}

    def run_lifecycle(_received: DeployedLifecycleContext) -> bool:
        ops.install(replace(settings.runtime_config, persistent=True, wait_for_completion=False))
        return False

    return CompositeSdoAgent(
        settings,
        ops=ops,
        lifecycle_inputs=lambda: DeployedLifecycle(fingerprint="fp", context=context),
        run_lifecycle=run_lifecycle,
        clock=_clock(),
        reader=reader,
        report_dir=tmp_path / "reports",
        composite=FAST,
    )


def _inject() -> InjectionWindow:
    return InjectionWindow(started_at=INJECTED, finished_at=INJECTED + timedelta(seconds=2))


@pytest.mark.parametrize("recovery_oracle_failed", [False, True])
def test_failed_composite_resets_only_after_verified_harness_recovery(
    tmp_path: Path, recovery_oracle_failed: bool, caplog: pytest.LogCaptureFixture
) -> None:
    class RestartOps(FakeOps):
        installs = 0

        def namespace_exists(self, namespace: str) -> bool:
            return namespace in self.states

        def install(self, config: RuntimeConfig) -> bool:
            self.installs += 1
            return super().install(config)

    ops = RestartOps(second_incident=False)
    agent = _agent(tmp_path, ops)
    resets: list[str] = []

    class Driver:
        incidents = 0
        recovered = False

        def inject(self, _problem: str) -> InjectionWindow:
            assert ops.controller_pod("hotel-reservation-sdo") is not None
            self.incidents += 1
            self.recovered = False
            return _inject()

        def oracle(self) -> OracleVerdict:
            success = self.incidents > 1 or (self.recovered and not recovery_oracle_failed)
            return OracleVerdict(kind="sregym-mitigation-oracle", success=success)

        def recover(self) -> float:
            self.recovered = True
            return 0.0

    def reset_failed() -> None:
        # The reset protocol keeps the accepted repository and control namespace,
        # deletes Jobs, and invalidates the saved controller Pod identity.
        state = PersistentState.load(tmp_path / "state.json")
        record = state.controllers["hotel-reservation"]
        ops.pods.pop(record.control_namespace)
        record.controller_pod_uid = record.controller_pod_name = ""
        record.pending = None
        state.save(tmp_path / "state.json")
        resets.append("reset")

    config = LoopConfig(
        "composite-reset",
        (PROBLEM, "single"),
        2,
        tmp_path / "rows.jsonl",
        continue_on_error=True,
        recovery_settle_seconds=1,
    )
    with caplog.at_level("INFO"):
        rows = run_incidents(agent, Driver(), config, on_failed_incident=reset_failed)

    if recovery_oracle_failed:
        assert len(rows) == 1
        assert rows[0].recovery_error is not None
        assert resets == []
        return
    assert len(rows) == 2
    assert rows[0].error is not None
    assert "oracle reported failure" in rows[0].error
    assert rows[1].error is None
    assert ops.installs == 2
    assert resets == ["reset"]
    assert "run_end" in caplog.text
    assert "completed=2 requested=2" in caplog.text


def test_controller_keeps_serving_until_every_fault_is_resolved(tmp_path: Path) -> None:
    ops = FakeOps()
    agent = _agent(tmp_path, ops)

    outcome = agent.resolve(0, PROBLEM, _inject)
    outcome = agent.learn(outcome)

    report = json.loads((tmp_path / "reports" / f"composite_000_{PROBLEM}.json").read_text(encoding="utf-8"))
    assert [item["incident_id"] for item in report["incidents"]] == ["incident-1", "incident-2"]
    assert report["stop_reason"] == "all_faults_resolved"
    assert report["faults_resolved"] == 3
    assert report["resolved_s"]["network_policy:recommendation"] is not None
    assert ops.maintenance[-1] == "paused"
    assert outcome.responder_tokens == TokenCounts(input_tokens=200, cached_input_tokens=100, output_tokens=20)
    assert outcome.reflection_tokens == TokenCounts(input_tokens=80, output_tokens=8)
    assert outcome.warm_path is True
    assert "cause of incident-1" in outcome.diagnosis
    assert "cause of incident-2" in outcome.diagnosis


def test_composite_agent_recovers_single_incident_times_from_durable_outcome(tmp_path: Path) -> None:
    class MissedClosureOps(FakeOps):
        def inject_after_resume(
            self, control_namespace: str, generation: str, inject: Callable[[], None]
        ) -> dict[str, float]:
            inject()
            self.states[control_namespace]["last_acknowledged_incident_id"] = "incident-1"
            self.logs[control_namespace].extend(
                [
                    json.dumps({"controller_closure_restart": "incident-1"}),
                    json.dumps({"controller_supervisor": "relaunch", "launches": 1}),
                ]
            )
            return {}

        def collect_receipt(self, config: RuntimeConfig, incident_id: str, artifacts_dir: Path) -> dict[str, Any]:
            receipt = super().collect_receipt(config, incident_id, artifacts_dir)
            receipt.pop("phase_timings_seconds")
            receipt["native_ledger"] = {
                "outcome_document": {
                    "timestamps": {
                        "detected_at": INJECTED.isoformat(),
                        "mitigated_at": (INJECTED + timedelta(seconds=20)).isoformat(),
                        "verified_at": (INJECTED + timedelta(seconds=30)).isoformat(),
                    }
                }
            }
            return receipt

    agent = _agent(tmp_path, MissedClosureOps())
    resolved = agent.resolve(0, "single", _inject)
    assert resolved.detected_at is None
    learned = agent.learn(resolved)

    assert learned.detected_at == INJECTED
    assert learned.mitigation_applied_at is not None
    assert learned.resolved_at == INJECTED + timedelta(seconds=30)


def test_composite_row_carries_closure_phases_and_reflection_skip(tmp_path: Path) -> None:
    ops = FakeOps(second_incident=False)
    collect = ops.collect_receipt

    def receipt(config: RuntimeConfig, incident_id: str, artifacts_dir: Path) -> dict[str, Any]:
        result = collect(config, incident_id, artifacts_dir)
        result["reflection_skipped_reason"] = "verified repeat"
        result["native_ledger"] = {
            "closure_phase_timestamps": {
                "closure_start": "2026-10-01T12:00:00Z",
                "broker_commit": "2026-10-01T12:00:02Z",
            }
        }
        return result

    ops.collect_receipt = receipt  # type: ignore[method-assign]
    agent = _agent(tmp_path, ops)

    learned = agent.learn(agent.resolve(0, PROBLEM, _inject))

    assert learned.reflection_skipped_reason == "verified repeat"
    assert learned.closure_phase_seconds == {"broker_commit": 2.0}


def test_lifecycle_failure_before_injection_skips_pause_and_logs_cause(tmp_path: Path, caplog: Any) -> None:
    ops = FakeOps()
    agent = _agent(tmp_path, ops)

    def fail_lifecycle(_context: DeployedLifecycleContext) -> bool:
        raise RuntimeError("lifecycle fixture failed")

    agent._run_lifecycle = fail_lifecycle
    with pytest.raises(RuntimeError, match="lifecycle fixture failed"):
        agent.resolve(0, PROBLEM, _inject)

    assert ops.maintenance == []
    assert "lifecycle fixture failed" in caplog.text


def test_composite_detection_miss_stays_a_detection_miss(tmp_path: Path) -> None:
    class LatentOps(FakeOps):
        def inject_after_resume(
            self, control_namespace: str, generation: str, inject: Callable[[], None]
        ) -> dict[str, float]:
            inject()
            return {"controller_baseline_wait": 1.0}

    agent = _agent(tmp_path, LatentOps(), detection_timeout_seconds=10)

    with pytest.raises(DetectionMissError, match="no incident opened within 10s"):
        agent.resolve(0, PROBLEM, _inject)


def test_composite_drains_pending_incident_with_scripted_agent_config(tmp_path: Path) -> None:
    ops = FakeOps()
    registrations = tmp_path / "scripted-agents.json"
    agent = _agent(tmp_path, ops, agent_registrations=registrations)

    outcome = agent.resolve(0, PROBLEM, _inject)
    agent.learn(outcome)

    assert ops.receipts == ["incident-1", "incident-2"]
    assert [config.agent_registrations for config in ops.receipt_configs] == [registrations, registrations]


def test_run_stops_when_no_further_incident_arrives_while_a_fault_remains(tmp_path: Path) -> None:
    ops = FakeOps(second_incident=False)
    agent = _agent(tmp_path, ops)

    outcome = agent.resolve(0, PROBLEM, _inject)
    agent.learn(outcome)

    report = json.loads((tmp_path / "reports" / f"composite_000_{PROBLEM}.json").read_text(encoding="utf-8"))
    assert report["stop_reason"] == "no_further_incident"
    assert report["faults_resolved"] == 2
    assert report["resolved_s"]["network_policy:recommendation"] is None
    assert len(report["incidents"]) == 1


def test_failed_closure_keeps_pending_outcome_evidence_and_stops(tmp_path: Path) -> None:
    ops = FakeOps(second_incident=False)
    agent = _agent(tmp_path, ops)
    runtime_state = ops.runtime_state

    def failed_state(control: str) -> dict[str, Any]:
        state = ops.states[control]
        return state if state.get("closure_state") == "CLOSURE_STATE_FAILED" else runtime_state(control)

    ops.runtime_state = failed_state  # type: ignore[method-assign]

    def fail_drain(*, first_label: str) -> None:
        state = ops.states[agent._settings.runtime_config.control_namespace]
        closure = state["pending_closure"]
        closure["result"].update(
            {
                "usage": {"input_tokens": 500, "cached_input_tokens": 400, "output_tokens": 50},
                "program_runs": [{"verdict": {"result": "RESULT_NO_MATCH"}}],
                "program_tool_calls": [{"command": "verify"}],
                "response_loops_used": 1,
            }
        )
        state["closure_state"] = "CLOSURE_STATE_FAILED"
        raise ClosureFailedError("reflection rejected the outcome")

    agent._drain_last = fail_drain  # type: ignore[method-assign]
    outcome = agent.resolve(0, PROBLEM, _inject)

    assert outcome.terminal_error is True
    assert outcome.error is not None
    assert "reflection rejected" in outcome.error
    assert outcome.detected_at is not None
    assert outcome.responder_finished_at is not None
    assert outcome.reflection_started_at is not None
    assert outcome.reflection_finished_at is not None
    assert outcome.reflection_seconds is not None
    assert outcome.responder_tokens.input_tokens == 500
    assert outcome.program_diagnoses == 1
    assert outcome.program_verdicts == {"RESULT_NO_MATCH": 1}
    assert outcome.tool_commands == {"verify": 1}
    report = json.loads((tmp_path / "reports" / f"composite_000_{PROBLEM}.json").read_text(encoding="utf-8"))
    assert report["stop_reason"] == "closure_failed"


@dataclass
class _Inner:
    name: str = "codex"
    model: str = "gpt-6-luna"
    fixed: set[str] = field(default_factory=set)

    def resolve(self, index: int, problem_id: str, inject: Callable[[], InjectionWindow]) -> AgentOutcome:
        window = inject()
        self.fixed.add("readiness:geo")
        time.sleep(0.2)
        return AgentOutcome(injection=window, mitigation_applied_at=window.finished_at + timedelta(seconds=30))

    def learn(self, outcome: AgentOutcome) -> AgentOutcome:
        return outcome

    def close(self) -> None:
        pass


def test_tracked_agent_reports_partial_fault_resolution(tmp_path: Path) -> None:
    inner = _Inner()

    def reader(kind: str, name: str) -> dict[str, Any] | None:
        if (kind, name) == ("deployment", "geo") and "readiness:geo" in inner.fixed:
            return {
                "metadata": {"generation": 1},
                "spec": {"replicas": 1},
                "status": {"observedGeneration": 1, "readyReplicas": 1, "updatedReplicas": 1, "replicas": 1},
            }
        return None

    agent = TrackedAgent(inner, reader=reader, report_dir=tmp_path, settings=FAST)

    agent.resolve(0, PROBLEM, _inject)

    report = json.loads((tmp_path / f"composite_000_{PROBLEM}.json").read_text(encoding="utf-8"))
    assert report["faults_resolved"] == 1
    assert report["all_resolved_s"] is None
    assert report["resolved_s"]["readiness:geo"] is not None
    assert report["agent"] == "codex"


def test_tracked_agent_passes_single_fault_problems_through(tmp_path: Path) -> None:
    inner = _Inner()
    agent = TrackedAgent(inner, reader=lambda kind, name: None, report_dir=tmp_path, settings=FAST)
    outcome = agent.resolve(0, "missing_configmap_hotel_reservation", _inject)
    assert outcome.injection == _inject()
    assert list(tmp_path.iterdir()) == []


def test_tracked_agent_uses_resolved_stream_faults(tmp_path: Path) -> None:
    problem_id = "mf__example"
    agent = TrackedAgent(
        _Inner(),
        reader=lambda kind, name: None,
        report_dir=tmp_path,
        settings=FAST,
        stream_faults={problem_id: (FaultSpec("target_port", "geo"), FaultSpec("incorrect_image", "rate"))},
    )

    agent.resolve(0, problem_id, _inject)

    report = json.loads((tmp_path / f"composite_000_{problem_id}.json").read_text(encoding="utf-8"))
    assert report["faults"] == ["target_port:geo", "incorrect_image:rate"]


def test_a_controller_that_never_closes_the_incident_is_reported_not_waited_on(tmp_path: Path) -> None:
    ops = FakeOps(wedge=True)
    agent = _agent(tmp_path, ops)

    outcome = agent.resolve(0, PROBLEM, _inject)
    outcome = agent.learn(outcome)

    report = json.loads((tmp_path / "reports" / f"composite_000_{PROBLEM}.json").read_text(encoding="utf-8"))
    assert report["stop_reason"] == "no_closure_within_deadline"
    assert report["faults_resolved"] == 2
    assert report["resolved_s"]["network_policy:recommendation"] is None
    assert outcome.resolved_at is None
    assert "geo and mongodb-rate" in outcome.diagnosis
    assert outcome.responder_tokens == TokenCounts(input_tokens=500, cached_input_tokens=400, output_tokens=50)
    assert outcome.error is not None
    assert "without a verified closure" in outcome.error
    assert ops.maintenance[-1] == "paused"
