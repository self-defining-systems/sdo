from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.sregym.adapter import ControllerInstallError, RuntimeConfig
from benchmarks.sregym.adapter.driver import DeployedLifecycle, DeployedLifecycleContext
from benchmarks.sregym.adapter.persistent import Clock, ControllerPod, PersistentState, control_namespace_for
from benchmarks.sregym.fastloop.loop import InjectionWindow
from benchmarks.sregym.fastloop.sdo_agent import SdoAgentSettings, SdoPersistentAgent

if TYPE_CHECKING:
    from collections.abc import Callable

INJECTED = datetime(2026, 9, 27, 12, 0, 0, tzinfo=timezone.utc)
DETECTED = INJECTED + timedelta(seconds=9)
VERIFIED = DETECTED + timedelta(seconds=60)
RECEIPT_RECORDED = VERIFIED + timedelta(seconds=12)
ROOT_CAUSES = [{"summary": "mongo-geo-script ConfigMap is missing"}]
REPAIRS = [
    {"summary": "failed attempt", "success": False, "completed_at": "2026-09-27T12:00:50Z"},
    {"summary": "restored ConfigMap", "success": True, "completed_at": "2026-09-27T12:00:30Z"},
]


def _receipt(incident_id: str, *, warm: bool) -> dict[str, Any]:
    return {
        "incident_id": incident_id,
        # The two volatile close-out signals the strict gate samples; a solved
        # incident reports them settled, so the drain's settle wait returns at once.
        "completed": True,
        "remaining_worktrees": [],
        "usage": {"input_tokens": 300000, "cached_input_tokens": 250000, "output_tokens": 3000},
        "reflection_usage": {} if warm else {"input_tokens": 800000, "cached_input_tokens": 700000, "output_tokens": 9},
        "reflection_attempts": 0 if warm else 1,
        "reflection_skipped_reason": "repeated exact-match success" if warm else None,
        "memory_reuse": {"warm_path": warm, "match_reasons": ["exact-fingerprint"] if warm else []},
        "confirmed_root_causes": ROOT_CAUSES,
        "repair_actions": REPAIRS,
        "recorded_at": RECEIPT_RECORDED.isoformat(),
        # As the runtime receipt derives them from the broker closure's timestamps.
        "phase_timings_seconds": {
            "operational_recovery": (VERIFIED - DETECTED).total_seconds(),
            "post_recovery_learning_and_receipt": (RECEIPT_RECORDED - VERIFIED).total_seconds(),
            "total": (RECEIPT_RECORDED - DETECTED).total_seconds(),
        },
    }


@dataclass
class FakeOps:
    installs: int = 0
    incidents: int = 0
    pods: dict[str, ControllerPod] = field(default_factory=dict)
    states: dict[str, dict[str, Any]] = field(default_factory=dict)
    logs: dict[str, list[str]] = field(default_factory=dict)
    reject_receipts: bool = False
    #: Replaces the synthetic receipt, for a test that needs one that passes strict validation.
    receipt_override: dict[str, Any] | None = None
    #: Reflection finishes before the driver's first poll sees the verified closure.
    reflect_before_first_poll: bool = False
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
        assert config.submission_api_base is None
        assert config.submission_relay_target_base is None
        if config.reuse_existing and control in self.pods:
            return True
        self.installs += 1
        self.pods[control] = ControllerPod(name=f"pod-{self.installs}", uid=f"uid-{self.installs}")
        self.states[control] = {}
        self.logs[control] = []
        return False

    def set_maintenance(self, control_namespace: str, *, paused: bool, generation: str) -> None:
        mode = "paused" if paused else "active"
        self.logs[control_namespace].append(
            json.dumps({"controller_maintenance": mode, "maintenance_generation": generation})
        )

    def _reflect(self, control_namespace: str, incident_id: str) -> None:
        state = self.states[control_namespace]
        state.pop("pending_closure", None)
        state["last_acknowledged_incident_id"] = incident_id
        self.logs[control_namespace].extend(
            [
                json.dumps({"controller_closure_restart": incident_id}),
                json.dumps({"controller_supervisor": "relaunch", "launches": 1}),
                json.dumps({"controller_maintenance": "paused", "maintenance_generation": "relaunched"}),
            ]
        )

    def runtime_state(self, control_namespace: str) -> dict[str, Any]:
        state = self.states[control_namespace]
        closure = state.get("pending_closure")
        if isinstance(closure, dict) and closure.get("reflect_on_next_poll"):
            self._reflect(control_namespace, closure["request"]["incident_id"])
        elif isinstance(closure, dict):
            closure["reflect_on_next_poll"] = True
        return state

    def controller_logs(self, control_namespace: str) -> str:
        return "\n".join(self.logs.get(control_namespace, []))

    def inject_after_resume(
        self, control_namespace: str, generation: str, inject: Callable[[], None]
    ) -> dict[str, float]:
        inject()
        self.incidents += 1
        incident_id = f"incident-{self.incidents}"
        if self.reflect_before_first_poll:
            self._reflect(control_namespace, incident_id)
            return {"controller_baseline_wait": 1.5, "fault_injection_request": 6.0}
        self.states[control_namespace]["pending_closure"] = {
            "request": {"incident_id": incident_id},
            "result": {"confirmed_root_causes": ROOT_CAUSES, "repair_actions": REPAIRS},
            "detected_at": DETECTED.isoformat(),
            "dispatched_at": DETECTED.isoformat(),
            "responder_completed_at": (DETECTED + timedelta(seconds=40)).isoformat(),
            "verified_at": VERIFIED.isoformat(),
        }
        return {"controller_baseline_wait": 1.5, "fault_injection_request": 6.0}

    def collect_receipt(self, config: RuntimeConfig, incident_id: str, artifacts_dir: Path) -> dict[str, Any]:
        receipt = _receipt(incident_id, warm=incident_id != "incident-1")
        if self.receipt_override is not None:
            receipt = {**self.receipt_override, "incident_id": incident_id}
        if self.reject_receipts:
            receipt["completed"] = False
        return receipt

    def reap_orphan_worktrees(self, config: RuntimeConfig, keep_dirnames: set[str]) -> list[str]:
        self.worktrees = {name for name in self.worktrees if name in keep_dirnames}
        return [f"/workspace/worktrees/{name}" for name in sorted(self.worktrees)]

    def export_runtime_artifacts(self, config: Any, artifacts_dir: Path) -> dict[str, str | None]:
        self.last_runtime_export = artifacts_dir
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
    lifecycle_calls: list[str],
) -> SdoPersistentAgent:
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
        ),
        state_path=tmp_path / "sdo_persistent_controller.json",
        results_dir=tmp_path / "incidents",
        verification_timeout_seconds=600,
    )
    context = DeployedLifecycleContext(health_objective="objective", active_resources=[])

    def run_lifecycle(received: DeployedLifecycleContext) -> bool:
        assert received == context
        lifecycle_calls.append("lifecycle")
        ops.install(replace(settings.runtime_config, persistent=True, wait_for_completion=False))
        return False

    return SdoPersistentAgent(
        settings,
        ops=ops,
        lifecycle_inputs=lambda: DeployedLifecycle(context=context, fingerprint="topology-1"),
        run_lifecycle=run_lifecycle,
        clock=_clock(),
    )


def _inject() -> InjectionWindow:
    return InjectionWindow(started_at=INJECTED, finished_at=INJECTED + timedelta(seconds=6))


def test_first_incident_installs_once_and_reports_resolution_from_controller_evidence(tmp_path: Path) -> None:
    ops, calls = FakeOps(), []
    agent = _agent(tmp_path, ops, calls)

    outcome = agent.resolve(0, "missing_configmap_hotel_reservation", _inject)

    assert calls == ["lifecycle"]
    assert ops.installs == 1
    assert outcome.detected_at == DETECTED
    # Only successful repairs count as the mitigation.
    assert outcome.mitigation_applied_at == datetime(2026, 9, 27, 12, 0, 30, tzinfo=timezone.utc)
    assert outcome.resolved_at == VERIFIED
    assert outcome.diagnosis == "mongo-geo-script ConfigMap is missing"
    assert outcome.mitigation == "restored ConfigMap"
    assert outcome.baseline_gate_seconds == pytest.approx(1.5)
    assert outcome.controller_installed is True
    assert outcome.lifecycle_validation_source is None  # the validation cache is opt-in
    assert outcome.previous_reflection_drain_seconds == pytest.approx(0.0)
    assert outcome.artifacts_dir == str(tmp_path / "incidents" / "000_missing_configmap_hotel_reservation")


def test_learning_drains_reflection_and_reads_tokens_and_warm_path_from_the_strict_receipt(tmp_path: Path) -> None:
    ops, calls = FakeOps(), []
    agent = _agent(tmp_path, ops, calls)

    cold = agent.learn(agent.resolve(0, "p", _inject))
    warm = agent.learn(agent.resolve(1, "p", _inject))

    assert calls == ["lifecycle"]
    assert ops.installs == 1
    assert (cold.warm_path, warm.warm_path) == (False, True)
    assert cold.responder_tokens.input_tokens == 300000
    assert cold.reflection_tokens.input_tokens == 800000
    assert warm.reflection_tokens.total_tokens == 0
    assert warm.reflection_skipped_reason == "repeated exact-match success"
    assert warm.match_reasons == ("exact-fingerprint",)
    assert cold.reflection_seconds is not None
    assert cold.reflection_seconds >= 0
    assert warm.controller_installed is False
    state = PersistentState.load(tmp_path / "sdo_persistent_controller.json")
    assert state.controllers["hotel-reservation"].pending is None


def test_learning_exports_reflection_session_before_checkpointing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ops, calls = FakeOps(), []
    agent = _agent(tmp_path, ops, calls)
    resolved = agent.resolve(0, "p", _inject)
    ops.last_runtime_export = None
    agent._settings = replace(agent._settings, kubeconfig="captured-kubeconfig")
    seen: list[Path] = []
    monkeypatch.setattr(agent, "_checkpoint", lambda _index, _incident, path: seen.append(path))

    agent.learn(resolved)

    assert ops.last_runtime_export == Path(str(resolved.artifacts_dir))
    assert seen == [Path(str(resolved.artifacts_dir))]


def test_learning_exports_monotonic_closure_phase_durations(tmp_path: Path) -> None:
    receipt = _receipt("incident-1", warm=True)
    receipt["native_ledger"] = {
        "closure_phase_timestamps": {
            "closure_start": "2026-09-27T12:01:00Z",
            "outcome_write": "2026-09-27T12:01:02Z",
            "reflection_dispatch": "2026-09-27T12:01:03Z",
            "worker_result_picked_up": "2026-09-27T12:01:05Z",
        }
    }
    ops, calls = FakeOps(receipt_override=receipt), []
    agent = _agent(tmp_path, ops, calls)
    learned = agent.learn(agent.resolve(0, "p", _inject))
    assert learned.closure_phase_seconds == {
        "outcome_write": 2.0,
        "reflection_dispatch": 1.0,
        "worker_result_picked_up": 2.0,
    }


def test_a_rejected_receipt_keeps_its_tokens_reports_the_error_and_unblocks_the_next_incident(tmp_path: Path) -> None:
    ops, calls = FakeOps(reject_receipts=True), []
    agent = _agent(tmp_path, ops, calls)

    learned = agent.learn(agent.resolve(0, "p", _inject))

    assert learned.error is not None
    assert "receipt" in learned.error
    assert learned.responder_tokens.input_tokens == 300000
    assert (
        PersistentState.load(tmp_path / "sdo_persistent_controller.json").controllers["hotel-reservation"].pending
        is None
    )
    agent.resolve(1, "p", _inject)
    assert ops.incidents == 2


def test_an_incident_cleared_without_sdo_action_is_recorded_as_such_not_as_a_mitigation(tmp_path: Path) -> None:
    fixtures = Path(__file__).resolve().parents[4] / "fixtures" / "sregym" / "phase1_rejected_receipts"
    real = json.loads((fixtures / "c5.json").read_text(encoding="utf-8"))["receipt"]
    ops, calls = FakeOps(receipt_override=real), []
    agent = _agent(tmp_path, ops, calls)

    learned = agent.learn(agent.resolve(0, "p", _inject))

    assert learned.error is None
    assert learned.sdo_resolution == "cleared_without_sdo_action"


def test_an_injection_failure_propagates_to_the_loop(tmp_path: Path) -> None:
    ops, calls = FakeOps(), []
    agent = _agent(tmp_path, ops, calls)

    def failing_inject() -> InjectionWindow:
        raise ControllerInstallError("worker could not inject")

    with pytest.raises(ControllerInstallError, match="could not inject"):
        agent.resolve(0, "p", failing_inject)


def test_a_closure_missed_between_polls_takes_its_times_from_the_receipt(tmp_path: Path) -> None:
    ops, calls = FakeOps(reflect_before_first_poll=True), []
    agent = _agent(tmp_path, ops, calls)

    resolved = agent.resolve(0, "missing_configmap_hotel_reservation", _inject)
    assert (resolved.detected_at, resolved.resolved_at) == (None, None)
    learned = agent.learn(resolved)

    assert learned.detected_at == DETECTED
    assert learned.resolved_at == VERIFIED
    assert learned.mitigation_applied_at == datetime(2026, 9, 27, 12, 0, 30, tzinfo=timezone.utc)
    assert learned.diagnosis == "mongo-geo-script ConfigMap is missing"
    assert learned.mitigation == "restored ConfigMap"


def test_times_seen_at_verification_are_not_replaced_by_the_receipt(tmp_path: Path) -> None:
    ops, calls = FakeOps(), []
    agent = _agent(tmp_path, ops, calls)
    resolved = agent.resolve(0, "p", _inject)
    earlier = resolved.with_learning(detected_at=DETECTED - timedelta(seconds=1))

    learned = agent.learn(earlier)

    assert learned.detected_at == DETECTED - timedelta(seconds=1)
    assert learned.resolved_at == VERIFIED


def test_learning_does_not_open_the_next_incident_without_receipt_evidence(tmp_path: Path) -> None:
    ops, calls = FakeOps(), []
    agent = _agent(tmp_path, ops, calls)
    resolved = agent.resolve(0, "p", _inject)
    agent._receipt = lambda _path: {}  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="receipt missing"):
        agent.learn(resolved)


def test_learning_uses_the_native_verified_rebuild_time(tmp_path: Path) -> None:
    ops, calls = FakeOps(), []
    agent = _agent(tmp_path, ops, calls)
    resolved = agent.resolve(0, "p", _inject)
    receipt = agent._receipt
    rebuilt = VERIFIED + timedelta(seconds=8)
    agent._receipt = lambda path: receipt(path) | {  # type: ignore[method-assign]
        "native_ledger": {
            "rollout_receipts": [{"success": True, "verified_at": rebuilt.isoformat()}],
        }
    }

    learned = agent.learn(resolved)

    assert learned.rebuild_done_at == rebuilt
