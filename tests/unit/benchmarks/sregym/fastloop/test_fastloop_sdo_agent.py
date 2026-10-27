from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.sregym.adapter import ControllerInstallError, RuntimeConfig
from benchmarks.sregym.adapter.driver import DeployedLifecycle, DeployedLifecycleContext
from benchmarks.sregym.adapter.persistent import (
    CONTROLLER_LOGS_SUBDIR,
    REJECTED_RECEIPT_FILENAME,
    Clock,
    ControllerJobFailedError,
    ControllerPod,
    PersistentControllerError,
    PersistentState,
    ReflectionDeadlineError,
    control_namespace_for,
    drain_pending_incident,
)
from benchmarks.sregym.fastloop.loop import InjectionWindow, LoopConfig, run_incidents
from benchmarks.sregym.fastloop.records import OracleVerdict, load_records
from benchmarks.sregym.fastloop.sdo_agent import SdoAgentSettings, SdoPersistentAgent
from tests.unit.benchmarks.sregym.adapter.test_persistent import _with_controller_busy_until


def test_program_metrics_exports_match_evidence_and_skip_reason() -> None:
    from benchmarks.sregym.fastloop.sdo_agent import _program_metrics

    fields = _program_metrics(
        {
            "program_runs": [
                {
                    "program_id": "targetport",
                    "binding_source": "hint",
                    "verdict": {"result": "RESULT_MATCH", "evidence": [{"fact": "wrong target port"}]},
                },
                {
                    "program_id": "targetport",
                    "binding_source": "finding",
                    "verdict": {"result": "RESULT_MATCH", "evidence": [{"fact": "wrong target port"}]},
                },
            ],
        }
    )
    assert fields["match_reasons"] == ("wrong target port",)
    assert fields["binding_sources"] == ("hint", "finding")


def test_fastloop_record_persists_program_match_and_skip_evidence(tmp_path: Path) -> None:
    from benchmarks.sregym.fastloop.loop import _outcome_fields
    from benchmarks.sregym.fastloop.records import IncidentRecord, append_record

    receipt = _receipt("incident-1", warm=False)
    receipt["native_ledger"] = {
        "outcome_document": {
            "resolution_path": "responder",
            "program_runs": [
                {
                    "program_id": "targetport",
                    "binding_source": "hint",
                    "verdict": {"result": "RESULT_MATCH", "evidence": [{"fact": "wrong target port"}]},
                },
                {
                    "program_id": "targetport",
                    "binding_source": "finding",
                    "verdict": {"result": "RESULT_MATCH", "evidence": [{"fact": "wrong target port"}]},
                },
            ],
        }
    }
    agent = _agent(tmp_path, FakeOps(receipt_override=receipt), [])
    learned = agent.learn(agent.resolve(0, "p", _inject))
    record = IncidentRecord(
        run_id="test",
        index=0,
        agent="sdo",
        problem_id="p",
        model="test",
        injection={"state": "injected", "started_at": INJECTED, "finished_at": INJECTED},
        **_outcome_fields(learned),
    )
    path = tmp_path / "incidents.jsonl"
    append_record(path, record)
    row = load_records(path)[0]
    assert row.match_reasons == ["wrong target port"]
    assert row.binding_sources == ["hint", "finding"]


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
    controller_result_override: dict[str, Any] = field(default_factory=dict)
    seen_closures: set[tuple[str, str]] = field(default_factory=set)
    #: Replaces the synthetic receipt, for a test that needs one that passes strict validation.
    receipt_override: dict[str, Any] | None = None
    #: Reflection finishes before the driver's first poll sees the verified closure.
    reflect_before_first_poll: bool = False
    #: Worktree directory names under the shared /workspace/worktrees root. Empty by
    #: default, so there is nothing for the quiescent-drain reap to remove.
    worktrees: set[str] = field(default_factory=set)

    def controller_pod(self, control_namespace: str) -> ControllerPod | None:
        return self.pods.get(control_namespace)

    def controller_job_failure(self, control_namespace: str) -> str | None:
        return None

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
        self.seen_closures = {key for key in self.seen_closures if key[0] != control}
        self.logs[control] = []
        return False

    def set_maintenance(
        self, control_namespace: str, *, paused: bool, generation: str, preserve_baseline: bool = False
    ) -> None:
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
        if isinstance(closure, dict):
            incident_id = closure["request"]["incident_id"]
            if (control_namespace, incident_id) in self.seen_closures:
                self._reflect(control_namespace, incident_id)
            else:
                self.seen_closures.add((control_namespace, incident_id))
        return _with_controller_busy_until(state)

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
            "result": {
                "confirmed_root_causes": ROOT_CAUSES,
                "repair_actions": REPAIRS,
                "usage": {"input_tokens": 300000, "output_tokens": 3000},
                **self.controller_result_override,
            },
            "detected_at": DETECTED.isoformat(),
            "dispatched_at": DETECTED.isoformat(),
            "responder_completed_at": (DETECTED + timedelta(seconds=40)).isoformat(),
            "verified_at": VERIFIED.isoformat(),
            "final_detector_states": [{"detector_id": "health", "status": "clear"}],
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
        log = artifacts_dir / CONTROLLER_LOGS_SUBDIR / "controller.log"
        log.parent.mkdir(parents=True, exist_ok=True)
        log.write_text(self.controller_logs(control_namespace) + "\n", encoding="utf-8")


def _clock() -> Clock:
    now = [0.0]

    def sleep(seconds: float) -> None:
        now[0] += seconds

    return Clock(monotonic=lambda: now[0], sleep=sleep, now=lambda: INJECTED)


def _agent(
    tmp_path: Path,
    ops: FakeOps,
    lifecycle_calls: list[str],
    *,
    detection_timeout_seconds: float | None = None,
    reflection_wait_seconds: float = 3600.0,
    clock: Clock | None = None,
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
        detection_timeout_seconds=detection_timeout_seconds,
        reflection_wait_seconds=reflection_wait_seconds,
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
        clock=clock or _clock(),
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


def test_undetected_fault_is_graded_recovered_and_next_incident_runs(tmp_path: Path) -> None:
    class LatentFirstOps(FakeOps):
        def inject_after_resume(
            self, control_namespace: str, generation: str, inject: Callable[[], None]
        ) -> dict[str, float]:
            if self.incidents == 0:
                inject()
                self.incidents += 1
                # A successful idle snapshot is distinct from an unavailable API.
                self.states[control_namespace] = {
                    "history": [{"detector_id": "health", "status": "clear", "evaluated_at": INJECTED.isoformat()}]
                }
                return {"controller_baseline_wait": 1.5}
            return super().inject_after_resume(control_namespace, generation, inject)

    class Driver:
        def __init__(self) -> None:
            self.events: list[str] = []
            self.incident = 0
            self.recovered = False

        def inject(self, problem_id: str) -> InjectionWindow:
            self.events.append(f"inject:{problem_id}")
            self.incident += 1
            self.recovered = False
            return _inject()

        def oracle(self) -> OracleVerdict:
            self.events.append("oracle")
            return OracleVerdict(kind="sregym-mitigation-oracle", success=self.recovered or self.incident == 2)

        def recover(self) -> float:
            self.events.append("recover")
            self.recovered = True
            return 0.0

    ops, calls, driver = LatentFirstOps(), [], Driver()
    agent = _agent(tmp_path, ops, calls, detection_timeout_seconds=10)
    config = LoopConfig(
        run_id="detection-miss",
        problems=("latent", "visible"),
        incidents=2,
        results_path=tmp_path / "results" / "incidents.jsonl",
    )

    records = run_incidents(agent, driver, config)

    assert [record.problem_id for record in records] == ["latent", "visible"]
    assert "no incident opened within 10s" in (records[0].error or "")
    assert records[0].detected_at is None
    assert records[0].mitigation_applied_at is None
    assert records[0].oracle is not None
    assert not records[0].oracle.success
    assert records[0].recovery_oracle is not None
    assert records[0].recovery_oracle.success
    assert "no incident opened" not in (records[1].error or "")
    assert records[1].detected_at == DETECTED
    assert records[1].oracle is not None
    assert records[1].oracle.success
    assert driver.events == [
        "inject:latent",
        "oracle",
        "recover",
        "oracle",
        "inject:visible",
        "oracle",
        "recover",
        "oracle",
    ]
    assert len(load_records(config.results_path)) == 2


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


def test_resolved_incident_keeps_responder_cost_and_mitigation_time_when_learning_fails(tmp_path: Path) -> None:
    ops, calls = FakeOps(), []
    agent = _agent(tmp_path, ops, calls)
    resolved = agent.resolve(0, "p", _inject)
    assert resolved.responder_tokens.input_tokens == 300000
    assert resolved.mitigation_applied_at == INJECTED + timedelta(seconds=30)
    assert resolved.resolved_at == VERIFIED


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
    ops, calls = (
        FakeOps(reject_receipts=True, controller_result_override={}),
        [],
    )
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


@pytest.mark.parametrize("classification", ["success", "external_recovery"])
def test_row_keeps_outcome_times_when_closure_and_receipt_timings_are_missing(
    tmp_path: Path, classification: str
) -> None:
    receipt = _receipt("incident-1", warm=True)
    receipt.pop("phase_timings_seconds", None)
    receipt.pop("recorded_at", None)
    receipt.pop("repair_actions", None)
    receipt["native_ledger"] = {
        "outcome_document": {
            "classification": classification,
            "timestamps": {
                "detected_at": DETECTED.isoformat(),
                "dispatched_at": DETECTED.isoformat(),
                "mitigated_at": (DETECTED + timedelta(seconds=30)).isoformat(),
                "verified_at": VERIFIED.isoformat(),
            },
            "repair_actions": REPAIRS,
        }
    }
    agent = _agent(tmp_path, FakeOps(reflect_before_first_poll=True, receipt_override=receipt), [])

    class Driver:
        def inject(self, _problem: str) -> InjectionWindow:
            return _inject()

        def oracle(self) -> OracleVerdict:
            return OracleVerdict(kind="sregym-mitigation-oracle", success=True)

        def recover(self) -> float:
            return 0.0

    config = LoopConfig("times", ("p",), 1, tmp_path / "incidents.jsonl")
    run_incidents(agent, Driver(), config)
    row = load_records(config.results_path)[0]

    assert row.detected_at == DETECTED
    assert row.resolved_at == VERIFIED
    assert row.responder_started_at == DETECTED
    assert row.injection_to_detection_seconds == 9
    assert row.detection_to_mitigation_seconds is not None


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
    agent._receipt = lambda path: (
        receipt(path)
        | {  # type: ignore[method-assign]
            "native_ledger": {
                "rollout_receipts": [{"success": True, "verified_at": rebuilt.isoformat()}],
            }
        }
    )

    learned = agent.learn(resolved)

    assert learned.rebuild_done_at == rebuilt


def test_checkpoint_opt_out_preserves_resolution_and_learning(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    agent = _agent(tmp_path, FakeOps(), [])
    agent._settings = replace(agent._settings, kubeconfig="unused-kubeconfig", checkpoint_enabled=False)

    def unexpected(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("checkpoint transport must be disabled")

    monkeypatch.setattr("benchmarks.sregym.adapter.kubectl", unexpected)
    learned = agent.learn(agent.resolve(0, "p", _inject))
    assert learned.incident_id == "incident-1"
    assert learned.resolved_at == VERIFIED
    assert learned.reflection_tokens.input_tokens > 0


def test_evidence_gap_count_preserves_unknown_zero_and_recorded_counts() -> None:
    from benchmarks.sregym.fastloop.sdo_agent import _receipt_evidence_gap_count

    assert _receipt_evidence_gap_count({}) is None
    assert _receipt_evidence_gap_count({"native_ledger": {"reflection_evidence_gap_count": 0}}) == 0
    assert _receipt_evidence_gap_count({"native_ledger": {"reflection_evidence_gap_count": 2}}) == 2
    assert _receipt_evidence_gap_count({"native_ledger": {"reflection_evidence_gap_count": True}}) is None


def test_program_metrics_exports_incident_open_coverage() -> None:
    from benchmarks.sregym.fastloop.sdo_agent import _program_metrics

    coverage = {
        "matches": [{"playbook_id": "routing", "explained_finding_fingerprints": ["one", "two"]}],
        "unexplained_finding_fingerprints": ["three"],
    }
    assert _program_metrics({"playbook_coverage": coverage})["playbook_coverage"] == coverage


def _ticking_clock() -> Clock:
    """A clock whose wall time follows its monotonic time, as a real one does."""
    elapsed = [0.0]
    return Clock(
        monotonic=lambda: elapsed[0],
        sleep=lambda seconds: elapsed.__setitem__(0, elapsed[0] + seconds),
        now=lambda: INJECTED + timedelta(seconds=elapsed[0]),
    )


def _followup_state(outcome_incident: str, busy_until: datetime | None) -> dict[str, Any]:
    state: dict[str, Any] = {"last_acknowledged_incident_id": outcome_incident}
    if busy_until is not None:
        state.update(
            incident_open=True,
            incident_request={"incident_id": "incident-late"},
            busy_until=busy_until.isoformat(),
        )
    return state


def test_recovery_reports_a_followup_incident_that_outlives_the_controllers_bound(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from benchmarks.sregym.fastloop.loop import RecoveryWaitTimeout

    ops, calls, clock = FakeOps(), [], _ticking_clock()
    agent = _agent(tmp_path, ops, calls, clock=clock)
    outcome = agent.learn(agent.resolve(0, "p", _inject))
    started = clock.now()
    bound = started + timedelta(seconds=480)
    monkeypatch.setattr(ops, "runtime_state", lambda _control: _followup_state(outcome.incident_id, bound))

    with pytest.raises(RecoveryWaitTimeout, match="incident-late") as raised:
        agent.prepare_recovery(outcome)

    assert raised.value.wait.graded_incident_id == outcome.incident_id
    assert raised.value.wait.followup_incident_ids == ["incident-late"]
    assert raised.value.wait.closed is False
    # The controller's own bound plus one closeout margin, never a fixed 600 s.
    assert raised.value.wait.waited_seconds == raised.value.wait.bound_seconds == 480.0 + 120.0


def test_recovery_waits_past_ten_minutes_for_a_followup_the_controller_still_owns(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """h9.5 52/012: a follow-up incident ran past a fixed 600 s recovery wait and lost the row (P4RUN2, REQUESTS)."""
    ops, calls, clock = FakeOps(), [], _ticking_clock()
    agent = _agent(tmp_path, ops, calls, clock=clock)
    outcome = agent.learn(agent.resolve(0, "p", _inject))
    started = clock.now()
    bound = started + timedelta(seconds=2102)
    follow_up_runs_for = 1500.0

    def state(_control: str) -> dict[str, Any]:
        running = (clock.now() - started).total_seconds() < follow_up_runs_for
        return _followup_state(outcome.incident_id, bound if running else None)

    monkeypatch.setattr(ops, "runtime_state", state)

    wait = agent.prepare_recovery(outcome)

    assert wait is not None
    assert wait.closed is True
    assert wait.followup_incident_ids == ["incident-late"]
    assert follow_up_runs_for <= wait.waited_seconds < follow_up_runs_for + 10
    assert wait.waited_seconds > 600.0
    assert wait.bound_seconds >= 2102.0


class _WebhookDriver:
    """Injects a cluster-scoped webhook that only the fault's own recover removes."""

    def __init__(self, events: list[str] | None = None) -> None:
        self.events = events if events is not None else []
        self.webhook = False

    def inject(self, problem_id: str) -> InjectionWindow:
        self.events.append("inject")
        self.webhook = True
        return _inject()

    def oracle(self) -> OracleVerdict:
        return OracleVerdict(kind="sregym-mitigation-oracle", success=True)

    def recover(self) -> float:
        self.events.append("recover")
        self.webhook = False
        return 0.0


class _ClosureRetiredOps(FakeOps):
    """SDO verifies the incident, then retires its closure permanently (the pending closure stays recorded)."""

    def _reflect(self, control_namespace: str, incident_id: str) -> None:
        self.states[control_namespace].update(
            {
                "closure_state": "CLOSURE_STATE_FAILED",
                "closure_failure": {
                    "incident_id": incident_id,
                    "permanent": True,
                    "last_error": "closure observation detector service-reachability is unconfirmed",
                },
            }
        )


def _single_row(tmp_path: Path) -> LoopConfig:
    return LoopConfig(
        run_id="leak4", problems=("webhook",), incidents=1, results_path=tmp_path / "results" / "incidents.jsonl"
    )


def test_a_closure_failed_row_still_recovers_the_injected_webhook(tmp_path: Path) -> None:
    driver = _WebhookDriver()

    [record] = run_incidents(_agent(tmp_path, _ClosureRetiredOps(), []), driver, _single_row(tmp_path))

    assert driver.events == ["inject", "recover"]
    assert driver.webhook is False
    assert record.recovery_error is None
    assert record.recovery_trigger == "closure_failed"
    assert "closure permanently failed" in (record.error or "")
    assert (Path(str(record.artifacts_dir)) / REJECTED_RECEIPT_FILENAME).is_file()
    state = PersistentState.load(tmp_path / "sdo_persistent_controller.json")
    assert state.controllers["hotel-reservation"].pending is None


def test_a_running_reflection_delays_recovery_until_it_ends(tmp_path: Path) -> None:
    events: list[str] = []

    class SlowReflectionOps(FakeOps):
        polls: int = 0

        def runtime_state(self, control_namespace: str) -> dict[str, Any]:
            closure = self.states[control_namespace].get("pending_closure")
            if (
                isinstance(closure, dict)
                and (control_namespace, closure["request"]["incident_id"]) in self.seen_closures
            ):
                self.polls += 1
                if self.polls < 3:
                    events.append("reflecting")
                    return _with_controller_busy_until(self.states[control_namespace])
                events.append("reflected")
            return super().runtime_state(control_namespace)

    driver = _WebhookDriver(events)

    [record] = run_incidents(_agent(tmp_path, SlowReflectionOps(), []), driver, _single_row(tmp_path))

    assert events == ["inject", "reflecting", "reflecting", "reflected", "recover"]
    assert record.recovery_trigger == "natural_closure"
    assert record.recovery_error is None


def test_recovery_past_the_reflection_deadline_leaves_the_closure_to_sdo(tmp_path: Path) -> None:
    class StuckReflectionOps(FakeOps):
        def _reflect(self, control_namespace: str, incident_id: str) -> None:
            """Reflection never finishes."""

    ops, driver = StuckReflectionOps(), _WebhookDriver()
    agent = _agent(tmp_path, ops, [])
    agent._settings = replace(agent._settings, reflection_wait_seconds=30)

    [record] = run_incidents(agent, driver, _single_row(tmp_path))

    assert driver.webhook is False
    assert record.recovery_trigger == "reflection_deadline"
    assert record.recovery_error is None
    assert "learning failed" in (record.error or "")
    state = PersistentState.load(tmp_path / "sdo_persistent_controller.json")
    assert state.controllers["hotel-reservation"].pending is not None


def test_a_transient_state_read_failure_does_not_skip_the_closure_fence(tmp_path: Path) -> None:
    events: list[str] = []

    class FlakyStateOps(FakeOps):
        reads: int = 0

        def runtime_state(self, control_namespace: str) -> dict[str, Any]:
            closure = self.states[control_namespace].get("pending_closure")
            if (
                isinstance(closure, dict)
                and (control_namespace, closure["request"]["incident_id"]) in self.seen_closures
            ):
                self.reads += 1
                if self.reads == 1:
                    events.append("unreadable")
                    return {}
                if self.reads == 2:
                    events.append("read error")
                    raise RuntimeError("kubectl: connection refused")
            return super().runtime_state(control_namespace)

    driver = _WebhookDriver(events)

    [record] = run_incidents(_agent(tmp_path, FlakyStateOps(), []), driver, _single_row(tmp_path))

    assert events == ["inject", "unreadable", "read error", "recover"]
    assert record.recovery_trigger == "natural_closure"
    assert record.recovery_error is None


def test_an_unreadable_state_at_the_deadline_fails_typed_without_recovering(tmp_path: Path) -> None:
    class UnreadableOps(FakeOps):
        def runtime_state(self, control_namespace: str) -> dict[str, Any]:
            closure = self.states[control_namespace].get("pending_closure")
            if (
                isinstance(closure, dict)
                and (control_namespace, closure["request"]["incident_id"]) in self.seen_closures
            ):
                return {}
            return super().runtime_state(control_namespace)

    ops, driver = UnreadableOps(), _WebhookDriver()
    agent = _agent(tmp_path, ops, [])
    agent._settings = replace(agent._settings, reflection_wait_seconds=30)

    [record] = run_incidents(agent, driver, _single_row(tmp_path))

    assert driver.events == ["inject"], "recovery mutated the app while the closure state was unknown"
    assert "closure state unknown" in (record.recovery_error or "")
    assert record.recovery_trigger != "reflection_deadline"


def _wall_clock() -> Clock:
    """A fake clock whose wall time advances with its monotonic time, as a real one does."""

    now = [0.0]

    def sleep(seconds: float) -> None:
        now[0] += seconds

    return Clock(monotonic=lambda: now[0], sleep=sleep, now=lambda: INJECTED + timedelta(seconds=now[0]))


class _NeverReflectsOps(FakeOps):
    #: The terminal condition of the controller Job, once it has one.
    job_failure: str | None = None

    def _reflect(self, control_namespace: str, incident_id: str) -> None:
        """Reflection never finishes."""

    def controller_job_failure(self, control_namespace: str) -> str | None:
        return self.job_failure


def test_one_reflection_deadline_spans_every_drain_of_an_incident(tmp_path: Path) -> None:
    ops, clock = _NeverReflectsOps(), _wall_clock()
    outcome = _agent(tmp_path, ops, [], reflection_wait_seconds=30, clock=clock).resolve(0, "p", _inject)
    state = PersistentState.load(tmp_path / "sdo_persistent_controller.json")
    record = state.controllers["hotel-reservation"]
    # A drain other than learn() (the composite loop's, or admission's) spends the incident's budget.
    with pytest.raises(ReflectionDeadlineError):
        drain_pending_incident(record, ops=ops, repository=tmp_path, drained_by="composite-000", clock=clock)
    # A restarted driver settles the same persisted closure before recovery.
    restarted = _agent(tmp_path, ops, [], reflection_wait_seconds=30, clock=clock)
    started = clock.monotonic()

    restarted.prepare_recovery(outcome)

    assert restarted.last_recovery_trigger == "reflection_deadline"
    assert clock.monotonic() - started < 30, "recovery waited a second reflection budget"


def test_a_failed_controller_job_ends_the_closure_wait_as_a_typed_failure(tmp_path: Path) -> None:
    ops, clock = _NeverReflectsOps(), _wall_clock()
    agent = _agent(tmp_path, ops, [], clock=clock)
    outcome = agent.resolve(0, "p", _inject)
    ops.job_failure = "BackoffLimitExceeded: Job has reached the specified backoff limit"
    started = clock.monotonic()

    with pytest.raises(ControllerJobFailedError, match="BackoffLimitExceeded"):
        agent.learn(outcome)
    # The recovery fence still refuses to mutate the app while the dead controller's state shows the incident open.
    with pytest.raises(PersistentControllerError, match="closed incident"):
        agent.prepare_recovery(outcome)

    assert clock.monotonic() - started < 60, "the drain waited out the deadline on a controller that cannot ACK"
    assert agent.last_recovery_trigger == "closure_failed"
    rejected = json.loads((Path(str(outcome.artifacts_dir)) / REJECTED_RECEIPT_FILENAME).read_text())
    assert "BackoffLimitExceeded" in json.dumps(rejected)
    assert (
        PersistentState.load(tmp_path / "sdo_persistent_controller.json").controllers["hotel-reservation"].pending
        is None
    )
