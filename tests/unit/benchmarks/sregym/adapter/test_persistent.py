from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.sregym.adapter.persistent import (
    REJECTED_RECEIPT_FILENAME,
    RESOLUTION_FILENAME,
    STRICT_RECEIPT_FILENAME,
    Clock,
    ClosureFailedError,
    ControllerPod,
    ControllerRecord,
    DetectionMissError,
    PendingIncident,
    PersistentControllerError,
    PersistentState,
    StageInputs,
    _resolution_timings,
    _wait_for_reflection_drain,
    collect_followup_incident,
    control_namespace_for,
    drain_pending_incident,
    publish_deferred_receipts,
    run_persistent_stage,
    runtime_config_from_payload,
    runtime_config_payload,
    teardown,
    wait_for_maintenance_ack,
)
from benchmarks.sregym.adapter.runtime import RuntimeConfig

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

DETECTED = datetime(2026, 9, 27, 12, 0, 0, tzinfo=timezone.utc)


def _receipt(incident_id: str) -> dict[str, Any]:
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
        # Reflection took four minutes after verification; it must never
        # become resolution time.
        "phase_timings_seconds": {"operational_recovery": 40.0, "post_recovery_learning_and_receipt": 240.0},
    }


@dataclass
class FakeOps:
    """In-memory cluster: one controller pod per control namespace, driven by the test."""

    events: list[tuple[str, ...]] = field(default_factory=list)
    pods: dict[str, ControllerPod] = field(default_factory=dict)
    states: dict[str, dict[str, Any]] = field(default_factory=dict)
    logs: dict[str, list[str]] = field(default_factory=dict)
    existing_namespaces: set[str] = field(default_factory=set)
    installs: int = 0
    incidents: int = 0
    # Reflection completes only for incidents listed here, which lets a test
    # prove that a stage returns before its reflection finishes.
    reflectable: set[str] = field(default_factory=set)
    # Incidents whose responder reported a non-completed status.
    incomplete: set[str] = field(default_factory=set)
    polls_until_reflected: dict[str, int] = field(default_factory=dict)
    stage_evidence_error: str | None = None
    # Worktree directory names present under the shared /workspace/worktrees root.
    # Empty by default, so a receipt reports remaining_worktrees=[] unless a test
    # models a leftover (for example a terminal incident's leaked worktree).
    worktrees: set[str] = field(default_factory=set)

    def controller_pod(self, control_namespace: str) -> ControllerPod | None:
        return self.pods.get(control_namespace)

    def namespace_exists(self, namespace: str) -> bool:
        return namespace in self.existing_namespaces or namespace in self.pods

    def delete_controller(self, control_namespace: str) -> None:
        self.events.append(("delete", control_namespace))
        self.pods.pop(control_namespace, None)
        self.states.pop(control_namespace, None)
        self.existing_namespaces.discard(control_namespace)

    def install(self, config: RuntimeConfig) -> bool:
        control = config.control_namespace
        assert config.persistent is True
        assert config.wait_for_completion is False
        if config.reuse_existing and control in self.pods:
            self.events.append(("reuse", control))
            return True
        self.installs += 1
        self.pods[control] = ControllerPod(name=f"sdo-controller-run-{self.installs}", uid=f"uid-{self.installs}")
        self.states[control] = {}
        self.logs[control] = []
        self.events.append(("install", control))
        return False

    def set_maintenance(
        self, control_namespace: str, *, paused: bool, generation: str, preserve_baseline: bool = False
    ) -> None:
        mode = "paused" if paused else "active"
        self.events.append((mode, control_namespace))
        self.logs[control_namespace].append(
            json.dumps({"controller_maintenance": mode, "maintenance_generation": generation})
        )

    def runtime_state(self, control_namespace: str) -> dict[str, Any]:
        state = self.states.get(control_namespace, {})
        closure = state.get("pending_closure")
        if isinstance(closure, dict):
            incident_id = closure["request"]["incident_id"]
            remaining = self.polls_until_reflected.get(incident_id, 0)
            if remaining > 0:
                self.polls_until_reflected[incident_id] = remaining - 1
            elif incident_id in self.reflectable:
                self._finish_reflection(control_namespace, incident_id)
        return self.states.get(control_namespace, {})

    def _finish_reflection(self, control_namespace: str, incident_id: str) -> None:
        state = self.states[control_namespace]
        state.pop("pending_closure")
        state["last_acknowledged_incident_id"] = incident_id
        self.events.append(("reflected", incident_id))
        self.logs[control_namespace].extend(
            [
                json.dumps({"controller_closure_restart": incident_id}),
                json.dumps({"controller_supervisor": "relaunch", "launches": 1}),
                json.dumps({"controller_maintenance": "paused", "maintenance_generation": "relaunched"}),
            ]
        )

    def controller_logs(self, control_namespace: str) -> str:
        return "\n".join(self.logs.get(control_namespace, []))

    def inject_after_resume(
        self, control_namespace: str, generation: str, inject: Callable[[], None]
    ) -> dict[str, float]:
        assert self.logs[control_namespace][-1] == json.dumps(
            {"controller_maintenance": "active", "maintenance_generation": generation}
        )
        inject()
        self.incidents += 1
        incident_id = f"incident-{self.incidents}"
        self.events.append(("inject", control_namespace, incident_id))
        verified = DETECTED + timedelta(seconds=40)
        self.states[control_namespace]["pending_closure"] = {
            "request": {"incident_id": incident_id},
            "result": {
                "confirmed_root_causes": [{"summary": "missing ConfigMap"}],
                "repair_actions": [{"summary": "restored ConfigMap"}],
            },
            "detected_at": DETECTED.isoformat(),
            "dispatched_at": (DETECTED + timedelta(seconds=1)).isoformat(),
            "responder_completed_at": (DETECTED + timedelta(seconds=35)).isoformat(),
            "verified_at": verified.isoformat(),
            "final_detector_states": [{"detector_id": "health", "status": "clear"}],
        }
        return {"controller_baseline_wait": 3.0, "fault_injection_request": 0.5}

    def collect_receipt(self, config: RuntimeConfig, incident_id: str, artifacts_dir: Path) -> dict[str, Any]:
        self.events.append(("receipt", incident_id, str(config.repository)))
        # The drain exports the controller PVC's cumulative runtime evidence.
        usage = artifacts_dir / "sdo_runtime" / "usage" / "controller-turns.jsonl"
        usage.parent.mkdir(parents=True, exist_ok=True)
        usage.write_text(json.dumps({"cwd": f"/wt/{incident_id}", "duration_seconds": 1.0}) + "\n", encoding="utf-8")
        rollout = artifacts_dir / "sdo_runtime" / "codex" / "sessions" / f"rollout-{incident_id}.jsonl"
        rollout.parent.mkdir(parents=True, exist_ok=True)
        rollout.write_bytes(b'{"type":"session_meta"}\n')
        receipt = _receipt(incident_id)
        receipt["artifacts_dir"] = str(artifacts_dir)
        receipt["completed"] = incident_id not in self.incomplete
        receipt["remaining_worktrees"] = [f"/workspace/worktrees/{name}" for name in sorted(self.worktrees)]
        return receipt

    def reap_orphan_worktrees(self, config: RuntimeConfig, keep_dirnames: set[str]) -> list[str]:
        self.events.append(("reap", tuple(sorted(keep_dirnames))))
        self.worktrees = {name for name in self.worktrees if name in keep_dirnames}
        return [f"/workspace/worktrees/{name}" for name in sorted(self.worktrees)]

    def export_runtime_artifacts(self, config: RuntimeConfig, artifacts_dir: Path) -> dict[str, str | None]:
        self.events.append(("stage_evidence", config.control_namespace))
        if self.stage_evidence_error is not None:
            return {"directory": None, "error": self.stage_evidence_error}
        # The responder's rollout and usage exist once the incident is verified.
        rollout = artifacts_dir / "sdo_runtime" / "codex" / "sessions" / f"rollout-responder-{self.incidents}.jsonl"
        rollout.parent.mkdir(parents=True, exist_ok=True)
        rollout.write_bytes(b'{"type":"session_meta"}\n')
        return {"directory": str(artifacts_dir / "sdo_runtime"), "error": None}

    def export_controller_logs(self, control_namespace: str, artifacts_dir: Path) -> None:
        self.events.append(("logs", control_namespace))
        log = artifacts_dir / "sdo_runtime" / "controller_logs" / "sdo-controller-run-1.log"
        log.parent.mkdir(parents=True, exist_ok=True)
        log.write_text(self.controller_logs(control_namespace) + "\n", encoding="utf-8")


def _clock(ops: FakeOps) -> Clock:
    now = [0.0]

    def sleep(seconds: float) -> None:
        now[0] += seconds

    return Clock(monotonic=lambda: now[0], sleep=sleep, now=lambda: DETECTED)


def test_maintenance_ack_timeout_follows_time_scale(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SDO_TIME_SCALE", "0.1")

    class NoAck:
        def controller_logs(self, _control: str) -> str:
            return ""

    elapsed = [0.0]
    clock = Clock(monotonic=lambda: elapsed[0], sleep=lambda seconds: elapsed.__setitem__(0, elapsed[0] + seconds))

    with pytest.raises(PersistentControllerError, match="did not acknowledge maintenance"):
        wait_for_maintenance_ack(NoAck(), "hotel-sdo", "generation", clock)  # type: ignore[arg-type]

    assert elapsed[0] == pytest.approx(60.0)


def test_drain_receipt_reports_ack_and_collection_phases(tmp_path: Path) -> None:
    from tests.unit.benchmarks.sregym.adapter.test_closeout_receipt_race import _drained_record

    ops = FakeOps()
    record = _drained_record(tmp_path, ops)
    collect = ops.collect_receipt
    elapsed = [0.0]

    def timed_collect(config: RuntimeConfig, incident_id: str, artifacts_dir: Path) -> dict[str, Any]:
        elapsed[0] += 35.0
        return collect(config, incident_id, artifacts_dir)

    ops.collect_receipt = timed_collect  # type: ignore[method-assign]
    clock = Clock(monotonic=lambda: elapsed[0], sleep=lambda seconds: elapsed.__setitem__(0, elapsed[0] + seconds))

    drain_pending_incident(
        record, ops=ops, repository=tmp_path / "s0" / "application_workspace", drained_by="test", clock=clock
    )

    receipt = json.loads((record.pending.receipt_dir / STRICT_RECEIPT_FILENAME).read_text(encoding="utf-8"))
    assert receipt["reflection_drain"]["phase_seconds"]["collect_settled_receipt"] == pytest.approx(35.0)
    assert set(receipt["reflection_drain"]["phase_timestamps"]) == {
        "drain_started",
        "ack_observed",
        "receipt_collected",
    }


def test_closeout_settle_timeout_follows_time_scale(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from tests.unit.benchmarks.sregym.adapter.test_closeout_receipt_race import _drained_record

    monkeypatch.setenv("SDO_TIME_SCALE", "0.1")
    ops = FakeOps()
    record = _drained_record(tmp_path, ops)
    collect = ops.collect_receipt

    def unsettled(config: RuntimeConfig, incident_id: str, artifacts_dir: Path) -> dict[str, Any]:
        receipt = collect(config, incident_id, artifacts_dir)
        receipt["completed"] = False
        return receipt

    ops.collect_receipt = unsettled  # type: ignore[method-assign]
    elapsed = [0.0]
    clock = Clock(monotonic=lambda: elapsed[0], sleep=lambda seconds: elapsed.__setitem__(0, elapsed[0] + seconds))

    result = drain_pending_incident(
        record, ops=ops, repository=tmp_path / "s0" / "application_workspace", drained_by="test", clock=clock
    )
    assert result.HasField("failure")
    assert result.failure.incident_id == record.pending.incident_id

    assert elapsed[0] == pytest.approx(12.0)


def _config(tmp_path: Path, namespace: str = "hotel", stage: str = "s0") -> RuntimeConfig:
    repository = tmp_path / stage / "application_workspace"
    repository.mkdir(parents=True, exist_ok=True)
    return RuntimeConfig(
        repository=repository,
        namespace=namespace,
        application="Hotel Reservation",
        controller_image="controller:test",
        responder_image="responder:test",
        repository_pvc="repository",
        credentials_secret="credentials",
        model="gpt-test",
        timeout_seconds=60,
        repair_policy="recorded-actions",
        controller_namespace=control_namespace_for(namespace),
    )


def _inputs(
    tmp_path: Path,
    stage: str,
    *,
    namespace: str = "hotel",
    application: str = "Hotel Reservation",
    fingerprint: str = "topology-1",
    receipt_dir: Path | None = None,
    verification_timeout_seconds: float = 30,
    detection_timeout_seconds: float | None = None,
) -> StageInputs:
    return StageInputs(
        detection_timeout_seconds=detection_timeout_seconds,
        stage_label=stage,
        application=application,
        namespace=namespace,
        lifecycle_fingerprint=fingerprint,
        runtime_config=_config(tmp_path, namespace, stage),
        receipt_dir=receipt_dir or tmp_path / stage / "agent",
        state_path=tmp_path / "sdo_persistent_controller.json",
        verification_timeout_seconds=verification_timeout_seconds,
    )


def _run(
    tmp_path: Path,
    ops: FakeOps,
    stage: str,
    lifecycle_calls: list[str],
    **kwargs: Any,
) -> dict[str, Any]:
    injected: list[str] = []

    def lifecycle() -> bool:
        lifecycle_calls.append(stage)
        return True

    def inject() -> None:
        injected.append(stage)
        ops.events.append(("fault", stage))

    resolution = run_persistent_stage(
        _inputs(tmp_path, stage, **kwargs), ops=ops, run_lifecycle=lifecycle, inject=inject, clock=_clock(ops)
    )
    assert injected == [stage]
    return resolution


def test_control_namespace_is_per_application_and_valid() -> None:
    assert control_namespace_for("hotel-reservation") == "hotel-reservation-sdo"
    with pytest.raises(PersistentControllerError):
        control_namespace_for("x" * 60)


def test_runtime_config_round_trips_all_options_through_payload_and_state(tmp_path: Path) -> None:
    config = RuntimeConfig(
        repository=tmp_path / "repository",
        namespace="hotel",
        application="Hotel Reservation",
        controller_image="controller:test",
        responder_image="responder:test",
        repository_pvc="repository",
        credentials_secret="credentials",
        model="gpt-test",
        timeout_seconds=61,
        validator_image="validator:test",
        verification_timeout_seconds=121,
        wait_for_completion=True,
        repair_policy="recorded-actions",
        agent_provider="claude",
        reflection_session="fresh",
        reflection_guidance="generalize",
        late_findings="pull",
        max_response_loops=6,
        healthy_baseline=True,
        controller_namespace="hotel-sdo",
        reuse_existing=True,
        submission_api_base="http://submit",
        submission_relay_target_base="http://relay",
        allow_test_lifecycle=True,
        artifacts_dir=tmp_path / "artifacts",
        persistent=True,
        buildkit_image="buildkit:test",
        registry_push_secret="registry",
        kind_cluster_name="kind-test",
        max_deployment_attempts=4,
        agent_registrations=tmp_path / "scripted-agents.json",
    )
    payload = runtime_config_payload(config)
    assert runtime_config_from_payload(payload) == config

    pending = PendingIncident(
        incident_id="incident-1",
        stage_label="s0",
        receipt_dir=tmp_path / "receipt",
        repository=config.repository,
        verified_at=DETECTED,
        stage_receipt={},
    )
    record = ControllerRecord(
        application=config.application,
        namespace=config.namespace,
        control_namespace=config.control_namespace,
        lifecycle_fingerprint="topology-1",
        controller_pod_uid="uid",
        controller_pod_name="pod",
        installed_by_stage="s0",
        installed_at=DETECTED,
        runtime_config=payload,
        pending=pending,
    )
    path = tmp_path / "state.json"
    PersistentState(controllers={config.namespace: record}).save(path)
    loaded = PersistentState.load(path)
    assert loaded.controllers[config.namespace].pending == pending
    assert runtime_config_from_payload(loaded.controllers[config.namespace].runtime_config) == config


@pytest.mark.parametrize("responder_namespaces", [(), ("policy-system", "cache")])
def test_runtime_config_json_preserves_reviewed_responder_grants(
    tmp_path: Path, responder_namespaces: tuple[str, ...]
) -> None:
    config = replace(
        _config(tmp_path),
        permissive_responder=True,
        responder_namespaces=responder_namespaces,
        deployed_manifest_paths=("deploy/app.yaml",),
    )
    payload = json.loads(json.dumps(runtime_config_payload(config)))

    assert runtime_config_from_payload(payload) == config


def test_recreated_namespace_regains_controller_access_before_resume_and_injection(tmp_path: Path) -> None:
    elapsed = [0.0]

    class NamespaceOps(FakeOps):
        grants_present = False

        def install(self, config: RuntimeConfig) -> bool:
            reused = super().install(config)
            # The real Go installer/inspector reapplies Role/RoleBinding pairs
            # in the app namespace, including on an unchanged controller.
            self.grants_present = True
            elapsed[0] += 2
            return reused

        def set_maintenance(
            self, control_namespace: str, *, paused: bool, generation: str, preserve_baseline: bool = False
        ) -> None:
            if not paused:
                assert self.grants_present, "recreated app namespace has no controller RBAC"
            super().set_maintenance(control_namespace, paused=paused, generation=generation)

    ops = NamespaceOps()
    lifecycle_calls: list[str] = []
    first = _run(tmp_path, ops, "s0", lifecycle_calls)
    # SREGym deletes/recreates only the app namespace; the Pod and its PVC survive.
    ops.grants_present = False
    ops.reflectable.add("incident-1")
    second = run_persistent_stage(
        _inputs(tmp_path, "s1"),
        ops=ops,
        run_lifecycle=lambda: pytest.fail("reused controller must preserve learned memory"),
        inject=lambda: None,
        clock=Clock(monotonic=lambda: elapsed[0], sleep=lambda seconds: elapsed.__setitem__(0, elapsed[0] + seconds)),
    )

    assert ops.grants_present
    assert ops.installs == 1
    assert lifecycle_calls == ["s0"]
    assert first["persistent_controller"]["controller_pod_uid"] == second["persistent_controller"]["controller_pod_uid"]
    assert ("inject", "hotel-sdo", "incident-2") in ops.events
    assert second["driver_phase_timings_seconds"]["controller_install_inspection"] == 2


def test_reuse_access_restoration_failure_blocks_resume_and_injection(tmp_path: Path) -> None:
    class FailedAccessOps(FakeOps):
        def install(self, config: RuntimeConfig) -> bool:
            if config.reuse_existing:
                raise PersistentControllerError("ensure RoleBinding hotel/sdo-controller: forbidden")
            return super().install(config)

    ops = FailedAccessOps()
    _run(tmp_path, ops, "s0", [])
    ops.reflectable.add("incident-1")
    ops.events.clear()

    with pytest.raises(PersistentControllerError, match="ensure RoleBinding hotel/sdo-controller"):
        _run(tmp_path, ops, "s1", [])

    assert not any(event[0] in {"active", "fault", "inject"} for event in ops.events)


def test_one_controller_serves_both_stages_and_the_second_skips_reinstall_and_lifecycle(tmp_path: Path) -> None:
    ops = FakeOps()
    lifecycle_calls: list[str] = []

    first = _run(tmp_path, ops, "s0", lifecycle_calls)
    ops.reflectable.add("incident-1")
    second = _run(tmp_path, ops, "s1", lifecycle_calls)

    assert ops.installs == 1
    assert lifecycle_calls == ["s0"]
    assert first["persistent_controller"]["controller_pod_uid"] == second["persistent_controller"]["controller_pod_uid"]
    assert first["persistent_controller"]["installed_this_stage"] is True
    assert second["persistent_controller"]["installed_this_stage"] is False
    assert second["persistent_controller"]["lifecycle_revalidation_skipped"] is True
    assert "controller_inventory" in first["driver_phase_timings_seconds"]
    assert "lifecycle" in first["driver_phase_timings_seconds"]
    assert "controller_install_inspection" in first["driver_phase_timings_seconds"]
    assert second["driver_phase_timings_seconds"]["controller_install_inspection"] >= 0
    kinds = [
        event[0] for event in ops.events if event[0] in {"install", "reuse", "active", "paused", "fault", "delete"}
    ]
    assert kinds == ["install", "active", "fault", "paused", "reuse", "active", "fault", "paused"]
    # Each stage correlates its own incident.
    assert (first["incident_id"], second["incident_id"]) == ("incident-1", "incident-2")


def test_fresh_lifecycle_controller_is_used_without_a_second_install_inspection(tmp_path: Path) -> None:
    ops = FakeOps()
    inputs = _inputs(tmp_path, "s0")

    def lifecycle() -> bool:
        ops.install(replace(inputs.runtime_config, persistent=True, wait_for_completion=False))
        return False

    result = run_persistent_stage(inputs, ops=ops, run_lifecycle=lifecycle, inject=lambda: None, clock=_clock(ops))

    assert [event[0] for event in ops.events if event[0] in {"install", "reuse"}] == ["install"]
    assert result["driver_phase_timings_seconds"]["controller_install_inspection"] == 0


def test_fresh_lifecycle_waits_for_its_pod_instead_of_reinstalling(tmp_path: Path) -> None:
    class DelayedOps(FakeOps):
        unseen = 1

        def controller_pod(self, control_namespace: str) -> ControllerPod | None:
            if self.pods and self.unseen:
                self.unseen -= 1
                return None
            return super().controller_pod(control_namespace)

    ops = DelayedOps()
    inputs = replace(_inputs(tmp_path, "s0"), lifecycle_installs_controller=True)

    def lifecycle() -> bool:
        ops.install(replace(inputs.runtime_config, persistent=True, wait_for_completion=False))
        return False

    result = run_persistent_stage(inputs, ops=ops, run_lifecycle=lifecycle, inject=lambda: None, clock=_clock(ops))

    assert [event[0] for event in ops.events if event[0] in {"install", "reuse"}] == ["install"]
    assert result["driver_phase_timings_seconds"]["controller_install_inspection"] == 0


def test_stage_reports_resolution_at_verified_health_before_reflection_finishes(tmp_path: Path) -> None:
    ops = FakeOps()

    resolution = _run(tmp_path, ops, "s0", [])

    receipt_dir = tmp_path / "s0" / "agent"
    assert ("reflected", "incident-1") not in ops.events
    assert (receipt_dir / RESOLUTION_FILENAME).is_file()
    assert not (receipt_dir / STRICT_RECEIPT_FILENAME).exists()
    # Detected 12:00:00, verified 12:00:40: reflection is excluded.
    assert resolution["incident_resolution_seconds"] == 40.0
    assert resolution["incident_resolution_scope"] == "detected_to_independently_verified_health"
    assert resolution["confirmed_root_causes"] == [{"summary": "missing ConfigMap"}]
    pending = PersistentState.load(tmp_path / "sdo_persistent_controller.json").controllers["hotel"].pending
    assert pending is not None
    assert pending.incident_id == "incident-1"


def test_next_stage_injects_only_after_previous_reflection_is_committed_and_rolled_out(tmp_path: Path) -> None:
    ops = FakeOps()
    _run(tmp_path, ops, "s0", [])
    ops.reflectable.add("incident-1")
    ops.polls_until_reflected["incident-1"] = 5

    second = _run(tmp_path, ops, "s1", [])

    order = [event[:2] for event in ops.events]
    assert order.index(("reflected", "incident-1")) < order.index(("receipt", "incident-1"))
    assert order.index(("receipt", "incident-1")) < order.index(("fault", "s1"))
    assert second["reflection_drain_seconds"] >= 5.0
    assert (
        second["pre_injection_costs_seconds"]["previous_incident_reflection_drain"]
        == second["reflection_drain_seconds"]
    )
    # The drain is a pre-injection cost, never resolution time.
    assert second["incident_resolution_seconds"] == 40.0
    first_receipt = json.loads((tmp_path / "s0" / "agent" / STRICT_RECEIPT_FILENAME).read_text(encoding="utf-8"))
    assert first_receipt["incident_id"] == "incident-1"
    assert first_receipt["incident_resolution_seconds"] == 40.0
    assert first_receipt["reflection_drain"]["drained_by"] == "s1"
    assert first_receipt["excluded_from_incident_resolution_seconds"]["post_recovery_learning_and_receipt"] == 240.0
    # The controller's repository is synced into the current stage's workspace.
    receipt_event = next(event for event in ops.events if event[0] == "receipt")
    assert receipt_event[2].endswith("s1/application_workspace")


def test_stages_for_different_applications_install_separate_controllers(tmp_path: Path) -> None:
    ops = FakeOps()
    lifecycle_calls: list[str] = []

    first = _run(tmp_path, ops, "s0", lifecycle_calls, namespace="hotel")
    second = _run(tmp_path, ops, "s1", lifecycle_calls, namespace="social", application="Social Network")

    assert ops.installs == 2
    assert lifecycle_calls == ["s0", "s1"]
    assert first["persistent_controller"]["control_namespace"] == "hotel-sdo"
    assert second["persistent_controller"]["control_namespace"] == "social-sdo"
    assert first["persistent_controller"]["controller_pod_uid"] != second["persistent_controller"]["controller_pod_uid"]
    state = PersistentState.load(tmp_path / "sdo_persistent_controller.json")
    assert set(state.controllers) == {"hotel", "social"}


def test_refuses_to_reuse_a_controller_for_a_different_application_in_the_same_namespace(tmp_path: Path) -> None:
    ops = FakeOps()
    _run(tmp_path, ops, "s0", [])

    with pytest.raises(PersistentControllerError, match="refusing to reuse"):
        _run(tmp_path, ops, "s1", [], application="Other App")
    assert ops.installs == 1


def test_a_controller_this_pipeline_did_not_install_is_replaced(tmp_path: Path) -> None:
    ops = FakeOps()
    ops.existing_namespaces.add("hotel-sdo")
    lifecycle_calls: list[str] = []

    _run(tmp_path, ops, "s0", lifecycle_calls)

    assert ops.events[0] == ("delete", "hotel-sdo")
    assert lifecycle_calls == ["s0"]


def test_changed_topology_drains_memory_then_reinstalls(tmp_path: Path) -> None:
    ops = FakeOps()
    lifecycle_calls: list[str] = []
    _run(tmp_path, ops, "s0", lifecycle_calls)
    ops.reflectable.add("incident-1")

    _run(tmp_path, ops, "s1", lifecycle_calls, fingerprint="topology-2")

    order = [event[:2] for event in ops.events]
    assert order.index(("receipt", "incident-1")) < order.index(("delete", "hotel-sdo"))
    assert lifecycle_calls == ["s0", "s1"]
    assert ops.installs == 2


def test_teardown_drains_the_last_incident_and_stops_every_controller(tmp_path: Path) -> None:
    ops = FakeOps()
    _run(tmp_path, ops, "s0", [])
    ops.reflectable.add("incident-1")

    errors = teardown(tmp_path / "sdo_persistent_controller.json", ops=ops, clock=_clock(ops))

    assert errors == []
    assert ("delete", "hotel-sdo") in ops.events
    receipt = json.loads((tmp_path / "s0" / "agent" / STRICT_RECEIPT_FILENAME).read_text(encoding="utf-8"))
    assert receipt["reflection_drain"]["drained_by"] == "pipeline-teardown"
    assert PersistentState.load(tmp_path / "sdo_persistent_controller.json").controllers == {}


def test_a_controller_lost_with_its_cluster_records_the_undrained_incident_and_the_next_stage_runs(
    tmp_path: Path,
) -> None:
    ops = FakeOps()
    lifecycle_calls: list[str] = []
    _run(tmp_path, ops, "s0", lifecycle_calls)
    # The harness recreated the cluster between stages: the controller, its namespace and its state are gone.
    ops.pods.clear()
    ops.states.clear()

    second = _run(tmp_path, ops, "s1", lifecycle_calls)

    assert lifecycle_calls == ["s0", "s1"]
    assert second["persistent_controller"]["installed_this_stage"] is True
    rejected = json.loads((tmp_path / "s0" / "agent" / REJECTED_RECEIPT_FILENAME).read_text(encoding="utf-8"))
    assert rejected["failure"]["incident_id"] == "incident-1"
    assert "controller pod" in rejected["failure"]["message"]
    assert "lost" in rejected["failure"]["message"]


def test_a_replaced_controller_pod_records_the_undrained_incident_instead_of_blocking_the_stage(tmp_path: Path) -> None:
    ops = FakeOps()
    _run(tmp_path, ops, "s0", [])
    ops.pods["hotel-sdo"] = ControllerPod(name="sdo-controller-run-other", uid="uid-other")

    _run(tmp_path, ops, "s1", [])

    rejected = json.loads((tmp_path / "s0" / "agent" / REJECTED_RECEIPT_FILENAME).read_text(encoding="utf-8"))
    assert "uid-other" in rejected["failure"]["message"]


def test_teardown_records_an_incident_whose_controller_was_lost_and_still_stops_the_rest(tmp_path: Path) -> None:
    ops = FakeOps()
    _run(tmp_path, ops, "s0", [])
    ops.pods.clear()

    errors = teardown(tmp_path / "sdo_persistent_controller.json", ops=ops, clock=_clock(ops))

    assert errors == []
    rejected = json.loads((tmp_path / "s0" / "agent" / REJECTED_RECEIPT_FILENAME).read_text(encoding="utf-8"))
    assert rejected["failure"]["incident_id"] == "incident-1"
    assert PersistentState.load(tmp_path / "sdo_persistent_controller.json").controllers == {}


def test_fail_soft_reset_restarts_controller_without_losing_drift_verdicts(tmp_path: Path) -> None:
    from benchmarks.sregym.fastloop.baseline_recovery import ApplicationBaseline
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment
    from benchmarks.sregym.fastloop.reset import reset_environment

    class RestartOps(FakeOps):
        def install(self, config: RuntimeConfig) -> bool:
            durable = self.states.get(config.control_namespace, {}).get("drift_events")
            installed = super().install(config)
            if durable is not None:
                self.states[config.control_namespace]["drift_events"] = durable
            return installed

    class ResetCluster:
        def __init__(self, ops: RestartOps) -> None:
            self.ops = ops
            self.objects: list[dict[str, Any]] = []

        def list_objects(self, _namespace: str) -> list[dict[str, Any]]:
            return self.objects

        def delete(self, _namespace: str, _kind: str, _name: str) -> None:
            self.objects.clear()

        def apply(self, _namespace: str, value: dict[str, Any]) -> None:
            self.objects = [value]

        def reset_controller(self, _seed_commit: str, *, preserve_repository: bool = False) -> None:
            assert preserve_repository
            self.ops.pods.pop("hotel-sdo", None)

    class HealthyWorker:
        def request(self, op: str, **_kwargs: Any) -> dict[str, bool]:
            return {"healthy": True} if op == "health" else {"success": True}

    ops = RestartOps()
    _run(tmp_path, ops, "s0", [])
    ops.existing_namespaces.add("hotel-sdo")
    ops.states["hotel-sdo"]["drift_events"] = [
        {"ref": {"kind": "Service", "name": "api"}, "investigation_verdict": "bug"}
    ]
    environment = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="kind-test",
        kubeconfig=tmp_path / "kubeconfig",
        namespace="hotel",
        application="Hotel Reservation",
        workspace=tmp_path / "workspace",
        sregym_dir=tmp_path / "sregym",
        private_tmp=None,
        seed_commit="a" * 40,
    )
    baseline = ApplicationBaseline("hotel", [{"kind": "Deployment", "metadata": {"name": "app"}}])
    environment.baseline_path.write_text(json.dumps({"fastloop_application": {"namespaces": [baseline.__dict__]}}))

    reset_environment(environment, ResetCluster(ops), HealthyWorker(), preserve_repository=True)
    calls: list[str] = []
    resolution = run_persistent_stage(
        replace(_inputs(tmp_path, "s1"), lifecycle_installs_controller=True),
        ops=ops,
        run_lifecycle=lambda: calls.append("lifecycle"),
        inject=lambda: ops.events.append(("fault", "s1")),
        clock=_clock(ops),
    )

    assert calls == []
    assert ("delete", "hotel-sdo") not in ops.events
    assert ops.states["hotel-sdo"]["drift_events"] == [
        {"ref": {"kind": "Service", "name": "api"}, "investigation_verdict": "bug"}
    ]
    assert resolution["persistent_controller"]["installed_this_stage"]


def test_deferred_receipts_are_published_into_the_run_the_harness_already_published(tmp_path: Path) -> None:
    # SREGym moves the staging tree to results/<agent>/<problem>/run_N when the
    # stage ends, before the drain can write the strict receipt.
    staging = tmp_path / ".runtime" / "sdo_codex" / "anon_abc"
    ops = FakeOps()
    _run(tmp_path, ops, "s0", [], receipt_dir=staging)
    run_dir = tmp_path / "pipeline" / "stage_0" / "results" / "sdo_codex" / "problem-1" / "run_1"
    run_dir.parent.mkdir(parents=True)
    staging.rename(run_dir)
    ops.reflectable.add("incident-1")
    state_path = tmp_path / "sdo_persistent_controller.json"

    assert teardown(state_path, ops=ops, clock=_clock(ops)) == []
    published = publish_deferred_receipts(state_path, tmp_path / "pipeline")

    receipt_path = run_dir / STRICT_RECEIPT_FILENAME
    assert receipt_path in published
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["incident_id"] == "incident-1"
    # The same opaque-id canonicalization the harness applies at publication.
    assert "anon_abc" not in receipt_path.read_text(encoding="utf-8")
    assert receipt["artifacts_dir"].endswith("problem-1")
    assert (run_dir / "sdo_runtime" / "controller_logs" / "sdo-controller-run-1.log").is_file()
    # Drain-time usage logs and transcripts back per-stage reflection and warm-prompt analysis.
    assert (run_dir / "sdo_runtime" / "usage" / "controller-turns.jsonl").is_file()
    assert (run_dir / "sdo_runtime" / "codex" / "sessions" / "rollout-incident-1.jsonl").read_bytes() == (
        b'{"type":"session_meta"}\n'
    )
    assert not (staging / STRICT_RECEIPT_FILENAME).exists()
    assert not staging.exists()


def test_a_rejected_receipt_is_kept_with_its_validation_error_and_logs(tmp_path: Path) -> None:
    ops = FakeOps()
    _run(tmp_path, ops, "s0", [])
    ops.reflectable.add("incident-1")
    ops.incomplete.add("incident-1")

    # The rejection is that incident's own outcome, not a teardown failure.
    errors = teardown(tmp_path / "sdo_persistent_controller.json", ops=ops, clock=_clock(ops))

    assert errors == []
    receipt_dir = tmp_path / "s0" / "agent"
    assert not (receipt_dir / STRICT_RECEIPT_FILENAME).exists()
    rejected = json.loads((receipt_dir / REJECTED_RECEIPT_FILENAME).read_text(encoding="utf-8"))
    assert "completed=true" in rejected["validation_error"]
    assert rejected["receipt"]["incident_id"] == "incident-1"
    assert ops.events.count(("logs", "hotel-sdo")) == 2
    assert ("delete", "hotel-sdo") in ops.events
    assert PersistentState.load(tmp_path / "sdo_persistent_controller.json").controllers == {}


def test_a_rejected_receipt_does_not_stop_the_next_stage_from_injecting(tmp_path: Path) -> None:
    ops = FakeOps()
    _run(tmp_path, ops, "s0", [])
    ops.reflectable.update({"incident-1", "incident-2"})
    ops.incomplete.add("incident-1")

    # _run asserts the s1 fault was injected after the previous drain was rejected.
    _run(tmp_path, ops, "s1", [])

    rejected = json.loads((tmp_path / "s0" / "agent" / REJECTED_RECEIPT_FILENAME).read_text(encoding="utf-8"))
    assert "completed=true" in rejected["validation_error"]
    state = PersistentState.load(tmp_path / "sdo_persistent_controller.json")
    assert state.controllers["hotel"].pending is not None
    assert state.controllers["hotel"].pending.incident_id == "incident-2"
    assert teardown(tmp_path / "sdo_persistent_controller.json", ops=ops, clock=_clock(ops)) == []
    assert (tmp_path / "s1" / "agent" / STRICT_RECEIPT_FILENAME).is_file()


@dataclass
class NativeReceiptOps(FakeOps):
    classification: str = "partial"

    def collect_receipt(self, config: RuntimeConfig, incident_id: str, artifacts_dir: Path) -> dict[str, Any]:
        # Preserve the same exports as the real collector, then exercise the
        # native constructor that validates before returning to the drain.
        super().collect_receipt(config, incident_id, artifacts_dir)
        from benchmarks.sregym.protocol.native_receipt import native_production_receipt
        from tests.unit.benchmarks.sregym.protocol.test_native_receipt import _ledger

        ledger = _ledger()
        ledger["closure"]["request"]["namespace"] = "hotel"
        ledger["outcome_document"]["classification"] = self.classification
        return native_production_receipt(
            ledger,
            namespace=config.namespace,
            control_namespace=config.control_namespace,
            controller_image=config.controller_image,
            responder_image=config.responder_image,
            validator_image=config.validator_image,
            controller_pod_uid="uid-1",
            responder_job_evidence=[
                {"incident_id": incident_id, "job_name": "responder-1", "job_uid": "worker-1", "succeeded": True}
            ],
        )


@pytest.mark.parametrize("classification", ["partial", "success"])
def test_native_prior_receipt_is_published_before_the_next_injection(tmp_path: Path, classification: str) -> None:
    staging = tmp_path / ".runtime" / "sdo_codex" / "anon_abc"
    ops = NativeReceiptOps(classification=classification)
    _run(tmp_path, ops, "s0", [], receipt_dir=staging)
    run_dir = tmp_path / "pipeline" / "stage_0" / "results" / "sdo_codex" / "problem-1" / "run_1"
    run_dir.parent.mkdir(parents=True)
    staging.rename(run_dir)
    ops.reflectable.add("incident-1")
    filename = REJECTED_RECEIPT_FILENAME if classification == "partial" else STRICT_RECEIPT_FILENAME

    def inject() -> None:
        # The previous stage's evidence must be published before this fault.
        assert (run_dir / filename).is_file()
        assert PersistentState.load(tmp_path / "sdo_persistent_controller.json").controllers["hotel"].pending is None
        ops.events.append(("fault", "s1"))

    resolution = run_persistent_stage(
        _inputs(tmp_path, "s1"), ops=ops, run_lifecycle=lambda: None, inject=inject, clock=_clock(ops)
    )

    assert resolution["incident_id"] == "incident-2"
    assert ("fault", "s1") in ops.events
    document = json.loads((run_dir / filename).read_text(encoding="utf-8"))
    if classification == "partial":
        assert "verified closure classification" in document["validation_error"]
        receipt = document["receipt"]
        assert not (run_dir / STRICT_RECEIPT_FILENAME).exists()
        assert not (tmp_path / "s1" / "agent" / REJECTED_RECEIPT_FILENAME).exists()
    else:
        receipt = document
        assert not (run_dir / REJECTED_RECEIPT_FILENAME).exists()
    assert receipt["incident_id"] == "incident-1"
    assert receipt["native_ledger"]["outcome_document"]["classification"] == classification
    assert (run_dir / "sdo_runtime" / "controller_logs" / "sdo-controller-run-1.log").is_file()
    pending = PersistentState.load(tmp_path / "sdo_persistent_controller.json").controllers["hotel"].pending
    assert pending is not None
    assert pending.incident_id == "incident-2"


def test_native_partial_receipt_is_recorded_without_failing_teardown(tmp_path: Path) -> None:
    ops = NativeReceiptOps()
    _run(tmp_path, ops, "s0", [])
    ops.reflectable.add("incident-1")
    state_path = tmp_path / "sdo_persistent_controller.json"

    assert teardown(state_path, ops=ops, clock=_clock(ops)) == []

    receipt_dir = tmp_path / "s0" / "agent"
    rejected = json.loads((receipt_dir / REJECTED_RECEIPT_FILENAME).read_text(encoding="utf-8"))
    assert "verified closure classification" in rejected["validation_error"]
    assert rejected["receipt"]["native_ledger"]["outcome_document"]["classification"] == "partial"
    assert not (receipt_dir / STRICT_RECEIPT_FILENAME).exists()
    assert ("delete", "hotel-sdo") in ops.events
    assert PersistentState.load(state_path).controllers == {}


def test_native_collection_transport_error_still_blocks_injection(tmp_path: Path) -> None:
    from benchmarks.sregym.adapter.native_operator import NativeOperatorError

    class UnavailableOps(FakeOps):
        def collect_receipt(self, config: RuntimeConfig, incident_id: str, artifacts_dir: Path) -> dict[str, Any]:
            raise NativeOperatorError("cannot read broker ledger: transport unavailable")

    ops = UnavailableOps()
    _run(tmp_path, ops, "s0", [])
    ops.reflectable.add("incident-1")
    with pytest.raises(NativeOperatorError, match="transport unavailable"):
        _run(tmp_path, ops, "s1", [])
    assert ("fault", "s1") not in ops.events
    assert not (tmp_path / "s0" / "agent" / REJECTED_RECEIPT_FILENAME).exists()
    pending = PersistentState.load(tmp_path / "sdo_persistent_controller.json").controllers["hotel"].pending
    assert pending is not None
    assert pending.incident_id == "incident-1"


def test_a_rejected_receipt_is_still_reported_to_a_caller_that_drains_one_incident(tmp_path: Path) -> None:
    ops = FakeOps()
    _run(tmp_path, ops, "s0", [])
    ops.reflectable.add("incident-1")
    ops.incomplete.add("incident-1")
    state = PersistentState.load(tmp_path / "sdo_persistent_controller.json")
    record = state.controllers["hotel"]

    result = drain_pending_incident(
        record,
        ops=ops,
        repository=tmp_path / "s0" / "application_workspace",
        drained_by="fastloop",
        clock=_clock(ops),
    )
    from sdo.contracts import ReceiptDrainResult, ReceiptFailure

    assert isinstance(result, ReceiptDrainResult)
    assert result.failure.code == ReceiptFailure.CODE_REJECTED_EVIDENCE
    assert result.failure.incident_id == "incident-1"
    assert "completed=true" in result.failure.message
    rejected = json.loads((record.pending.receipt_dir / REJECTED_RECEIPT_FILENAME).read_text())
    assert rejected["failure"]["code"] == "CODE_REJECTED_EVIDENCE"


def test_a_permanently_failed_closure_ends_the_drain_with_the_broker_error(tmp_path: Path) -> None:
    ops = FakeOps()
    _run(tmp_path, ops, "s0", [])
    state = ops.states["hotel-sdo"]
    state["closure_state"] = "CLOSURE_STATE_FAILED"
    state["closure_failure"] = {
        "incident_id": "incident-1",
        "attempts": 8,
        "max_attempts": 8,
        "permanent": True,
        "last_error": "source repair check failed: trailing whitespace",
        "action": "inspect the incident worktree and the broker ledger",
    }
    clock = _clock(ops)

    errors = teardown(tmp_path / "sdo_persistent_controller.json", ops=ops, clock=clock)

    assert len(errors) == 1
    assert "incident-1" in errors[0]
    assert "8 broker attempts" in errors[0]
    assert "trailing whitespace" in errors[0]
    assert "inspect the incident worktree" in errors[0]
    # The drain ends on the controller's verdict, not at its 3600 s timeout.
    assert clock.monotonic() < 60
    # The controller log is evidence of the rejected closure.
    assert ops.events.count(("logs", "hotel-sdo")) == 2
    assert ("delete", "hotel-sdo") in ops.events


def test_a_permanently_failed_closure_does_not_stall_the_next_incident(tmp_path: Path) -> None:
    ops = FakeOps()
    _run(tmp_path, ops, "s0", [])
    state = ops.states["hotel-sdo"]
    # The controller keeps the failed closure's state until the next incident begins its own closure.
    state["closure_state"] = "CLOSURE_STATE_FAILED"
    state["closure_failure"] = {
        "incident_id": "incident-1",
        "attempts": 8,
        "max_attempts": 8,
        "permanent": True,
        "last_error": "request ConfigMap is too long",
        "action": "inspect the broker ledger; later incidents continue",
    }

    second = _run(tmp_path, ops, "s1", [])

    assert ("inject", "hotel-sdo", "incident-2") in ops.events
    assert second["incident_resolution_seconds"] == 40.0
    rejected = json.loads((tmp_path / "s0" / "agent" / REJECTED_RECEIPT_FILENAME).read_text(encoding="utf-8"))
    assert rejected["failure"]["incident_id"] == "incident-1"
    assert "request ConfigMap is too long" in rejected["failure"]["message"]


def test_each_stage_exports_its_runtime_evidence_when_it_ends(tmp_path: Path) -> None:
    ops = FakeOps()

    resolution = _run(tmp_path, ops, "s0", [])

    receipt_dir = tmp_path / "s0" / "agent"
    # The responder's tokens survive a pipeline stopped before this incident drains.
    assert (receipt_dir / "sdo_runtime" / "codex" / "sessions" / "rollout-responder-1.jsonl").is_file()
    evidence = resolution["stage_end_runtime_artifacts"]
    assert evidence["error"] is None
    assert evidence["scope"] == "stage-end snapshot; the drained strict receipt supersedes it"
    assert set(evidence["export_seconds"]) == {"controller_logs", "runtime_artifacts"}
    assert all(seconds >= 0 for seconds in evidence["export_seconds"].values())
    order = [event[0] for event in ops.events]
    # Exported after verified recovery, so it never delays the fault or the responder.
    assert order.index("paused") < order.index("stage_evidence")
    recorded = json.loads((receipt_dir / RESOLUTION_FILENAME).read_text(encoding="utf-8"))
    assert recorded["stage_end_runtime_artifacts"]["error"] is None


def test_a_failed_stage_end_export_is_recorded_and_does_not_fail_the_stage(tmp_path: Path) -> None:
    ops = FakeOps(stage_evidence_error="kubectl exec failed: pod not ready")

    resolution = _run(tmp_path, ops, "s0", [])

    assert resolution["stage_end_runtime_artifacts"]["error"] == "kubectl exec failed: pod not ready"
    assert resolution["incident_resolution_seconds"] == 40.0


def test_a_drained_receipt_is_published_as_soon_as_the_next_stage_drains_it(tmp_path: Path) -> None:
    # A pipeline stopped during stage 1 must keep stage 0's strict receipt and tokens.
    staging = tmp_path / ".runtime" / "sdo_codex" / "anon_abc"
    ops = FakeOps()
    _run(tmp_path, ops, "s0", [], receipt_dir=staging)
    run_dir = tmp_path / "pipeline" / "stage_0" / "results" / "sdo_codex" / "problem-1" / "run_1"
    run_dir.parent.mkdir(parents=True)
    staging.rename(run_dir)
    ops.reflectable.add("incident-1")

    _run(tmp_path, ops, "s1", [])

    receipt = json.loads((run_dir / STRICT_RECEIPT_FILENAME).read_text(encoding="utf-8"))
    assert receipt["incident_id"] == "incident-1"
    assert "anon_abc" not in (run_dir / STRICT_RECEIPT_FILENAME).read_text(encoding="utf-8")
    assert (run_dir / "sdo_runtime" / "codex" / "sessions" / "rollout-incident-1.jsonl").is_file()
    assert (run_dir / "sdo_runtime" / "usage" / "controller-turns.jsonl").is_file()
    assert not staging.exists()
    # Teardown publishes nothing twice.
    ops.reflectable.add("incident-2")
    state_path = tmp_path / "sdo_persistent_controller.json"
    assert teardown(state_path, ops=ops, clock=_clock(ops)) == []
    assert run_dir / STRICT_RECEIPT_FILENAME not in publish_deferred_receipts(state_path, tmp_path / "pipeline")


@dataclass
class _StuckOps(FakeOps):
    """The responder reported failure, so the incident stays unverified until the stage times out."""

    def inject_after_resume(
        self, control_namespace: str, generation: str, inject: Callable[[], None]
    ) -> dict[str, float]:
        inject()
        self.states[control_namespace].update(
            {
                "incident_open": True,
                "responder_done": True,
                "incident_request": {"incident_id": "incident-stuck"},
                "incident_result": {
                    "incident_id": "incident-stuck",
                    "status": "failed",
                    "repair_changes": ["scripted plan stopped: no manifest defines ConfigMap x"],
                },
            }
        )
        return {}


@dataclass
class _ClosureFailedOps(FakeOps):
    """A previous closure failed permanently, so the controller opens no new incident."""

    def inject_after_resume(
        self, control_namespace: str, generation: str, inject: Callable[[], None]
    ) -> dict[str, float]:
        inject()
        self.states[control_namespace].update(
            {
                "dispatch_state": "DISPATCH_STATE_IDLE",
                "closure_state": "CLOSURE_STATE_FAILED",
                "closure_failure": {"incident_id": "incident-0", "permanent": True, "last_error": "broker exited 1"},
            }
        )
        return {}


class _UndetectedOps(FakeOps):
    """The fault is injected, but no detector ever opens an incident."""

    def inject_after_resume(
        self, control_namespace: str, generation: str, inject: Callable[[], None]
    ) -> dict[str, float]:
        inject()
        return {}


def test_no_incident_by_the_detection_deadline_is_a_detection_miss(tmp_path: Path) -> None:
    ops = _UndetectedOps()
    clock = _clock(ops)

    with pytest.raises(DetectionMissError) as raised:
        run_persistent_stage(
            _inputs(tmp_path, "s0", verification_timeout_seconds=3900, detection_timeout_seconds=900),
            ops=ops,
            run_lifecycle=lambda: True,
            inject=lambda: None,
            clock=clock,
        )

    assert "no incident opened within 900s" in str(raised.value)
    assert clock.monotonic() < 3900  # it did not wait out the verification budget


def test_an_incident_that_opened_keeps_the_full_verification_budget(tmp_path: Path) -> None:
    """The detection deadline only ends a stage in which nothing was ever detected."""

    ops = _StuckOps()
    clock = _clock(ops)

    with pytest.raises(PersistentControllerError) as raised:
        run_persistent_stage(
            _inputs(
                tmp_path,
                "s0",
                verification_timeout_seconds=60,
                detection_timeout_seconds=10,
            ),
            ops=ops,
            run_lifecycle=lambda: True,
            inject=lambda: None,
            clock=clock,
        )

    assert not isinstance(raised.value, DetectionMissError)
    assert "verified no new incident within 60s" in str(raised.value)


def test_a_timeout_behind_a_permanently_failed_closure_says_so(tmp_path: Path) -> None:
    with pytest.raises(ClosureFailedError) as raised:
        _run(tmp_path, _ClosureFailedOps(), "s0", [])

    assert "incident 'incident-0' closure permanently failed" in str(raised.value)
    assert "broker exited 1" in str(raised.value)


def test_an_unverified_incident_timeout_names_the_open_incident_and_why_it_is_stuck(tmp_path: Path) -> None:
    with pytest.raises(PersistentControllerError) as raised:
        _run(tmp_path, _StuckOps(), "s0", [])

    message = str(raised.value)
    assert "verified no new incident within 30s" in message
    assert "incident-stuck" in message
    assert "responder status 'failed'" in message
    assert "no manifest defines ConfigMap x" in message


def _stage_keep_running(tmp_path: Path, ops: FakeOps, stage: str = "s0") -> dict[str, Any]:
    return run_persistent_stage(
        _inputs(tmp_path, stage),
        ops=ops,
        run_lifecycle=lambda: True,
        inject=lambda: ops.events.append(("fault", stage)),
        clock=_clock(ops),
        pause_after_verified=False,
    )


def _second_closure(ops: FakeOps, incident_id: str = "incident-2") -> None:
    ops.states["hotel-sdo"]["pending_closure"] = {
        "request": {"incident_id": incident_id},
        "result": {
            "confirmed_root_causes": [{"summary": "network policy"}],
            "repair_actions": [{"summary": "deleted policy"}],
        },
        "detected_at": (DETECTED + timedelta(seconds=300)).isoformat(),
        "dispatched_at": (DETECTED + timedelta(seconds=301)).isoformat(),
        "responder_completed_at": (DETECTED + timedelta(seconds=330)).isoformat(),
        "verified_at": (DETECTED + timedelta(seconds=350)).isoformat(),
        "final_detector_states": [{"detector_id": "health", "status": "clear"}],
    }


def test_stage_can_leave_the_controller_running_after_the_first_verified_incident(tmp_path: Path) -> None:
    ops = FakeOps()

    resolution = _stage_keep_running(tmp_path, ops)

    assert resolution["incident_id"] == "incident-1"
    assert ("paused", "hotel-sdo") not in ops.events
    assert PersistentState.load(tmp_path / "sdo_persistent_controller.json").controllers["hotel"].pending is not None


def test_followup_incident_is_collected_after_the_first_is_drained(tmp_path: Path) -> None:
    ops = FakeOps()
    _stage_keep_running(tmp_path, ops)
    state_path = tmp_path / "sdo_persistent_controller.json"
    state = PersistentState.load(state_path)
    record = state.controllers["hotel"]
    ops.reflectable.add("incident-1")
    drain_pending_incident(record, ops=ops, repository=tmp_path / "s0" / "application_workspace", drained_by="t")
    record.pending = None
    state.save(state_path)
    _second_closure(ops)

    followup = collect_followup_incident(
        _inputs(tmp_path, "s0-followup-1", receipt_dir=tmp_path / "s0" / "followup1"),
        ops=ops,
        known={"incident-1"},
        timeout_seconds=30,
        clock=_clock(ops),
    )

    assert followup is not None
    assert followup["incident_id"] == "incident-2"
    assert followup["incident_resolution_seconds"] == 50.0
    assert followup["confirmed_root_causes"] == [{"summary": "network policy"}]
    pending = PersistentState.load(state_path).controllers["hotel"].pending
    assert pending is not None
    assert pending.incident_id == "incident-2"
    assert (tmp_path / "s0" / "followup1" / RESOLUTION_FILENAME).is_file()


def test_followup_returns_none_when_no_new_incident_is_verified(tmp_path: Path) -> None:
    ops = FakeOps()
    _stage_keep_running(tmp_path, ops)
    state_path = tmp_path / "sdo_persistent_controller.json"
    state = PersistentState.load(state_path)
    state.controllers["hotel"].pending = None
    state.save(state_path)

    followup = collect_followup_incident(
        _inputs(tmp_path, "s0-followup-1", receipt_dir=tmp_path / "s0" / "followup1"),
        ops=ops,
        known={"incident-1"},
        timeout_seconds=5,
        clock=_clock(ops),
    )

    assert followup is None


def test_followup_refuses_to_start_while_an_incident_is_undrained(tmp_path: Path) -> None:
    ops = FakeOps()
    _stage_keep_running(tmp_path, ops)

    with pytest.raises(PersistentControllerError, match="undrained"):
        collect_followup_incident(
            _inputs(tmp_path, "s0-followup-1", receipt_dir=tmp_path / "s0" / "followup1"),
            ops=ops,
            known={"incident-1"},
            timeout_seconds=5,
            clock=_clock(ops),
        )


def test_reflection_drain_accepts_acknowledged_closure_without_controller_relaunch() -> None:
    ops = FakeOps(states={"demo-sdo": {"last_acknowledged_incident_id": "incident-1"}})
    state = _wait_for_reflection_drain(
        ops, "demo-sdo", "incident-1", 1.0, Clock(sleep=lambda _: pytest.fail("closure should already be drained"))
    )
    assert state["last_acknowledged_incident_id"] == "incident-1"


@pytest.mark.parametrize("trust_threshold", [3, 7])
def test_driver_environment_settings_are_recorded_and_preserved_on_persistent_reuse(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, trust_threshold: int
) -> None:
    from benchmarks.sregym.adapter import driver, native_operator

    monkeypatch.setenv(
        "SREGYM_EXPERIMENT_AGENT_CONFIG",
        json.dumps(
            {
                "deployed_manifest_paths": ["kubernetes"],
                "program_fast_path": True,
                "program_trust_threshold": trust_threshold,
            }
        ),
    )
    monkeypatch.setenv("SREGYM_AGENT_WORKDIR", str(tmp_path))
    monkeypatch.setenv("SREGYM_KIND_CLUSTER_NAME", "sregym-w1")
    monkeypatch.setenv("SREGYM_BASE_KUBECONFIG", str(tmp_path / "config"))
    state_path = tmp_path / "persistent.json"
    monkeypatch.setenv(driver.PERSISTENT_STATE_ENV, str(state_path))
    sdo = tmp_path / ".sdo"
    (sdo / "diagnostics").mkdir(parents=True)
    (sdo / "diagnostics/manifest.yaml").write_text("detectors: []\n")
    (sdo / "lifecycle-provenance.yaml").write_text("{}\n")
    ops = FakeOps()
    monkeypatch.setattr(driver, "_cluster_ops", lambda *_args: ops)
    monkeypatch.setattr(driver, "poll_stage_sync", lambda *_args, **_kwargs: driver.AWAITING_FAULT_INJECTION)
    monkeypatch.setattr(driver, "get_app_info", lambda *_args: {"namespace": "hotel", "app_name": "hotel"})
    monkeypatch.setattr(
        driver,
        "_deployed_lifecycle_context",
        lambda *_args: driver.DeployedLifecycleContext(
            health_objective="healthy",
            active_resources=[],
        ),
    )
    monkeypatch.setattr(driver, "prepare_benchmark_resources", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(driver, "inspect_existing", lambda *_args: object())
    monkeypatch.setattr(driver, "request_fault_injection", lambda *_args: None)
    commands: list[list[str]] = []

    def run(command: str, args: list[str], **_kwargs: object) -> dict[str, object]:
        assert command == "install-existing"
        commands.append(args)
        return {"installed": True, "reused": False, "fingerprint": "abc", "accepted_commit": "a" * 40}

    monkeypatch.setattr(native_operator, "_run", run)
    for stage in ("stage_0_first", "stage_1_reused"):
        args = driver._parse_args(["--logs-dir", str(tmp_path / stage / "logs")])
        resolution = driver._run_persistent(args, "", driver.time.monotonic())
        record = PersistentState.load(state_path).controllers["hotel"]
        config = runtime_config_from_payload(record.runtime_config)
        assert config.deployed_manifest_paths == ("kubernetes",)
        assert config.program_fast_path is True
        assert config.program_trust_threshold == trust_threshold
        ops.reflectable.add(resolution["incident_id"])
    assert resolution["lifecycle_reused"] is True
    assert ops.installs == 1
    assert len(commands) == 1
    command = commands[0]
    assert command[command.index("--deployed-manifest-path") + 1] == "kubernetes"
    assert "--program-fast-path" in command
    assert command[command.index("--program-trust-threshold") + 1] == str(trust_threshold)


def test_unrepresentable_rejected_receipt_stays_one_incidents_typed_failure(tmp_path: Path) -> None:
    from benchmarks.sregym.protocol.native_receipt import NativeReceiptValidationError
    from sdo.contracts import ReceiptFailure

    class BadWireOps(FakeOps):
        def collect_receipt(self, config: RuntimeConfig, incident_id: str, artifacts_dir: Path) -> dict[str, Any]:
            if incident_id == "incident-1":
                raw = {"incident_id": incident_id, "not_json": float("nan")}
                raise NativeReceiptValidationError(
                    "non-finite wire value",
                    raw,
                    ReceiptFailure(
                        incident_id=incident_id, code=ReceiptFailure.CODE_INVALID_WIRE, message="non-finite wire value"
                    ),
                )
            return super().collect_receipt(config, incident_id, artifacts_dir)

    ops = BadWireOps()
    _run(tmp_path, ops, "s0", [])
    ops.reflectable.add("incident-1")
    state = PersistentState.load(tmp_path / "sdo_persistent_controller.json")
    record = state.controllers["hotel"]
    result = drain_pending_incident(
        record, ops=ops, repository=tmp_path / "s0" / "application_workspace", drained_by="test", clock=_clock(ops)
    )
    assert result.failure.code == ReceiptFailure.CODE_INVALID_WIRE
    rejection = json.loads((record.pending.receipt_dir / REJECTED_RECEIPT_FILENAME).read_text())
    assert "NaN" in rejection["receipt_json"]
    assert rejection["failure"]["incident_id"] == "incident-1"
    _run(tmp_path, ops, "s1", [])
    assert ("fault", "s1") in ops.events


def test_a_closure_cut_by_the_verification_timeout_reports_no_verified_timings() -> None:
    closure = {
        "detected_at": DETECTED.isoformat(),
        "dispatched_at": (DETECTED + timedelta(seconds=1)).isoformat(),
        "responder_completed_at": (DETECTED + timedelta(seconds=35)).isoformat(),
        "verified_at": (DETECTED + timedelta(seconds=155)).isoformat(),
    }

    assert _resolution_timings(closure) == {}
    closure["final_detector_states"] = [{"detector_id": "health", "status": "clear"}]
    assert _resolution_timings(closure)["incident_resolution_seconds"] == 155.0


def test_teardown_preserves_pending_state_when_controller_inventory_fails(tmp_path: Path) -> None:
    class UnavailableOps(FakeOps):
        unavailable = False

        def controller_pod(self, control_namespace: str) -> ControllerPod | None:
            if self.unavailable:
                raise PersistentControllerError("cannot inspect controller: connection refused")
            return super().controller_pod(control_namespace)

    ops = UnavailableOps()
    _run(tmp_path, ops, "s0", [])
    ops.events.clear()
    ops.unavailable = True

    errors = teardown(tmp_path / "sdo_persistent_controller.json", ops=ops, clock=_clock(ops))

    assert len(errors) == 1
    assert "connection refused" in errors[0]
    assert not any(event[0] == "delete" for event in ops.events)
    assert PersistentState.load(tmp_path / "sdo_persistent_controller.json").controllers["hotel"].pending is not None
    assert not (tmp_path / "s0" / "agent" / REJECTED_RECEIPT_FILENAME).exists()


def test_invalid_deferred_injection_pauses_without_incident_or_receipt_wait(tmp_path: Path) -> None:
    from benchmarks.sregym.protocol.injection import InvalidInjectionError

    ops = FakeOps()
    inputs = _inputs(tmp_path, "invalid")

    def invalid():
        raise InvalidInjectionError({"composite_valid": False})

    with pytest.raises(InvalidInjectionError):
        run_persistent_stage(inputs, ops=ops, run_lifecycle=lambda: True, inject=invalid, clock=_clock(ops))
    assert ops.incidents == 0
    assert ops.events[-1] == ("paused", inputs.runtime_config.control_namespace)
    assert not any(event[0] in {"receipt", "reflected"} for event in ops.events)
    outcome = json.loads((inputs.receipt_dir / "sdo_infrastructure_outcome.json").read_text())
    assert outcome["infra_error"] == "injection_invalid"
    assert outcome["incident_credit"] is False


def test_validity_barrier_keeps_invalid_partial_injection_unobservable_to_responders() -> None:
    from benchmarks.sregym.adapter.persistent import inject_while_paused
    from benchmarks.sregym.protocol.injection import InvalidInjectionError

    ops = FakeOps()
    control = "hotel-sdo"
    ops.logs[control] = []

    def invalid():
        assert ops.events[-1] == ("paused", control)
        raise InvalidInjectionError({"partial_write": True})

    with pytest.raises(InvalidInjectionError):
        inject_while_paused(ops, control, "validity", invalid, clock=_clock(ops))
    assert ops.events == [("paused", control)]


def test_valid_injection_barrier_retains_the_healthy_observation_baseline() -> None:
    from benchmarks.sregym.adapter.persistent import inject_while_paused

    class BarrierOps(FakeOps):
        def set_maintenance(self, control_namespace, *, paused, generation, preserve_baseline=False):
            if paused:
                assert preserve_baseline, "a valid fault must survive the observation pause"
            super().set_maintenance(control_namespace, paused=paused, generation=generation)

    ops = BarrierOps()
    ops.logs["hotel-sdo"] = []
    inject_while_paused(ops, "hotel-sdo", "valid", lambda: None, clock=_clock(ops))
    assert ops.events == [("paused", "hotel-sdo"), ("active", "hotel-sdo")]


def test_interrupted_install_retains_cleanup_target_before_controller_exists(tmp_path: Path) -> None:
    inputs = _inputs(tmp_path, "s0")
    ops = FakeOps()

    def interrupted() -> bool:
        state = PersistentState.load(inputs.state_path)
        assert state.controllers == {}
        assert state.cleanup_targets["hotel"].control_namespace == "hotel-sdo"
        raise RuntimeError("launcher killed during installation")

    with pytest.raises(RuntimeError, match="launcher killed"):
        run_persistent_stage(inputs, ops=ops, run_lifecycle=interrupted, inject=lambda: None, clock=_clock(ops))
    assert PersistentState.load(inputs.state_path).cleanup_targets["hotel"].namespace == "hotel"
