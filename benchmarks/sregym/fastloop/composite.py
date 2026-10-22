"""Composite (multi-fault) incidents for the fast loop.

A composite injects several independent faults at once. Two things differ from a
single-fault incident:

* Per-fault progress is recorded by read-only probes (:mod:`fault_tracker`) while
  the agent works, so the result says which fault was fixed when, not only whether all were.
* SDO's controller may close an incident before every fault is fixed. The composite
  SDO agent therefore keeps the controller running after each verified incident,
  drains that incident's reflection, and waits for the next incident until every
  fault is resolved, no further incident arrives, or the deadline passes.

Both agents write ``composite_<index>_<problem>.json`` beside the incident records:
fault timeline, per-fault resolution seconds since injection finished, and (SDO) the
incident sequence with the controller-state timeline.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.adapter import (
    ClosureFailedError,
    DetectionMissError,
    DetectorReviewRequiredError,
    PersistentControllerError,
    PersistentState,
    collect_followup_incident,
    drain_pending_incident,
    pause_controller,
)
from benchmarks.sregym.fastloop.fault_tracker import (
    BackgroundPoller,
    FaultSpec,
    FaultTracker,
    KubectlReader,
    composite_faults,
    probes_for,
)
from benchmarks.sregym.fastloop.loop import AgentOutcome
from benchmarks.sregym.fastloop.records import TokenCounts
from benchmarks.sregym.fastloop.sdo_agent import SdoPersistentAgent, _timestamp

if TYPE_CHECKING:
    from collections.abc import Callable

    from benchmarks.sregym.fastloop.loop import InjectionWindow
    from benchmarks.sregym.fastloop.records import AgentName

logger = logging.getLogger(__name__)

STATE_SAMPLE_SECONDS = 10.0


@dataclass(frozen=True)
class CompositeSettings:
    #: Longest the composite may run after injection before the agent is cut off.
    deadline_seconds: float = 2400.0
    #: Longest to wait for another incident while faults remain and none is in flight.
    idle_seconds: float = 600.0
    #: How long a fault must stay green to count as resolved.
    stable_seconds: float = 20.0
    poll_seconds: float = 5.0
    #: Stop as soon as the controller stops itself for detector review (False: wait out the deadline).
    stop_on_detector_review: bool = True

    def __post_init__(self) -> None:
        for name in ("deadline_seconds", "idle_seconds", "stable_seconds", "poll_seconds"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")


def make_tracker(
    problem_id: str,
    reader: Callable[[str, str], dict[str, Any] | None],
    settings: CompositeSettings,
    *,
    faults: tuple[FaultSpec, ...] = (),
):
    faults = faults or composite_faults(problem_id)
    if not faults:
        return None
    return FaultTracker(probes_for(faults, reader), stable_seconds=settings.stable_seconds)


def write_composite_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(report, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    temporary.replace(path)


def _fault_report(tracker: FaultTracker, injected_offset: float) -> dict[str, Any]:
    """Seconds are measured from the end of injection (tracker polling starts there)."""

    resolution = tracker.resolution_times()
    first = tracker.first_green()
    last = tracker.samples[-1].states if tracker.samples else {}
    return {
        "faults": list(resolution),
        "resolved_s": resolution,
        "first_green_s": first,
        "ever_red": tracker.ever_red(),
        "final_green": last,
        "faults_resolved": sum(1 for value in resolution.values() if value is not None),
        "faults_total": len(resolution),
        "all_resolved_s": tracker.all_resolved_at(),
        "poll_origin_offset_s": injected_offset,
        # Seconds are measured from the end of injection (older reports measured from agent start).
        "origin": "injection_end",
        "timeline": tracker.timeline(),
    }


class TrackedAgent:
    """Wraps an incident agent (the Codex baselines) with per-fault resolution tracking."""

    def __init__(
        self,
        inner: Any,
        *,
        reader: Callable[[str, str], dict[str, Any] | None],
        report_dir: Path,
        settings: CompositeSettings | None = None,
        stream_faults: dict[str, tuple[FaultSpec, ...]] | None = None,
    ) -> None:
        self._inner = inner
        self._reader = reader
        self._report_dir = report_dir
        self._settings = settings or CompositeSettings()
        self._stream_faults = stream_faults or {}

    @property
    def name(self) -> AgentName:
        return self._inner.name

    @property
    def model(self) -> str:
        return self._inner.model

    def resolve(self, index: int, problem_id: str, inject: Callable[[], InjectionWindow]) -> AgentOutcome:
        tracker = make_tracker(problem_id, self._reader, self._settings, faults=self._stream_faults.get(problem_id, ()))
        if tracker is None:
            return self._inner.resolve(index, problem_id, inject)
        poller = BackgroundPoller(tracker, interval_seconds=self._settings.poll_seconds)
        started = time.monotonic()
        state: dict[str, Any] = {}

        def tracked_inject() -> InjectionWindow:
            window = inject()
            state["poller"] = poller.__enter__()
            state["offset"] = time.monotonic() - started
            return window

        outcome: AgentOutcome | None = None
        try:
            outcome = self._inner.resolve(index, problem_id, tracked_inject)
        finally:
            if "poller" in state:
                poller.__exit__(None, None, None)
            report = _fault_report(tracker, state.get("offset", 0.0))
            report.update({"agent": self.name, "problem_id": problem_id})
            if outcome is not None:
                returned_at = outcome.resolved_at or outcome.mitigation_applied_at
                report["agent_returned_after_injection_s"] = (
                    (returned_at - outcome.injection.finished_at).total_seconds() if returned_at else None
                )
            write_composite_report(self._report_dir / f"composite_{index:03d}_{problem_id}.json", report)
        assert outcome is not None  # resolve() raising skips this return
        return outcome

    def learn(self, outcome: AgentOutcome) -> AgentOutcome:
        return self._inner.learn(outcome)

    def close(self) -> None:
        self._inner.close()


class CompositeSdoAgent(SdoPersistentAgent):
    """SDO's persistent controller serving a composite: several incidents, one injection."""

    name: AgentName = "sdo"

    def __init__(
        self,
        *args: Any,
        reader: Callable[[str, str], dict[str, Any] | None],
        report_dir: Path,
        composite: CompositeSettings | None = None,
        stream_faults: dict[str, tuple[FaultSpec, ...]] | None = None,
        **kwargs: Any,
    ) -> None:
        self._composite = composite or CompositeSettings()
        super().__init__(
            *args, keep_running=True, stop_on_detector_review=self._composite.stop_on_detector_review, **kwargs
        )
        self._reader = reader
        self._stream_faults = stream_faults or {}
        self._report_dir = report_dir
        self._incidents: list[dict[str, Any]] = []
        self._base_dir: Path | None = None
        self._wedge_usage: dict[str, Any] | None = None
        self._reflection_drain_seconds = 0.0
        self._reflection_started_at = None

    def resolve(self, index: int, problem_id: str, inject: Callable[[], InjectionWindow]) -> AgentOutcome:
        composite = self._composite
        tracker = make_tracker(problem_id, self._reader, composite, faults=self._stream_faults.get(problem_id, ()))
        if tracker is None:
            if problem_id.startswith(("mf__", "composite")):
                raise ValueError(f"{problem_id!r} is not a registered composite")
            self._reflection_drain_seconds = 0.0
            self._reflection_started_at = None
            self._keep_running = False
            try:
                single = super().resolve(index, problem_id, inject)
            finally:
                self._keep_running = True
            self._incidents = [
                {"incident_id": single.incident_id, "receipt_dir": str(single.artifacts_dir), "outcome": single}
            ]
            return single
        settings = self._settings
        control = settings.runtime_config.control_namespace
        assert control is not None
        poller = BackgroundPoller(tracker, interval_seconds=composite.poll_seconds)
        sampler_stop = threading.Event()
        controller_samples: list[dict[str, Any]] = []
        started = time.monotonic()
        marks: dict[str, float] = {}

        def sample_controller() -> None:
            while not sampler_stop.is_set():
                try:
                    state = self._ops.runtime_state(control)
                    closure = state.get("pending_closure") or {}
                    request = state.get("incident_request") or {}
                    controller_samples.append(
                        {
                            "t": round(time.monotonic() - started, 1),
                            "incident_request": request.get("incident_id") if isinstance(request, dict) else None,
                            "pending_closure": (closure.get("request") or {}).get("incident_id")
                            if isinstance(closure, dict)
                            else None,
                            "closure_verified_at": closure.get("verified_at") if isinstance(closure, dict) else None,
                            "last_acknowledged": state.get("last_acknowledged_incident_id"),
                            "closure_state": state.get("closure_state"),
                        }
                    )
                except Exception as exc:  # telemetry must not stop the run
                    controller_samples.append({"t": round(time.monotonic() - started, 1), "error": str(exc)})
                sampler_stop.wait(STATE_SAMPLE_SECONDS)

        injected: list[InjectionWindow] = []

        def tracked_inject() -> InjectionWindow:
            window = inject()
            injected.append(window)
            marks["poll_start"] = time.monotonic() - started
            poller.__enter__()
            return window

        sampler = threading.Thread(target=sample_controller, name="controller-state", daemon=True)
        sampler.start()
        self._incidents = []
        self._reflection_drain_seconds = 0.0
        self._reflection_started_at = None
        outcomes: list[AgentOutcome] = []
        stop_reason = "error"
        wedged = False
        try:
            try:
                first = super().resolve(index, problem_id, tracked_inject)
            except ClosureFailedError as stage_error:
                if not injected:
                    raise
                first = self._failed_closure_outcome(
                    AgentOutcome(
                        injection=injected[0],
                        artifacts_dir=str(settings.results_dir / f"{index:03d}_{problem_id}"),
                    ),
                    self._ops.runtime_state(control),
                    stage_error,
                )
                stop_reason = "closure_failed"
                wedged = True
            except DetectionMissError:
                stop_reason = "detection_miss"
                raise
            except PersistentControllerError as stage_error:
                if not injected:
                    raise
                if isinstance(stage_error, DetectorReviewRequiredError):
                    wedge = stage_error
                    stop_reason = "detector_review_required"
                else:
                    state = self._ops.runtime_state(control)
                    request = state.get("incident_request")
                    wedge = DetectorReviewRequiredError(
                        str(request.get("incident_id")) if isinstance(request, dict) else "none",
                        f"no verified closure: {stage_error}",
                        state,
                    )
                    stop_reason = "no_closure_within_deadline"
                wedged = True
                first = self._wedged_outcome(index, problem_id, injected[0], wedge)
            self._base_dir = Path(str(first.artifacts_dir))
            outcomes.append(first)
            self._incidents.append(
                {
                    "incident_id": first.incident_id,
                    "receipt_dir": str(first.artifacts_dir),
                    "outcome": first,
                    "fallback_usage": self._wedge_usage if wedged else None,
                }
            )
            deadline = started + composite.deadline_seconds
            known = {str(first.incident_id)}
            lifecycle = self._lifecycle_inputs()
            while not wedged:
                if self._reflection_started_at is None:
                    self._reflection_started_at = self._clock.now()
                try:
                    self._drain_last(first_label=f"composite-{index:03d}")
                except ClosureFailedError as exc:
                    finished_at = self._clock.now()
                    failed = replace(
                        self._failed_closure_outcome(outcomes[-1], self._ops.runtime_state(control), exc),
                        reflection_started_at=self._reflection_started_at,
                        reflection_finished_at=finished_at,
                        reflection_seconds=(finished_at - self._reflection_started_at).total_seconds(),
                    )
                    outcomes[-1] = failed
                    self._incidents[-1]["outcome"] = failed
                    stop_reason = "closure_failed"
                    wedged = True
                    break
                if tracker.all_resolved_at() is not None or self._wait_resolved(tracker, composite):
                    stop_reason = "all_faults_resolved"
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    stop_reason = "deadline"
                    break
                number = len(self._incidents) + 1
                receipt_dir = (
                    Path(str(first.artifacts_dir)).parent / f"{Path(str(first.artifacts_dir)).name}_incident{number}"
                )
                followup = collect_followup_incident(
                    self.stage_inputs(f"fastloop-{index:03d}-incident{number}", receipt_dir, lifecycle.fingerprint),
                    ops=self._ops,
                    known=known,
                    timeout_seconds=min(composite.idle_seconds, remaining),
                    clock=self._clock,
                )
                if followup is None:
                    stop_reason = "no_further_incident" if remaining > composite.idle_seconds else "deadline"
                    break
                known.add(str(followup["incident_id"]))
                outcomes.append(self._outcome_from_resolution(first, followup, receipt_dir))
                self._incidents.append(
                    {"incident_id": followup["incident_id"], "receipt_dir": str(receipt_dir), "outcome": outcomes[-1]}
                )
        except Exception:
            logger.exception("composite %s failed before completion", problem_id)
            raise
        finally:
            sampler_stop.set()
            sampler.join(timeout=30)
            marks["last_resolve_s"] = time.monotonic() - started
            if "poll_start" in marks:
                poller.__exit__(None, None, None)
            try:
                if injected and stop_reason in {
                    "detector_review_required",
                    "no_closure_within_deadline",
                    "closure_failed",
                }:
                    # The controller stopped itself; nothing will acknowledge a pause.
                    self._ops.set_maintenance(control, paused=True, generation=f"composite-{index:03d}-wedged")
                elif injected:
                    pause_controller(self._ops, control, label=f"composite-{index:03d}", clock=self._clock)
            except Exception:
                logger.exception("could not pause the controller after the composite")
            report = _fault_report(tracker, marks.get("poll_start", 0.0))
            report.update(
                {
                    "agent": "sdo",
                    "problem_id": problem_id,
                    "stop_reason": stop_reason,
                    "incidents": [
                        {
                            "incident_id": item["incident_id"],
                            "receipt_dir": item["receipt_dir"],
                            "detected_at": _iso(item["outcome"].detected_at),
                            "resolved_at": _iso(item["outcome"].resolved_at),
                            "diagnosis": item["outcome"].diagnosis,
                            "mitigation": item["outcome"].mitigation,
                        }
                        for item in self._incidents
                    ],
                    "controller_state_timeline": controller_samples,
                    "controller_job": _controller_job_summary(control),
                }
            )
            write_composite_report(self._report_dir / f"composite_{index:03d}_{problem_id}.json", report)
        return self._merge(outcomes)

    @staticmethod
    def _failed_closure_outcome(first: AgentOutcome, state: dict[str, Any], exc: ClosureFailedError) -> AgentOutcome:
        from benchmarks.sregym.fastloop.sdo_agent import (
            _last_successful_repair,
            _program_metrics,
            _summaries,
            _timestamp,
        )

        closure = state.get("pending_closure")
        closure = closure if isinstance(closure, dict) else {}
        raw_result = closure.get("result") or state.get("incident_result")
        result = raw_result if isinstance(raw_result, dict) else {}
        return replace(
            first,
            incident_id=first.incident_id or str((closure.get("request") or {}).get("incident_id") or "unknown"),
            detected_at=first.detected_at
            or _timestamp(closure.get("detected_at") or state.get("incident_detected_at")),
            responder_started_at=first.responder_started_at or _timestamp(closure.get("dispatched_at")),
            responder_finished_at=first.responder_finished_at or _timestamp(closure.get("responder_completed_at")),
            mitigation_applied_at=first.mitigation_applied_at or _last_successful_repair(result.get("repair_actions")),
            resolved_at=first.resolved_at or _timestamp(closure.get("verified_at")),
            diagnosis=first.diagnosis or _summaries(result.get("confirmed_root_causes")),
            mitigation=first.mitigation or _summaries(result.get("repair_actions"), successful_only=True),
            responder_tokens=TokenCounts.from_usage(result.get("usage")),
            **_program_metrics(result),
            error=f"closure failed permanently: {exc}",
            terminal_error=True,
        )

    def _wedged_outcome(
        self, index: int, problem_id: str, window: InjectionWindow, wedge: DetectorReviewRequiredError
    ) -> AgentOutcome:
        """The controller stopped on an uncleared incident: keep what its state and PVC hold as evidence."""

        from benchmarks.sregym.fastloop.sdo_agent import _last_successful_repair, _summaries, _timestamp

        settings = self._settings
        control = settings.runtime_config.control_namespace
        assert control is not None
        receipt_dir = settings.results_dir / f"{index:03d}_{problem_id}"
        receipt_dir.mkdir(parents=True, exist_ok=True)
        state = wedge.state
        raw_result = state.get("incident_result")
        result: dict[str, Any] = raw_result if isinstance(raw_result, dict) else {}
        self._ops.export_controller_logs(control, receipt_dir)
        evidence = self._ops.export_runtime_artifacts(
            replace(settings.runtime_config, artifacts_dir=receipt_dir), receipt_dir
        )
        write_composite_report(
            receipt_dir / "detector_review_state.json",
            {"reason": wedge.reason, "runtime_state": state, "runtime_artifacts": evidence},
        )
        self._wedge_usage = result.get("usage") if isinstance(result.get("usage"), dict) else None
        return AgentOutcome(
            injection=window,
            detected_at=_timestamp(state.get("incident_detected_at")),
            mitigation_applied_at=_last_successful_repair(result.get("repair_actions")),
            resolved_at=None,
            diagnosis=_summaries(result.get("confirmed_root_causes")),
            mitigation=_summaries(result.get("repair_actions"), successful_only=True),
            incident_id=wedge.incident_id,
            artifacts_dir=str(receipt_dir),
            error=f"controller stopped for detector review: {wedge.reason}",
        )

    @staticmethod
    def _wait_resolved(tracker: FaultTracker, composite: CompositeSettings) -> bool:
        """When every fault is green right now, give the stability window time to confirm it."""

        tracker.poll()
        if not tracker.all_green_now():
            return False
        time.sleep(composite.stable_seconds + 2 * composite.poll_seconds)
        tracker.poll()
        return tracker.all_resolved_at() is not None

    def _drain_last(self, *, first_label: str) -> None:
        settings = self._settings
        state = PersistentState.load(settings.state_path)
        record = state.controllers.get(settings.namespace)
        if record is None or record.pending is None:
            return
        if self._reflection_started_at is None:
            self._reflection_started_at = self._clock.now()
        started = self._clock.monotonic()
        drained = drain_pending_incident(
            record,
            ops=self._ops,
            repository=settings.repository,
            drained_by=first_label,
            clock=self._clock,
        )
        if drained.HasField("failure"):
            logger.warning("incident %s strict receipt rejected: %s", drained.incident_id, drained.failure.message)
        self._reflection_drain_seconds += self._clock.monotonic() - started
        record.pending = None
        state.save(settings.state_path)

    def _outcome_from_resolution(
        self, first: AgentOutcome, resolution: dict[str, Any], receipt_dir: Path
    ) -> AgentOutcome:
        from benchmarks.sregym.fastloop.sdo_agent import _last_successful_repair, _summaries, _timestamp

        verified = _timestamp(resolution.get("verified_at"))
        seconds = resolution.get("incident_resolution_seconds")
        detected = None
        if verified is not None and isinstance(seconds, (int, float)):
            from datetime import timedelta

            detected = verified - timedelta(seconds=float(seconds))
        return replace(
            first,
            detected_at=detected,
            responder_started_at=_timestamp(resolution.get("responder_started_at")),
            responder_finished_at=_timestamp(resolution.get("responder_finished_at")),
            mitigation_applied_at=_last_successful_repair(resolution.get("repair_actions")),
            resolved_at=verified,
            diagnosis=_summaries(resolution.get("confirmed_root_causes")),
            mitigation=_summaries(resolution.get("repair_actions"), successful_only=True),
            incident_id=str(resolution["incident_id"]),
            artifacts_dir=str(receipt_dir),
        )

    def _merge(self, outcomes: list[AgentOutcome]) -> AgentOutcome:
        first = outcomes[0]
        last = outcomes[-1]
        return replace(
            first,
            responder_started_at=min(
                (item.responder_started_at for item in outcomes if item.responder_started_at), default=None
            ),
            responder_finished_at=max(
                (item.responder_finished_at for item in outcomes if item.responder_finished_at), default=None
            ),
            mitigation_applied_at=max(
                (item.mitigation_applied_at for item in outcomes if item.mitigation_applied_at), default=None
            ),
            resolved_at=max((item.resolved_at for item in outcomes if item.resolved_at), default=None),
            diagnosis=" || ".join(item.diagnosis for item in outcomes if item.diagnosis),
            mitigation=" || ".join(item.mitigation for item in outcomes if item.mitigation),
            incident_id=last.incident_id,
            artifacts_dir=last.artifacts_dir,
            responder_tokens=last.responder_tokens,
            program_diagnoses=last.program_diagnoses,
            program_verdicts=last.program_verdicts,
            program_tool_calls=last.program_tool_calls,
            tool_commands=last.tool_commands,
            response_loops_used=last.response_loops_used,
            error=last.error,
            terminal_error=last.terminal_error,
        )

    def learn(self, outcome: AgentOutcome) -> AgentOutcome:
        settings = self._settings
        started = self._clock.monotonic()
        observed_at = self._clock.now()
        state = PersistentState.load(settings.state_path)
        record = state.controllers.get(settings.namespace)
        error: str | None = None
        if record is not None and record.pending is not None:
            drained = drain_pending_incident(
                record,
                ops=self._ops,
                repository=settings.repository,
                drained_by=f"fastloop-learn-{outcome.incident_id}",
                clock=self._clock,
            )
            if drained.HasField("failure"):
                error = f"drain failed: {drained.failure.message}"
            record.pending = None
            state.save(settings.state_path)
        usage = TokenCounts()
        reflection = TokenCounts()
        attempts = 0
        from benchmarks.sregym.fastloop.sdo_agent import _program_metrics, _receipt_closure_fields, _with_receipt_times

        diagnoses = 0
        calls = 0
        verdicts: dict[str, int] = {}
        commands: dict[str, int] = {}
        loops = 0
        warm = False
        reasons: list[str] = []
        binding_sources: list[str] = []
        fast_path_skip_reasons: list[str] = []
        receipts_seen = 0
        rebuild_verified = None
        closure_phases: dict[str, float] = {}
        skipped_reasons: list[str] = []
        paths: list[str] = []
        fast_programs: list[str] = []
        relation_covered: list[dict[str, Any]] = []
        timed_outcomes: list[AgentOutcome] = []
        for item in self._incidents:
            receipt = self._receipt(Path(item["receipt_dir"]))
            timed_outcomes.append(_with_receipt_times(item["outcome"], receipt))
            if not receipt and item.get("fallback_usage"):
                # No strict receipt exists for an incident that never closed; its responder usage is in state.
                usage = _add(usage, TokenCounts.from_usage(item["fallback_usage"]))
                receipts_seen += 1
                continue
            if not receipt:
                continue
            receipts_seen += 1
            phases, skipped = _receipt_closure_fields(receipt)
            for name, seconds in phases.items():
                closure_phases[name] = closure_phases.get(name, 0.0) + seconds
            if skipped:
                skipped_reasons.append(skipped)
            ledger = receipt.get("native_ledger")
            ledger = ledger if isinstance(ledger, dict) else {}
            for rollout in ledger.get("rollout_receipts") or []:
                if isinstance(rollout, dict) and rollout.get("success") is True:
                    verified = _timestamp(rollout.get("verified_at"))
                    if verified is not None and (rebuild_verified is None or verified > rebuild_verified):
                        rebuild_verified = verified
            outcome_document = ledger.get("outcome_document")
            outcome_document = outcome_document if isinstance(outcome_document, dict) else {}
            metrics = _program_metrics(outcome_document)
            if metrics["resolution_path"] is not None:
                paths.append(metrics["resolution_path"])
            fast_programs.extend(metrics["fast_path_programs"])
            relation_covered.extend(metrics["fast_path_relation_covered_findings"])
            reasons.extend(metrics["match_reasons"])
            binding_sources.extend(metrics["binding_sources"])
            if metrics["fast_path_skipped_reason"]:
                fast_path_skip_reasons.append(metrics["fast_path_skipped_reason"])
            diagnoses += metrics["program_diagnoses"] or 0
            calls += metrics["program_tool_calls"] or 0
            loops += metrics["response_loops_used"] or 0
            for name, count in metrics["program_verdicts"].items():
                verdicts[name] = verdicts.get(name, 0) + count
            for name, count in metrics["tool_commands"].items():
                commands[name] = commands.get(name, 0) + count
            usage = _add(usage, TokenCounts.from_usage(receipt.get("usage") or outcome_document.get("usage")))
            reflection = _add(reflection, TokenCounts.from_usage(receipt.get("reflection_usage")))
            raw_memory = receipt.get("memory_reuse")
            memory: dict[str, Any] = raw_memory if isinstance(raw_memory, dict) else {}
            warm = warm or bool(memory.get("warm_path"))
            reasons.extend(str(reason) for reason in memory.get("match_reasons", []) or [])
        if receipts_seen < len(self._incidents):
            raise RuntimeError(
                f"{len(self._incidents) - receipts_seen} incident receipt(s) missing after reflection drain"
            )
        timed = self._merge(timed_outcomes)
        return outcome.with_learning(
            detected_at=outcome.detected_at or timed.detected_at,
            mitigation_applied_at=outcome.mitigation_applied_at or timed.mitigation_applied_at,
            resolved_at=outcome.resolved_at or timed.resolved_at,
            responder_started_at=outcome.responder_started_at or timed.responder_started_at,
            responder_finished_at=outcome.responder_finished_at or timed.responder_finished_at,
            responder_tokens=usage,
            reflection_tokens=reflection,
            warm_path=warm,
            match_reasons=tuple(dict.fromkeys(reasons)),
            binding_sources=tuple(dict.fromkeys(binding_sources)),
            fast_path_skipped_reason="; ".join(dict.fromkeys(fast_path_skip_reasons)) or None,
            reflection_attempts=attempts,
            reflection_skipped_reason="; ".join(dict.fromkeys(skipped_reasons)) or None,
            closure_phase_seconds=closure_phases,
            reflection_seconds=self._reflection_drain_seconds + self._clock.monotonic() - started,
            reflection_started_at=self._reflection_started_at or observed_at,
            reflection_finished_at=self._clock.now(),
            rebuild_done_at=rebuild_verified or self._clock.now(),
            program_diagnoses=diagnoses,
            program_verdicts=verdicts,
            program_tool_calls=calls,
            tool_commands=commands,
            response_loops_used=loops,
            resolution_path="fast_path"
            if paths and all(path == "fast_path" for path in paths)
            else "responder"
            if paths
            else None,
            fast_path_programs=tuple(dict.fromkeys(fast_programs)),
            fast_path_relation_covered_findings=tuple(relation_covered),
            error="; ".join(filter(None, [outcome.error, error])) or None,
        )


def _controller_job_summary(control: str) -> dict[str, Any]:
    """How many controller pods ran and how they ended (a stopped controller is restarted by its Job)."""

    from benchmarks.sregym.adapter import kubectl

    try:
        completed = kubectl(
            ["get", "pods", "--selector", "job-name=sdo-controller-run", "-o", "json"], namespace=control, check=False
        )
        pods = json.loads(completed.stdout).get("items", []) if completed.returncode == 0 else []
        return {
            "controller_pods": len(pods),
            "phases": [pod.get("status", {}).get("phase") for pod in pods],
        }
    except Exception as exc:  # evidence only
        return {"error": f"{type(exc).__name__}: {exc}"}


def _add(left: TokenCounts, right: TokenCounts) -> TokenCounts:
    return TokenCounts(
        input_tokens=left.input_tokens + right.input_tokens,
        cached_input_tokens=left.cached_input_tokens + right.cached_input_tokens,
        output_tokens=left.output_tokens + right.output_tokens,
    )


def _iso(value: Any) -> str | None:
    return value.isoformat() if value is not None else None


def reader_for(namespace: str, kubeconfig: Path | str) -> KubectlReader:
    return KubectlReader(namespace=namespace, kubeconfig=kubeconfig)
