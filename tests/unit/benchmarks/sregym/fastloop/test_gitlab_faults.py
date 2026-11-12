"""Generic fault selection and the per-fault state machine, against a fake cluster with a virtual clock."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.sregym.fastloop import gitlab_faults as gf
from benchmarks.sregym.fastloop.generic_faults import (
    Action,
    FaultKind,
    Workload,
    build,
    parse_inventory,
    select_faults,
)
from benchmarks.sregym.fastloop.records import Injected, NotInjected, load_records

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

NS = "app"
EPOCH = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _workload(
    kind: str, name: str, *, replicas: int = 1, pvc: str | None = None, labels: dict[str, str] | None = None
) -> dict[str, Any]:
    labels = labels if labels is not None else {"app": name}
    spec: dict[str, Any] = {"containers": [{"name": name, "image": f"registry.example/{name}:1.0"}]}
    if pvc:
        spec["volumes"] = [{"name": "data", "persistentVolumeClaim": {"claimName": pvc}}]
    return {
        "kind": kind,
        "metadata": {"namespace": NS, "name": name},
        "spec": {"replicas": replicas, "template": {"metadata": {"labels": labels}, "spec": spec}},
    }


def _service(name: str, selector: dict[str, str]) -> dict[str, Any]:
    return {"kind": "Service", "metadata": {"namespace": NS, "name": name}, "spec": {"selector": selector}}


def inventory() -> dict[str, Any]:
    return {
        "items": [
            _workload("Deployment", "web"),
            _workload("Deployment", "worker"),
            _workload("Deployment", "files", pvc="files-pvc"),
            _workload("StatefulSet", "db", pvc="db-pvc"),
            {
                **_workload("Deployment", "managed"),
                "metadata": {"namespace": NS, "name": "managed", "ownerReferences": [{}]},
            },
            _service("web", {"app": "web"}),
            _service("db", {"app": "db"}),
        ]
    }


def workloads(**kwargs: Any) -> list[Workload]:
    return parse_inventory(inventory(), **kwargs)


def test_inventory_skips_owner_managed_workloads_and_attaches_services() -> None:
    found = {w.name: w for w in workloads()}
    assert set(found) == {"web", "worker", "files", "db"}
    assert found["web"].services == ("web",)
    assert found["worker"].services == ()


def test_plan_ids_restrict_targets_to_the_plan() -> None:
    assert [w.name for w in workloads(plan_ids={f"Deployment/{NS}/web"})] == ["web"]


def test_eligibility_follows_workload_facts() -> None:
    by_name = {w.name: w for w in workloads()}
    assert build(FaultKind.NETWORK_DENY, by_name["worker"]) is None  # nothing routes to it
    assert build(FaultKind.NETWORK_DENY, by_name["web"]) is not None
    assert build(FaultKind.SERVICE_SELECTOR, by_name["worker"]) is None
    assert build(FaultKind.PVC_MISMATCH, by_name["db"]) is None  # StatefulSet claims are not a recoverable fault
    assert build(FaultKind.PVC_MISMATCH, by_name["web"]) is None
    assert build(FaultKind.PVC_MISMATCH, by_name["files"]) is not None


def test_revert_restores_the_captured_original() -> None:
    web = next(w for w in workloads() if w.name == "web")
    image = build(FaultKind.BAD_IMAGE, web)
    assert image is not None
    assert "registry.example/web:fault-nonexistent-tag" in image.inject[0].args[-1]
    assert "registry.example/web:1.0" in image.revert[0].args[-1]
    scale = build(FaultKind.SCALE_ZERO, web)
    assert scale is not None
    assert scale.inject[0].args[-1] == "--replicas=0"
    assert scale.revert[0].args[-1] == "--replicas=1"
    selector = build(FaultKind.SERVICE_SELECTOR, web)
    assert selector is not None
    assert json.loads(selector.revert[0].args[-1])["spec"]["selector"] == {"benchmark.fault": None}


def test_network_deny_object_is_benchmark_owned() -> None:
    web = next(w for w in workloads() if w.name == "web")
    spec = build(FaultKind.NETWORK_DENY, web)
    assert spec is not None
    policy = json.loads(spec.inject[0].stdin or "")
    assert policy["metadata"]["labels"] == {"app.kubernetes.io/managed-by": "benchmark-fault"}
    from benchmarks.sregym.fastloop.clean_start import benchmark_owned

    assert benchmark_owned(policy)


def test_selection_is_deterministic_distinct_and_spreads_over_workloads() -> None:
    first = select_faults(workloads(), 8, seed=3)
    again = select_faults(workloads(), 8, seed=3)
    assert [s.problem_id for s in first.faults] == [s.problem_id for s in again.faults]
    assert [s.problem_id for s in first.faults] != [s.problem_id for s in select_faults(workloads(), 8, seed=4).faults]
    pairs = [(s.kind, s.workload.id) for s in first.faults]
    assert len(pairs) == len(set(pairs))
    assert all(a.workload.id != b.workload.id for a, b in zip(first.faults, first.faults[1:], strict=False))
    assert len({s.workload.id for s in first.faults}) >= 3


def test_selection_honours_exclusions_and_reports_ineligible_kinds() -> None:
    selection = select_faults([w for w in workloads() if w.name == "worker"], 3, seed=0, exclude={"nothing"})
    assert {s.workload.name for s in selection.faults} == {"worker"}
    assert {"network_deny", "service_selector", "pvc_mismatch"} <= set(selection.skipped)
    with pytest.raises(ValueError, match="no workloads"):
        select_faults(workloads(), 1, seed=0, exclude={"web", "worker", "files", "db"})


def test_selection_reports_when_fewer_faults_exist_than_requested() -> None:
    selection = select_faults(workloads(), 500, seed=0)
    assert len(selection.faults) < 500
    assert "_count" in selection.skipped


# --- the state machine ----------------------------------------------------------------------------------------


CLOSING = (
    "resolve",
    "unclassified",
    "unconfirmed_self_heal",
    "program_residual",
    "cleared_self_heal",
    "repaired_self_heal",
)
SELF_HEALING = ("unconfirmed_self_heal", "cleared_self_heal", "repaired_self_heal")
CLASSIFICATION = {
    "resolve": "success",
    "unclassified": "failed",
    "unconfirmed_self_heal": "unconfirmed",
    "program_residual": "cleared_without_action",
    "cleared_self_heal": "cleared_without_action",
    "repaired_self_heal": "unconfirmed",
}


def repairs_for(behaviour: str, started: str) -> list[dict[str, Any]]:
    if behaviour in ("unconfirmed_self_heal", "cleared_self_heal"):
        return []
    if behaviour == "program_residual":
        return [{"success": False, "failure_reason": "source_residual", "started_at": started}]
    return [{"success": True, "started_at": started}]


class FakeLane:
    """A cluster with a virtual clock and a scripted SDO. ``behaviour`` picks how SDO reacts to an injection."""

    def __init__(self, behaviour: str = "resolve", *, detect_after: float = 20, fix_after: float = 60) -> None:
        self.t = 0.0
        self.behaviour = behaviour
        self.detect_after = detect_after
        self.fix_after = fix_after
        self.injected_at: float | None = None
        self.broken: str | None = None
        self.open = False
        self.docs: list[gf.OutcomeRead] = []
        self.applied: list[Action] = []
        self.human = 0
        self.revert_fails = False
        self.inject_fails = False
        self.stuck_unhealthy = False
        self.stuck_after_injection = False
        self.spec_reverts: set[Action] = set()
        self._healed_by_platform = False
        self.restorer: str | None = None  # field manager that last changed the fault's object after injection
        self.restored: tuple[str, datetime] | None = None
        self.leftover: gf.OutcomeRead | None = None  # an incident from before the injection, closing in PRECHECK
        self.leftover_closes_at = 10.0
        self.startup_until = 0.0  # SDO holds a new install's openers until it has seen it healthy

    def startup_holding(self) -> bool:
        return self.t < self.startup_until

    def clock(self) -> float:
        return self.t

    def now(self) -> datetime:
        return EPOCH + timedelta(seconds=self.t)

    def sleep(self, seconds: float) -> None:
        self.t += seconds
        self._react()

    def _react(self) -> None:
        if self.leftover is not None and self.t >= self.leftover_closes_at:
            self.docs.append(self.leftover)
            self.leftover, self.open = None, False
        if self.injected_at is None:
            return
        age = self.t - self.injected_at
        if self.behaviour == "self_heal" and age >= 15:
            self.broken = None
        if self.behaviour in CLOSING and age >= self.detect_after and (self.broken or self.behaviour in SELF_HEALING):
            self.open = True
        if self.behaviour == "unconfirmed_self_heal" and age >= 15:
            self.broken = None  # the platform restarts the pod; SDO's closure stays open and ends unconfirmed
        if self.behaviour in CLOSING and age >= self.fix_after and (self.broken or self.behaviour in SELF_HEALING):
            self.broken = None
            self.open = False
            if self.restorer and self.restored is None:
                self.restored = (self.restorer, self.now())
            stamps = lambda s: (EPOCH + timedelta(seconds=self.injected_at + s)).isoformat().replace("+00:00", "Z")  # noqa: E731
            self.docs.append(
                gf.parse_outcome(
                    {
                        "incident_id": f"inc-{len(self.docs)}",
                        "classification": CLASSIFICATION[self.behaviour],
                        "timestamps": {
                            "detected_at": stamps(self.detect_after),
                            "dispatched_at": stamps(self.detect_after + 2),
                            "mitigated_at": stamps(self.fix_after - 10),
                            "verified_at": stamps(self.fix_after),
                        },
                        "usage": {"llm_calls": 3, "input_tokens": 100, "output_tokens": 10},
                        "repair_actions": repairs_for(self.behaviour, stamps(self.fix_after - 20)),
                        **(
                            {"applied_playbooks": ["deployment-source-drift"]}
                            if self.behaviour == "program_residual"
                            else {}
                        ),
                    }
                )
            )

    def inventory(self) -> list[Workload]:
        return workloads()

    def apply(self, actions: Sequence[Action]) -> None:
        for action in actions:
            self.applied.append(action)
        if actions and actions[0] in self.spec_reverts:
            if self.revert_fails:
                raise RuntimeError("kubectl failed")
            self.broken = None
            self.open = False
            return
        if self.inject_fails:
            raise RuntimeError("kubectl failed")
        self.injected_at = self.t
        self.broken = actions[0].args[0]

    def unhealthy(self) -> list[str]:
        return (
            [f"Deployment/{NS}/web"]
            if self.broken or self.stuck_unhealthy or (self.stuck_after_injection and self.injected_at is not None)
            else []
        )

    def incident_open(self) -> bool:
        return self.open

    def outcomes(self) -> list[gf.OutcomeRead]:
        return list(self.docs)

    def human_edits(self) -> int:
        return self.human

    def restored_by(self, fault: Any, since: datetime) -> str | None:
        return self.restored[0] if self.restored and self.restored[1] >= since else None


def pre_incident(at: float) -> gf.OutcomeRead:
    """A closed incident that opened ``at`` seconds on the lane clock (negative: before the injection)."""
    stamp = (EPOCH + timedelta(seconds=at)).isoformat().replace("+00:00", "Z")
    return gf.parse_outcome(
        {
            "incident_id": "inc-pre",
            "classification": "success",
            "timestamps": {"detected_at": stamp, "verified_at": stamp},
        }
    )


def config(**overrides: float) -> gf.StreamConfig:
    base = {"detect": 120, "recover": 300, "baseline": 60, "stable": 10, "settle": 30, "poll": 5}
    return gf.StreamConfig(**{**base, **overrides})  # type: ignore[arg-type]


def spec(kind: FaultKind = FaultKind.BAD_IMAGE, name: str = "web"):
    selected = build(kind, next(w for w in workloads() if w.name == name))
    assert selected is not None
    return selected


def run(lane: FakeLane, fault=None, **overrides: float) -> gf.FaultResult:
    fault = fault or spec()
    lane.spec_reverts = set(fault.revert[:1])
    return gf.run_fault(lane, fault, config(**overrides), 0)


def test_sdo_resolves_and_the_row_records_detect_mitigate_and_turns() -> None:
    result = run(FakeLane("resolve"))
    assert result.outcome is gf.Outcome.RESOLVED
    assert not result.harness_reverted
    assert result.baseline_restored
    assert result.human_edits == 0
    assert [p for p, _ in result.phases] == [
        gf.Phase.PRECHECK,
        gf.Phase.INJECT,
        gf.Phase.DETECT,
        gf.Phase.RECOVER,
        gf.Phase.VERIFY,
        gf.Phase.DONE,
    ]
    row = gf.stream_row(result)
    assert row["time_to_detect_seconds"] == 20
    assert row["time_to_mitigate_seconds"] == 50
    assert row["time_to_resolve_seconds"] == 60
    assert row["responder_turns"] == 3
    assert row["human_edits"] == 0
    record = gf.record_for(result, config())
    assert isinstance(record.injection, Injected)
    assert record.sdo_resolution == "sdo_mitigated"
    assert record.oracle is not None
    assert record.oracle.success
    assert record.responder_tokens.input_tokens == 100
    assert record.time_to_fix_seconds is None or record.time_to_fix_seconds >= 0


def test_self_healing_fault_is_not_a_resolution() -> None:
    result = run(FakeLane("self_heal"), spec(FaultKind.POD_KILL))
    assert result.outcome is gf.Outcome.SELF_HEALED
    assert result.baseline_restored
    assert gf.stream_row(result)["responder_turns"] is None


@pytest.mark.parametrize("behaviour", ["unconfirmed_self_heal", "cleared_self_heal"])
def test_self_healed_incident_without_repair_is_a_correct_outcome(behaviour: str) -> None:
    result = run(FakeLane(behaviour), spec(FaultKind.POD_KILL))
    assert result.outcome is gf.Outcome.SELF_HEALED
    assert result.baseline_restored
    assert not result.harness_reverted
    assert result.error is None
    record = gf.record_for(result, config())
    assert record.sdo_resolution is None  # not credited to SDO
    assert record.oracle is not None
    assert record.oracle.success


def test_unconfirmed_closure_stays_unresolved_unless_the_fault_self_heals_without_repair() -> None:
    assert run(FakeLane("unconfirmed_self_heal"), spec(FaultKind.BAD_IMAGE)).outcome is gf.Outcome.UNRESOLVED
    assert run(FakeLane("repaired_self_heal"), spec(FaultKind.POD_KILL)).outcome is gf.Outcome.UNRESOLVED


def test_learned_program_repair_with_a_source_residual_is_credited_to_sdo() -> None:
    result = run(FakeLane("program_residual"), spec(FaultKind.SCALE_ZERO))
    assert result.outcome is gf.Outcome.RESOLVED
    assert gf.record_for(result, config()).sdo_resolution == "sdo_mitigated"
    assert gf.stream_row(result)["time_to_resolve_seconds"] == 60


def test_credit_needs_the_baseline_back() -> None:
    lane = FakeLane("program_residual")
    lane.stuck_after_injection = True
    result = run(lane, spec(FaultKind.SCALE_ZERO), baseline=20)
    assert result.outcome is gf.Outcome.UNRESOLVED
    assert not result.baseline_restored


def test_undetected_fault_is_reverted_by_the_harness_and_labelled() -> None:
    lane = FakeLane("silent")
    result = run(lane, detect=60)
    assert result.outcome is gf.Outcome.NOT_DETECTED
    assert result.harness_reverted
    assert result.baseline_restored
    assert lane.applied[-1] == spec().revert[0]
    record = gf.record_for(result, config())
    assert record.recovery_error
    assert record.oracle is not None
    assert not record.oracle.success


def test_failed_closure_is_unresolved_not_a_resolution() -> None:
    result = run(FakeLane("unclassified"))
    assert result.outcome is gf.Outcome.UNRESOLVED
    assert result.incident is not None
    assert result.error
    assert "failed" in result.error


def test_slow_recovery_times_out_and_the_harness_reverts() -> None:
    result = run(FakeLane("resolve", fix_after=10_000), recover=100)
    assert result.outcome is gf.Outcome.UNRESOLVED
    assert result.harness_reverted
    assert result.baseline_restored


def test_unhealthy_baseline_before_injection_is_an_infra_error_without_injecting() -> None:
    lane = FakeLane("resolve")
    lane.stuck_unhealthy = True
    result = run(lane, baseline=20)
    assert result.outcome is gf.Outcome.INFRA_ERROR
    assert result.infra_stage == "pre_injection"
    assert lane.applied == []
    assert isinstance(gf.record_for(result, config()).injection, NotInjected)


def test_injection_waits_for_the_start_up_period() -> None:
    """A controller still holding a new install's openers would hide the fault; the baseline gate waits it out."""
    lane = FakeLane("resolve")
    lane.startup_until = 30
    result = run(lane, baseline=60)
    assert result.injected is not None
    assert lane.injected_at is not None
    assert lane.injected_at >= 30
    assert result.outcome is gf.Outcome.RESOLVED


@pytest.mark.parametrize(
    ("state", "holding"),
    [
        ({}, False),
        ({"startup": {"began_at": "2026-01-01T00:00:00Z", "bound_at": "2026-01-01T00:30:00Z"}}, True),
        ({"startup": {"beganAt": "2026-01-01T00:00:00Z"}}, True),
        ({"startup": {"began_at": "t", "established_at": "t"}}, False),
        ({"startup": {"began_at": "t", "bound_reached_at": "t"}}, False),
        ({"startup": {"skipped_reason": "restored-state"}}, False),
    ],
)
def test_startup_holding_reads_the_controller_state(state: dict[str, Any], holding: bool) -> None:
    assert gf.startup_holding(state) is holding


def test_open_incident_before_injection_blocks_the_fault() -> None:
    lane = FakeLane("resolve")
    lane.open = True
    assert run(lane, baseline=20).infra_stage == "pre_injection"


def test_incident_that_closes_during_precheck_is_not_the_faults_incident() -> None:
    """Live4: a spontaneous incident closed while PRECHECK waited and was taken as the fault's detection."""
    lane = FakeLane("resolve")
    lane.open = True
    lane.leftover = pre_incident(-30)
    result = run(lane)
    assert result.outcome is gf.Outcome.RESOLVED
    assert result.incident is not None
    assert result.incident.incident_id == "inc-1"
    assert result.injected is not None
    assert result.detected_at is not None
    assert result.detected_at >= result.injected[0]
    gf.record_for(result, config())  # must not raise incident_predates_injection


def test_outcome_that_predates_injection_is_set_aside_not_attributed() -> None:
    """An outcome document that lands after the snapshot but opened before the injection is not this fault's."""
    lane = FakeLane("resolve")
    original = lane.apply

    def apply(actions: Sequence[Action]) -> None:
        original(actions)
        if not lane.docs:
            lane.docs.append(pre_incident(-5))

    lane.apply = apply  # type: ignore[method-assign]
    result = run(lane)
    assert result.incident is not None
    assert result.incident.incident_id == "inc-1"
    assert result.preexisting_incidents == ["inc-pre"]
    assert gf.record_for(result, config()).injection_evidence["preexisting_incidents"] == ["inc-pre"]


def test_incident_still_open_after_the_settle_bound_is_a_typed_row_not_an_injection() -> None:
    lane = FakeLane("resolve")
    lane.open = True
    result = run(lane, baseline=20)
    assert result.outcome is gf.Outcome.INFRA_ERROR
    assert result.infra_stage == "pre_injection"
    assert result.error is not None
    assert result.error.startswith("incident_open_before_injection")
    assert lane.applied == []
    assert gf.record_for(result, config()).infra_error == "incident_open_before_injection"


def test_next_fault_waits_for_the_previous_incident_and_is_not_credited_with_it(tmp_path: Path) -> None:
    lane = FakeLane("resolve")
    first, second = (spec(FaultKind.BAD_IMAGE, "web"), spec(FaultKind.BAD_IMAGE, "worker"))
    lane.spec_reverts = {first.revert[0], second.revert[0]}
    lane.docs.append(pre_incident(-100))  # the previous fault's incident, already closed
    lane.open = True  # ... but a late one is still open when the next fault starts
    lane.leftover = pre_incident(-20)
    summary = gf.run_stream(lane, [first, second], config(), tmp_path)
    ids = [r["incident_id"] for r in summary["faults_detail"]]
    assert "inc-pre" not in ids
    assert len(set(ids)) == 2
    assert all(r["outcome"] == "resolved_by_sdo" for r in summary["faults_detail"])


def test_sdo_restore_without_an_incident_action_is_credited_to_sdo() -> None:
    """Live4 service_selector: the controller's accepted-source apply restored the Service; no responder action."""
    lane = FakeLane("cleared_self_heal")
    lane.restorer = "sdo-deployer"
    result = run(lane, spec(FaultKind.SERVICE_SELECTOR))
    assert result.outcome is gf.Outcome.RESOLVED
    assert result.restored_by == "sdo-deployer"
    record = gf.record_for(result, config())
    assert record.sdo_resolution == "sdo_mitigated"
    assert record.injection_evidence["restored_by"] == "sdo-deployer"


def test_external_restore_is_not_credited_to_sdo() -> None:
    lane = FakeLane("cleared_self_heal")
    lane.restorer = "kubectl-patch"
    result = run(lane, spec(FaultKind.SERVICE_SELECTOR))
    assert result.outcome is gf.Outcome.CLEARED
    assert gf.record_for(result, config()).sdo_resolution is None


def test_restore_before_the_injection_is_not_credited_to_sdo() -> None:
    lane = FakeLane("cleared_self_heal")
    lane.restored = ("sdo-deployer", EPOCH - timedelta(seconds=500))
    assert run(lane, spec(FaultKind.SERVICE_SELECTOR)).outcome is gf.Outcome.CLEARED


def test_selector_and_workload_faults_name_the_object_to_inspect() -> None:
    web = next(w for w in workloads() if w.name == "web")
    selector = build(FaultKind.SERVICE_SELECTOR, web)
    scale = build(FaultKind.SCALE_ZERO, web)
    kill = build(FaultKind.POD_KILL, web)
    assert selector is not None
    assert scale is not None
    assert kill is not None
    assert selector.target == "service/web"
    assert scale.target == web.ref
    assert kill.target is None


def test_kubectl_restorer_reads_sdo_managed_fields_after_the_injection() -> None:
    def ops_with(entries: list[dict[str, Any]]) -> gf.KubectlFaultOps:
        ops = gf.KubectlFaultOps.__new__(gf.KubectlFaultOps)
        ops.namespace = NS

        class Done:
            stdout = json.dumps({"metadata": {"managedFields": entries}})

        ops._kubectl = lambda *a, **k: Done()  # type: ignore[method-assign, assignment]
        return ops

    fault = spec(FaultKind.SERVICE_SELECTOR)
    since = EPOCH + timedelta(seconds=100, microseconds=500_000)
    late = (EPOCH + timedelta(seconds=100)).isoformat().replace("+00:00", "Z")  # same second as the injection
    early = (EPOCH + timedelta(seconds=50)).isoformat().replace("+00:00", "Z")
    assert ops_with([{"manager": "sdo-deployer", "time": late}]).restored_by(fault, since) == "sdo-deployer"
    assert ops_with([{"manager": "sdo-deployer", "time": early}]).restored_by(fault, since) is None
    assert ops_with([{"manager": "kubectl-patch", "time": late}]).restored_by(fault, since) is None


def test_failed_injection_is_recorded_not_raised() -> None:
    lane = FakeLane("resolve")
    lane.inject_fails = True
    result = run(lane)
    assert result.outcome is gf.Outcome.INFRA_ERROR
    assert result.infra_stage == "injection"


def test_human_edits_are_counted_from_the_accepted_history() -> None:
    lane = FakeLane("resolve")
    original_react = lane._react

    def react() -> None:
        original_react()
        if lane.docs:
            lane.human = 2

    lane._react = react  # type: ignore[method-assign]
    assert run(lane).human_edits == 2


def test_stream_records_every_fault_in_the_toy_format_and_stops_when_baseline_is_lost(tmp_path: Path) -> None:
    lane = FakeLane("resolve")
    faults = select_faults(workloads(), 3, seed=1).faults
    lane.spec_reverts = {f.revert[0] for f in faults if f.revert}
    summary = gf.run_stream(lane, faults, config(), tmp_path)
    rows = load_records(tmp_path / "incidents.jsonl")
    assert [r.problem_id for r in rows] == [f.problem_id for f in faults[: len(rows)]]
    assert (tmp_path / "incidents.csv").exists()
    stream = json.loads((tmp_path / "fault-stream.json").read_text())
    assert stream["human_edits"] == 0
    assert stream["stopped"] is None
    assert summary["resolved_by_sdo"] + summary["outcomes"]["self_healed"] == 3

    broken = FakeLane("silent")
    broken.revert_fails = True
    broken.spec_reverts = {f.revert[0] for f in faults if f.revert}
    stopped = gf.run_stream(broken, faults, config(detect=30), tmp_path / "second")
    assert stopped["faults"] == 1
    assert stopped["stopped"]
    assert "baseline not restored" in stopped["stopped"]


def test_summary_counts_resolution_rate_and_mttr_over_injected_faults() -> None:
    rows = [
        {"outcome": "resolved_by_sdo", "time_to_resolve_seconds": 60.0, "human_edits": 0, "harness_reverted": False},
        {"outcome": "resolved_by_sdo", "time_to_resolve_seconds": 100.0, "human_edits": 0, "harness_reverted": False},
        {"outcome": "not_detected", "time_to_resolve_seconds": None, "human_edits": 0, "harness_reverted": True},
        {"outcome": "infra_error", "time_to_resolve_seconds": None, "human_edits": 0, "harness_reverted": False},
    ]
    summary = gf.summarize_stream(rows, {}, None)
    assert summary["injected"] == 3
    assert summary["mttr_seconds"] == 80.0
    assert summary["resolution_rate"] == pytest.approx(2 / 3)
    assert summary["harness_reverts"] == 1


def test_outcome_documents_parse_in_snake_and_camel_case() -> None:
    snake = gf.parse_outcome(
        {"incident_id": "a", "timestamps": {"verified_at": "2026-01-01T00:00:00Z"}, "usage": {"llm_calls": 2}}
    )
    camel = gf.parse_outcome(
        {"incidentId": "a", "timestamps": {"verifiedAt": "2026-01-01T00:00:00Z"}, "usage": {"llmCalls": 2}}
    )
    assert snake.verified_at == camel.verified_at
    assert snake.llm_calls == camel.llm_calls == 2
    with pytest.raises(ValueError, match="incident_id"):
        gf.parse_outcome({})


def test_unready_workloads_ignore_owner_managed_and_count_stale_rollouts() -> None:
    listing = {
        "items": [
            {**_workload("Deployment", "ok"), "status": {"readyReplicas": 1, "updatedReplicas": 1}},
            {**_workload("Deployment", "stale"), "status": {"readyReplicas": 1, "updatedReplicas": 0}},
            {**_workload("StatefulSet", "down"), "status": {}},
        ]
    }
    assert gf.workload_unready(listing) == [f"Deployment/{NS}/stale", f"StatefulSet/{NS}/down"]


def test_dry_run_prints_the_plan_without_a_lane(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    path = tmp_path / "inventory.json"
    path.write_text(json.dumps(inventory()))
    from argparse import ArgumentParser

    parser = ArgumentParser()
    gf.add_faults_parser(parser.add_subparsers(dest="gitlab_command"))
    args = parser.parse_args(
        ["faults", "--run-dir", str(tmp_path), "--count", "4", "--seed", "2", "--dry-run", "--inventory", str(path)]
    )
    assert args.handler(args) == 0
    report = json.loads(capsys.readouterr().out)
    assert len(report["faults"]) == 4
    assert all(f["inject"] and "workload" in f for f in report["faults"])
    with pytest.raises(ValueError, match="known kinds"):
        gf.parse_kinds(["nope"])


def test_fastloop_cli_registers_gitlab_faults(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    from benchmarks.sregym.fastloop.cli import build_parser

    path = tmp_path / "inventory.json"
    path.write_text(json.dumps(inventory()))
    args = build_parser().parse_args(
        ["gitlab", "faults", "--run-dir", str(tmp_path), "--count", "2", "--dry-run", "--inventory", str(path)]
    )
    assert args.handler(args) == 0
    assert len(json.loads(capsys.readouterr().out)["faults"]) == 2
