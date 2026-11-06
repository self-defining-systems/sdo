"""``fastloop gitlab faults``: a stream of generic faults against a steady lane, one fault at a time.

Each fault walks one state machine: PRECHECK (healthy baseline, no open incident) -> INJECT -> DETECT (SDO opens an
incident, or the platform self-heals) -> RECOVER (SDO's verified closure and a healthy app) -> VERIFY (healthy baseline
for a stable window) -> RECORD. The harness never repairs for SDO: if detection or recovery times out it reverts the
fault itself so the next one starts clean, and the row says so (``harness_reverted``); the stream stops when the
baseline cannot be restored. Rows are ``IncidentRecord`` (the toy/SREGym recording format, ``incidents.jsonl`` plus
CSV), so ``fastloop summary`` and analyze-experiment read them; the stream totals, including human edits (accepted
commits a human or the harness authored; SDO's own are not counted), are in ``fault-stream.json``.

Targets come from the lane's workload inventory (``generic_faults``), never from hard-coded names. Everything the
state machine touches is behind ``FaultOps``, so it runs against a fake cluster without a lane.
"""

from __future__ import annotations

import json
import statistics
import subprocess
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any, Protocol

from benchmarks.sregym.adapter import WaitKind, wait_for
from benchmarks.sregym.fastloop.generic_faults import (
    DEFAULT_KINDS,
    Action,
    FaultKind,
    FaultSpec,
    Workload,
    parse_inventory,
    select_faults,
)
from benchmarks.sregym.fastloop.records import (
    IncidentRecord,
    Injected,
    NotInjected,
    OracleVerdict,
    TokenCounts,
    append_record,
)
from benchmarks.sregym.protocol.injection import FaultComponent

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from pathlib import Path

    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

STREAM_FILENAME = "fault-stream.json"
INCIDENTS_FILENAME = "incidents.jsonl"
MODEL = "gpt-6-luna"
#: ``SDO-Actor`` trailers of commits a person or the harness authored; a commit with no trailer counts too.
HUMAN_ACTORS = frozenset({"", "human", "upstream"})
CONTROLLER_REPOSITORY = "/workspace/application"
STATE_CONFIGMAP = "sdo-controller-state"
CONTROLLER_SELECTOR = "job-name=sdo-controller-run"


class Phase(str, Enum):
    PRECHECK = "precheck"
    INJECT = "inject"
    DETECT = "detect"
    RECOVER = "recover"
    VERIFY = "verify"
    DONE = "done"


class Outcome(str, Enum):
    RESOLVED = "resolved_by_sdo"
    CLEARED = "cleared_without_sdo_action"
    SELF_HEALED = "self_healed"
    NOT_DETECTED = "not_detected"
    UNRESOLVED = "unresolved"
    INFRA_ERROR = "infra_error"


@dataclass(frozen=True)
class OutcomeRead:
    """What SDO's durable outcome document says about one incident (``.sdo/outcomes/<hash>.json``)."""

    incident_id: str
    classification: str
    detected_at: datetime | None
    dispatched_at: datetime | None
    mitigated_at: datetime | None
    verified_at: datetime | None
    llm_calls: int
    usage: dict[str, Any]
    repair_actions: int
    first_repair_at: datetime | None = None


def _time(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _field(document: dict[str, Any], snake: str) -> Any:
    camel = "".join(part.title() if i else part for i, part in enumerate(snake.split("_")))
    return document.get(snake, document.get(camel))


def parse_outcome(document: dict[str, Any]) -> OutcomeRead:
    """Parse an outcome document; ProtoJSON may spell fields in snake or camel case."""
    incident_id = _field(document, "incident_id")
    if not isinstance(incident_id, str) or not incident_id:
        raise ValueError("outcome document has no incident_id")
    stamps = _field(document, "timestamps") or {}
    usage = _field(document, "usage") or _field(document, "responder_usage") or {}
    repairs = _field(document, "repair_actions") or []
    started = [_time(_field(r, "started_at")) for r in repairs if isinstance(r, dict) and r.get("success") is True]
    return OutcomeRead(
        incident_id=incident_id,
        classification=str(_field(document, "classification") or ""),
        detected_at=_time(_field(stamps, "detected_at")),
        dispatched_at=_time(_field(stamps, "dispatched_at")),
        mitigated_at=_time(_field(stamps, "mitigated_at")),
        verified_at=_time(_field(stamps, "verified_at")),
        llm_calls=int(_field(usage, "llm_calls") or 0),
        usage=dict(usage) if isinstance(usage, dict) else {},
        repair_actions=len(repairs),
        first_repair_at=min((t for t in started if t), default=None),
    )


class FaultOps(Protocol):
    """The clock and the lane's reads and writes; fakes implement it for tests."""

    def clock(self) -> float: ...
    def now(self) -> datetime: ...
    def sleep(self, seconds: float) -> None: ...
    def inventory(self) -> list[Workload]: ...
    def apply(self, actions: Sequence[Action]) -> None: ...
    def unhealthy(self) -> list[str]: ...
    def incident_open(self) -> bool: ...
    def outcomes(self) -> list[OutcomeRead]: ...
    def human_edits(self) -> int: ...


@dataclass(frozen=True)
class StreamConfig:
    """Bounds in seconds. Defaults are the shared eval budgets; none is a new wait."""

    detect: float
    recover: float
    baseline: float
    stable: float
    settle: float
    poll: float
    run_id: str = "gitlab-faults"
    model: str = MODEL

    def __post_init__(self) -> None:
        for name in ("detect", "recover", "baseline", "stable", "settle", "poll"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if not self.run_id:
            raise ValueError("run_id must not be empty")

    @classmethod
    def standard(cls, run_id: str = "gitlab-faults") -> StreamConfig:
        return cls(
            detect=wait_for(WaitKind.OPERATION),
            recover=wait_for(WaitKind.INCIDENT_VERIFICATION),
            baseline=wait_for(WaitKind.OPERATION),
            stable=wait_for(WaitKind.MINUTE),
            settle=wait_for(WaitKind.MINUTE, 2),
            poll=wait_for(WaitKind.POLL),
            run_id=run_id,
        )


@dataclass
class FaultResult:
    """Everything one fault produced; the record and the stream totals derive from it."""

    index: int
    spec: FaultSpec
    outcome: Outcome = Outcome.INFRA_ERROR
    phases: list[tuple[Phase, float]] = field(default_factory=list)
    injected: tuple[datetime, datetime] | None = None
    detected_at: datetime | None = None
    observed_detect_at: datetime | None = None
    resolved_at: datetime | None = None
    incident: OutcomeRead | None = None
    ever_unhealthy: bool = False
    harness_reverted: bool = False
    baseline_restored: bool = False
    human_edits: int = 0
    error: str | None = None
    infra_stage: str | None = None
    wall_seconds: float = 0.0


def _stable_healthy(ops: FaultOps, window: float, poll: float) -> bool:
    """Healthy at every poll across ``window`` seconds."""
    end = ops.clock() + window
    while True:
        if ops.unhealthy() or ops.incident_open():
            return False
        if ops.clock() >= end:
            return True
        ops.sleep(poll)


def _wait_baseline(ops: FaultOps, config: StreamConfig) -> bool:
    """Wait for a healthy, incident-free app that stays so for the stable window."""
    deadline = ops.clock() + config.baseline
    while True:
        if _stable_healthy(ops, config.stable, config.poll):
            return True
        if ops.clock() >= deadline:
            return False
        ops.sleep(config.poll)


def _new_outcome(ops: FaultOps, known: set[str]) -> OutcomeRead | None:
    fresh = [o for o in ops.outcomes() if o.incident_id not in known]
    return (
        min(fresh, key=lambda o: o.verified_at or o.detected_at or datetime.max.replace(tzinfo=timezone.utc))
        if fresh
        else None
    )


def run_fault(ops: FaultOps, spec: FaultSpec, config: StreamConfig, index: int) -> FaultResult:
    """Drive one fault through the state machine. Never raises for a lane condition; it is recorded."""
    result = FaultResult(index, spec)
    started = ops.clock()

    def enter(phase: Phase) -> None:
        result.phases.append((phase, ops.clock() - started))

    def finish(outcome: Outcome) -> FaultResult:
        result.outcome = outcome
        enter(Phase.DONE)
        result.wall_seconds = ops.clock() - started
        return result

    def revert() -> bool:
        try:
            ops.apply(spec.revert)
        except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
            result.error = f"{result.error or ''} harness revert failed: {exc}".strip()
            return False
        result.harness_reverted = True
        return True

    enter(Phase.PRECHECK)
    edits_before = ops.human_edits()
    known = {o.incident_id for o in ops.outcomes()}
    if not _wait_baseline(ops, config):
        result.infra_stage = "pre_injection"
        result.error = "baseline_not_healthy: the app was not healthy and incident-free before injection"
        return finish(Outcome.INFRA_ERROR)

    enter(Phase.INJECT)
    t0 = ops.now()
    try:
        ops.apply(spec.inject)
    except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
        result.infra_stage = "injection"
        result.error = f"injection failed: {exc}"
        revert()
        return finish(Outcome.INFRA_ERROR)
    result.injected = (t0, ops.now())

    enter(Phase.DETECT)
    deadline = ops.clock() + config.detect
    healed_since: float | None = None
    outcome: OutcomeRead | None = None
    detected = False
    while True:
        outcome = _new_outcome(ops, known)
        if outcome is not None or ops.incident_open():
            detected = True
            result.observed_detect_at = ops.now()
            break
        if ops.unhealthy():
            result.ever_unhealthy = True
            healed_since = None
        elif result.ever_unhealthy or spec.self_heals:
            healed_since = healed_since if healed_since is not None else ops.clock()
            if ops.clock() - healed_since >= config.settle:
                result.baseline_restored = True
                return finish(Outcome.SELF_HEALED)
        if ops.clock() >= deadline:
            break
        ops.sleep(config.poll)
    if not detected:
        result.error = f"no incident within {config.detect:g}s (workload ever unhealthy: {result.ever_unhealthy})"
        revert()
        return _verify(ops, config, result, finish, Outcome.NOT_DETECTED)

    enter(Phase.RECOVER)
    deadline = ops.clock() + config.recover
    while True:
        outcome = outcome or _new_outcome(ops, known)
        if outcome is not None and not ops.incident_open() and not ops.unhealthy():
            break
        if ops.clock() >= deadline:
            result.error = f"no verified recovery within {config.recover:g}s after detection"
            revert()
            result.incident = outcome
            return _verify(ops, config, result, finish, Outcome.UNRESOLVED)
        ops.sleep(config.poll)
    result.incident = outcome
    result.detected_at = outcome.detected_at or result.observed_detect_at
    result.resolved_at = outcome.verified_at or ops.now()
    result.human_edits = ops.human_edits() - edits_before
    if outcome.classification == "success":
        label = Outcome.RESOLVED
    elif outcome.classification == "cleared_without_action":
        label = Outcome.CLEARED
    else:
        result.error = f"incident closed as {outcome.classification or 'unclassified'}"
        label = Outcome.UNRESOLVED
        if ops.unhealthy():
            revert()
    return _verify(ops, config, result, finish, label)


def _verify(
    ops: FaultOps, config: StreamConfig, result: FaultResult, finish: Callable[[Outcome], FaultResult], label: Outcome
) -> FaultResult:
    result.phases.append((Phase.VERIFY, 0.0))
    result.baseline_restored = _wait_baseline(ops, config)
    if not result.baseline_restored:
        result.error = f"{result.error or ''} baseline_not_restored".strip()
    result.human_edits = max(result.human_edits, 0)
    return finish(label)


def _seconds(start: datetime | None, end: datetime | None) -> float | None:
    return (end - start).total_seconds() if start and end else None


def record_for(result: FaultResult, config: StreamConfig) -> IncidentRecord:
    """The ``IncidentRecord`` of a fault: injection event, SDO's incident timings and outcome, health verdict."""
    spec = result.spec
    incident = result.incident
    base: dict[str, Any] = {
        "run_id": config.run_id,
        "index": result.index,
        "agent": "sdo",
        "problem_id": spec.problem_id,
        "model": config.model,
        "fault_components": [
            FaultComponent(
                fault=0, incident=result.index, problem_id=spec.problem_id, injection_order=0, identity=spec.identity
            )
        ],
        "injection_evidence": {
            "generic_fault": spec.identity,
            "phases": [[phase.value, round(at, 3)] for phase, at in result.phases],
            "stream_outcome": result.outcome.value,
            "ever_unhealthy": result.ever_unhealthy,
            "harness_reverted": result.harness_reverted,
            "human_edits": result.human_edits,
        },
        "incident_wall_seconds": round(result.wall_seconds, 3),
        "error": result.error,
    }
    if result.injected is None:
        base["injection"] = NotInjected(stage=result.infra_stage or "pre_injection")  # type: ignore[arg-type]
        base["infra_error"] = (result.error or "infra_error").split(":", 1)[0]
        return IncidentRecord(**base)
    base["injection"] = Injected(started_at=result.injected[0], finished_at=result.injected[1])
    base["detected_at"] = result.detected_at or result.observed_detect_at
    if incident is not None:
        base.update(
            incident_id=incident.incident_id,
            dispatched_at=incident.dispatched_at,
            responder_started_at=incident.dispatched_at,
            mitigation_applied_at=incident.mitigated_at,
            first_repair_action_at=incident.first_repair_at,
            resolved_at=result.resolved_at,
            sdo_outcome_classification=incident.classification or None,
            sdo_resolution="sdo_mitigated" if result.outcome is Outcome.RESOLVED else None,
            responder_tokens=TokenCounts.from_usage(incident.usage),
        )
    if result.harness_reverted:
        base["recovery_error"] = "the harness reverted the fault; SDO did not recover the app"
    healthy = result.outcome in (Outcome.RESOLVED, Outcome.CLEARED, Outcome.SELF_HEALED) and result.baseline_restored
    base["oracle"] = OracleVerdict(
        kind="health-check",
        success=healthy,
        details={"outcome": result.outcome.value, "baseline_restored": result.baseline_restored},
    )
    return IncidentRecord(**base)


def stream_row(result: FaultResult) -> dict[str, Any]:
    """The per-fault line of ``fault-stream.json``: the measures the GitLab plan asks for."""
    incident = result.incident
    t0 = result.injected[0] if result.injected else None
    detected = result.detected_at or result.observed_detect_at
    return {
        "index": result.index,
        "problem_id": result.spec.problem_id,
        "kind": result.spec.kind.value,
        "workload": result.spec.workload.id,
        "outcome": result.outcome.value,
        "time_to_detect_seconds": _seconds(t0, detected),
        "time_to_mitigate_seconds": _seconds(t0, incident.mitigated_at) if incident else None,
        "time_to_resolve_seconds": _seconds(t0, result.resolved_at),
        "responder_turns": incident.llm_calls if incident else None,
        "incident_id": incident.incident_id if incident else None,
        "classification": incident.classification if incident else None,
        "human_edits": result.human_edits,
        "harness_reverted": result.harness_reverted,
        "baseline_restored": result.baseline_restored,
        "error": result.error,
    }


def summarize_stream(rows: list[dict[str, Any]], skipped: dict[str, str], stopped: str | None) -> dict[str, Any]:
    """Fault resolution rate, MTTR and human edits over the faults that were injected."""
    injected = [r for r in rows if r["outcome"] != Outcome.INFRA_ERROR.value]
    resolved = [r for r in injected if r["outcome"] == Outcome.RESOLVED.value]
    times = [r["time_to_resolve_seconds"] for r in resolved if r["time_to_resolve_seconds"] is not None]
    return {
        "faults": len(rows),
        "injected": len(injected),
        "resolved_by_sdo": len(resolved),
        "resolution_rate": len(resolved) / len(injected) if injected else None,
        "mttr_seconds": statistics.mean(times) if times else None,
        "median_ttr_seconds": statistics.median(times) if times else None,
        "outcomes": {o.value: sum(r["outcome"] == o.value for r in rows) for o in Outcome},
        "human_edits": sum(r["human_edits"] for r in rows),
        "harness_reverts": sum(r["harness_reverted"] for r in rows),
        "skipped_kinds": skipped,
        "stopped": stopped,
        "faults_detail": rows,
    }


def run_stream(
    ops: FaultOps,
    specs: Sequence[FaultSpec],
    config: StreamConfig,
    results_dir: Path,
    skipped: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Run ``specs`` in order, recording each row as it completes; stop when the baseline cannot be restored."""
    rows: list[dict[str, Any]] = []
    stopped: str | None = None
    for index, spec in enumerate(specs):
        result = run_fault(ops, spec, config, index)
        append_record(results_dir / INCIDENTS_FILENAME, record_for(result, config))
        rows.append(stream_row(result))
        if result.outcome is Outcome.INFRA_ERROR and result.infra_stage == "pre_injection":
            stopped = f"fault {index}: {result.error}"
        elif not result.baseline_restored:
            stopped = f"fault {index} ({spec.problem_id}): baseline not restored"
        summary = summarize_stream(rows, skipped or {}, stopped)
        (results_dir / STREAM_FILENAME).write_text(json.dumps(summary, indent=2, default=str) + "\n", encoding="utf-8")
        if stopped:
            break
    return summarize_stream(rows, skipped or {}, stopped)


# ---------------------------------------------------------------------------------------------------------------
# The live edge: kubectl reads and writes against the lane. Untested without a cluster; every read fails loudly.


def workload_unready(listing: dict[str, Any]) -> list[str]:
    """Workload IDs whose ready replicas differ from the desired count in ``kubectl get deployments,statefulsets``."""
    bad = []
    for item in listing.get("items", []):
        if item.get("kind") not in ("Deployment", "StatefulSet") or item["metadata"].get("ownerReferences"):
            continue
        want = int(item["spec"].get("replicas", 1))
        status = item.get("status", {})
        if int(status.get("readyReplicas", 0)) != want or int(status.get("updatedReplicas", 0)) != want:
            bad.append(f"{item['kind']}/{item['metadata']['namespace']}/{item['metadata']['name']}")
    return sorted(bad)


class KubectlFaultOps:
    """``FaultOps`` over the lane kubeconfig, the application namespace and the ``<namespace>-sdo`` control one."""

    def __init__(self, environment: FastloopEnvironment, *, plan_ids: Sequence[str] | None = None) -> None:
        self.environment = environment
        self.namespace = environment.namespace
        self.control = environment.namespace + "-sdo"
        self.plan_ids = set(plan_ids) if plan_ids is not None else None

    def clock(self) -> float:
        return time.monotonic()

    def now(self) -> datetime:
        return datetime.now(timezone.utc)

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)

    def _kubectl(
        self, namespace: str, *args: str, stdin: str | None = None, check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["kubectl", "--kubeconfig", str(self.environment.kubeconfig), "-n", namespace, *args],
            input=stdin,
            capture_output=True,
            text=True,
            timeout=wait_for(WaitKind.SLOW_COMMAND),
            check=check,
        )

    def _listing(self) -> dict[str, Any]:
        return json.loads(
            self._kubectl(self.namespace, "get", "deployments,statefulsets,services", "-o", "json").stdout
        )

    def inventory(self) -> list[Workload]:
        return parse_inventory(self._listing(), plan_ids=self.plan_ids)

    def apply(self, actions: Sequence[Action]) -> None:
        for action in actions:
            self._kubectl(self.namespace, *action.args, stdin=action.stdin)

    def unhealthy(self) -> list[str]:
        bad = workload_unready(self._listing())
        return [b for b in bad if self.plan_ids is None or b in self.plan_ids]

    def incident_open(self) -> bool:
        state = json.loads(self._kubectl(self.control, "get", "configmap", STATE_CONFIGMAP, "-o", "json").stdout)
        return bool(json.loads(state["data"]["runtime-state.json"]).get("incident_open"))

    def _controller(self) -> str:
        pods = json.loads(self._kubectl(self.control, "get", "pods", "-l", CONTROLLER_SELECTOR, "-o", "json").stdout)
        for pod in pods["items"]:
            if pod.get("status", {}).get("phase") == "Running" and not pod["metadata"].get("deletionTimestamp"):
                return str(pod["metadata"]["name"])
        raise RuntimeError("no running controller pod")

    def _git(self, *args: str) -> str:
        return self._kubectl(
            self.control,
            "exec",
            self._controller(),
            "-c",
            "controller",
            "--",
            "git",
            "-C",
            CONTROLLER_REPOSITORY,
            *args,
        ).stdout

    def outcomes(self) -> list[OutcomeRead]:
        tree = self._git("ls-tree", "--name-only", "HEAD", ".sdo/outcomes/")
        return [parse_outcome(json.loads(self._git("show", f"HEAD:{path}"))) for path in tree.split()]

    def human_edits(self) -> int:
        from benchmarks.sregym.fastloop.toy_scenarios import ACCEPTED_LOG_ARGS, parse_accepted_log

        commits = parse_accepted_log(self._git(*ACCEPTED_LOG_ARGS))
        return sum(1 for c in commits if c.trailers.get("SDO-Actor", "") in HUMAN_ACTORS)


# ---------------------------------------------------------------------------------------------------------------
# CLI


def parse_kinds(names: Sequence[str] | None) -> tuple[FaultKind, ...]:
    if not names:
        return DEFAULT_KINDS
    try:
        return tuple(FaultKind(name) for name in names)
    except ValueError as exc:
        raise ValueError(f"{exc}; known kinds: {', '.join(k.value for k in FaultKind)}") from exc


def plan_report(selection: Any) -> dict[str, Any]:
    return {
        "faults": [
            {
                "problem_id": s.problem_id,
                "kind": s.kind.value,
                "workload": s.workload.id,
                "self_heals": s.self_heals,
                "inject": [list(a.args) for a in s.inject],
                "revert": [list(a.args) for a in s.revert],
            }
            for s in selection.faults
        ],
        "skipped_kinds": selection.skipped,
    }


def add_faults_parser(gitlab_commands: Any) -> None:
    """Register ``faults`` on the ``fastloop gitlab`` subparsers."""
    from pathlib import Path

    faults = gitlab_commands.add_parser("faults", help="run a stream of generic faults against a steady gitlab lane")
    faults.add_argument("--run-dir", type=Path, required=True, help="the lane directory (fastloop.json)")
    faults.add_argument("--count", type=int, default=len(DEFAULT_KINDS))
    faults.add_argument("--seed", type=int, default=0)
    faults.add_argument("--kinds", nargs="*", help=f"fault kinds, default all of {[k.value for k in DEFAULT_KINDS]}")
    faults.add_argument("--exclude", nargs="*", default=[], help="workload names or IDs that must stay untouched")
    faults.add_argument("--plan", type=Path, help="deployment plan JSON; restricts targets to its expected workloads")
    faults.add_argument("--inventory", type=Path, help="with --dry-run: read this kubectl listing instead of the lane")
    faults.add_argument("--dry-run", action="store_true", help="print the selected faults and actions; change nothing")
    faults.add_argument("--run-id", default="gitlab-faults")
    faults.add_argument("--detect-seconds", type=float)
    faults.add_argument("--recover-seconds", type=float)
    faults.set_defaults(handler=faults_command)


def _plan_ids(path: Path | None) -> list[str] | None:
    if path is None:
        return None
    plan = json.loads(path.read_text(encoding="utf-8"))
    ids = plan.get("expected_workload_ids") or plan.get("expectedWorkloadIds")
    if not ids:
        raise ValueError(f"{path} has no expected_workload_ids")
    return list(ids)


def faults_command(args: Any) -> int:
    from dataclasses import replace

    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

    plan_ids = _plan_ids(args.plan)
    kinds = parse_kinds(args.kinds)
    if args.dry_run and args.inventory is not None:
        workloads = parse_inventory(json.loads(args.inventory.read_text(encoding="utf-8")), plan_ids=plan_ids)
        selection = select_faults(workloads, args.count, seed=args.seed, kinds=kinds, exclude=args.exclude)
        print(json.dumps(plan_report(selection), indent=2))
        return 0
    environment = FastloopEnvironment.load(args.run_dir.resolve())
    if environment.app != "gitlab":
        raise ValueError(f"{args.run_dir} is a {environment.app} lane; gitlab faults need a gitlab lane")
    ops = KubectlFaultOps(environment, plan_ids=plan_ids)
    selection = select_faults(ops.inventory(), args.count, seed=args.seed, kinds=kinds, exclude=args.exclude)
    if args.dry_run:
        print(json.dumps(plan_report(selection), indent=2))
        return 0
    config = StreamConfig.standard(args.run_id)
    if args.detect_seconds:
        config = replace(config, detect=args.detect_seconds)
    if args.recover_seconds:
        config = replace(config, recover=args.recover_seconds)
    results = environment.run_dir / "results" / args.run_id
    summary = run_stream(ops, selection.faults, config, results, selection.skipped)
    print(json.dumps({k: v for k, v in summary.items() if k != "faults_detail"}, indent=2, default=str))
    return 1 if summary["stopped"] else 0


def main(argv: Sequence[str] | None = None) -> int:
    """``python -m benchmarks.sregym.fastloop.gitlab_faults faults ...``; ``fastloop gitlab faults`` calls the same."""
    import argparse

    parser = argparse.ArgumentParser(prog="gitlab_faults")
    add_faults_parser(parser.add_subparsers(dest="gitlab_command", required=True))
    args = parser.parse_args(argv)
    return int(args.handler(args))


if __name__ == "__main__":
    raise SystemExit(main())
