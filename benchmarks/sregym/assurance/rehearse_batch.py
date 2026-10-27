"""Batch scripted rehearsal over every planned problem: find plumbing bugs in batches, not one live row at a time.

    uv run python -m benchmarks.sregym.assurance.rehearse_batch --code-root <freeze> --tag <tag> --image-tag <tag>

Reads the Phase 4 plan (every problem, its fault classes, and the object kinds each fault touches), then runs
every stage for every problem without stopping at the first failure:

* ``catalog``: the plan resolves the problem's fault classes and footprint kinds;
* ``rbac``: the installed ``sdo-controller`` may read and patch (and delete, for kinds repairs remove) every
  kind the problem's faults touch;
* ``vehicle``: the problem's fault classes that a toy fault reproduces (target port, image, readiness probe,
  network policy) run through the real controller, broker, judge refresh and strict receipts with scripted
  workers (``rehearse_harness`` scenarios: first encounter, then a second incident on a sibling workload).

Faults a toy cannot reproduce (webhooks, PVCs, quotas, scheduling, probes on real services, ...) are a named
coverage class (``no-toy-vehicle``): they need the real app, so the table says which problems still do.

Vehicles are spread over ``--clusters`` private toy kind clusters (default 4) that run in parallel. The summary
(``summary.json``, ``summary.md``) has one row per problem and one row per error class.
"""

from __future__ import annotations

import argparse
import importlib
import json
import re
import subprocess
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.assurance.rehearse_harness import (
    WAIT_BACKSTOP_SECONDS,
    Check,
    Gate,
    Scenario,
    ScenarioResult,
    fastloop,
    missing_images,
    preflight,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

EXP_ROOT = Path("/mnt/data/shli/sdo-exp/mixed10")
DEFAULT_PLAN = EXP_ROOT / "LE/P4/layout-b/phase4-plan.json"
DEFAULT_DRAWS = EXP_ROOT / "stream/plan-rs.json"
DEFAULT_OUT = EXP_ROOT / "LE/REH"
SUMMARY_JSON = "summary.json"
SUMMARY_MD = "summary.md"
DEFAULT_CLUSTERS = 4

#: Toy fault each SREGym fault class reduces to (``apps/toy/faults.py`` kinds); classes absent here have no toy analog.
TOY_VEHICLE: dict[str, str] = {
    "K8STargetPortMisconfig": "target_port",
    "ServicePortConflict": "target_port",
    "WrongServiceSelector": "target_port",
    "MissingService": "target_port",
    "IncorrectImage": "image",
    "ReadinessProbeMisconfiguration": "readiness",
    "FailedReadinessProbe": "readiness",
    "LivenessProbeMisconfiguration": "readiness",
    "LivenessProbeTooAggressive": "readiness",
    "NetworkPolicyBlock": "network_policy",
}

#: Resource name (for ``kubectl auth can-i``) of each object kind a fault footprint names. Namespaces are created by
#: the fault itself and Secrets are never read, so neither is probed.
KIND_RESOURCE: dict[str, str] = {
    "Deployment": "deployments.apps",
    "Service": "services",
    "ConfigMap": "configmaps",
    "NetworkPolicy": "networkpolicies.networking.k8s.io",
    "MutatingWebhookConfiguration": "mutatingwebhookconfigurations.admissionregistration.k8s.io",
    "ValidatingWebhookConfiguration": "validatingwebhookconfigurations.admissionregistration.k8s.io",
    "PersistentVolumeClaim": "persistentvolumeclaims",
    "HorizontalPodAutoscaler": "horizontalpodautoscalers.autoscaling",
    "Ingress": "ingresses.networking.k8s.io",
    "ResourceQuota": "resourcequotas",
}
#: Kinds a repair removes (the others are patched in place).
DELETE_KINDS = frozenset({"NetworkPolicy", "MutatingWebhookConfiguration", "ValidatingWebhookConfiguration"})
STAGES = ("catalog", "rbac", "responder-write", "vehicle")
RESPONDER_GAP_CLASS = "responder-write-gap"
COVERAGE_CLASS = "no-toy-vehicle"
#: Classes that name a missing rehearsal vehicle, not a defect in what was rehearsed.
COVERAGE_CLASSES = frozenset({COVERAGE_CLASS, "no-lane"})
LEFTOVER_CLASS = "cluster-leftover"

#: First matching rule names the error class of a failed check; the check name and its detail are searched.
CLASS_RULES: tuple[tuple[str, str], ...] = (
    (r"unsupported repair/snapshot scope", "fault-unsupported"),
    (r"fastloop up failed", "cluster-up-failed"),
    (r"reset failed", "reset-failed"),
    (r"context deadline exceeded", "deadline-exceeded"),
    (r"forbidden", "rbac-forbidden"),
    (r"ImagePullBackOff|ErrImagePull", "image-pull"),
    (r"controller can ", "rbac-missing-verb"),
    (r": detected", "no-detection"),
    (r": oracle|oracle reported failure", "oracle-failed"),
    (r": sdo mitigated", "not-mitigated"),
    (r"receipt valid", "receipt-invalid"),
    (r"strict-receipts", "receipt-missing"),
    (r"broker outcome", "no-outcome-commit"),
    (r"outcome commit accepted", "outcome-not-accepted"),
    (r"incident-count|incidents-recorded", "incidents-missing"),
    (r"timed out", "timeout"),
    (r": no error", "incident-error"),
    (r"stream-exit", "stream-error"),
)


def classify(name: str, detail: str) -> str:
    """Error class of a failed check (stable names, so one bug class is one summary row)."""

    text = f"{name} {detail}"
    return next((label for pattern, label in CLASS_RULES if re.search(pattern, text, re.IGNORECASE)), "other")


@dataclass(frozen=True)
class Problem:
    slot: int
    problem_id: str
    app: str
    fault_classes: tuple[str, ...]
    #: Object kinds the problem's fault footprints touch (from the stream plan).
    kinds: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.problem_id or not self.fault_classes:
            raise ValueError("a problem needs an id and at least one fault class")

    @property
    def vehicles(self) -> tuple[str, ...]:
        return tuple(sorted({TOY_VEHICLE[c] for c in self.fault_classes if c in TOY_VEHICLE}))

    @property
    def unvehicled(self) -> tuple[str, ...]:
        return tuple(c for c in self.fault_classes if c not in TOY_VEHICLE)


def load_problems(plan: Path, draws: Path) -> list[Problem]:
    """Every distinct planned problem (the exact repeat shares its first encounter's row), in slot order."""

    planned = json.loads(plan.read_text(encoding="utf-8"))["problems"]
    footprints = {item["id"]: item["draws"] for item in json.loads(draws.read_text(encoding="utf-8"))}
    problems: dict[str, Problem] = {}
    for item in planned:
        problem_id = str(item["problem_id"])
        if problem_id in problems:
            continue
        kinds = sorted(
            {k for draw in footprints.get(problem_id, ()) for k in re.findall(r"Footprint\(kind='(\w+)'", draw)}
        )
        problems[problem_id] = Problem(
            slot=int(item["slot"]),
            problem_id=problem_id,
            app=str(item["app"]),
            fault_classes=tuple(item["fault_classes"]),
            kinds=tuple(kinds),
        )
    return sorted(problems.values(), key=lambda p: p.slot)


@dataclass
class Stage:
    name: str
    status: str  # pass | fail | skip
    error_class: str = ""
    detail: str = ""
    evidence: str = ""

    def __post_init__(self) -> None:
        if self.status not in {"pass", "fail", "skip"}:
            raise ValueError(f"unknown stage status {self.status!r}")


@dataclass
class Row:
    problem: Problem
    stages: list[Stage] = field(default_factory=list)
    #: Wall time of the problem's real-app run (0 when it only ran static stages).
    seconds: float = 0.0

    @property
    def stage_reached(self) -> str:
        ran = [s for s in self.stages if s.status != "skip"]
        return ran[-1].name if ran else "none"

    @property
    def findings(self) -> list[Stage]:
        """Every stage with an error class, real failures before coverage gaps."""

        found = [s for s in self.stages if s.error_class]
        return sorted(found, key=lambda s: s.error_class in COVERAGE_CLASSES)

    @property
    def failure(self) -> Stage | None:
        return next(iter(self.findings), None)

    def as_dict(self) -> dict[str, Any]:
        failure = self.failure
        return {
            "slot": self.problem.slot,
            "problem": self.problem.problem_id,
            "app": self.problem.app,
            "fault_classes": list(self.problem.fault_classes),
            "stage_reached": self.stage_reached,
            "seconds": self.seconds,
            "first_failure": f"{failure.name}: {failure.detail}"[:300] if failure else "",
            "error_class": failure.error_class if failure else "",
            "evidence": failure.evidence if failure else "",
            "stages": [asdict(s) for s in self.stages],
        }


def vehicle_stream(vehicle: str) -> dict[str, Any]:
    """One first encounter and one sibling-workload encounter of a toy fault (learn, then reuse)."""

    return {"diagnosis_grading": "off", "incidents": [f"toy_{vehicle}_frontend", f"toy_{vehicle}_backend"]}


def vehicle_scenario(vehicle: str, streams: Path) -> Scenario:
    path = streams / f"{vehicle}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(vehicle_stream(vehicle), indent=2) + "\n", encoding="utf-8")
    return Scenario(
        name=f"vehicle-{vehicle}",
        covers=("batch",),
        stream=str(path.resolve()),
        checks=("incidents", "native-receipt"),
        expected_incidents=2,
    )


def failed_stage(name: str, result: ScenarioResult, evidence: str) -> Stage:
    """The first broken contract of a finished vehicle scenario."""

    if result.passed:
        return Stage(name, "pass", evidence=evidence)
    broken = next((c for c in result.checks if not c.ok), None)
    check_name, detail = (broken.name, broken.detail) if broken else ("scenario", result.detail)
    if result.status == "error":
        check_name, detail = "scenario", result.detail
    return Stage(name, "fail", classify(check_name, detail), f"{check_name}: {detail}"[:300], evidence)


def probe_rbac(
    kinds: Sequence[str], command: Callable[[Sequence[str]], tuple[int, str]], *, namespace: str, kubeconfig: str
) -> dict[str, list[str]]:
    """Kinds whose verbs the installed ``sdo-controller`` lacks, as ``{kind: [verb resource, ...]}``."""

    subject = f"system:serviceaccount:{namespace}-sdo:sdo-controller"
    missing: dict[str, list[str]] = {}
    for kind in sorted(set(kinds) & set(KIND_RESOURCE)):
        resource = KIND_RESOURCE[kind]
        verbs = ("get", "patch", "delete") if kind in DELETE_KINDS else ("get", "patch")
        for verb in verbs:
            argv = [
                "kubectl",
                "--kubeconfig",
                kubeconfig,
                "auth",
                "can-i",
                verb,
                resource,
                "-n",
                namespace,
                "--as",
                subject,
            ]
            code, output = command(argv)
            if code or output.strip().splitlines()[-1:] != ["yes"]:
                missing.setdefault(kind, []).append(f"{verb} {resource}")
    return missing


def probe_responder_write(draws: Path, kubeconfig: str, control_namespace: str) -> dict[str, str]:
    """Kinds some fault touches that the installed responder cannot write, as ``{kind: denied verbs}``.

    Uses the fast loop's class check (SubjectAccessReview as ``sdo-responder``); an empty result means the class
    is covered. Raises ``ImportError`` when the check is not in this checkout.
    """

    module = importlib.import_module("benchmarks.sregym.fastloop.responder_write_check")
    allowed = module.subject_access_review(Path(kubeconfig), control_namespace=control_namespace)
    rows = module.check(module.fault_objects(draws), allowed)
    return {row.kind: ",".join(row.denied_verbs) for row in module.gaps(rows)}


def build_rows(
    problems: Sequence[Problem],
    vehicles: dict[str, Stage],
    rbac_missing: dict[str, list[str]],
    *,
    rbac_error: str = "",
    responder_gaps: dict[str, str] | None = None,
    responder_error: str = "",
) -> list[Row]:
    """Combine the shared probe and vehicle results into one row per problem; no stage stops the others."""

    rows = []
    for problem in problems:
        row = Row(problem)
        known = bool(problem.kinds)
        row.stages.append(
            Stage("catalog", "pass")
            if known
            else Stage("catalog", "fail", "plan-gap", "the stream plan lists no footprints for this problem")
        )
        missing = {k: v for k, v in rbac_missing.items() if k in problem.kinds}
        if rbac_error:
            row.stages.append(Stage("rbac", "fail", "cluster-up-failed", rbac_error[:300]))
        elif missing:
            detail = "; ".join(f"{kind}: {', '.join(verbs)}" for kind, verbs in sorted(missing.items()))
            row.stages.append(Stage("rbac", "fail", "rbac-missing-verb", f"sdo-controller lacks {detail}"))
        else:
            row.stages.append(Stage("rbac", "pass"))
        gaps = {k: v for k, v in (responder_gaps or {}).items() if k in problem.kinds}
        if responder_error:
            row.stages.append(Stage("responder-write", "skip", detail=responder_error[:300]))
        elif gaps:
            detail = "; ".join(f"{kind} (denied {verbs})" for kind, verbs in sorted(gaps.items()))
            row.stages.append(
                Stage("responder-write", "fail", RESPONDER_GAP_CLASS, f"sdo-responder cannot write {detail}")
            )
        else:
            row.stages.append(Stage("responder-write", "pass"))
        if problem.vehicles:
            outcomes = [vehicles[v] for v in problem.vehicles if v in vehicles]
            chosen = next((s for s in outcomes if s.status == "fail"), outcomes[0] if outcomes else None)
            row.stages.append(
                Stage("vehicle", chosen.status, chosen.error_class, chosen.detail, chosen.evidence)
                if chosen
                else Stage("vehicle", "skip")
            )
        else:
            row.stages.append(Stage("vehicle", "skip"))
        if problem.unvehicled:
            row.stages.append(
                Stage(
                    "vehicle-coverage",
                    "skip",
                    COVERAGE_CLASS,
                    f"no toy analog for {', '.join(problem.unvehicled)}; needs the {problem.app} app",
                )
            )
        rows.append(row)
    return rows


def group_by_class(rows: Sequence[Row]) -> list[dict[str, Any]]:
    """One entry per error class: count, problems (slot order) and the first evidence."""

    grouped: dict[str, list[tuple[Row, Stage]]] = defaultdict(list)
    for row in rows:
        for stage in row.findings:
            grouped[stage.error_class].append((row, stage))
    classes = []
    for error_class, found in grouped.items():
        members = list({id(row): row for row, _ in found}.values())
        first = found[0][1]
        classes.append(
            {
                "error_class": error_class,
                "kind": "coverage" if error_class in COVERAGE_CLASSES else "failure",
                "count": len(members),
                "problems": [m.problem.problem_id for m in members],
                "slots": [m.problem.slot for m in members],
                "first_evidence": first.evidence or first.detail,
                "first_detail": first.detail,
            }
        )
    return sorted(classes, key=lambda c: (c["kind"] == "coverage", -c["count"], c["error_class"]))


def render_markdown(summary: dict[str, Any]) -> str:
    lines = [
        f"# Batch rehearsal {summary['tag']} ({summary['code_sha'][:9]})",
        "",
        f"{summary['problems']} problems, {summary['clusters']} cluster(s), {summary['seconds']}s wall "
        f"(about {summary['seconds_per_problem']}s per problem). "
        f"Clean: {summary['clean']}; failing: {summary['failing']}; coverage gaps: {summary['coverage_gaps']}.",
        "",
        "## Error classes",
        "",
        "| class | kind | count | problems | first evidence |",
        "|---|---|---|---|---|",
    ]
    for item in summary["classes"]:
        shown = "slots " + ", ".join(str(slot) for slot in item["slots"])
        lines.append(
            f"| {item['error_class']} | {item['kind']} | {item['count']} | {shown} | "
            f"{str(item['first_evidence'])[:140].replace('|', '/')} |"
        )
    lines += [
        "",
        "## Problems",
        "",
        "| slot | problem | stage reached | seconds | error class | first failure | evidence |",
        "|---|---|---|---|---|---|---|",
    ]
    for row in summary["rows"]:
        lines.append(
            f"| {row['slot']} | {row['problem']} | {row['stage_reached']} | {row['seconds'] or ''} | "
            f"{row['error_class']} | "
            f"{row['first_failure'][:120].replace('|', '/')} | {row['evidence']} |"
        )
    return "\n".join(lines) + "\n"


@dataclass
class Batch:
    code_root: Path
    tag: str
    out_dir: Path
    problems: list[Problem]
    image_tag: str = "fastloop"
    clusters: int = DEFAULT_CLUSTERS
    draws: Path = DEFAULT_DRAWS
    make_gate: Callable[[str, Path, tuple[Scenario, ...]], Gate] | None = None
    clock: Callable[[], float] = time.monotonic

    def __post_init__(self) -> None:
        if self.clusters < 1:
            raise ValueError("clusters must be at least 1")
        if not self.problems:
            raise ValueError("the plan has no problems")

    def _gate(self, lane: str, run_dir: Path, scenarios: tuple[Scenario, ...]) -> Gate:
        if self.make_gate is not None:
            return self.make_gate(lane, run_dir, scenarios)
        return Gate(
            self.code_root,
            lane,
            run_dir,
            image_tag=self.image_tag,
            scenarios=scenarios,
            up_backstop=WAIT_BACKSTOP_SECONDS,
        )

    def lanes(self) -> list[tuple[str, Path, tuple[Scenario, ...]]]:
        """Vehicle scenarios dealt round-robin to the clusters (lane 0 always exists for the RBAC probe)."""

        vehicles = sorted({v for p in self.problems for v in p.vehicles})
        scenarios = [vehicle_scenario(v, self.out_dir / "streams") for v in vehicles]
        count = max(1, min(self.clusters, len(scenarios)))
        return [(f"{self.tag}{k}", self.out_dir / f"lane{k}", tuple(scenarios[k::count])) for k in range(count)]

    def run(self) -> dict[str, Any]:
        t0 = self.clock()
        self.out_dir.mkdir(parents=True, exist_ok=True)
        gates = [(self._gate(lane, run_dir, scenarios), scenarios) for lane, run_dir, scenarios in self.lanes()]
        for gate, _ in gates:
            gate.run_dir.mkdir(parents=True, exist_ok=True)
        with ThreadPoolExecutor(max_workers=len(gates)) as pool:
            summaries = list(pool.map(lambda item: item[0].run(), gates))
        vehicles: dict[str, Stage] = {}
        setup_error = ""
        for (gate, _), summary in zip(gates, summaries, strict=True):
            for scenario, result in zip(gate.scenarios, summary["scenarios"], strict=True):
                restored = ScenarioResult(
                    result["name"],
                    tuple(result["covers"]),
                    result["status"],
                    result["seconds"],
                    [_check(c) for c in result["checks"]],
                    result["detail"],
                )
                evidence = str(gate.run_dir / "results")
                vehicles[scenario.name.removeprefix("vehicle-")] = failed_stage("vehicle", restored, evidence)
                if restored.status == "error" and "fastloop up failed" in restored.detail:
                    setup_error = restored.detail
        rbac_missing, rbac_error = {}, setup_error
        if not rbac_error:
            gate = gates[0][0]
            try:
                environment = gate.load_environment(gate.run_dir)
                rbac_missing = probe_rbac(
                    sorted({k for p in self.problems for k in p.kinds}),
                    lambda argv: gate._step(argv, 60),
                    namespace=environment.namespace,
                    kubeconfig=str(environment.kubeconfig),
                )
            except Exception as exc:  # the batch reports every stage it could not run
                rbac_error = f"rbac probe failed: {type(exc).__name__}: {exc}"
        responder_gaps: dict[str, str] = {}
        responder_error = rbac_error
        if not responder_error:
            try:
                environment = gates[0][0].load_environment(gates[0][0].run_dir)
                responder_gaps = probe_responder_write(
                    self.draws, str(environment.kubeconfig), f"{environment.namespace}-sdo"
                )
            except ImportError as exc:
                responder_error = f"responder-write check not in this checkout (needs #449): {exc}"
            except Exception as exc:  # the batch reports every stage it could not run
                responder_error = f"responder-write probe failed: {type(exc).__name__}: {exc}"
        rows = build_rows(
            self.problems,
            vehicles,
            rbac_missing,
            rbac_error=rbac_error,
            responder_gaps=responder_gaps,
            responder_error=responder_error,
        )
        seconds = round(self.clock() - t0, 1)
        classes = group_by_class(rows)
        failing = sum(1 for r in rows if any(s.error_class not in COVERAGE_CLASSES for s in r.findings))
        gaps = sum(1 for r in rows if any(s.error_class in COVERAGE_CLASSES for s in r.findings))
        summary = {
            "tag": self.tag,
            "code_root": str(self.code_root),
            "code_sha": _sha(summaries),
            "started_at": datetime.now(timezone.utc).isoformat(),
            "clusters": len(gates),
            "problems": len(rows),
            "seconds": seconds,
            "seconds_per_problem": round(seconds / len(rows), 1),
            "clean": sum(1 for r in rows if not r.findings),
            "failing": failing,
            "coverage_gaps": gaps,
            "responder_write_note": responder_error if responder_error != rbac_error else "",
            "classes": classes,
            "rows": [r.as_dict() for r in rows],
        }
        (self.out_dir / SUMMARY_JSON).write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        (self.out_dir / SUMMARY_MD).write_text(render_markdown(summary), encoding="utf-8")
        return summary


def remaining_clusters(prefix: str) -> list[str]:
    """Kind clusters whose name starts with ``prefix`` (``fastloop down`` has been seen to leave them behind)."""

    done = subprocess.run(["kind", "get", "clusters"], capture_output=True, text=True, check=False)
    return sorted(name for name in done.stdout.split() if name.startswith(prefix))


def verify_teardown(prefix: str, *, listing: Callable[[str], list[str]] = remaining_clusters) -> list[str]:
    """Clusters still present after teardown, each deleted here; callers fail loudly on a non-empty result."""

    left = listing(prefix)
    for name in left:
        subprocess.run(["kind", "delete", "cluster", "--name", name], capture_output=True, check=False, timeout=300)
    return left


def _check(raw: dict[str, Any]) -> Check:
    return Check(raw["name"], raw["ok"], raw.get("detail", ""))


def _sha(summaries: Sequence[dict[str, Any]]) -> str:
    return str(summaries[0].get("code_sha", "unknown")) if summaries else "unknown"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="rehearse-batch", description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--code-root", type=Path, required=True, help="the harness freeze worktree under test")
    parser.add_argument(
        "--tag", required=True, help="batch tag; names the output dir batch-<tag> and the clusters reh<tag><k>-"
    )
    parser.add_argument("--image-tag", default="fastloop", help="tag of the freeze's scripted-worker images")
    parser.add_argument(
        "--clusters", type=int, default=DEFAULT_CLUSTERS, help="private toy clusters to run in parallel"
    )
    parser.add_argument(
        "--plan", type=Path, default=DEFAULT_PLAN, help="Phase 4 plan JSON (problems and fault classes)"
    )
    parser.add_argument(
        "--draws", type=Path, default=DEFAULT_DRAWS, help="stream plan JSON (fault footprints per problem)"
    )
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT)
    parser.add_argument(
        "--dry-run", action="store_true", help="print the plan of work without touching Docker or a cluster"
    )
    parser.add_argument(
        "--teardown", action="store_true", help="remove the clusters afterwards and verify they are gone"
    )
    parser.add_argument(
        "--real-apps",
        action="store_true",
        help="run every problem on its real app lane (scripted workers) instead of the toy cluster",
    )
    parser.add_argument(
        "--lane",
        action="append",
        default=[],
        metavar="APP=ENV_DIR[:LEASE]",
        help="real-apps: the lane environment of an app (repeatable); defaults to hotel_reservation on lane 55b",
    )
    parser.add_argument("--time-scale", type=float, default=1.0, help="real-apps: controller and wait window scale")
    parser.add_argument("--only-slot", type=int, action="append", default=[], help="real-apps: run only these slots")
    return parser


def dry_run(batch: Batch) -> str:
    lines = [f"{len(batch.problems)} problems -> {sum(1 for p in batch.problems if p.vehicles)} with a toy vehicle"]
    for lane, run_dir, scenarios in batch.lanes():
        lines.append(f"lane {lane} ({run_dir}): {', '.join(s.name for s in scenarios) or 'rbac probe only'}")
    gap = defaultdict(list)
    for problem in batch.problems:
        for cls in problem.unvehicled:
            gap[cls].append(problem.slot)
    lines += [f"  no toy vehicle: {cls} (slots {slots})" for cls, slots in sorted(gap.items())]
    return "\n".join(lines)


def format_summary(summary: dict[str, Any]) -> str:
    lines = [
        f"rehearse-batch {summary['tag']} {summary['code_sha'][:9]}: {summary['clean']}/{summary['problems']} clean, "
        f"{summary['failing']} failing, {summary['coverage_gaps']} coverage gaps in {summary['seconds']}s "
        f"({summary['seconds_per_problem']}s/problem, {summary['clusters']} cluster(s))"
    ]
    lines += [
        f"  {c['kind']:8} {c['error_class']:22} x{c['count']}  {str(c['first_evidence'])[:100]}"
        for c in summary["classes"]
    ]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.real_apps:
        from benchmarks.sregym.assurance import rehearse_real

        return rehearse_real.main_real(args)
    code_root = args.code_root.resolve()
    out_dir = (args.out_root / f"batch-{args.tag}").resolve()
    batch = Batch(
        code_root,
        args.tag,
        out_dir,
        load_problems(args.plan, args.draws),
        image_tag=args.image_tag,
        clusters=args.clusters,
        draws=args.draws,
    )
    if args.dry_run:
        print(dry_run(batch))
        print("first command:", " ".join(fastloop("up", "--app", "toy", "--cluster-prefix", f"reh{args.tag}0-")))
        return 0
    problems = preflight(code_root, absent_images=missing_images(args.image_tag))
    if problems:
        print("rehearse-batch refused to start:\n  " + "\n  ".join(problems), file=sys.stderr)
        return 2
    summary = batch.run()
    print(format_summary(summary))
    print(f"summary: {out_dir / SUMMARY_MD}")
    left: list[str] = []
    if args.teardown:
        for lane, run_dir, _ in batch.lanes():
            batch._gate(lane, run_dir, ())._step(fastloop("down", "--run-dir", str(run_dir)), 300)
        left = verify_teardown(f"reh{args.tag}")
        if left:
            print(f"{LEFTOVER_CLASS}: fastloop down left kind clusters {left}; deleted them here", file=sys.stderr)
    return 1 if summary["failing"] or left else 0


if __name__ == "__main__":
    sys.exit(main())
