"""Real-app mode of the batch rehearsal: every planned problem on its real app lane, scripted workers, no model.

    uv run python -m benchmarks.sregym.assurance.rehearse_batch --real-apps --code-root <freeze> --tag <tag> \
        --image-tag <tag> --lane hotel_reservation=<lane environment dir>

One lane cluster per app; an app's problems run in sequence on it, apps in parallel. Each problem goes through
``fastloop run`` with the scripted registrations (admission, the real SREGym injection, detection, the scripted
responder acting through SDO's normal act path, closure, harness recovery and full cleanup), then through a
cluster-object diff and a guarded reset so the next problem starts from a ready lane. Failures never stop the
batch; every stage of every problem ends in a table grouped by error class.

The lane's own environment directory holds recorded data and the lane's accepted memory, so it is never written:
the run uses a private copy (``private_environment``) with its own application workspace clone and results.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.assurance.rehearse_batch import Problem, Row, Stage, classify

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

SCRIPTED_REGISTRATIONS = Path(__file__).resolve().parents[1] / "adapter/scripted_registrations"
#: Stage names of a real-app problem, in order. ``lane-ready`` is the guarded reset the next problem starts from.
REAL_STAGES = ("admission", "inject", "detect", "responder", "closure", "recovery", "cleanup", "lane-ready")
COORD = "/mnt/data/shli/coord/coord"
PROBLEM_TIMEOUT_SECONDS = 1500
RESET_TIMEOUT_SECONDS = 600
#: Kinds whose leftovers a fault can leave behind (namespaced first, then cluster-scoped).
INVENTORY_KINDS = (
    "deployment",
    "statefulset",
    "daemonset",
    "service",
    "job",
    "cronjob",
    "configmap",
    "ingress",
    "networkpolicy",
    "resourcequota",
    "limitrange",
    "persistentvolumeclaim",
    "horizontalpodautoscaler",
    "poddisruptionbudget",
)
CLUSTER_INVENTORY_KINDS = ("validatingwebhookconfiguration", "mutatingwebhookconfiguration")
#: First matching text of a record's error names a responder that could not express the repair.
NO_REPAIR_MARKERS = ("no source repair", "no scripted repair", "unsupported kind")


#: The detecting incident predates the injection: the controller opened one on the clean lane (INV1, astronomy 52/012-013).
BASELINE_INCIDENT_CLASS = "baseline-incident"


def _when(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _before(first: str, second: str) -> bool:
    return _when(first) < _when(second)


@dataclass(frozen=True)
class RealLane:
    """The cluster, namespace and images one app's problems run on."""

    app: str
    run_dir: Path
    cluster: str
    kubeconfig: Path
    namespace: str
    lease: str = ""

    def __post_init__(self) -> None:
        if not self.app or not self.cluster or not self.namespace:
            raise ValueError("a real lane needs an app, a cluster and a namespace")

    @classmethod
    def load(cls, app: str, run_dir: Path, lease: str = "") -> RealLane:
        raw = json.loads((run_dir / "fastloop.json").read_text(encoding="utf-8"))
        return cls(app, run_dir, raw["cluster"], Path(raw["kubeconfig"]), raw["namespace"], lease)


def private_environment(
    source: Path, target: Path, *, images: dict[str, str], clone: Callable[[Path, Path], None] | None = None
) -> Path:
    """A private copy of a lane's fastloop environment: own workspace clone, results, temp dir and delivery record.

    The lane keeps its cluster, kubeconfig and baseline; only paths that hold recorded data or accepted memory
    move. ``images`` replaces the lane's controller, responder and validator refs with the scripted-worker ones.
    """

    raw = json.loads((source / "fastloop.json").read_text(encoding="utf-8"))
    for role in ("controller", "responder", "validator"):
        if role not in images:
            raise ValueError(f"image for {role} missing")
    target.mkdir(parents=True, exist_ok=True)
    workspace = target / "application_workspace"
    if not workspace.exists():
        (clone or _git_clone)(Path(raw["workspace"]), workspace)
    private_tmp = target / "tmp"
    private_tmp.mkdir(parents=True, exist_ok=True)
    worker_env = dict(raw.get("worker_env", {}))
    worker_env["SREGYM_APP_SOURCE_DIR"] = str(workspace)
    worker_env["FASTLOOP_DELIVERY_RECORD"] = str(target / "image_delivery.jsonl")
    required = [images["controller"], images["responder"], images["validator"]]
    worker_env["SREGYM_KIND_REQUIRED_IMAGES"] = json.dumps(required)
    worker_env["SREGYM_KIND_NETWORK_POLICY_CANARY_IMAGE"] = images["validator"]
    raw.update(
        run_dir=str(target),
        workspace=str(workspace),
        private_tmp=str(private_tmp),
        images=images,
        worker_env=worker_env,
        dev_mount=None,
        dev_mount_snapshot=False,
    )
    baseline = source / "cluster_baseline_state.json"
    if baseline.is_file():
        shutil.copyfile(baseline, target / baseline.name)
    path = target / "fastloop.json"
    path.write_text(json.dumps(raw, indent=2) + "\n", encoding="utf-8")
    return path


def _git_clone(source: Path, target: Path) -> None:
    """Clone the lane's accepted workspace (a git repository) without touching it."""

    subprocess.run(["git", "clone", "--quiet", "--no-hardlinks", str(source), str(target)], check=True, timeout=300)


def image_source_problems(
    code_root: Path, images: dict[str, str], check: Callable[..., None] | None = None
) -> list[str]:
    """Why the images cannot run from ``code_root`` (empty when they can).

    Uses the lane's own predicate (``assert_runtime_images``: image source labels against the code root), so the
    batch refuses before the clone, delivery and reset instead of failing at the lane's first reset.
    """

    if check is None:
        from benchmarks.sregym.fastloop.source_preflight import assert_runtime_images as check
    from benchmarks.sregym.fastloop.environment import Images

    try:
        check(Images(**images), code_root, dev_mount=False)
    except (ValueError, OSError, subprocess.SubprocessError) as exc:
        return [f"{exc}"]
    return []


def deliver_images(lane: RealLane, tags: Sequence[str]) -> None:
    """Load the scripted-worker images into the lane cluster's registry and record it in the private environment."""

    from benchmarks.sregym.fastloop.image_delivery import registry_delivery, save_delivery

    started = time.monotonic()
    evidence = registry_delivery(lane.cluster, list(tags))
    save_delivery(
        lane.run_dir / "image_delivery.jsonl",
        {
            "path": "registry",
            "reason": "rehearse_batch real-apps scripted images",
            "cluster": lane.cluster,
            "seconds": time.monotonic() - started,
            "images": list(tags),
            "evidence": evidence,
        },
    )


def run_arguments(lane: RealLane, problem_id: str, *, timeout: int, time_scale: float) -> list[str]:
    """``fastloop run`` for one problem with scripted registrations, behind the lane's lease/Secret fence."""

    guard = ["uv", "run", "python", "-m", "benchmarks.sregym.fastloop.lane_guard", "--cluster", lane.cluster]
    guard += ["--kubeconfig", str(lane.kubeconfig), "--lease", lane.lease or "none", "--coord", COORD, "--"]
    return [
        *guard,
        "benchmarks.sregym.fastloop",
        "run",
        "--run-dir",
        str(lane.run_dir),
        "--agent",
        "sdo",
        "--agent-registrations",
        str(SCRIPTED_REGISTRATIONS),
        "--problem",
        problem_id,
        "--diagnosis-grading",
        "off",
        "--healthy-baseline",
        "--no-checkpoints",
        "--require-full-cleanup",
        "--continue-on-error",
        "--time-scale",
        str(time_scale),
        "--timeout",
        str(timeout),
    ]


def last_record(run_dir: Path) -> dict[str, Any] | None:
    """The newest incident record under the environment's results (None when the run wrote none)."""

    records = sorted((run_dir / "results").glob("*/incidents.jsonl"), key=lambda p: p.stat().st_mtime)
    for path in reversed(records):
        lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        if lines:
            return json.loads(lines[-1])
    return None


def inventory(command: Callable[[Sequence[str]], tuple[int, str]], namespace: str, kubeconfig: Path) -> set[str]:
    """Names of the app namespace's workload and config objects and the cluster-scoped webhook configurations.

    Pods and ReplicaSets are controller-owned churn, and Secrets are never listed.
    """

    names: set[str] = set()
    base = ["kubectl", "--kubeconfig", str(kubeconfig)]
    for kind in INVENTORY_KINDS:
        code, output = command([*base, "-n", namespace, "get", kind, "-o", "name"])
        if code:
            raise RuntimeError(f"inventory of {kind} failed: {output.strip()[:200]}")
        names.update(f"{namespace}/{line.strip()}" for line in output.splitlines() if line.strip())
    for kind in CLUSTER_INVENTORY_KINDS:
        code, output = command([*base, "get", kind, "-o", "name"])
        if code:
            raise RuntimeError(f"inventory of {kind} failed: {output.strip()[:200]}")
        names.update(line.strip() for line in output.splitlines() if line.strip())
    return names


def leftovers(before: set[str], after: set[str]) -> tuple[list[str], list[str]]:
    """Objects the fault added and nothing removed, and baseline objects that are gone."""

    return sorted(after - before), sorted(before - after)


def record_stages(record: dict[str, Any] | None, error: str = "") -> list[Stage]:
    """Stage results of one ``fastloop run`` incident record; each stage is judged from typed record fields only."""

    if record is None:
        detail = error or "fastloop run wrote no incident record"
        found = classify("admission", detail) if error else "no-record"
        last = next((line for line in reversed(detail.splitlines()) if line.strip()), detail)
        return [Stage("admission", "fail", "run-crashed" if found == "other" else found, last[:300])]
    stages: list[Stage] = []
    text = " ".join(str(record.get(k) or "") for k in ("infra_error", "error", "recovery_error"))
    injected = (record.get("injection") or {}).get("state") == "injected"
    if record.get("infra_error") and not injected:
        where = (record.get("injection") or {}).get("stage", "pre_injection")
        stage = "admission" if where == "pre_injection" else "inject"
        return [Stage(stage, "fail", "infra-error", str(record["infra_error"])[:300])]
    stages.append(Stage("admission", "pass"))
    if not injected:
        stages.append(Stage("inject", "fail", "injection-failed", (record.get("error") or "not injected")[:300]))
        return stages
    stages.append(Stage("inject", "pass"))
    if not record.get("detected_at"):
        stages.append(Stage("detect", "fail", "no-detection", (record.get("error") or "no incident opened")[:300]))
        return stages
    started = (record.get("injection") or {}).get("started_at")
    if started and _before(str(record["detected_at"]), str(started)):
        gap = (_when(str(started)) - _when(str(record["detected_at"]))).total_seconds()
        stages.append(
            Stage(
                "detect",
                "fail",
                BASELINE_INCIDENT_CLASS,
                f"the incident bound to this row opened {gap:.0f}s before the injection started: SDO opened an "
                "incident on the fresh lane during the admission proof (no fault)",
            )
        )
        return stages
    stages.append(Stage("detect", "pass"))
    if any(marker in text.lower() for marker in NO_REPAIR_MARKERS):
        stages.append(Stage("responder", "fail", "no-scripted-repair", text.strip()[:300]))
    elif not record.get("responder_finished_at"):
        stages.append(Stage("responder", "fail", classify("responder", text), text.strip()[:300]))
    else:
        stages.append(Stage("responder", "pass"))
    if record.get("recovery_trigger") == "closure_failed" or not record.get("resolved_at"):
        detail = (record.get("error") or f"recovery_trigger={record.get('recovery_trigger')}")[:300]
        stages.append(Stage("closure", "fail", "closure-failed", detail))
    elif record.get("sdo_resolution") != "sdo_mitigated":
        stages.append(Stage("closure", "fail", "not-mitigated", f"sdo_resolution={record.get('sdo_resolution')}"))
    else:
        stages.append(Stage("closure", "pass"))
    recovery = record.get("recovery_oracle") or {}
    if record.get("recovery_error") or recovery.get("success") is False:
        detail = record.get("recovery_error") or "recovery oracle still faulty after harness recovery"
        stages.append(Stage("recovery", "fail", "recovery-failed", str(detail)[:300]))
    else:
        stages.append(Stage("recovery", "pass"))
    notes = [n for n in record.get("eval_notes", []) if n.get("kind") == "app_namespace_inventory_drift"]
    if notes or not record.get("full_cleanup_verified"):
        added = [name for n in notes for name in n.get("not_in_baseline", [])]
        gone = [name for n in notes for name in n.get("missing_from_baseline", [])]
        detail = f"left {added}; missing {gone}" if notes else str(record.get("cleanup_skipped_reason") or "unverified")
        stages.append(Stage("cleanup", "fail", "fault-leftover", detail[:300]))
    else:
        stages.append(Stage("cleanup", "pass"))
    return stages


def program_reuse_note(record: dict[str, Any] | None, *, earlier_learning: bool) -> Stage:
    """Whether a learned program was applied on a later incident: a failure only when one matched and did not run."""

    if record is None or not earlier_learning:
        return Stage("program-reuse", "skip", detail="no earlier incident learned a program")
    matched = (record.get("program_verdicts") or {}).get("Match", 0)
    if not matched:
        return Stage("program-reuse", "skip", detail="no learned program matched this fault; nothing to reuse")
    applied = (record.get("tool_commands") or {}).get("apply", 0)
    if not applied:
        return Stage("program-reuse", "fail", "program-not-applied", f"{matched} program(s) matched; no apply receipt")
    return Stage("program-reuse", "pass", detail=f"{applied} apply command(s) after {matched} match(es)")


def _execute(argv: Sequence[str], *, cwd: Path, timeout: float, log: Path) -> tuple[int, str]:
    log.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    with log.open("a", encoding="utf-8") as stream:
        stream.write(f"$ {' '.join(argv)}\n")
        try:
            done = subprocess.run(
                list(argv), cwd=cwd, capture_output=True, text=True, timeout=timeout, check=False, errors="replace"
            )
        except subprocess.TimeoutExpired:
            stream.write(f"timed out after {timeout}s\n")
            return 124, f"timed out after {timeout}s"
        output = done.stdout + done.stderr
        stream.write(output[-4000:] + f"\n[exit {done.returncode} in {time.monotonic() - started:.0f}s]\n")
    return done.returncode, output


@dataclass
class RealApp:
    """One app's lane: its problems in sequence."""

    lane: RealLane
    problems: list[Problem]
    code_root: Path
    out_dir: Path
    images: dict[str, str]
    time_scale: float = 1.0
    execute: Callable[..., tuple[int, str]] = _execute
    deliver: Callable[[RealLane, Sequence[str]], None] | None = None
    seconds: dict[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.problems:
            raise ValueError(f"app {self.lane.app} has no problems")

    def _step(self, argv: Sequence[str], timeout: float) -> tuple[int, str]:
        return self.execute(argv, cwd=self.code_root, timeout=timeout, log=self.out_dir / "real-app.log")

    def _kubectl(self, argv: Sequence[str]) -> tuple[int, str]:
        return self._step(argv, 120)

    def prepare(self) -> RealLane:
        """Private environment for this app; the lane's own directory stays untouched."""

        path = private_environment(self.lane.run_dir, self.out_dir / "environment", images=self.images)
        lane = RealLane.load(self.lane.app, path.parent, self.lane.lease)
        (self.deliver or deliver_images)(lane, list(self.images.values()))
        return lane

    def reset(self, lane: RealLane) -> Stage:
        argv = ["uv", "run", "python", "-m", "benchmarks.sregym.fastloop", "reset", "--run-dir", str(lane.run_dir)]
        code, output = self._step(argv, RESET_TIMEOUT_SECONDS)
        if code:
            return Stage("lane-ready", "fail", "reset-failed", output[-300:])
        return Stage("lane-ready", "pass")

    def run_problem(self, lane: RealLane, problem: Problem, earlier_learning: bool) -> tuple[Row, bool]:
        row = Row(problem)
        t0 = time.monotonic()
        try:
            before = inventory(self._kubectl, lane.namespace, lane.kubeconfig)
        except RuntimeError as exc:
            row.stages.append(Stage("admission", "fail", "inventory-failed", str(exc)))
            return row, earlier_learning
        argv = run_arguments(lane, problem.problem_id, timeout=1200, time_scale=self.time_scale)
        code, output = self._step(argv, PROBLEM_TIMEOUT_SECONDS)
        record = last_record(lane.run_dir)
        error = "" if code == 0 or record is not None else output[-400:]
        row.stages.extend(record_stages(record, error))
        row.stages.append(program_reuse_note(record, earlier_learning=earlier_learning))
        try:
            after = inventory(self._kubectl, lane.namespace, lane.kubeconfig)
            added, gone = leftovers(before, after)
            if added or gone:
                row.stages = [s for s in row.stages if s.name != "cleanup"]
                row.stages.append(Stage("cleanup", "fail", "fault-leftover", f"left {added}; missing {gone}"[:300]))
        except RuntimeError as exc:
            row.stages.append(Stage("cleanup", "fail", "inventory-failed", str(exc)))
        row.stages.append(self.reset(lane))
        self.seconds[problem.problem_id] = round(time.monotonic() - t0, 1)
        learned = earlier_learning or bool(record and record.get("reflection_finished_at"))
        return row, learned

    def run(self) -> list[Row]:
        try:
            lane = self.prepare()
        except Exception as exc:  # the batch reports every stage it could not run
            detail = f"private environment failed: {type(exc).__name__}: {exc}"
            return [Row(p, [Stage("admission", "fail", "cluster-up-failed", detail[:300])]) for p in self.problems]
        start = self.reset(lane)
        if start.status == "fail":
            return [Row(p, [Stage("admission", "fail", start.error_class, start.detail)]) for p in self.problems]
        rows, learned = [], False
        for problem in self.problems:
            row, learned = self.run_problem(lane, problem, learned)
            rows.append(row)
        return rows


DEFAULT_LANES = {
    "hotel_reservation": (Path("/mnt/data/shli/sdo-exp/mixed10/LE/P4/live/lane-55b/environment"), "le-p4b")
}
STATIC_STAGES = ("catalog", "rbac", "responder-write")


def parse_lanes(values: Sequence[str]) -> dict[str, tuple[Path, str]]:
    """``APP=ENV_DIR[:LEASE]`` values; the hotel lane 55b when none are given."""

    if not values:
        return dict(DEFAULT_LANES)
    lanes: dict[str, tuple[Path, str]] = {}
    for value in values:
        app, separator, rest = value.partition("=")
        if not separator or not app or not rest:
            raise ValueError(f"--lane wants APP=ENV_DIR[:LEASE], got {value!r}")
        path, _, lease = rest.partition(":")
        lanes[app] = (Path(path), lease)
    return lanes


def merge_rows(static: Sequence[Row], real: dict[str, Row], lanes: dict[str, tuple[Path, str]]) -> list[Row]:
    """Static stages of every problem followed by its real-app stages (or a ``no-lane`` coverage stage)."""

    merged = []
    for row in static:
        row.stages = [s for s in row.stages if s.name in STATIC_STAGES]
        ran = real.get(row.problem.problem_id)
        if ran is not None:
            row.stages.extend(ran.stages)
            row.seconds = ran.seconds
        elif row.problem.app not in lanes:
            row.stages.append(Stage("real-app", "skip", "no-lane", f"no real lane given for app {row.problem.app}"))
        else:
            row.stages.append(Stage("real-app", "skip", detail="not selected"))
        merged.append(row)
    return merged


def run_real(
    problems: Sequence[Problem],
    lanes: dict[str, tuple[Path, str]],
    *,
    code_root: Path,
    out_dir: Path,
    images: dict[str, str],
    time_scale: float,
    draws: Path,
    execute: Callable[..., tuple[int, str]] = _execute,
) -> list[Row]:
    """Static stages for every problem, real-app stages for the apps with a lane (apps in parallel)."""

    from concurrent.futures import ThreadPoolExecutor

    from benchmarks.sregym.assurance.rehearse_batch import build_rows, probe_rbac, probe_responder_write

    apps = {
        app: RealApp(
            RealLane.load(app, path, lease),
            [p for p in problems if p.app == app],
            code_root,
            out_dir / app,
            images,
            time_scale,
            execute,
        )
        for app, (path, lease) in lanes.items()
        if any(p.app == app for p in problems)
    }
    with ThreadPoolExecutor(max_workers=max(1, len(apps))) as pool:
        results = list(pool.map(lambda app: app.run(), apps.values()))
    real: dict[str, Row] = {}
    for app, rows in zip(apps.values(), results, strict=True):
        for row in rows:
            row.seconds = app.seconds.get(row.problem.problem_id, 0.0)
            real[row.problem.problem_id] = row
    missing: dict[str, list[str]] = {}
    gaps: dict[str, str] = {}
    rbac_error = responder_error = ""
    for app in apps.values():
        lane = app.lane
        try:
            kinds = sorted({k for p in app.problems for k in p.kinds})
            found = probe_rbac(
                kinds,
                lambda argv: execute(argv, cwd=code_root, timeout=60, log=out_dir / "static.log"),
                namespace=lane.namespace,
                kubeconfig=str(lane.kubeconfig),
            )
            missing.update(found)
            gaps.update(probe_responder_write(draws, str(lane.kubeconfig), f"{lane.namespace}-sdo"))
        except ImportError as exc:
            responder_error = f"responder-write check not in this checkout: {exc}"
        except Exception as exc:  # the batch reports every stage it could not run
            rbac_error = f"static probe failed: {type(exc).__name__}: {exc}"
    static = build_rows(
        problems, {}, missing, rbac_error=rbac_error, responder_gaps=gaps, responder_error=responder_error
    )
    return merge_rows(static, real, lanes)


def main_real(args: Any) -> int:
    """Entry for ``rehearse_batch --real-apps``."""

    from benchmarks.sregym.assurance import rehearse_batch as batch
    from benchmarks.sregym.assurance.rehearse_harness import image_refs, missing_images, preflight

    code_root = args.code_root.resolve()
    out_dir = (args.out_root / f"real-{args.tag}").resolve()
    lanes = parse_lanes(args.lane)
    problems = batch.load_problems(args.plan, args.draws)
    if args.only_slot:
        problems = [p for p in problems if p.slot in set(args.only_slot)]
    if args.dry_run:
        for app, (path, lease) in lanes.items():
            names = [str(p.slot) for p in problems if p.app == app]
            print(f"{app}: lane {path} (lease {lease or 'none'}) slots {', '.join(names) or 'none'}")
        print("apps without a lane:", sorted({p.app for p in problems} - set(lanes)) or "none")
        return 0
    refused = preflight(code_root, absent_images=missing_images(args.image_tag))
    if refused:
        print("rehearse-batch --real-apps refused to start:\n  " + "\n  ".join(refused), file=sys.stderr)
        return 2
    refused = image_source_problems(code_root, image_refs(args.image_tag))
    if refused:
        print("rehearse-batch --real-apps refused to start:\n  " + "\n  ".join(refused), file=sys.stderr)
        return 2
    t0 = time.monotonic()
    rows = run_real(
        problems,
        lanes,
        code_root=code_root,
        out_dir=out_dir,
        images=image_refs(args.image_tag),
        time_scale=args.time_scale,
        draws=args.draws,
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    seconds = round(time.monotonic() - t0, 1)
    summary = real_summary(rows, tag=args.tag, code_root=code_root, lanes=lanes, seconds=seconds)
    (out_dir / batch.SUMMARY_JSON).write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    (out_dir / batch.SUMMARY_MD).write_text(batch.render_markdown(summary), encoding="utf-8")
    print(batch.format_summary(summary))
    print(f"summary: {out_dir / batch.SUMMARY_MD}")
    return 1 if summary["failing"] else 0


def real_summary(
    rows: Sequence[Row], *, tag: str, code_root: Path, lanes: dict[str, tuple[Path, str]], seconds: float
) -> dict[str, Any]:
    from datetime import datetime, timezone

    from benchmarks.sregym.assurance import rehearse_batch as batch

    ran = [r for r in rows if r.seconds]
    return {
        "tag": tag,
        "code_root": str(code_root),
        "code_sha": subprocess.run(
            ["git", "-C", str(code_root), "rev-parse", "HEAD"], capture_output=True, text=True, check=False
        ).stdout.strip()
        or "unknown",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "clusters": len(lanes),
        "problems": len(rows),
        "seconds": seconds,
        "seconds_per_problem": round(sum(r.seconds for r in ran) / len(ran), 1) if ran else 0.0,
        "clean": sum(1 for r in rows if not r.findings),
        "failing": sum(1 for r in rows if any(s.error_class not in batch.COVERAGE_CLASSES for s in r.findings)),
        "coverage_gaps": sum(1 for r in rows if any(s.error_class in batch.COVERAGE_CLASSES for s in r.findings)),
        "responder_write_note": "",
        "classes": batch.group_by_class(rows),
        "rows": [r.as_dict() for r in rows],
    }
