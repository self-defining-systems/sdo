"""Scripted rehearsal gate for a new harness: catch plumbing bugs before live LLM rows.

    uv run python -m benchmarks.sregym.assurance.rehearse_harness --code-root <freeze> --lane <id>

Runs four scenarios serially on one private toy kind cluster (real controller,
broker, judge refresh and strict receipts; scripted workers, no model, no Secret
read) built from ``--code-root`` through the fast loop's source mount:

* ``baseline``: learn a program, then reuse it (the RH3 machinery on the toy app);
* ``p51-restart-in-pause``: the controller Pod is replaced inside the injection pause;
* ``p52-source-repair-refresh``: the responder's own source repair moves accepted
  memory and the judge refresh rewrites a health detector before closure;
* ``p53-program-write-rbac``: the installed controller may delete the ReplicaSets and
  Pods a learned program's plan deletes.

A new harness may switch lanes only after this exits 0 on one lane. The summary
JSON (``rehearse-harness-summary.json``) records every scenario's checks.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURES = Path("tests/fixtures/sregym/rehearse_harness")
SUMMARY_FILENAME = "rehearse-harness-summary.json"
STRICT_RECEIPT = "sdo_production_receipt_strict.json"
#: The whole gate, cold cluster included, must finish within this wall time.
BUDGET_SECONDS = 900
UP_BUDGET_SECONDS = 240
LOAD_LIMIT = 40.0
DISK_FLOOR_GIB = 110
TIME_SCALE = "0.1"
#: What the P51 hang looks like in controller logs: resume gave up on a cache that was never started.
BLIND_RESUME_LINE = "resume observation of namespace"
BLIND_RESUME_CAUSE = "context deadline exceeded"
HEALTH_DETECTORS = ".sdo/diagnostics/detectors/health"
#: Writes a learned program's plan may emit that the installed controller must be allowed to perform.
PROGRAM_WRITE_PROBES: tuple[tuple[str, str], ...] = (
    ("delete", "replicasets.apps"),
    ("patch", "replicasets.apps"),
    ("delete", "pods"),
)


@dataclass(frozen=True)
class Scenario:
    name: str
    #: The failures this scenario reproduces (P-numbers from FAILURE_PATTERNS) or the baseline it keeps.
    covers: tuple[str, ...]
    stream: str | None
    budget_seconds: int
    #: Extra ``fastloop run`` flags.
    run_flags: tuple[str, ...] = ()
    #: Names from :data:`CHECKS` run after the stream.
    checks: tuple[str, ...] = ()
    expected_incidents: int = 1

    def __post_init__(self) -> None:
        if not self.name or not self.covers:
            raise ValueError("a scenario needs a name and the failures it covers")
        if self.budget_seconds <= 0 or self.expected_incidents < 1:
            raise ValueError("a scenario needs a positive budget and incident count")
        unknown = sorted(set(self.checks) - set(CHECKS))
        if unknown:
            raise ValueError(f"unknown checks {unknown}")


@dataclass
class Check:
    name: str
    ok: bool
    detail: str = ""


@dataclass
class ScenarioResult:
    name: str
    covers: tuple[str, ...]
    status: str = "not-run"
    seconds: float = 0.0
    checks: list[Check] = field(default_factory=list)
    detail: str = ""

    @property
    def passed(self) -> bool:
        return self.status == "pass"


@dataclass(frozen=True)
class Context:
    """Everything a check may read about one finished scenario."""

    run_dir: Path
    results: Path
    workspace: Path
    scenario: Scenario
    #: Runs a command and returns ``(exit code, combined output)`` (used by the cluster probes).
    command: Callable[[Sequence[str]], tuple[int, str]]
    namespace: str = ""
    kubeconfig: str = ""


# --- checks ------------------------------------------------------------------------------------------------


def _receipts(results: Path) -> list[dict[str, Any]]:
    return [json.loads(path.read_text(encoding="utf-8")) for path in sorted(results.rglob(STRICT_RECEIPT))]


def check_incidents(ctx: Context) -> list[Check]:
    """Every incident closed through SDO, passed its oracle and had no harness error."""

    from benchmarks.sregym.fastloop.records import load_records

    path = ctx.results / "incidents.jsonl"
    if not path.is_file():
        return [Check("incidents-recorded", False, f"{path} is missing")]
    records = load_records(path)
    checks = [
        Check(
            "incident-count",
            len(records) == ctx.scenario.expected_incidents,
            f"{len(records)} recorded, expected {ctx.scenario.expected_incidents}",
        )
    ]
    for record in records:
        label = f"incident {record.index} {record.problem_id}"
        failure = record.error or record.infra_error
        checks.append(Check(f"{label}: no error", not failure, str(failure or "")[:300]))
        checks.append(Check(f"{label}: detected", record.detected_at is not None, "no detector opened the incident"))
        checks.append(
            Check(f"{label}: oracle", bool(record.oracle and record.oracle.success), str(record.oracle)[:200])
        )
        checks.append(
            Check(f"{label}: sdo mitigated", record.sdo_resolution == "sdo_mitigated", str(record.sdo_resolution))
        )
    return checks


def check_native_receipt(ctx: Context) -> list[Check]:
    """A strict native receipt per incident, with a broker outcome commit contained in the accepted checkout."""

    from benchmarks.sregym.protocol.native_receipt import validate_native_production_receipt

    receipts = _receipts(ctx.results)
    checks = [
        Check(
            "strict-receipts",
            len(receipts) == ctx.scenario.expected_incidents,
            f"{len(receipts)} strict receipt(s), expected {ctx.scenario.expected_incidents}",
        )
    ]
    for receipt in receipts:
        label = str(receipt.get("incident_id"))
        try:
            validate_native_production_receipt(receipt)
            valid, why = True, ""
        except (ValueError, KeyError, TypeError) as exc:
            valid, why = False, f"{type(exc).__name__}: {exc}"[:300]
        checks.append(Check(f"{label}: receipt valid", valid, why))
        ledger = receipt.get("native_ledger") or {}
        outcome = receipt.get("outcome_commit")
        checks.append(
            Check(
                f"{label}: broker outcome",
                bool(outcome) and bool(ledger.get("outcome_document")),
                f"outcome_commit={outcome}",
            )
        )
        if outcome:
            checks.append(
                Check(f"{label}: outcome commit accepted", _is_ancestor(ctx.workspace, str(outcome)), str(outcome))
            )
    return checks


def _git(workspace: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["git", "-C", str(workspace), *args], capture_output=True, text=True, check=False)


def _is_ancestor(workspace: Path, commit: str) -> bool:
    return _git(workspace, "merge-base", "--is-ancestor", commit, "HEAD").returncode == 0


def check_source_repair_refresh(ctx: Context) -> list[Check]:
    """P52: the incident's own source repair is accepted, and the judge refresh rewrote a health detector after it."""

    checks: list[Check] = []
    for receipt in _receipts(ctx.results):
        label = str(receipt.get("incident_id"))
        proposal, outcome = receipt.get("proposal_commit"), receipt.get("outcome_commit")
        checks.append(Check(f"{label}: own source proposal committed", bool(proposal), f"proposal_commit={proposal}"))
        if not (proposal and outcome):
            continue
        touched = _git(ctx.workspace, "diff", "--name-only", str(proposal), str(outcome), "--", HEALTH_DETECTORS)
        names = touched.stdout.split()
        checks.append(
            Check(
                f"{label}: judge refresh rewrote a health detector",
                touched.returncode == 0 and bool(names),
                f"health paths changed between proposal and outcome: {names[:4]}",
            )
        )
    if not checks:
        checks.append(Check("source-repair-receipt", False, "no strict receipt to inspect"))
    return checks


def check_restart_in_pause(ctx: Context) -> list[Check]:
    """P51: the controller Pod really was replaced inside the pause, and its resume did not hang."""

    evidence = ctx.results / "restart-in-pause.jsonl"
    rows = (
        [json.loads(line) for line in evidence.read_text(encoding="utf-8").splitlines() if line.strip()]
        if evidence.is_file()
        else []
    )
    replaced = [row for row in rows if row.get("old_uid") and row.get("new_uid") and row["old_uid"] != row["new_uid"]]
    checks = [
        Check(
            "controller-restarted-in-pause",
            len(replaced) >= ctx.scenario.expected_incidents,
            f"{len(replaced)} replacement(s) recorded in {evidence.name}",
        )
    ]
    blind: list[str] = []
    for log in ctx.results.rglob("*"):
        if log.is_file() and log.parent.name == "controller_logs":
            lines = log.read_text(encoding="utf-8", errors="replace").splitlines()
            blind.extend(
                f"{log.name}: {line.strip()[:160]}"
                for line in lines
                if BLIND_RESUME_LINE in line and BLIND_RESUME_CAUSE in line
            )
    checks.append(Check("no-blind-resume", not blind, "; ".join(blind[:2])))
    return checks


def check_program_write_rbac(ctx: Context) -> list[Check]:
    """P53: the installed controller may perform the deletes a learned program's plan emits."""

    control = f"{ctx.namespace}-sdo"
    subject = f"system:serviceaccount:{control}:sdo-controller"
    checks = []
    for verb, resource in PROGRAM_WRITE_PROBES:
        code, output = ctx.command(
            [
                "kubectl",
                "--kubeconfig",
                ctx.kubeconfig,
                "auth",
                "can-i",
                verb,
                resource,
                "-n",
                ctx.namespace,
                "--as",
                subject,
            ]
        )
        allowed = output.strip().splitlines()[-1:] == ["yes"] if code == 0 else False
        checks.append(Check(f"controller can {verb} {resource}", allowed, output.strip()[:200]))
    return checks


CHECKS: dict[str, Callable[[Context], list[Check]]] = {
    "incidents": check_incidents,
    "native-receipt": check_native_receipt,
    "source-repair-refresh": check_source_repair_refresh,
    "restart-in-pause": check_restart_in_pause,
    "program-write-rbac": check_program_write_rbac,
}

SCENARIOS: tuple[Scenario, ...] = (
    Scenario(
        name="baseline",
        covers=("RH3",),
        stream="baseline.json",
        budget_seconds=300,
        checks=("incidents", "native-receipt"),
        expected_incidents=2,
    ),
    Scenario(
        name="p51-restart-in-pause",
        covers=("P51",),
        stream="single_image_fault.json",
        budget_seconds=150,
        run_flags=("--restart-controller-in-injection-pause",),
        checks=("incidents", "native-receipt", "restart-in-pause"),
    ),
    Scenario(
        name="p52-source-repair-refresh",
        covers=("P52",),
        stream="single_image_fault.json",
        budget_seconds=150,
        checks=("incidents", "native-receipt", "source-repair-refresh"),
    ),
    Scenario(
        name="p53-program-write-rbac",
        covers=("P53",),
        stream=None,
        budget_seconds=30,
        checks=("program-write-rbac",),
        expected_incidents=1,
    ),
)


# --- running -----------------------------------------------------------------------------------------------


def fastloop(*args: str) -> list[str]:
    return ["uv", "run", "python", "-m", "benchmarks.sregym.fastloop", *args]


IMAGE_ROLES = ("controller", "responder", "validator")
IMAGE_NAMES = {
    "controller": "sdo-controller",
    "responder": "sdo-sregym-responder",
    "validator": "sdo-detector-validator",
}


def image_refs(tag: str) -> dict[str, str]:
    return {role: f"{IMAGE_NAMES[role]}:{tag}" for role in IMAGE_ROLES}


def build_command(tag: str) -> str:
    return f"SDO_IMAGE_TAG={tag} SDO_SCRIPTED_WORKER=1 bash scripts/build_sdo_images.sh"


def up_arguments(code_root: Path, run_dir: Path, lane: str, image_tag: str = "fastloop") -> list[str]:
    images = [arg for role, ref in image_refs(image_tag).items() for arg in (f"--{role}-image", ref)]
    return fastloop(
        "up",
        "--app",
        "toy",
        "--run-dir",
        str(run_dir),
        "--cluster-prefix",
        f"reh{lane}-",
        "--worker-id",
        "0",
        "--dev-mount",
        str(code_root),
        *images,
    )


def run_arguments(code_root: Path, run_dir: Path, scenario: Scenario, run_id: str) -> list[str]:
    assert scenario.stream is not None
    flags = list(scenario.run_flags)
    if "--restart-controller-in-injection-pause" in flags:
        flags.insert(
            flags.index("--restart-controller-in-injection-pause") + 1,
            str(run_dir / "results" / run_id / "restart-in-pause.jsonl"),
        )
    return fastloop(
        "run",
        "--run-dir",
        str(run_dir),
        "--agent",
        "sdo",
        "--stream",
        str(code_root / FIXTURES / scenario.stream),
        "--run-id",
        run_id,
        "--reflection-session",
        "fresh",
        "--diagnosis-grading",
        "off",
        "--time-scale",
        TIME_SCALE,
        "--timeout",
        "300",
        "--detection-timeout",
        "75",
        "--no-checkpoints",
        *flags,
    )


def reset_arguments(code_root: Path, run_dir: Path) -> list[str]:
    return fastloop("reset", "--run-dir", str(run_dir))


def missing_images(tag: str) -> list[str]:
    """Scripted-worker image refs Docker does not have (read-only inspect)."""

    return [
        ref
        for ref in image_refs(tag).values()
        if subprocess.run(["docker", "image", "inspect", ref], capture_output=True, check=False).returncode
    ]


def preflight(
    code_root: Path, *, load: float | None = None, free_gib: float | None = None, absent_images: list[str] | None = None
) -> list[str]:
    """Reasons the gate must not start (empty when it may)."""

    problems = []
    if absent_images:
        problems.append(f"missing images {absent_images}; build them from the freeze: {build_command('<tag>')}")
    required = ("benchmarks/sregym/fastloop/cli.py", str(FIXTURES / "baseline.json"))
    problems.extend(
        f"{code_root} is not a harness checkout (missing {relative})"
        for relative in required
        if not (code_root / relative).is_file()
    )
    load = os.getloadavg()[0] if load is None else load
    if load >= LOAD_LIMIT:
        problems.append(f"host load {load:.0f} >= {LOAD_LIMIT:.0f}; wait")
    free = shutil.disk_usage("/mnt/data").free / 2**30 if free_gib is None else free_gib
    if free < DISK_FLOOR_GIB:
        problems.append(f"{free:.0f} GiB free on /mnt/data, need {DISK_FLOOR_GIB}")
    return problems


def _execute(argv: Sequence[str], *, cwd: Path, timeout: float, log: Path) -> tuple[int, str]:
    env = {**os.environ, "SDO_TIME_SCALE": TIME_SCALE}
    log.parent.mkdir(parents=True, exist_ok=True)
    with log.open("a", encoding="utf-8") as handle:
        handle.write(f"$ {' '.join(argv)}\n")
        handle.flush()
        try:
            completed = subprocess.run(
                list(argv),
                cwd=cwd,
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=timeout,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            output = str(exc.stdout) if isinstance(exc.stdout, str) else ""
            handle.write(f"{output}\n[timed out after {timeout:.0f}s]\n")
            return 124, output
        handle.write(completed.stdout)
        return completed.returncode, completed.stdout


def _load_environment(run_dir: Path) -> Any:
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

    return FastloopEnvironment.load(run_dir)


@dataclass
class Gate:
    code_root: Path
    lane: str
    run_dir: Path
    clock: Callable[[], float] = time.monotonic
    execute: Callable[..., tuple[int, str]] = _execute
    load_environment: Callable[[Path], Any] = _load_environment
    image_tag: str = "fastloop"
    scenarios: tuple[Scenario, ...] = SCENARIOS

    def log(self) -> Path:
        return self.run_dir / "rehearse-harness.log"

    def _step(self, argv: Sequence[str], timeout: float) -> tuple[int, str]:
        return self.execute(argv, cwd=self.code_root, timeout=timeout, log=self.log())

    def run(self) -> dict[str, Any]:
        started = datetime.now(timezone.utc)
        t0 = self.clock()
        results: list[ScenarioResult] = []
        setup_error = ""
        if not (self.run_dir / "fastloop.json").is_file():
            code, output = self._step(
                up_arguments(self.code_root, self.run_dir, self.lane, self.image_tag), UP_BUDGET_SECONDS
            )
            if code:
                setup_error = f"fastloop up failed (exit {code}): {output[-400:]}"
        for scenario in self.scenarios:
            if setup_error:
                results.append(ScenarioResult(scenario.name, scenario.covers, "error", detail=setup_error))
                continue
            results.append(self._scenario(scenario))
        seconds = self.clock() - t0
        summary = {
            "code_root": str(self.code_root),
            "code_sha": _head(self.code_root),
            "lane": self.lane,
            "run_dir": str(self.run_dir),
            "started_at": started.isoformat(),
            "seconds": round(seconds, 1),
            "budget_seconds": BUDGET_SECONDS,
            "within_budget": seconds <= BUDGET_SECONDS,
            "passed": bool(results) and all(r.passed for r in results) and seconds <= BUDGET_SECONDS,
            "scenarios": [{**asdict(r), "passed": r.passed} for r in results],
        }
        (self.run_dir / SUMMARY_FILENAME).write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        return summary

    def _scenario(self, scenario: Scenario) -> ScenarioResult:
        result = ScenarioResult(scenario.name, scenario.covers)
        t0 = self.clock()
        run_id = f"reh-{scenario.name}-{int(time.time())}"
        results = self.run_dir / "results" / run_id
        try:
            code, output = self._step(reset_arguments(self.code_root, self.run_dir), 300)
            if code:
                result.status, result.detail = "error", f"reset failed (exit {code}): {output[-300:]}"
                return result
            if scenario.stream is not None:
                code, output = self._step(
                    run_arguments(self.code_root, self.run_dir, scenario, run_id), scenario.budget_seconds
                )
                if code:
                    # A failed stream still leaves records; the checks name the first broken contract.
                    result.detail = f"fastloop run exit {code}: {output[-300:]}"
            environment = self.load_environment(self.run_dir)
            ctx = Context(
                run_dir=self.run_dir,
                results=results,
                workspace=environment.workspace,
                scenario=scenario,
                command=lambda argv: self._step(argv, 60),
                namespace=environment.namespace,
                kubeconfig=str(environment.kubeconfig),
            )
            for name in scenario.checks:
                result.checks.extend(CHECKS[name](ctx))
            if result.detail and all(check.ok for check in result.checks):
                result.checks.append(Check("stream-exit", False, result.detail))
            result.status = "pass" if result.checks and all(check.ok for check in result.checks) else "fail"
        except Exception as exc:  # the gate reports every contract it could not check
            result.status, result.detail = "error", f"{type(exc).__name__}: {exc}"
        finally:
            result.seconds = round(self.clock() - t0, 1)
            if result.seconds > scenario.budget_seconds:
                result.checks.append(Check("scenario-budget", False, f"{result.seconds}s > {scenario.budget_seconds}s"))
                result.status = "fail" if result.status == "pass" else result.status
        return result


def _head(code_root: Path) -> str:
    completed = subprocess.run(
        ["git", "-C", str(code_root), "rev-parse", "HEAD"], capture_output=True, text=True, check=False
    )
    return completed.stdout.strip() or "unknown"


def format_summary(summary: dict[str, Any]) -> str:
    lines = [
        f"rehearse-harness lane {summary['lane']} {summary['code_sha'][:9]}: "
        f"{'PASS' if summary['passed'] else 'FAIL'} in {summary['seconds']}s (budget {summary['budget_seconds']}s)"
    ]
    for scenario in summary["scenarios"]:
        covers = ",".join(scenario["covers"])
        lines.append(
            f"  {'PASS' if scenario['passed'] else 'FAIL'} {scenario['name']} [{covers}] "
            f"{scenario['seconds']}s {scenario['detail'][:120]}"
        )
        lines.extend(f"      failed: {c['name']}: {c['detail'][:160]}" for c in scenario["checks"] if not c["ok"])
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="rehearse-harness", description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "--code-root", type=Path, required=True, help="the harness freeze worktree under test (mounted into the lane)"
    )
    parser.add_argument(
        "--lane", required=True, help="rehearsal lane id; names the private cluster reh<lane>-toy and its run dir"
    )
    parser.add_argument(
        "--run-dir", type=Path, default=None, help="default: /mnt/data/shli/sdo-exp/mixed10/LE/REH/lane<lane>"
    )
    parser.add_argument("--image-tag", default="fastloop", help="tag of the freeze's scripted-worker images")
    parser.add_argument(
        "--dry-run", action="store_true", help="print the commands without touching Docker or a cluster"
    )
    parser.add_argument("--teardown", action="store_true", help="remove the controller and private cluster afterwards")
    return parser


def dry_run(code_root: Path, run_dir: Path, lane: str, image_tag: str = "fastloop") -> str:
    lines = [" ".join(up_arguments(code_root, run_dir, lane, image_tag))]
    for scenario in SCENARIOS:
        lines.append(" ".join(reset_arguments(code_root, run_dir)))
        if scenario.stream is not None:
            lines.append(" ".join(run_arguments(code_root, run_dir, scenario, f"reh-{scenario.name}-<ts>")))
        lines.append(f"# checks: {', '.join(scenario.checks)}  (covers {', '.join(scenario.covers)})")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    code_root = args.code_root.resolve()
    run_dir = (args.run_dir or Path(f"/mnt/data/shli/sdo-exp/mixed10/LE/REH/lane{args.lane}")).resolve()
    if args.dry_run:
        print(dry_run(code_root, run_dir, args.lane, args.image_tag))
        return 0
    problems = preflight(code_root, absent_images=missing_images(args.image_tag))
    if problems:
        print("rehearse-harness refused to start:\n  " + "\n  ".join(problems), file=sys.stderr)
        return 2
    run_dir.mkdir(parents=True, exist_ok=True)
    gate = Gate(code_root, args.lane, run_dir, image_tag=args.image_tag)
    summary = gate.run()
    print(format_summary(summary))
    print(f"summary: {run_dir / SUMMARY_FILENAME}")
    if args.teardown:
        gate._step(fastloop("down", "--run-dir", str(run_dir)), 300)
    return 0 if summary["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
