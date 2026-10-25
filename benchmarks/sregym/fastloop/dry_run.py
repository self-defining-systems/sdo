"""Bounded, scripted, no-fault execution of the real pre-injection path."""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

INVARIANTS = (
    "lane_resources",
    "restore",
    "onboarding",
    "setup_memory",
    "rbac_equality",
    "stabilization",
    "probe_placement",
    "baseline",
    "closure_policy",
)


def code_revision() -> str:
    root = Path(__file__).resolve().parents[3]
    result = subprocess.run(
        ["git", "-C", str(root), "describe", "--always", "--abbrev=40", "--dirty", "--match=__eval_no_tag__"],
        check=True,
        text=True,
        capture_output=True,
        timeout=5,
    )
    return result.stdout.strip()


@dataclass
class DryRunReport:
    lane: str
    code_revision: str = field(default_factory=code_revision)
    started_at: float = field(default_factory=time.time)
    seconds: float = 0
    passed: bool = False
    fault_injected: bool = False
    live_model_calls: bool = False
    invariants: dict[str, dict[str, Any]] = field(
        default_factory=lambda: {name: {"passed": False, "reason": "not reached"} for name in INVARIANTS}
    )

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(asdict(self), indent=2) + "\n")


def budget(value: str) -> int:
    seconds = int(value)
    if not 0 < seconds <= 300:
        raise argparse.ArgumentTypeError("eval dry run budget must be in 1..300 seconds")
    return seconds


def execute(run_dir: Path, timeout: int) -> int:
    directory = run_dir.resolve() / "eval-dry-run"
    directory.mkdir(parents=True, exist_ok=True)
    report_path = directory / "report.json"
    report = DryRunReport(str(run_dir))
    report.save(report_path)
    command = [sys.executable, "-m", __name__, "--run-dir", str(run_dir), "--timeout", str(timeout)]
    started = time.monotonic()
    with (directory / "worker.log").open("w") as log:
        child = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            code = child.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(child.pid, signal.SIGTERM)
            try:
                child.wait(timeout=2)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait()
            code = 1
            document = json.loads(report_path.read_text())
            document.update(passed=False, error=f"eval: dry run exceeded {timeout}s budget")
            report_path.write_text(json.dumps(document, indent=2) + "\n")
    document = json.loads(report_path.read_text())
    document["seconds"] = time.monotonic() - started
    report_path.write_text(json.dumps(document, indent=2) + "\n")
    print(json.dumps(document, indent=2))
    return code


class NoFaultReady(Exception):
    """The real pre-injection callback was reached; there is deliberately no fault."""


def run_lane(run_dir: Path, timeout: int) -> int:
    from benchmarks.sregym.adapter import assert_lane_resources, closure_pending, control_namespace_for, kubectl
    from benchmarks.sregym.fastloop import cli
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

    environment = FastloopEnvironment.load(run_dir.resolve())
    directory = environment.run_dir / "eval-dry-run"
    report_path = directory / "report.json"
    report = DryRunReport(environment.cluster)
    report.save(report_path)
    started = time.monotonic()

    def passed(name: str, **evidence: Any) -> None:
        report.invariants[name] = {"passed": True, **evidence}
        report.seconds = time.monotonic() - started
        report.save(report_path)

    try:
        resources = assert_lane_resources(environment.kubeconfig, app_namespaces={environment.namespace})
        passed("lane_resources", **resources)
        cli._reset(argparse.Namespace(run_dir=environment.run_dir, dev_mount=None))
        passed("restore", source="accepted installed manifests; independent traffic and stable workloads")
        os.environ["KUBECONFIG"] = str(environment.kubeconfig)
        os.environ["SDO_TIME_SCALE"] = "1"
        if environment.dev_mount:
            os.environ["SDO_DEV_MOUNT"] = "1"
            os.environ["SDO_OPERATOR_BIN"] = str(environment.dev_mount / ".fastloop/sdo-operator")
        args = cli.build_parser().parse_args(
            [
                "run",
                "--run-dir",
                str(run_dir),
                "--agent",
                "sdo",
                "--agent-registrations",
                str(cli.REPO_ROOT / "benchmarks/sregym/adapter/scripted_registrations"),
                "--diagnosis-grading",
                "off",
                "--healthy-baseline",
                "--no-checkpoints",
                "--timeout",
                str(timeout),
            ]
        )
        # Leave a bounded share for the one app-only restore and retry.
        remaining = timeout - (time.monotonic() - started) - 5
        args.admission_timeout_seconds = max(1, (remaining - 60) / 2)
        args.admission_progress = passed
        control = control_namespace_for(environment.namespace)
        with cli._worker(environment) as worker:
            agent = cli._sdo_agent(args, environment, directory, worker=worker)

            def no_fault() -> Any:
                onboarding = json.loads(
                    kubectl(
                        ["get", "configmap/sdo-namespace-onboarding", "-o", "json"],
                        namespace=control,
                        kubeconfig=environment.kubeconfig,
                    ).stdout
                )
                record = json.loads(onboarding["data"]["onboarding.json"])
                if not record.get("acceptedCommit") or not record.get("objects"):
                    raise ValueError("eval: native onboarding record is incomplete")
                passed("onboarding", accepted_commit=record["acceptedCommit"])
                for name, filename in (("setup_memory", "setup-lineage.json"), ("rbac_equality", "rbac.json")):
                    path = directory / "admission" / filename
                    value = json.loads(path.read_text())
                    if value.get("passed") is not True:
                        raise ValueError(f"eval: {name} did not pass")
                    passed(name, evidence=str(path))
                passed("stabilization", evidence=str(directory / "admission"))
                passed("probe_placement", namespace="sregym")
                passed("baseline", source="real healthy snapshot capture before the no-fault callback")
                state = json.loads(
                    kubectl(
                        ["get", "configmap/sdo-controller-state", "-o", "json"],
                        namespace=control,
                        kubeconfig=environment.kubeconfig,
                    ).stdout
                )
                if closure_pending(json.loads(state["data"]["runtime-state.json"])):
                    raise ValueError("eval: closure work pending in a no-fault rehearsal")
                passed(
                    "closure_policy",
                    source="no incident or closure pending; unit regression covers early-report ordering",
                )
                raise NoFaultReady

            try:
                agent.resolve(0, "eval-dry-run", no_fault)
            except NoFaultReady:
                report.passed = all(row["passed"] for row in report.invariants.values())
            else:
                raise ValueError("eval: pre-injection callback was not reached")
    except Exception as exc:
        import traceback

        traceback.print_exc()
        report.invariants["failure"] = {"passed": False, "class": "eval", "reason": str(exc)}
    report.seconds = time.monotonic() - started
    report.save(report_path)
    return 0 if report.passed else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--timeout", type=budget, default=300)
    arguments = parser.parse_args()
    raise SystemExit(run_lane(arguments.run_dir, arguments.timeout))
