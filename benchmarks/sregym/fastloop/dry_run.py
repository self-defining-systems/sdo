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
    phase: str = "setup"
    setup_seconds: float = 0
    steady_seconds: float = 0
    steady_started: float | None = None
    fault_injected: bool = False
    live_model_calls: bool = False
    invariants: dict[str, dict[str, Any]] = field(
        default_factory=lambda: {name: {"passed": False, "reason": "not reached"} for name in INVARIANTS}
    )

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(asdict(self), indent=2) + "\n")
        temporary.replace(path)


def budget(value: str) -> int:
    seconds = int(value)
    if not 0 < seconds <= 300:
        raise argparse.ArgumentTypeError("eval dry run budget must be in 1..300 seconds")
    return seconds


def setup_budget(value: str) -> int:
    seconds = int(value)
    if not 0 < seconds <= 900:
        raise argparse.ArgumentTypeError("eval setup budget must be in 1..900 seconds")
    return seconds


def phase_deadline(document: dict[str, Any], started: float, setup_timeout: int, steady_timeout: int) -> float:
    steady_started = document.get("steady_started")
    return float(steady_started) + steady_timeout if steady_started is not None else started + setup_timeout


def execute(
    run_dir: Path,
    timeout: int,
    setup_timeout: int = 900,
    *,
    worker_module: str = __name__,
    directory_name: str = "eval-dry-run",
    extra_arguments: list[str] | None = None,
) -> int:
    directory = run_dir.resolve() / directory_name
    directory.mkdir(parents=True, exist_ok=True)
    report_path = directory / "report.json"
    DryRunReport(str(run_dir)).save(report_path)
    command = [
        sys.executable,
        "-m",
        worker_module,
        "--run-dir",
        str(run_dir),
        "--timeout",
        str(timeout),
        "--setup-timeout",
        str(setup_timeout),
    ]
    command.extend(extra_arguments or [])
    started = time.monotonic()
    with (directory / "worker.log").open("w") as log:
        child = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        while child.poll() is None:
            document = json.loads(report_path.read_text())
            remaining = phase_deadline(document, started, setup_timeout, timeout) - time.monotonic()
            if remaining <= 0:
                os.killpg(child.pid, signal.SIGTERM)
                try:
                    child.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid, signal.SIGKILL)
                    child.wait()
                phase = "setup" if document.get("steady_started") is None else "steady-state"
                document.update(
                    passed=False,
                    error=(
                        f"eval: {document.get('stage', phase)} exceeded its "
                        f"{setup_timeout if phase == 'setup' else timeout}s budget"
                    ),
                )
                report_path.write_text(json.dumps(document, indent=2) + "\n")
                break
            try:
                child.wait(timeout=min(1, remaining))
            except subprocess.TimeoutExpired:
                pass
    document = json.loads(report_path.read_text())
    document["passed"] = document.get("passed") is True and child.returncode == 0
    document["seconds"] = time.monotonic() - started
    if document.get("steady_started") is not None:
        document["steady_seconds"] = time.monotonic() - document["steady_started"]
    else:
        document["setup_seconds"] = document["seconds"]
    report_path.write_text(json.dumps(document, indent=2) + "\n")
    print(json.dumps(document, indent=2))
    return 0 if document.get("passed") is True else 1


class NoFaultReady(Exception):
    """The real pre-injection callback was reached; there is deliberately no fault."""


def prepare_scripted_credentials(kubeconfig: Path, control: str) -> str:
    """Scripted transports need an install mount, never model credentials."""
    from benchmarks.sregym.adapter import kubectl

    kubectl(
        ["apply", "-f", "-"],
        namespace=None,
        kubeconfig=kubeconfig,
        input_text=json.dumps({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": control}}),
    )
    name = "sdo-scripted-credentials"
    result = kubectl(["create", "secret", "generic", name], namespace=control, kubeconfig=kubeconfig, check=False)
    if result.returncode:
        # kubectl's create-secret path does not consistently retain API reason
        # names in stderr. Confirm the exact reserved name using metadata only;
        # never read credential values or infer existence from English errors.
        existing = kubectl(
            ["get", "secret", name, "-o", "name", "--ignore-not-found=true"],
            namespace=control,
            kubeconfig=kubeconfig,
            check=False,
        )
        if existing.returncode or existing.stdout.strip() != f"secret/{name}":
            raise RuntimeError(f"eval: scripted credential mount creation failed: {result.stderr}")
    return name


def run_lane(run_dir: Path, timeout: int, setup_timeout: int = 900) -> int:
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
                str(setup_timeout + timeout),
            ]
        )
        # Setup is separately bounded; only steady proof consumes five minutes.
        args.admission_timeout_seconds = max(1, (timeout - 60) / 2)
        args.admission_progress = passed
        control = control_namespace_for(environment.namespace)
        args.credentials_secret = prepare_scripted_credentials(environment.kubeconfig, control)

        def prepare(control: str, required: set[str]) -> None:
            last_error = "controller has not evaluated its installed checks"
            while time.monotonic() < started + setup_timeout:
                try:
                    state = json.loads(
                        kubectl(
                            ["get", "configmap/sdo-controller-state", "-o", "json"],
                            namespace=control,
                            kubeconfig=environment.kubeconfig,
                        ).stdout
                    )
                    runtime = json.loads(state["data"]["runtime-state.json"])
                    observed = {row.get("detector_id") for row in runtime.get("history") or []}
                    if required and required.issubset(observed):
                        value = json.loads(
                            kubectl(
                                ["get", "configmap/sdo-namespace-onboarding", "-o", "json"],
                                namespace=control,
                                kubeconfig=environment.kubeconfig,
                            ).stdout
                        )
                        record = json.loads(value["data"]["onboarding.json"])
                        if record.get("acceptedCommit") and record.get("objects"):
                            passed("onboarding", accepted_commit=record["acceptedCommit"])
                            report.setup_seconds = time.monotonic() - started
                            report.steady_started = time.monotonic()
                            report.phase = "steady"
                            report.save(report_path)
                            return
                except (RuntimeError, ValueError, KeyError) as exc:
                    last_error = str(exc)
                time.sleep(min(2, max(0, started + setup_timeout - time.monotonic())))
            raise TimeoutError(f"eval: onboarding/setup exceeded {setup_timeout}s: {last_error}")

        args.admission_before_proof = prepare
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
                for name, filename in (
                    ("setup_memory", "setup-lineage.json"),
                    ("rbac_equality", "rbac.json"),
                    ("lane_resources", "lane-resources.json"),
                ):
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
    if report.steady_started is not None:
        report.steady_seconds = time.monotonic() - report.steady_started
    else:
        report.setup_seconds = report.seconds
    report.save(report_path)
    return 0 if report.passed else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--timeout", type=budget, default=300)
    parser.add_argument("--setup-timeout", type=setup_budget, default=900)
    arguments = parser.parse_args()
    raise SystemExit(run_lane(arguments.run_dir, arguments.timeout, arguments.setup_timeout))
