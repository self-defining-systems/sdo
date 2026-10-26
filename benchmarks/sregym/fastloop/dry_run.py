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
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.fastloop.observation_conformance import check_observations
from benchmarks.sregym.fastloop.setup_proof import INVARIANTS

if TYPE_CHECKING:
    from collections.abc import Callable

    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

SCRIPTED_REGISTRATIONS = Path(__file__).resolve().parents[1] / "adapter/scripted_registrations"


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
    fingerprint: dict[str, Any] | None = None
    setup_proof: dict[str, Any] = field(default_factory=dict)
    responder_image: dict[str, Any] = field(default_factory=dict)
    seed: dict[str, Any] = field(default_factory=dict)
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


def execute(run_dir: Path, timeout: int, setup_timeout: int = 900) -> int:
    directory = run_dir.resolve() / "eval-dry-run"
    directory.mkdir(parents=True, exist_ok=True)
    report_path = directory / "report.json"
    DryRunReport(str(run_dir)).save(report_path)
    command = [
        sys.executable,
        "-m",
        __name__,
        "--run-dir",
        str(run_dir),
        "--timeout",
        str(timeout),
        "--setup-timeout",
        str(setup_timeout),
    ]
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
                    error=f"eval: {phase} exceeded its {setup_timeout if phase == 'setup' else timeout}s budget",
                )
                report_path.write_text(json.dumps(document, indent=2) + "\n")
                break
            try:
                child.wait(timeout=min(1, remaining))
            except subprocess.TimeoutExpired:
                pass
    document = json.loads(report_path.read_text())
    document["seconds"] = time.monotonic() - started
    if document.get("steady_started") is not None:
        document["steady_seconds"] = time.monotonic() - document["steady_started"]
    else:
        document["setup_seconds"] = document["seconds"]
    report_path.write_text(json.dumps(document, indent=2) + "\n")
    print(json.dumps(document, indent=2))
    return 0 if document.get("passed") is True else 1


def registration_modules(registrations: Path) -> list[str]:
    """Python modules the registered Jobs run inside the lane's responder image."""
    modules: set[str] = set()
    for path in sorted(registrations.glob("*.json")):
        job = json.loads(path.read_text())["kubernetes_job"]
        if job["image"] != "${RESPONDER_IMAGE}":
            continue
        command = job["command"]
        if "-m" not in command:
            raise ValueError(f"eval: registration {path} does not name a Python module to preflight")
        modules.add(command[command.index("-m") + 1])
    if not modules:
        raise ValueError(f"eval: no responder-image registrations under {registrations}")
    return sorted(modules)


def assert_responder_runs_registrations(
    image: str,
    registrations: Path,
    *,
    source_root: Path | None = None,
    run: Callable[..., Any] = subprocess.run,
) -> dict[str, Any]:
    """Fail in seconds when the responder image cannot import the registered workers.

    Production responder images omit the scripted worker; without this check every
    controller-launched Job fails on import and setup burns its whole budget.
    """
    from benchmarks.sregym.adapter import assert_image_entrypoints

    modules = registration_modules(registrations)
    started = time.monotonic()
    try:
        assert_image_entrypoints(image, modules, source_root=source_root, run=run)
    except ValueError as exc:
        tag = image.rpartition(":")[2] if ":" in image.rpartition("/")[2] else "latest"
        raise ValueError(
            f"eval: responder image {image} cannot run the scripted registrations ({exc}). "
            "Production responder images omit the scripted worker; build the scripted variant from the same "
            f"commit (SDO_SCRIPTED_WORKER=1 SDO_IMAGES=benchmark SDO_IMAGE_TAG={tag}-scripted "
            "bash scripts/build_sdo_images.sh) and point the lane's responder image at it"
        ) from exc
    return {"image": image, "modules": modules, "seconds": round(time.monotonic() - started, 3)}


def assert_seed_supports_scripted_judge(workspace: Path, seed_commit: str | None) -> dict[str, Any]:
    """Fail in seconds when the seed lacks what the scripted health judge reads.

    The scripted judge only retains an accepted bundle (rendered lock, goal and
    judge-owned detectors). A fresh, un-onboarded seed needs a real judge, so
    the rehearsal would otherwise burn its whole setup budget. Reads the seed
    commit the reset restores, never the lane.
    """
    import io
    import tarfile
    import tempfile

    from benchmarks.sregym.adapter import scripted_judge_seed

    started = time.monotonic()
    label = seed_commit[:12] if seed_commit else f"{workspace} working tree"
    try:
        with tempfile.TemporaryDirectory(prefix="sdo-dry-run-seed-") as temporary:
            checkout = Path(temporary)
            if seed_commit:
                archive = subprocess.run(
                    ["git", "-C", str(workspace), "archive", "--format=tar", seed_commit, "--", ".sdo"],
                    capture_output=True,
                    timeout=30,
                )
                if archive.returncode:
                    raise ValueError(f"seed has no accepted .sdo ({archive.stderr.decode().strip()})")
                with tarfile.open(fileobj=io.BytesIO(archive.stdout)) as tar:
                    tar.extractall(checkout, filter="data")
            else:
                checkout = workspace
            seed = scripted_judge_seed(checkout)
    except ValueError as exc:
        raise ValueError(
            f"eval: seed {label} cannot pass the scripted no-fault rehearsal: {exc}. "
            "The scripted health judge only retains an accepted bundle; a fresh, un-onboarded seed needs the "
            "real judge. Skip eval-dry-run for this seed and let the first live row run the production admission "
            "proof (results/<run>/setup-proof.json)"
        ) from exc
    return {
        "seed_commit": seed_commit,
        "toy": seed.toy,
        "detector_ids": list(seed.detector_ids),
        "seconds": round(time.monotonic() - started, 3),
    }


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


def assert_active_intake(environment: FastloopEnvironment) -> None:
    from benchmarks.sregym.adapter import kubectl

    document = json.loads(
        kubectl(
            ["get", "configmap/sdo-controller-maintenance", "-o", "json"],
            namespace=environment.namespace + "-sdo",
            kubeconfig=environment.kubeconfig,
        ).stdout
    )
    if document.get("data", {}).get("state") != "active":
        raise ValueError("eval: setup reuse requires active SDO intake")


def refresh_reused_proof(
    environment: FastloopEnvironment,
    directory: Path,
    report: DryRunReport,
    passed: Callable[..., None],
    timeout: int,
) -> None:
    from benchmarks.sregym.adapter import KubectlClusterOps
    from benchmarks.sregym.fastloop import cli
    from benchmarks.sregym.fastloop.admission import LaneAdmission

    receipt = json.loads(Path(report.setup_proof["receipt"]).read_text())
    report.invariants = {
        name: {**row, "reused_from": report.setup_proof["receipt"]}
        for name, row in receipt["report"]["invariants"].items()
    }
    report.phase = "steady"
    report.steady_started = time.monotonic()
    assert_active_intake(environment)
    with cli._worker(environment) as worker:
        admission = LaneAdmission(
            environment,
            worker,
            KubectlClusterOps(),
            directory / "admission",
            progress=passed,
            timeout_seconds=timeout,
        )
        admission.prove(environment.namespace + "-sdo")
    passed("observation_conformance", **check_observations(environment, directory))
    passed("stabilization", source="fresh live admission", reused_setup=report.setup_proof["receipt"])
    report.passed = True


def run_lane(run_dir: Path, timeout: int, setup_timeout: int = 900) -> int:
    from benchmarks.sregym.adapter import assert_lane_resources, control_namespace_for, incident_work_open, kubectl
    from benchmarks.sregym.fastloop import cli
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment
    from benchmarks.sregym.fastloop.setup_proof import RECEIPTS, check_reuse, collect_fingerprint, save_receipt

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

    os.environ["KUBECONFIG"] = str(environment.kubeconfig)
    os.environ["SDO_TIME_SCALE"] = "1"
    if environment.dev_mount:
        os.environ["SDO_DEV_MOUNT"] = "1"
        os.environ["SDO_OPERATOR_BIN"] = str(environment.dev_mount / ".fastloop/sdo-operator")
    try:
        initial, report.setup_proof = check_reuse(environment)
        report.save(report_path)
        if report.setup_proof["reused"]:
            refresh_reused_proof(environment, directory, report, passed, timeout)
            final = collect_fingerprint(environment)
            if initial != final:
                report.passed = False
                raise ValueError("eval: setup fingerprint changed during fresh admission")
            report.code_revision = final.code_revision
            report.fingerprint = asdict(final)
            report.seconds = time.monotonic() - started
            report.steady_seconds = report.seconds
            report.save(report_path)
            return 0
        # A reused receipt already bound these image digests and accepted memory; a
        # fresh install must not spend its setup budget on Jobs that cannot import
        # their worker or a seed the scripted judge cannot retain.
        report.seed = assert_seed_supports_scripted_judge(environment.workspace, environment.seed_commit)
        report.save(report_path)
        report.responder_image = assert_responder_runs_registrations(
            environment.images.responder, SCRIPTED_REGISTRATIONS, source_root=environment.dev_mount
        )
        report.save(report_path)
        resources = assert_lane_resources(environment.kubeconfig, app_namespaces={environment.namespace})
        passed("lane_resources", **resources)
        cli._reset(argparse.Namespace(run_dir=environment.run_dir, dev_mount=None))
        passed("restore", source="accepted installed manifests; independent traffic and stable workloads")
        args = cli.build_parser().parse_args(
            [
                "run",
                "--run-dir",
                str(run_dir),
                "--agent",
                "sdo",
                "--agent-registrations",
                str(SCRIPTED_REGISTRATIONS),
                "--diagnosis-grading",
                "off",
                "--healthy-baseline",
                "--no-checkpoints",
                "--timeout",
                str(setup_timeout + timeout),
            ]
        )
        # Setup is separately bounded; only steady proof consumes five minutes.
        args.admission_timeout_seconds = timeout
        args.admission_onboarding_timeout_seconds = max(1, started + setup_timeout - time.monotonic())
        args.admission_progress = passed
        control = control_namespace_for(environment.namespace)
        args.credentials_secret = prepare_scripted_credentials(environment.kubeconfig, control)

        def prepare(control: str, onboarding: dict[str, Any]) -> None:
            # Admission has already waited for (and reported) completed onboarding
            # on the setup budget; the steady five-minute clock starts here.
            report.setup_seconds = time.monotonic() - started
            report.steady_started = time.monotonic()
            report.phase = "steady"
            report.save(report_path)

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
                passed("observation_conformance", **check_observations(environment, directory))
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
                if incident_work_open(json.loads(state["data"]["runtime-state.json"])):
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
        report.passed = False
        report.invariants["failure"] = {"passed": False, "class": "eval", "reason": str(exc)}
    report.seconds = time.monotonic() - started
    if report.steady_started is not None:
        report.steady_seconds = time.monotonic() - report.steady_started
    else:
        report.setup_seconds = report.seconds
    if report.passed:
        try:
            fingerprint = collect_fingerprint(environment)
            report.fingerprint = asdict(fingerprint)
            save_receipt(environment.run_dir / RECEIPTS, fingerprint, asdict(report))
        except (OSError, ValueError, KeyError, TypeError, TimeoutError, subprocess.SubprocessError) as exc:
            report.passed = False
            report.invariants["fingerprint"] = {"passed": False, "class": "eval", "reason": str(exc)}
    report.save(report_path)
    return 0 if report.passed else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--timeout", type=budget, default=300)
    parser.add_argument("--setup-timeout", type=setup_budget, default=900)
    arguments = parser.parse_args()
    raise SystemExit(run_lane(arguments.run_dir, arguments.timeout, arguments.setup_timeout))
