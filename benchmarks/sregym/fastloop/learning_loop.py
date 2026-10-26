"""Real-lane, model-free learning-loop proof and fail-closed live admission."""

from __future__ import annotations

import json
import subprocess
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.fastloop.dry_run import code_revision

if TYPE_CHECKING:
    import argparse
    from collections.abc import Callable

    from benchmarks.sregym.fastloop.environment import FastloopEnvironment
    from benchmarks.sregym.fastloop.learning_loop_watch import ValidatorWatch
    from benchmarks.sregym.fastloop.loop import AgentOutcome, IncidentAgent, InjectionWindow

SCHEMA = "sdo.learning-loop-proof/v1"
STAGES = ("injection", "closure", "grade", "publication", "recovery", "repeat")
REPORT = "learning-loop-proof/report.json"


def scripted_agents(args: argparse.Namespace, environment: FastloopEnvironment) -> bool:
    from benchmarks.sregym.fastloop.cli import REPO_ROOT

    registrations = getattr(args, "agent_registrations", None)
    return args.agent == "sdo" and (
        registrations.resolve() == (REPO_ROOT / "benchmarks/sregym/adapter/scripted_registrations").resolve()
        if registrations is not None
        else environment.app == "toy"
    )


def image_identity(environment: FastloopEnvironment) -> dict[str, dict[str, str]]:
    """Resolve the installed image content on every lane node, including mutable tags."""
    nodes = subprocess.run(
        ["kind", "get", "nodes", "--name", environment.cluster],
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    ).stdout.splitlines()
    if not nodes:
        raise ValueError("learning-loop: lane has no nodes")
    result: dict[str, dict[str, str]] = {}
    for role, reference in environment.images.model_dump().items():
        result[role] = {}
        for node in nodes:
            inspected = subprocess.run(
                ["docker", "exec", node, "crictl", "inspecti", reference],
                capture_output=True,
                text=True,
                check=True,
                timeout=10,
            )
            digest = json.loads(inspected.stdout)["status"]["id"]
            if not isinstance(digest, str) or not digest.startswith("sha256:"):
                raise ValueError(f"learning-loop: {role} on {node} has no content digest")
            result[role][node] = digest
    return result


def validate_report(report: dict[str, Any], *, revision: str, lane: str, images: dict[str, Any]) -> None:
    """A proof is usable only for the exact measured launch inputs and every stage."""
    if not isinstance(report, dict):
        raise ValueError("learning-loop: report must be a JSON object")
    expected = {
        "schema_version": SCHEMA,
        "passed": True,
        "code_revision": revision,
        "lane": lane,
        "images": images,
        "live_model_calls": False,
    }
    if revision.endswith("-dirty"):
        raise ValueError("learning-loop: proof requires committed code")
    for key, value in expected.items():
        if (key in {"passed", "live_model_calls"} and report.get(key) is not value) or report.get(key) != value:
            raise ValueError(f"learning-loop: missing or mismatched {key}")
    seconds = report.get("seconds")
    if isinstance(seconds, bool) or not isinstance(seconds, (int, float)) or not 0 < seconds <= 300:
        raise ValueError("learning-loop: proof must complete within 300 seconds")
    stages = report.get("stages")
    for stage in STAGES:
        row = stages.get(stage) if isinstance(stages, dict) else None
        if not isinstance(row, dict) or row.get("passed") is not True or not row.get("evidence"):
            raise ValueError(f"learning-loop: {stage} has no passing evidence")


def require_proof(environment: FastloopEnvironment, path: Path | None = None) -> None:
    path = path or environment.run_dir / REPORT
    command = f"uv run python -m benchmarks.sregym.fastloop learning-loop-proof --run-dir {environment.run_dir}"
    try:
        report = json.loads(path.read_text())
        validate_report(report, revision=code_revision(), lane=environment.cluster, images=image_identity(environment))
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as exc:
        raise ValueError(f"learning-loop preflight refused live run: {exc}; run: {command}") from exc


# The report is persisted after every boundary so the bounded supervisor can
# identify the stage even when Kubernetes never starts a worker/validator Pod.
@dataclass
class StageReceipt:
    passed: bool = False
    evidence: dict[str, Any] = field(default_factory=dict)


@dataclass
class ProofReport:
    lane: str
    code_revision: str
    images: dict[str, dict[str, str]]
    schema_version: str = SCHEMA
    passed: bool = False
    live_model_calls: bool = False
    seconds: float = 0
    stage: str = "setup"
    stages: dict[str, StageReceipt] = field(default_factory=lambda: {name: StageReceipt() for name in STAGES})
    error: str = ""

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(asdict(self), indent=2) + "\n")
        temporary.replace(path)


class ProofAgent:
    """Observe the existing agent protocol; all operations remain on its real path."""

    def __init__(
        self,
        agent: IncidentAgent,
        report: ProofReport,
        path: Path,
        environment: FastloopEnvironment,
        watcher: ValidatorWatch,
        lease: str | None,
    ) -> None:
        self.agent, self.report, self.path, self.environment = agent, report, path, environment
        self.name, self.model = agent.name, agent.model
        self.index = 0
        self.watcher, self.lease = watcher, lease

    def stage(self, name: str) -> None:
        self.report.stage = name
        self.report.save(self.path)

    def passed(self, name: str, **evidence: Any) -> None:
        self.report.stages[name] = StageReceipt(True, evidence)
        self.report.save(self.path)

    def resolve(self, index: int, problem_id: str, inject: Callable[[], InjectionWindow]) -> AgentOutcome:
        self.index = index
        if index == 1 and not self.report.stages["publication"].passed:
            raise ValueError("learning-loop: repeat cannot start before publication")
        self.stage("injection" if index == 0 else "repeat")

        def observed_inject() -> InjectionWindow:
            check_lease(self.environment, self.lease)
            window = inject()
            if index == 0:
                self.passed(
                    "injection", started_at=window.started_at.isoformat(), finished_at=window.finished_at.isoformat()
                )
                self.stage("closure")
            return window

        outcome = self.agent.resolve(index, problem_id, observed_inject)
        if not outcome.incident_id or not outcome.resolved_at or outcome.terminal_error or outcome.error:
            raise ValueError(f"learning-loop: {self.report.stage} has no successful native closure: {outcome.error}")
        if index == 0:
            self.passed("closure", incident_id=outcome.incident_id, verified_at=outcome.resolved_at.isoformat())
            self.stage("grade")
        return outcome

    def learn(self, outcome: AgentOutcome) -> AgentOutcome:
        from benchmarks.sregym.adapter import STRICT_RECEIPT_FILENAME
        from benchmarks.sregym.protocol.native_receipt import validate_native_production_receipt

        if outcome.artifacts_dir is None:
            raise ValueError("learning-loop: missing incident artifact directory")
        directory = Path(outcome.artifacts_dir)
        grade_path = directory.parent / "incidents.grades" / f"{self.index:06d}.json"
        grade = json.loads(grade_path.read_text())
        if grade.get("incident_id") != outcome.incident_id or (grade.get("oracle") or {}).get("success") is not True:
            raise ValueError("learning-loop: grade is not a successful frozen closure grade for this incident")
        if self.index == 0:
            self.passed(
                "grade",
                checkpoint=str(grade_path),
                incident_id=outcome.incident_id,
                graded_at=grade.get("oracle_finished_at"),
            )
            self.stage("publication")
        learned = self.agent.learn(outcome)
        receipt_path = directory / STRICT_RECEIPT_FILENAME
        receipt = json.loads(receipt_path.read_text())
        validate_native_production_receipt(receipt)
        ledger = receipt["native_ledger"]
        if receipt.get("incident_id") != outcome.incident_id or receipt.get("resolution") != "sdo_mitigated":
            raise ValueError("learning-loop: strict receipt does not credit this incident's SDO repair")
        if self.index == 0:
            decision = ledger.get("reflection_decision") or {}
            program_id = "service-targetport-mismatch"
            if decision.get("learning_decision") != "updated" or program_id not in decision.get("playbook_ids", []):
                raise ValueError("learning-loop: scripted reflection did not propose the known executable playbook")
            if (
                not ledger.get("reflection_commit")
                or ledger.get("reflection_rejected")
                or not ledger.get("acknowledged")
            ):
                raise ValueError("learning-loop: broker did not publish and acknowledge reflection")
            tree = subprocess.run(
                ["git", "-C", str(self.environment.workspace), "rev-parse", ledger["reflection_commit"] + "^{tree}"],
                check=True,
                text=True,
                capture_output=True,
                timeout=5,
            ).stdout.strip()
            jobs = self.watcher.jobs()
            jobs_path = directory / "learning-loop-validator-jobs.json"
            jobs_path.write_text(json.dumps(jobs, indent=2) + "\n")
            completed = validator_jobs(jobs, repository_pvc="sdo-application-repository", candidate_tree=tree)
            if not completed:
                raise ValueError(
                    "learning-loop: no successful real validator Job on repository PVC with healthy baseline"
                )
            self.passed(
                "publication",
                receipt=str(receipt_path),
                reflection_commit=ledger["reflection_commit"],
                playbook_id=program_id,
                validator_jobs=completed,
                jobs=str(jobs_path),
            )
            self.stage("recovery")
        else:
            document = ledger.get("outcome_document") or {}
            program_id = "service-targetport-mismatch"
            applications = [
                item
                for item in document.get("plan_applications", [])
                if program_id in item.get("program_ids", []) and item.get("verification_passed") is True
            ]
            if program_id not in document.get("applied_playbooks", []) or not applications:
                raise ValueError("learning-loop: repeat receipt lacks verified application of the published playbook")
            self.passed(
                "repeat",
                receipt=str(receipt_path),
                incident_id=outcome.incident_id,
                applications=applications,
                grade=str(grade_path),
            )
        return learned

    def prepare_recovery(self, outcome: AgentOutcome | None) -> None:
        check_lease(self.environment, self.lease)
        self.agent.prepare_recovery(outcome)
        if self.index == 0 and outcome is not None:
            self.passed(
                "recovery",
                incident_id=outcome.incident_id,
                intake="paused-generation acknowledged; fresh idle snapshot",
            )

    def close(self) -> None:
        self.agent.close()


def validator_jobs(document: dict[str, Any], *, repository_pvc: str, candidate_tree: str) -> list[str]:
    """Require an executed validator using one PVC volume and two read-only mounts."""
    result = []
    for job in document.get("items", []):
        spec = job.get("spec", {}).get("template", {}).get("spec", {})
        containers = [item for item in spec.get("containers", []) if item.get("name") == "validator"]
        claims = [
            item
            for item in spec.get("volumes", [])
            if item.get("persistentVolumeClaim", {}).get("claimName") == repository_pvc
        ]
        if len(claims) != 1 or not containers or job.get("status", {}).get("succeeded") != 1:
            continue
        if not any(
            item.get("name") == "SDO_CANDIDATE_TREE" and item.get("value") == candidate_tree
            for item in containers[0].get("env", [])
        ):
            continue
        mounts = containers[0].get("volumeMounts", [])
        if all(
            any(
                m.get("name") == claims[0]["name"] and m.get("readOnly") is True and m.get("mountPath") == path
                for m in mounts
            )
            for path in ("/workspace", "/sdo/healthy-baseline")
        ):
            result.append(job["metadata"]["name"])
    return result


def check_lease(environment: FastloopEnvironment, lease: str | None) -> None:
    if lease:
        subprocess.run(["coord", "check", environment.cluster, "--lease", lease], check=True, timeout=10)
        subprocess.run(["coord", "gate", "--lease", lease], check=True, timeout=10)


def execute(run_dir: Path, timeout: int, lease: str | None = None) -> int:
    """Reuse the dry-run process supervisor, with one total five-minute budget."""
    from benchmarks.sregym.fastloop import dry_run

    result = dry_run.execute(
        run_dir,
        timeout,
        timeout,
        worker_module=__name__,
        directory_name="learning-loop-proof",
        extra_arguments=["--lease", lease] if lease else [],
    )
    if result:
        # SIGKILL may have cut off the worker's finally block. The supervisor
        # owns the same supported stop path and records evidence before stopping.
        import os

        from benchmarks.sregym.adapter import control_namespace_for, kubectl, stop_runtime
        from benchmarks.sregym.fastloop.environment import FastloopEnvironment

        fixture = run_dir.resolve() / "learning-loop-proof/environment"
        if not (fixture / "fastloop.json").is_file():
            return result
        environment = FastloopEnvironment.load(fixture)
        os.environ["KUBECONFIG"] = str(environment.kubeconfig)
        if environment.dev_mount:
            os.environ["SDO_OPERATOR_BIN"] = str(environment.dev_mount / ".fastloop/sdo-operator")
        path = run_dir.resolve() / REPORT
        report = json.loads(path.read_text())
        try:
            snapshot = kubectl(
                ["get", "jobs,pods,events", "-o", "json"],
                namespace=control_namespace_for(environment.namespace),
                kubeconfig=environment.kubeconfig,
            )
            snapshot_path = path.parent / "supervisor-failure-kubernetes.json"
            snapshot_path.write_text(snapshot.stdout)
            report["failure_evidence"] = str(snapshot_path)
            check_lease(environment, lease)
            report["supervisor_stop"] = stop_runtime(
                control_namespace_for(environment.namespace), kubeconfig=str(environment.kubeconfig)
            )
        except Exception as exc:
            report["supervisor_stop_error"] = str(exc)
        report["passed"] = False
        path.write_text(json.dumps(report, indent=2) + "\n")
    return result


def run_lane(run_dir: Path, timeout: int, lease: str | None = None) -> int:
    import argparse
    import os
    import signal

    from benchmarks.sregym.adapter import control_namespace_for
    from benchmarks.sregym.fastloop import cli
    from benchmarks.sregym.fastloop.dry_run import prepare_scripted_credentials
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

    environment = FastloopEnvironment.load(run_dir.resolve())
    path = environment.run_dir / REPORT
    report = ProofReport(environment.cluster, code_revision(), {})
    report.save(path)
    started = time.monotonic()
    isolated = False

    def deadline(_signum: int, _frame: Any) -> None:
        raise TimeoutError(f"learning-loop: {report.stage} exceeded {timeout}s total budget")

    signal.signal(signal.SIGTERM, deadline)
    try:
        cli.up_preflight(cli.build_parser().parse_args(["preflight", "--dev-mount", str(cli.REPO_ROOT)]))
        from benchmarks.sregym.fastloop.source_preflight import assert_runtime_images

        assert_runtime_images(environment.images, cli.REPO_ROOT)
        if not cli.node_images_match(environment.cluster, environment.images):
            raise ValueError("eval: lane image source differs from checked local images; redeliver with fastloop up")
        report.images = image_identity(environment)
        report.save(path)
        os.environ["KUBECONFIG"] = str(environment.kubeconfig)
        if environment.dev_mount:
            if environment.dev_mount.resolve() != cli.REPO_ROOT.resolve():
                raise ValueError("learning-loop: dev mount must be the command's exact worktree")
            os.environ["SDO_DEV_MOUNT"] = "1"
            os.environ["SDO_OPERATOR_BIN"] = str(environment.dev_mount / ".fastloop/sdo-operator")
        check_lease(environment, lease)
        from benchmarks.sregym.fastloop.learning_loop_fixture import prepare

        environment = prepare(environment, cli.REPO_ROOT)
        isolated = True
        os.environ["SDO_DEV_MOUNT"] = "1"
        os.environ["SDO_OPERATOR_BIN"] = str(cli.REPO_ROOT / ".fastloop/sdo-operator")
        check_lease(environment, lease)
        cli._reset(argparse.Namespace(run_dir=environment.run_dir, dev_mount=None, restart_toy=False))
        args = cli.build_parser().parse_args(
            [
                "run",
                "--run-dir",
                str(environment.run_dir),
                "--agent",
                "sdo",
                "--run-id",
                f"learning-loop-proof-{time.time_ns()}",
                "--agent-registrations",
                str(cli.REPO_ROOT / "benchmarks/sregym/adapter/scripted_registrations"),
                "--diagnosis-grading",
                "off",
                "--healthy-baseline",
                "--no-checkpoints",
                "--time-scale",
                "0.1",
                "--timeout",
                "120",
                "--incidents",
                "2",
                "--require-faulty-oracle",
                "--problem",
                "toy_target_port_backend"
                if environment.app == "toy"
                else "k8s_target_port_misconfig_hotel_reservation__v_reservation_9999",
            ]
        )
        args.credentials_secret = prepare_scripted_credentials(
            environment.kubeconfig, control_namespace_for(environment.namespace)
        )
        args.admission_timeout_seconds = min(60, timeout / 3)
        os.environ["KUBECONFIG"] = str(environment.kubeconfig)
        check_lease(environment, lease)
        from benchmarks.sregym.fastloop.learning_loop_watch import ValidatorWatch

        with ValidatorWatch(
            environment.kubeconfig, control_namespace_for(environment.namespace), path.parent / "validator-events.jsonl"
        ) as watcher:
            args.agent_wrapper = lambda agent: ProofAgent(agent, report, path, environment, watcher, lease)
            result = cli._run(args)
        report.seconds = time.monotonic() - started
        if result != 0:
            raise ValueError(
                f"learning-loop: {report.stage} failed; inspect {environment.run_dir / 'results/learning-loop-proof'}"
            )
        from benchmarks.sregym.fastloop.records import load_records

        records_path = environment.run_dir / "results" / args.run_id / "incidents.jsonl"
        records = load_records(records_path)
        if len(records) != 2 or any(
            record.error
            or record.recovery_finished_at is None
            or record.recovery_oracle is None
            or not record.recovery_oracle.success
            for record in records
        ):
            raise ValueError("learning-loop: both encounters require successful completed recovery receipts")
        recovered_at = records[0].recovery_finished_at
        assert recovered_at is not None
        report.stages["recovery"].evidence.update(records=str(records_path), recovered_at=recovered_at.isoformat())
        report.passed = True
        validate_report(
            asdict(report), revision=code_revision(), lane=environment.cluster, images=image_identity(environment)
        )
    except Exception as exc:
        import traceback

        traceback.print_exc()
        report.passed = False
        report.error = f"learning-loop: {report.stage}: {exc}"
        from benchmarks.sregym.adapter import kubectl

        evidence_path = path.parent / "failure-kubernetes.json"
        try:
            snapshot = kubectl(
                ["get", "jobs,pods,events", "-o", "json"],
                namespace=control_namespace_for(environment.namespace),
                kubeconfig=environment.kubeconfig,
            )
            evidence_path.write_text(snapshot.stdout)
            report.stages.setdefault(report.stage, StageReceipt()).evidence.update(snapshot=str(evidence_path))
        except Exception as evidence_error:
            report.error += f"; evidence capture: {evidence_error}"
    finally:
        report.seconds = time.monotonic() - started
        report.save(path)
        # The next live run must start with its selected registrations and prove
        # fresh admission; never leave the scripted controller operating the app.
        check_lease(environment, lease)
        from benchmarks.sregym.adapter import stop_runtime

        try:
            stopped = (
                stop_runtime(control_namespace_for(environment.namespace), kubeconfig=str(environment.kubeconfig))
                if isolated
                else {"removed": [], "leftovers": []}
            )
            if stopped["leftovers"]:
                raise ValueError(f"runtime stop left {stopped['leftovers']}")
        except Exception as stop_error:
            report.passed = False
            report.error += f"; shutdown: {stop_error}"
        report.seconds = time.monotonic() - started
        if report.seconds > timeout:
            report.passed = False
            report.error += "; learning-loop exceeded total budget including shutdown"
        report.save(path)
    return 0 if report.passed else 1


if __name__ == "__main__":
    import argparse

    from benchmarks.sregym.fastloop.dry_run import budget

    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--timeout", type=budget, default=300)
    parser.add_argument("--setup-timeout", type=int, default=300)
    parser.add_argument("--lease")
    arguments = parser.parse_args()
    raise SystemExit(run_lane(arguments.run_dir, arguments.timeout, arguments.lease))
