"""Fast inner-loop CLI: ``uv run python -m benchmarks.sregym.fastloop <command>``.

    up       create or reuse a kind cluster, deploy shared infrastructure and the
             application once, and record the warm environment
    run      loop N incidents with SDO's persistent controller or the raw Codex baseline
    summary  per-agent and per-incident timings, tokens, warm path, and reflection
    down     drain and remove the persistent SDO controller; optionally delete the cluster

Example::

    uv run python -m benchmarks.sregym.fastloop up --run-dir ../fastloop-runs/dev \\
        --seed ../fastloop-runs/seeds/lifecycle-873a9a9
    uv run python -m benchmarks.sregym.fastloop run --run-dir ../fastloop-runs/dev --agent sdo -n 3
    uv run python -m benchmarks.sregym.fastloop run --run-dir ../fastloop-runs/dev --agent codex -n 1
    uv run python -m benchmarks.sregym.fastloop summary ../fastloop-runs/dev
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import socket
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from benchmarks.sregym.adapter import kubectl
from benchmarks.sregym.fastloop.environment import DEFAULT_PROBLEM, FastloopEnvironment, Images
from benchmarks.sregym.fastloop.fault_driver import SregymFaultDriver
from benchmarks.sregym.fastloop.loop import LoopConfig, run_incidents
from benchmarks.sregym.fastloop.records import AgentSummary, IncidentRecord, load_records, summarize
from benchmarks.sregym.fastloop.stream import load_stream_config
from benchmarks.sregym.fastloop.worker_client import SregymWorker, worker_argv

if TYPE_CHECKING:
    from benchmarks.sregym.adapter import ClusterOps
    from benchmarks.sregym.fastloop.composite import CompositeSettings
    from benchmarks.sregym.fastloop.fault_tracker import FaultSpec
    from benchmarks.sregym.fastloop.loop import DiagnosisGrader, IncidentAgent

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SREGYM_DIR = REPO_ROOT / "third_party" / "sregym"
INCIDENTS_FILENAME = "incidents.jsonl"
CLUSTER_OP_TIMEOUT_SECONDS = 1800.0
DEPLOY_OP_TIMEOUT_SECONDS = 2400.0


def _worker(environment: FastloopEnvironment) -> SregymWorker:
    return SregymWorker(
        worker_argv(environment.sregym_dir, private_tmp=environment.private_tmp),
        env=environment.worker_process_env(),
        log_path=environment.run_dir / "worker.log",
    )


def _ensure_builder(name: str) -> None:
    inspected = subprocess.run(["docker", "buildx", "inspect", name], capture_output=True, text=True, check=False)
    if inspected.returncode != 0:
        subprocess.run(["docker", "buildx", "create", "--name", name, "--driver", "docker-container"], check=True)


def node_images_match(cluster: str, images: Images) -> bool:
    """Reuse completed kind preflight only while every node has the same image content."""

    nodes = subprocess.run(["kind", "get", "nodes", "--name", cluster], capture_output=True, text=True, check=False)
    if nodes.returncode != 0 or not nodes.stdout.strip():
        return False
    for image in (images.controller, images.responder, images.validator):
        local = subprocess.run(
            ["docker", "image", "inspect", "--format", "{{.Id}}", image],
            capture_output=True,
            text=True,
            check=False,
        )
        if local.returncode != 0 or not local.stdout.strip():
            return False
        for node in nodes.stdout.splitlines():
            installed = subprocess.run(
                ["docker", "exec", node, "crictl", "inspecti", image],
                capture_output=True,
                text=True,
                check=False,
            )
            if installed.returncode != 0:
                return False
            try:
                digest = json.loads(installed.stdout)["status"]["id"]
            except (ValueError, KeyError, TypeError):
                return False
            if digest != local.stdout.strip():
                return False
    return True


def up_worker_environment(args: argparse.Namespace, *, workspace: Path) -> dict[str, str]:
    """The SREGym worker environment for ``up``: one reused kind lane deploying ``workspace`` from source."""

    images = Images(controller=args.controller_image, responder=args.responder_image, validator=args.validator_image)
    worker_env = {
        "SREGYM_KIND_CLUSTER_PREFIX": args.cluster_prefix,
        "SREGYM_KIND_CLUSTER_NAME": f"{args.cluster_prefix}{args.worker_id}",
        "SREGYM_KIND_REQUIRE_NETWORK_POLICY": "1",
        "SREGYM_KIND_NETWORK_POLICY_CANARY_IMAGE": images.validator,
        "SREGYM_KIND_REQUIRED_IMAGES": json.dumps([images.controller, images.responder, images.validator]),
        "SREGYM_REUSE_CLUSTER": "1",
        "SREGYM_WORKER_CPU_LIMIT": args.cpu_limit,
        "SREGYM_DEPLOY_FROM_SOURCE": "1",
        "SREGYM_APP_SOURCE_DIR": str(workspace),
        "SREGYM_PRESERVE_INFRASTRUCTURE": "1",
        # `up --redeploy` reuses the app image while its build context is unchanged.
        "SREGYM_SOURCE_BUILD_CACHE": "1",
        # One control plane plus this many workers, as the experiment lanes (`kind_worker_nodes`).
        "SREGYM_KIND_WORKER_NODES": str(args.kind_worker_nodes),
    }
    if args.builder:
        worker_env["SREGYM_DOCKER_BUILDER"] = args.builder
    return worker_env


def _up(args: argparse.Namespace) -> int:
    if args.kind_worker_nodes < 0:
        raise SystemExit("--kind-worker-nodes must not be negative")
    run_dir = args.run_dir.resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    workspace = run_dir / "application_workspace"
    if not workspace.exists():
        if args.seed is None:
            raise SystemExit("--seed is required the first time (a git repository with the application source)")
        subprocess.run(["git", "clone", "--quiet", str(args.seed.resolve()), str(workspace)], check=True)
    private_tmp = None if args.no_sandbox else run_dir / "tmp"
    if private_tmp is not None:
        private_tmp.mkdir(exist_ok=True)
    images = Images(controller=args.controller_image, responder=args.responder_image, validator=args.validator_image)
    worker_env = up_worker_environment(args, workspace=workspace)
    if not args.redeploy and (run_dir / "fastloop.json").is_file():
        previous = FastloopEnvironment.load(run_dir)
        if (
            previous.cluster == worker_env["SREGYM_KIND_CLUSTER_NAME"]
            and previous.images == images
            and previous.worker_env == worker_env
            and previous.deploy_problem == args.problem
            and previous.kubeconfig.is_file()
            and node_images_match(previous.cluster, images)
        ):
            with _worker(previous) as cached_worker:
                health = cached_worker.request("health", namespace=previous.namespace, timeout=60)
            if health.get("healthy"):
                print(f"fastloop environment ready: reused verified node image digests -> {previous.path}")
                return 0
    if args.builder:
        _ensure_builder(args.builder)
    placeholder = FastloopEnvironment(
        run_dir=run_dir,
        cluster=worker_env["SREGYM_KIND_CLUSTER_NAME"],
        kubeconfig=run_dir / "kubeconfigs" / f"worker_{args.worker_id}.kubeconfig",
        namespace="",
        application="",
        workspace=workspace,
        sregym_dir=args.sregym_dir.resolve(),
        private_tmp=private_tmp,
        deploy_problem=args.problem,
        images=images,
        worker_env=worker_env,
    )
    worker = SregymWorker(
        worker_argv(placeholder.sregym_dir, private_tmp=private_tmp),
        env={**worker_env, "PYTHONUNBUFFERED": "1"},
        log_path=run_dir / "worker.log",
    )
    with worker:
        cluster = worker.request(
            "cluster", worker_id=args.worker_id, log_dir=str(run_dir), timeout=CLUSTER_OP_TIMEOUT_SECONDS
        )
        deployed = worker.request(
            "deploy",
            problem_id=args.problem,
            baseline_path=str(placeholder.baseline_path),
            redeploy=args.redeploy,
            timeout=DEPLOY_OP_TIMEOUT_SECONDS,
        )
        app = worker.request("app_info", problem_id=args.problem)
    environment = FastloopEnvironment.model_validate(
        {
            **placeholder.model_dump(),
            "kubeconfig": Path(str(cluster["kubeconfig"])),
            "namespace": str(app["namespace"]),
            "application": str(app["app_name"]),
            "description": str(app["descriptions"]),
        }
    )
    environment.save()
    print(
        f"fastloop environment ready: cluster={environment.cluster} namespace={environment.namespace} "
        f"deployed={deployed.get('deployed')} ({float(deployed.get('seconds', 0.0)):.0f}s) -> {environment.path}"
    )
    return 0


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def _composite_settings(args: argparse.Namespace) -> CompositeSettings:
    from benchmarks.sregym.fastloop.composite import CompositeSettings

    return CompositeSettings(
        deadline_seconds=args.composite_deadline,
        idle_seconds=args.composite_idle,
        stop_on_detector_review=not args.no_stop_on_review,
    )


def _is_composite(problems: tuple[str, ...]) -> bool:
    from benchmarks.sregym.fastloop.fault_tracker import composite_faults

    flags = {bool(composite_faults(problem)) for problem in problems}
    if len(flags) > 1:
        raise SystemExit("do not mix composite and single-fault problems in one run")
    return flags == {True}


def _cluster_ops(args: argparse.Namespace, namespace: str) -> ClusterOps:
    """Cluster operations for the SDO agent, with the opt-in harness wrappers applied."""

    from benchmarks.sregym.adapter import KubectlClusterOps, kubectl_capture_ops

    ops: ClusterOps = KubectlClusterOps()
    if args.inject_before_resume:
        from benchmarks.sregym.fastloop.simultaneous import InjectBeforeResumeOps

        ops = InjectBeforeResumeOps(ops)  # type: ignore[assignment]
    if args.healthy_baseline:
        ops = kubectl_capture_ops(ops, namespace=namespace, kubectl_runner=kubectl)  # type: ignore[assignment]
    return ops


def _sdo_agent(
    args: argparse.Namespace,
    environment: FastloopEnvironment,
    results_dir: Path,
    *,
    composite: bool = False,
    stream_faults: dict[str, tuple[FaultSpec, ...]] | None = None,
) -> IncidentAgent:
    from benchmarks.sregym.adapter import (
        RuntimeConfig,
        control_namespace_for,
        deployed_lifecycle,
        launch_native_lifecycle,
    )
    from benchmarks.sregym.fastloop.sdo_agent import SdoAgentSettings, SdoPersistentAgent

    runtime_config = RuntimeConfig(
        repository=environment.workspace,
        namespace=environment.namespace,
        application=environment.application,
        controller_image=environment.images.controller,
        responder_image=environment.images.responder,
        validator_image=environment.images.validator,
        repository_pvc="sdo-application-repository",
        credentials_secret="sdo-agent-credentials",
        model=args.model,
        timeout_seconds=args.timeout,
        repair_policy=args.repair_policy,
        agent_provider=args.provider,
        reflection_session=args.reflection_session,
        reflection_guidance=args.reflection_guidance,
        late_findings=args.late_findings,
        max_follow_ups=args.max_follow_ups,
        follow_up_cooldown_seconds=args.follow_up_cooldown_seconds,
        healthy_baseline=args.healthy_baseline,
        controller_namespace=control_namespace_for(environment.namespace),
        kind_cluster_name=environment.cluster,
        persistent=True,
    )
    settings = SdoAgentSettings(
        repository=environment.workspace,
        namespace=environment.namespace,
        application=environment.application,
        runtime_config=runtime_config,
        state_path=environment.state_path,
        results_dir=results_dir,
        kubeconfig=str(environment.kubeconfig),
        verification_timeout_seconds=float(args.composite_deadline if composite else args.timeout + 300),
    )
    ops = _cluster_ops(args, environment.namespace)

    def start_native(context: object) -> bool:
        launch_native_lifecycle(runtime_config, context.health_objective, "")  # type: ignore[attr-defined]
        return False

    agent_arguments = {
        "ops": ops,
        "lifecycle_inputs": lambda: deployed_lifecycle(environment.namespace),
        "run_lifecycle": start_native,
    }
    if composite:
        from benchmarks.sregym.fastloop.composite import CompositeSdoAgent, reader_for

        return CompositeSdoAgent(
            settings,
            **agent_arguments,
            reader=reader_for(environment.namespace, environment.kubeconfig),
            report_dir=results_dir,
            composite=_composite_settings(args),
            stream_faults=stream_faults,
        )
    return SdoPersistentAgent(settings, **agent_arguments)


def _run(args: argparse.Namespace) -> int:
    environment = FastloopEnvironment.load(args.run_dir.resolve())
    os.environ["KUBECONFIG"] = str(environment.kubeconfig)
    run_id = args.run_id or f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{args.agent}"
    results_dir = environment.run_dir / "results" / run_id
    stream_config = load_stream_config(args.stream) if args.stream else None
    stream = stream_config.incidents if stream_config else ()
    grading_mode = args.diagnosis_grading or (stream_config.diagnosis_grading if stream_config else "off")
    judge_model_id = args.judge_model_id or (stream_config.judge_model_id if stream_config else "codex-gpt-6-luna")
    from benchmarks.sregym.fastloop.grading import SregymDiagnosisGrader

    grader = SregymDiagnosisGrader(environment.sregym_dir, judge_model_id) if grading_mode != "off" else None
    problems = tuple(args.problem or [environment.deploy_problem]) if not stream else tuple(ids[0] for ids in stream)
    if stream:
        from benchmarks.sregym.fastloop.fault_tracker import composite_faults, describe_sregym_faults, stream_faults

        tracked = [ids for ids in stream if len(ids) > 1 or ids[0].startswith("mf__")]
        tracked_ids = tuple(dict.fromkeys(problem_id for ids in tracked for problem_id in ids))
        catalog = describe_sregym_faults(tracked_ids, sregym_dir=environment.sregym_dir)
        faults_by_incident = {"+".join(ids): stream_faults(ids, catalog=catalog) for ids in tracked}
        registered_composites = bool(faults_by_incident) or any(
            composite_faults(ids[0]) for ids in stream if len(ids) == 1
        )
    else:
        faults_by_incident = {}
        registered_composites = _is_composite(problems)
    config = LoopConfig(
        run_id=run_id,
        problems=problems,
        incidents=len(stream) if stream else args.incidents,
        results_path=results_dir / INCIDENTS_FILENAME,
        stream=stream,
    )
    with _worker(environment) as worker:
        worker.request(
            "deploy",
            problem_id=environment.deploy_problem,
            baseline_path=str(environment.baseline_path),
            timeout=DEPLOY_OP_TIMEOUT_SECONDS,
        )
        driver = SregymFaultDriver(worker, namespace=environment.namespace, health_timeout_seconds=args.health_timeout)
        if args.agent == "sdo":
            records = run_incidents(
                _sdo_agent(
                    args,
                    environment,
                    results_dir,
                    composite=registered_composites,
                    stream_faults=faults_by_incident,
                ),
                driver,
                config,
                grader=grader,
                diagnosis_grading=grading_mode,
            )
        else:
            records = _run_codex(
                args, environment, results_dir, worker, driver, config, grader, grading_mode, faults_by_incident
            )
    _print_summary(records)
    print(f"results: {config.results_path}")
    return (
        0
        if records
        and all(
            record.error is None
            and (grading_mode == "off" or (record.diagnosis_grade is not None and record.diagnosis_grade.error is None))
            for record in records
        )
        else 1
    )


def _run_codex(
    args: argparse.Namespace,
    environment: FastloopEnvironment,
    results_dir: Path,
    worker: SregymWorker,
    driver: SregymFaultDriver,
    config: LoopConfig,
    grader: DiagnosisGrader | None = None,
    grading_mode: str = "off",
    stream_faults: dict[str, tuple[FaultSpec, ...]] | None = None,
) -> list[IncidentRecord]:
    from benchmarks.sregym.fastloop.codex_agent import CodexBaselineAgent, CodexSettings, SubmissionStub

    auth = Path(os.environ.get("CODEX_HOME", Path.home() / ".codex")) / "auth.json"
    if not auth.is_file():
        raise SystemExit(f"{auth} is required for the Codex baseline")
    # The worker writes the agent kubeconfig here before the first incident creates the directory.
    results_dir.mkdir(parents=True, exist_ok=True)
    proxy = worker.request(
        "proxy",
        port=args.proxy_port or _free_port(),
        hide=[f"{environment.namespace}-sdo"],
        kubeconfig=str(results_dir / "agent.kubeconfig"),
    )
    settings = CodexSettings(
        model=args.model,
        auth_file=auth,
        kubeconfig=Path(str(proxy["kubeconfig"])),
        results_dir=results_dir,
        timeout_seconds=float(args.timeout),
        reasoning_effort=args.reasoning_effort,
    )
    app_info = {
        "app_name": environment.application,
        "namespace": environment.namespace,
        "descriptions": environment.description,
    }
    with SubmissionStub(app_info=app_info) as stub:
        agent = CodexBaselineAgent(
            settings,
            stub=stub,
            prompt_for=lambda problem_id, api_base: str(
                worker.request("codex_prompt", problem_id=problem_id.split("+", 1)[0], api_base=api_base)["prompt"]
            ),
        )
        if config.stream:
            from benchmarks.sregym.fastloop.fault_tracker import composite_faults

            registered = bool(stream_faults) or any(composite_faults(ids[0]) for ids in config.stream if len(ids) == 1)
        else:
            registered = _is_composite(config.problems)
        if registered:
            from benchmarks.sregym.fastloop.composite import TrackedAgent, reader_for

            agent = TrackedAgent(  # type: ignore[assignment]
                agent,
                reader=reader_for(environment.namespace, environment.kubeconfig),
                report_dir=results_dir,
                settings=_composite_settings(args),
                stream_faults=stream_faults,
            )
        return run_incidents(agent, driver, config, grader=grader, diagnosis_grading=grading_mode)


def _collect(paths: list[Path]) -> list[IncidentRecord]:
    records: list[IncidentRecord] = []
    for path in paths:
        files = [path] if path.is_file() else sorted(path.rglob(INCIDENTS_FILENAME))
        for file in files:
            records.extend(load_records(file))
    return records


def _cell(value: object) -> str:
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, float):
        return f"{value:.1f}"
    return str(value)


def format_incidents(records: list[IncidentRecord]) -> str:
    header = (
        "run_id",
        "#",
        "agent",
        "problem",
        "oracle",
        "inj->det_s",
        "det->mit_s",
        "inj->mit_s",
        "inj->verified_s",
        "gate_s",
        "reflection_s",
        "wall_s",
        "resp_tok",
        "refl_tok",
        "warm",
        "error",
    )
    return _table([header, *(_incident_cells(record) for record in records)])


def _incident_cells(record: IncidentRecord) -> tuple[str, ...]:
    return tuple(
        _cell(value)
        for value in (
            record.run_id,
            record.index,
            record.agent,
            record.problem_id,
            record.oracle.success if record.oracle else None,
            record.injection_to_detection_seconds,
            record.detection_to_mitigation_seconds,
            record.injection_to_mitigation_seconds,
            record.injection_to_resolution_seconds,
            record.baseline_gate_seconds,
            record.reflection_seconds,
            record.incident_wall_seconds,
            record.responder_tokens.total_tokens,
            record.reflection_tokens.total_tokens,
            record.warm_path,
            (record.error or "")[:60] or None,
        )
    )


def format_summary(records: list[IncidentRecord]) -> str:
    header = (
        "agent",
        "n",
        "oracle_pass",
        "errors",
        "warm",
        "med_wall_s",
        "med_inj->det_s",
        "med_det->mit_s",
        "med_inj->mit_s",
        "med_reflection_s",
        "med_resp_tok",
        "med_refl_tok",
    )
    return _table([header, *(_summary_cells(row) for row in summarize(records))])


def _summary_cells(row: AgentSummary) -> tuple[str, ...]:
    return tuple(
        _cell(value)
        for value in (
            row.agent,
            row.incidents,
            row.oracle_passed,
            row.errors,
            row.warm_path_fired,
            row.median_incident_wall_seconds,
            row.median_injection_to_detection_seconds,
            row.median_detection_to_mitigation_seconds,
            row.median_injection_to_mitigation_seconds,
            row.median_reflection_seconds,
            row.median_responder_tokens,
            row.median_reflection_tokens,
        )
    )


def _table(rows: list[tuple[str, ...]]) -> str:
    widths = [max(len(row[column]) for row in rows) for column in range(len(rows[0]))]
    return "\n".join("  ".join(cell.ljust(width) for cell, width in zip(row, widths, strict=True)) for row in rows)


def _print_summary(records: list[IncidentRecord]) -> None:
    print(format_incidents(records))
    print()
    print(format_summary(records))


def _summary(args: argparse.Namespace) -> int:
    records = _collect(args.paths)
    if not records:
        print("no fast-loop incidents found", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps([row.model_dump() for row in summarize(records)], indent=2))
    else:
        _print_summary(records)
    return 0


def _down(args: argparse.Namespace) -> int:
    from benchmarks.sregym.adapter import KubectlClusterOps, teardown

    environment = FastloopEnvironment.load(args.run_dir.resolve())
    os.environ["KUBECONFIG"] = str(environment.kubeconfig)
    errors = teardown(environment.state_path, ops=KubectlClusterOps()) if environment.state_path.exists() else []
    for error in errors:
        logger.error("persistent controller teardown: %s", error)
    if args.delete_cluster:
        subprocess.run(["kind", "delete", "cluster", "--name", environment.cluster], check=True)
    return 1 if errors else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="fastloop", description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    commands = parser.add_subparsers(dest="command", required=True)

    up = commands.add_parser("up", help="create or reuse the cluster and deploy the application once")
    up.add_argument("--run-dir", type=Path, required=True)
    up.add_argument("--seed", type=Path, help="git repository cloned as the application workspace (first time only)")
    up.add_argument("--problem", default=DEFAULT_PROBLEM, help="problem whose application is deployed")
    up.add_argument("--cluster-prefix", default="fastloop-w")
    up.add_argument("--worker-id", type=int, default=0)
    up.add_argument("--cpu-limit", default="3", help="docker --cpus per kind node, as the benchmark workers use")
    up.add_argument(
        "--kind-worker-nodes",
        type=int,
        default=1,
        help="kind worker nodes beside the control plane (default 1, as every experiment lane)",
    )
    up.add_argument("--builder", default="fastloop", help="private buildx builder for the application image")
    up.add_argument("--controller-image", default=Images().controller)
    up.add_argument("--responder-image", default=Images().responder)
    up.add_argument("--validator-image", default=Images().validator)
    up.add_argument("--sregym-dir", type=Path, default=DEFAULT_SREGYM_DIR)
    up.add_argument("--redeploy", action="store_true", help="redeploy even if the application is healthy")
    up.add_argument(
        "--no-sandbox",
        action="store_true",
        help="run the SREGym worker without a private /tmp (unsafe next to other SREGym runs)",
    )
    up.set_defaults(handler=_up)

    run = commands.add_parser("run", help="loop incidents against the warm environment")
    run.add_argument("--run-dir", type=Path, required=True)
    run.add_argument("--agent", choices=("sdo", "codex"), required=True)
    run.add_argument("--problem", action="append", help="problem id; repeat to cycle through several")
    run.add_argument("--stream", type=Path, help="JSON array or object with incidents and grading settings")
    run.add_argument("-n", "--incidents", type=int, default=1)
    run.add_argument("--model", default="gpt-6-luna")
    run.add_argument("--provider", choices=("codex", "claude"), default="codex", help="SDO agent provider")
    run.add_argument("--repair-policy", choices=("recorded-actions", "commit"), default="recorded-actions")
    run.add_argument("--diagnosis-grading", choices=("off", "deferred", "inline"), default=None)
    run.add_argument("--judge-model-id", default=None, help="SREGym JUDGE_MODEL_ID for diagnosis grading")
    run.add_argument("--reflection-session", choices=("resume", "fresh"), default="resume")
    run.add_argument("--reflection-guidance", choices=("baseline", "generalize", "generalize-spec"), default="baseline")
    run.add_argument("--late-findings", choices=("off", "pull"), default="off")
    run.add_argument(
        "--healthy-baseline",
        action="store_true",
        help="record the healthy namespace before each injection and reject learned incident detectors that fire on it",
    )
    run.add_argument(
        "--inject-before-resume",
        action="store_true",
        help="composite SDO runs: inject every fault while the controller is paused, then resume it (simultaneous)",
    )
    run.add_argument("--max-follow-ups", type=int, default=0, help="follow-up responders for residual health findings")
    run.add_argument("--follow-up-cooldown-seconds", type=int, default=30)
    run.add_argument("--reasoning-effort", default=None, help="Codex baseline reasoning effort (default: Codex's)")
    run.add_argument("--timeout", type=int, default=3600, help="per-incident agent timeout in seconds")
    run.add_argument("--health-timeout", type=float, default=300.0, help="seconds to wait for health after recovery")
    run.add_argument("--proxy-port", type=int, default=0, help="Codex baseline API proxy port (default: free port)")
    run.add_argument("--run-id", default=None)
    run.add_argument(
        "--composite-deadline",
        type=float,
        default=2400.0,
        help="composite problems: seconds after injection before SDO stops waiting for further incidents",
    )
    run.add_argument(
        "--no-stop-on-review",
        action="store_true",
        help="composite problems: keep waiting (until the deadline) after the controller stops for detector review",
    )
    run.add_argument(
        "--composite-idle",
        type=float,
        default=600.0,
        help="composite problems: seconds SDO waits for another incident while faults remain",
    )
    run.set_defaults(handler=_run)

    summary = commands.add_parser("summary", help="summarize incidents.jsonl files or run directories")
    summary.add_argument("paths", type=Path, nargs="+")
    summary.add_argument("--json", action="store_true")
    summary.set_defaults(handler=_summary)

    down = commands.add_parser("down", help="drain and remove the persistent SDO controller")
    down.add_argument("--run-dir", type=Path, required=True)
    down.add_argument("--delete-cluster", action="store_true")
    down.set_defaults(handler=_down)
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = build_parser().parse_args(argv)
    if getattr(args, "incidents", 1) < 1:
        raise SystemExit("--incidents must be positive")
    return int(args.handler(args))
