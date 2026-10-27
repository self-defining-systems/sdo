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
import platform
import socket
import subprocess
import sys
import time
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.adapter import WaitKind, assert_host_disk_headroom, assert_host_headroom, kubectl, wait_for
from benchmarks.sregym.fastloop.accepted_source import AcceptedSource, manifest_paths_for
from benchmarks.sregym.fastloop.baseline_recovery import (
    KubectlApplicationCluster,
    application_namespaces,
    capture_application_baseline,
    load_application_baseline,
    save_application_baseline,
)
from benchmarks.sregym.fastloop.environment import DEFAULT_PROBLEM, FastloopEnvironment, Images
from benchmarks.sregym.fastloop.fault_driver import SregymFaultDriver
from benchmarks.sregym.fastloop.loop import LoopConfig, run_incidents, write_loop_manifest
from benchmarks.sregym.fastloop.records import AgentSummary, IncidentRecord, load_records, summarize
from benchmarks.sregym.fastloop.stream import load_stream_config
from benchmarks.sregym.fastloop.wipe import (
    AttemptMarker,
    KubectlLaneCluster,
    application_pod_problems,
    run_tier2,
    unrecovered_rows,
)
from benchmarks.sregym.fastloop.worker_client import SregymWorker, worker_argv

if TYPE_CHECKING:
    from collections.abc import Callable

    from benchmarks.sregym.adapter import ClusterOps
    from benchmarks.sregym.fastloop.composite import CompositeSettings
    from benchmarks.sregym.fastloop.fault_tracker import FaultSpec
    from benchmarks.sregym.fastloop.loop import DiagnosisGrader, IncidentAgent
    from benchmarks.sregym.fastloop.wipe import Tier1Receipt, Tier2Reason

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SREGYM_DIR = REPO_ROOT / "third_party" / "sregym"
INCIDENTS_FILENAME = "incidents.jsonl"
CLUSTER_OP_TIMEOUT_SECONDS = wait_for(WaitKind.CLUSTER_OP)
DEPLOY_OP_TIMEOUT_SECONDS = wait_for(WaitKind.DEPLOY_OP)


def _worker(environment: FastloopEnvironment) -> SregymWorker:
    if environment.app == "toy":
        from benchmarks.sregym.fastloop.toy import ToyWorker

        return ToyWorker(environment)  # type: ignore[return-value]
    from benchmarks.sregym.fastloop.source_preflight import assert_harness_sources, assert_runtime_images

    assert_harness_sources(environment.sregym_dir)
    assert_runtime_images(
        environment.images, environment.dev_mount or REPO_ROOT, dev_mount=environment.dev_mount is not None
    )
    if not node_images_match(environment.cluster, environment.images):
        raise ValueError("eval: lane image source differs from the checked local images; redeliver with fastloop up")
    return SregymWorker(
        worker_argv(environment.sregym_dir, private_tmp=environment.private_tmp),
        env={**environment.worker_process_env(), "PYTHONPATH": str(REPO_ROOT)},
        log_path=environment.run_dir / "worker.log",
    )


def _reset_failed_incident(environment: FastloopEnvironment, worker: SregymWorker) -> None:
    from benchmarks.sregym.adapter import control_namespace_for
    from benchmarks.sregym.fastloop.reset import KubectlResetCluster, RestoreChecks, reset_environment

    reset_environment(
        environment,
        KubectlResetCluster(environment.kubeconfig, control_namespace_for(environment.namespace)),
        worker,
        preserve_repository=True,
        checks=RestoreChecks(environment),
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
            [
                "docker",
                "image",
                "inspect",
                "--platform",
                "linux/" + ("arm64" if platform.machine().lower() in {"aarch64", "arm64"} else "amd64"),
                "--format",
                "{{.Id}}",
                image,
            ],
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
                from benchmarks.sregym.fastloop.image_delivery import image_ref

                manifest = subprocess.run(
                    ["docker", "exec", node, "ctr", "-n", "k8s.io", "images", "ls", "name==" + image_ref(image)],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                if manifest.returncode != 0 or local.stdout.strip() not in manifest.stdout.split():
                    return False
    return True


def up_worker_environment(args: argparse.Namespace, *, workspace: Path) -> dict[str, str]:
    """The SREGym worker environment for ``up``: one reused kind lane deploying ``workspace`` from source."""

    images = Images(controller=args.controller_image, responder=args.responder_image, validator=args.validator_image)
    worker_env = {
        "SREGYM_KIND_CLUSTER_PREFIX": args.cluster_prefix,
        "SREGYM_KIND_CLUSTER_NAME": f"{args.cluster_prefix}{args.worker_id}",
        "SREGYM_KIND_REQUIRE_NETWORK_POLICY": "1",
        "FASTLOOP_DELIVERY_RECORD": str(args.run_dir.resolve() / "image_delivery.jsonl"),
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
    if args.dev_mount is not None:
        worker_env["FASTLOOP_KIND_CONFIG"] = str(args.run_dir.resolve() / "kind-dev.yaml")
    return worker_env


def _neighbor_capacity() -> int | None:
    try:
        return int(Path("/proc/sys/net/ipv4/neigh/default/gc_thresh3").read_text().strip())
    except (OSError, ValueError):
        return None


def _free_bytes(path: Path) -> int | None:
    import shutil

    while not path.exists() and path != path.parent:
        path = path.parent
    try:
        return shutil.disk_usage(path).free
    except OSError:
        return None


def check_host_capacity(args: argparse.Namespace) -> None:
    assert_host_headroom()
    assert_host_disk_headroom()
    # Match the live launch gate without depending on the legacy runner.
    minimum_neighbors = 65536
    limit = _neighbor_capacity()
    if limit is not None and limit < minimum_neighbors:
        raise ValueError(
            f"fastloop preflight: IPv4 neighbor capacity {limit} < {minimum_neighbors}; "
            "ask the host administrator to increase capacity"
        )
    paths = [getattr(args, "run_dir", REPO_ROOT)]
    inspected = subprocess.run(
        ["docker", "info", "--format", "{{.DockerRootDir}}"], check=True, capture_output=True, text=True
    )
    if inspected.stdout.strip():
        paths.append(Path(inspected.stdout.strip()))
    for path in paths:
        free = _free_bytes(path)
        if free is not None and free < 100 * 1000**3:
            raise ValueError(f"fastloop preflight: less than 100 GB free at {path}")


def up_preflight(args: argparse.Namespace) -> None:
    """Read-only host checks run before any compiler, builder, or cluster mutation."""
    import shutil
    import warnings

    if getattr(args, "reuse_cluster_only", False):
        if args.dev_mount is not None:
            raise ValueError("attach-only up requires --no-dev-mount; existing node mounts cannot be changed")
        if not os.environ.get("KUBECONFIG") or not Path(os.environ["KUBECONFIG"]).is_file():
            raise ValueError("attach-only up requires the existing lane KUBECONFIG")
    if args.kind_worker_nodes < 0:
        raise SystemExit("--kind-worker-nodes must not be negative")
    if getattr(args, "parallel_root", None) is not None and (args.dev_mount is None or args.app == "toy"):
        raise ValueError("--parallel-root requires an SREGym --dev-mount lane")
    for executable in ("docker", "kind", "kubectl", "go"):
        if shutil.which(executable) is None:
            raise ValueError(f"fastloop preflight: missing {executable}")
    subprocess.run(["docker", "info", "--format", "{{.ServerVersion}}"], check=True, capture_output=True, text=True)
    check_host_capacity(args)
    if args.dev_mount is None:
        warnings.warn("--no-dev-mount: code changes require image rebuilds", stacklevel=2)
    elif not (args.dev_mount.resolve() / "sdo/operator/go.mod").is_file():
        raise ValueError(f"fastloop preflight: not an SDO worktree: {args.dev_mount}")
    from benchmarks.sregym.fastloop.source_preflight import assert_harness_sources

    assert_harness_sources(getattr(args, "sregym_dir", DEFAULT_SREGYM_DIR))
    from benchmarks.sregym.adapter import BENCHMARK_IMAGE_ENTRY_POINTS, assert_image_entrypoints

    for image, modules in (
        (args.controller_image, ()),
        (args.responder_image, BENCHMARK_IMAGE_ENTRY_POINTS),
        (args.validator_image, ()),
    ):
        if image is None:
            continue
        subprocess.run(["docker", "image", "inspect", image], check=True, capture_output=True, text=True)
        assert_image_entrypoints(image, modules, source_root=args.dev_mount if modules else None, run=subprocess.run)


def _up(args: argparse.Namespace) -> int:
    up_preflight(args)
    if args.app == "toy":
        from benchmarks.sregym.fastloop.toy import up

        return up(args)
    if args.kind_worker_nodes < 0:
        raise SystemExit("--kind-worker-nodes must not be negative")
    if args.dev_mount is not None:
        args.dev_mount = args.dev_mount.resolve()
        if args.cluster_prefix == "fastloop-w":
            args.cluster_prefix = "fastloop-dev-w"
    run_dir = args.run_dir.resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    prior = FastloopEnvironment.load(run_dir) if (run_dir / "fastloop.json").is_file() else None
    if prior is not None and prior.shared_accepted_id:
        raise ValueError("shared accepted memory cannot be reseeded by up")
    parallel_root = args.parallel_root.resolve() if getattr(args, "parallel_root", None) is not None else None
    if parallel_root is not None:
        parallel_root.mkdir(parents=True, exist_ok=True)
    if prior is not None and prior.parallel_root != parallel_root:
        raise ValueError("--parallel-root must match the root mounted when this lane was created")
    if prior is not None and prior.dev_mount != args.dev_mount:
        raise ValueError(
            "up --dev-mount must match the worktree used to create this environment; "
            "use --no-dev-mount for a lane created without a dev mount"
        )
    workspace = run_dir / "application_workspace"
    if not workspace.exists():
        if args.seed is None:
            raise SystemExit("--seed is required the first time (a git repository with the application source)")
        subprocess.run(["git", "clone", "--quiet", str(args.seed.resolve()), str(workspace)], check=True)
    private_tmp = None if args.no_sandbox else run_dir / "tmp"
    if private_tmp is not None:
        private_tmp.mkdir(exist_ok=True)
    images = Images(controller=args.controller_image, responder=args.responder_image, validator=args.validator_image)
    if args.dev_mount is not None:
        from benchmarks.sregym.fastloop.dev import prepare_dev_mount

        if parallel_root is None:
            prepare_dev_mount(args.dev_mount, args.sregym_dir.resolve(), run_dir)
        else:
            prepare_dev_mount(args.dev_mount, args.sregym_dir.resolve(), run_dir, parallel_root)
    seed_commit = prior.seed_commit if prior is not None else None
    if seed_commit is None and (workspace / ".git").exists():
        seed_commit = subprocess.run(
            ["git", "-C", str(workspace), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    worker_env = up_worker_environment(args, workspace=workspace)
    if args.dev_mount is not None and not (run_dir / "fastloop.json").exists():
        clusters = subprocess.run(
            ["kind", "get", "clusters"], capture_output=True, text=True, check=True
        ).stdout.splitlines()
        if worker_env["SREGYM_KIND_CLUSTER_NAME"] in clusters:
            raise ValueError("dev mount needs a new kind cluster name; choose --cluster-prefix")
    if not args.redeploy and (run_dir / "fastloop.json").is_file():
        previous = FastloopEnvironment.load(run_dir)
        if (
            previous.cluster == worker_env["SREGYM_KIND_CLUSTER_NAME"]
            and previous.dev_mount == args.dev_mount
            and previous.images == images
            and previous.worker_env == worker_env
            and previous.deploy_problem == args.problem
            and previous.kubeconfig.is_file()
            and node_images_match(previous.cluster, images)
        ):
            if previous.namespace == "astronomy-shop":
                from benchmarks.sregym.adapter import prepare_oracle_observability, route_astronomy_traces

                prepare_oracle_observability(
                    kubeconfig=previous.kubeconfig,
                    sregym_dir=previous.sregym_dir,
                    cluster=previous.cluster,
                )
                route_astronomy_traces(previous.kubeconfig)
            with _worker(previous) as cached_worker:
                health = cached_worker.request(
                    "health", namespace=previous.namespace, timeout=wait_for(WaitKind.SLOW_COMMAND)
                )
            if health.get("healthy"):
                if previous.dev_mount is not None and previous.state_path.is_file():
                    return _reset(argparse.Namespace(run_dir=previous.run_dir, dev_mount=previous.dev_mount))
                from benchmarks.sregym.fastloop.baseline_recovery import seal_existing_install

                seal_existing_install(
                    previous.baseline_path, KubectlApplicationCluster(previous.kubeconfig), previous.namespace
                )
                print(f"fastloop environment ready: reused verified node image digests -> {previous.path}")
                return 0
    if args.builder:
        _ensure_builder(args.builder)
    placeholder = FastloopEnvironment(
        run_dir=run_dir,
        cluster=worker_env["SREGYM_KIND_CLUSTER_NAME"],
        kubeconfig=(
            Path(os.environ["KUBECONFIG"]).resolve()
            if args.reuse_cluster_only
            else run_dir / "kubeconfigs" / f"worker_{args.worker_id}.kubeconfig"
        ),
        namespace="",
        application="",
        workspace=workspace,
        sregym_dir=args.sregym_dir.resolve(),
        private_tmp=private_tmp,
        deploy_problem=args.problem,
        seed_commit=seed_commit,
        dev_mount=args.dev_mount,
        images=images,
        worker_env=worker_env,
        parallel_root=parallel_root,
    )
    worker = SregymWorker(
        worker_argv(placeholder.sregym_dir, private_tmp=private_tmp),
        env={
            **worker_env,
            # kubernetes.config freezes its default path when the child imports it.
            "KUBECONFIG": str(placeholder.kubeconfig),
            "PYTHONPATH": str(REPO_ROOT),
            "PYTHONUNBUFFERED": "1",
        },
        log_path=run_dir / "worker.log",
    )
    with worker:
        cluster = worker.request(
            "attach_existing" if args.reuse_cluster_only else "cluster",
            worker_id=args.worker_id,
            log_dir=str(run_dir),
            timeout=CLUSTER_OP_TIMEOUT_SECONDS,
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
    cluster = KubectlApplicationCluster(environment.kubeconfig)
    namespaces = application_namespaces(environment.baseline_path, environment.kubeconfig, environment.namespace)
    if deployed.get("deployed") is True:
        save_application_baseline(
            environment.baseline_path,
            tuple(capture_application_baseline(cluster, namespace) for namespace in namespaces),
            installed=True,
        )
    else:
        from benchmarks.sregym.fastloop.baseline_recovery import seal_existing_install

        seal_existing_install(environment.baseline_path, cluster, environment.namespace)
    delivery_path = run_dir / "image_delivery.jsonl"
    if delivery_path.is_file():
        from benchmarks.sregym.fastloop.image_delivery import delivery_state

        delivery = delivery_state(delivery_path)
        environment.image_delivery, environment.image_delivery_reason = delivery["path"], delivery["reason"]
        environment.image_delivery_seconds, environment.image_delivery_images = delivery["seconds"], delivery["images"]
    if environment.app != "toy":
        from benchmarks.sregym.fastloop.wipe import cluster_scoped, inventory_path, save_inventory

        save_inventory(
            inventory_path(environment.kubeconfig),
            cluster_scoped(KubectlLaneCluster(environment.kubeconfig).list_objects()),
        )
    environment.save()
    if environment.parallel_root is not None:
        from benchmarks.sregym.fastloop.shared_memory import provision_parallel_volume

        provision_parallel_volume(environment)
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
        stable_seconds=wait_for(WaitKind.STABLE),
        poll_seconds=wait_for(WaitKind.POLL_FAST),
    )


def _cluster_ops(args: argparse.Namespace, namespace: str, *, ops: ClusterOps | None = None) -> ClusterOps:
    """Cluster operations for the SDO agent, with the opt-in harness wrappers applied."""

    from benchmarks.sregym.adapter import KubectlClusterOps, kubectl_capture_ops

    selected: ClusterOps = ops or KubectlClusterOps()
    if args.inject_before_resume:
        from benchmarks.sregym.fastloop.simultaneous import InjectBeforeResumeOps

        selected = InjectBeforeResumeOps(selected)  # type: ignore[assignment]
    if args.healthy_baseline:
        selected = kubectl_capture_ops(selected, namespace=namespace, kubectl_runner=kubectl)  # type: ignore[assignment]
    restart_evidence = getattr(args, "restart_controller_in_injection_pause", None)
    if restart_evidence is not None:
        from benchmarks.sregym.fastloop.pause_restart import RestartControllerInPauseOps

        selected = RestartControllerInPauseOps(selected, restart_evidence)  # type: ignore[assignment]
    return selected


def _sdo_agent(
    args: argparse.Namespace,
    environment: FastloopEnvironment,
    results_dir: Path,
    *,
    composite: bool = False,
    stream_faults: dict[str, tuple[FaultSpec, ...]] | None = None,
    worker: SregymWorker | None = None,
) -> IncidentAgent:
    from benchmarks.sregym.adapter import (
        RuntimeConfig,
        control_namespace_for,
        deployed_lifecycle,
        launch_native_lifecycle,
    )
    from benchmarks.sregym.fastloop.sdo_agent import SdoAgentSettings, SdoPersistentAgent

    registrations = args.agent_registrations.resolve() if args.agent_registrations else None
    if registrations is None and environment.app == "toy":
        registrations = REPO_ROOT / "benchmarks/sregym/adapter/scripted_registrations"
    if registrations is None and args.provider == "codex":
        from benchmarks.sregym.fastloop.recording_worker import recording_registrations

        registrations = recording_registrations(results_dir, environment.images.responder)

    runtime_config = RuntimeConfig(
        repository=environment.workspace,
        namespace=environment.namespace,
        application=environment.application,
        controller_image=environment.images.controller,
        responder_image=environment.images.responder,
        validator_image=environment.images.validator,
        deployed_manifest_paths=manifest_paths_for(environment.application),
        repository_pvc="sdo-application-repository",
        credentials_secret=getattr(args, "credentials_secret", "sdo-agent-credentials"),
        model=args.model,
        timeout_seconds=args.timeout,
        repair_policy=args.repair_policy,
        agent_provider=args.provider,
        reflection_session=args.reflection_session,
        reflection_guidance=args.reflection_guidance,
        code_path=getattr(args, "code_path", "go"),
        late_findings=args.late_findings,
        healthy_baseline=args.healthy_baseline,
        controller_namespace=control_namespace_for(environment.namespace),
        kind_cluster_name=environment.cluster,
        persistent=True,
        agent_registrations=registrations,
        permissive_responder=getattr(args, "permissive_responder", False),
        responder_namespaces=getattr(args, "responder_namespaces", ()),
        shared_accepted_id=environment.shared_accepted_id,
        reuse_existing=bool(environment.shared_accepted_id),
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
        reflection_wait_seconds=wait_for(WaitKind.REFLECTION_DRAIN),
        detection_timeout_seconds=args.detection_timeout,
        checkpoint_enabled=not args.no_checkpoints,
    )
    if environment.app == "toy":
        from benchmarks.sregym.fastloop.toy_ops import ToyClusterOps

        ops = _cluster_ops(args, environment.namespace, ops=ToyClusterOps(environment.kubeconfig))
    else:
        ops = _cluster_ops(args, environment.namespace)

    from benchmarks.sregym.adapter import PersistentState, reset_control_state, stop_runtime

    # A new state path (including the v2 launcher's fresh-seed clone) starts a
    # memory stream. Retained records identify warm repeats and warm resets.
    fresh_memory_start = environment.namespace not in PersistentState.load(environment.state_path).controllers
    if worker is not None and environment.app != "toy":
        from benchmarks.sregym.adapter import native_install_config, sync_existing
        from benchmarks.sregym.fastloop.admission import EvalAdmissionOps, LaneAdmission

        install_config = native_install_config(runtime_config)
        admission = LaneAdmission(
            environment,
            ops,
            results_dir / "admission",
            code_path=runtime_config.code_path,
            timeout_seconds=getattr(args, "admission_timeout_seconds", 300),
            onboarding_timeout_seconds=getattr(args, "admission_onboarding_timeout_seconds", 600),
            install_config=install_config,
            synchronize=lambda: sync_existing(install_config),
            progress=getattr(args, "admission_progress", None),
            before_proof=getattr(args, "admission_before_proof", None),
        )
        ops = EvalAdmissionOps(ops, admission, fresh=fresh_memory_start)  # type: ignore[assignment]

    def start_native(context: object) -> bool:
        nonlocal fresh_memory_start
        if environment.shared_accepted_id:
            from benchmarks.sregym.adapter import inspect_existing, native_install_config

            inspect_existing(native_install_config(runtime_config))
            return True
        if fresh_memory_start and environment.app == "toy":
            control = control_namespace_for(environment.namespace)
            stop_runtime(control, kubeconfig=str(environment.kubeconfig))
            receipt = reset_control_state(control, kubeconfig=str(environment.kubeconfig))
            logger.info("Fresh-memory control state reset: %s", json.dumps(receipt, sort_keys=True))
        fresh_memory_start = False
        if environment.app == "toy":
            from benchmarks.sregym.fastloop.toy import install_controller

            install_controller(environment, runtime_config)
        else:
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


def _wipes_at_row_start(args: argparse.Namespace, environment: FastloopEnvironment) -> bool:
    return args.agent == "sdo" and environment.app != "toy" and not environment.shared_accepted_id


def _wipe_sdo_and_harness(args: argparse.Namespace, environment: FastloopEnvironment) -> Tier1Receipt:
    """Tier 1: delete SDO's control namespace and every harness leftover, then re-provision and re-seed SDO's inputs."""
    from benchmarks.sregym.fastloop.wipe import credentials_hook, wipe_environment

    if args.credentials_hook is None:
        raise SystemExit("eval: a row-start wipe deletes the SDO namespace; pass --credentials-hook to re-provision it")
    planned = args.planned_commit or environment.seed_commit
    if not planned:
        raise ValueError("eval: the row-start wipe needs --planned-commit or a seed commit captured by fastloop up")
    receipt = wipe_environment(
        environment,
        KubectlLaneCluster(environment.kubeconfig),
        applications=application_namespaces(environment.baseline_path, environment.kubeconfig, environment.namespace),
        provision_credentials=credentials_hook(args.credentials_hook.resolve(), environment.kubeconfig),
        planned_commit=planned,
        timeout_seconds=wait_for(WaitKind.ROLLOUT),
    )
    logger.info(
        "Tier-1 wipe: %.1fs, removed %s, force-finalized %s",
        receipt.seconds,
        ", ".join(receipt.removed) or "nothing",
        ", ".join(receipt.force_finalized) or "nothing",
    )
    return receipt


def _recapture_application_baseline(environment: FastloopEnvironment) -> None:
    """A redeployed app defines the next captured baseline, as in ``fastloop up``."""
    cluster = KubectlApplicationCluster(environment.kubeconfig)
    namespaces = application_namespaces(environment.baseline_path, environment.kubeconfig, environment.namespace)
    save_application_baseline(
        environment.baseline_path,
        tuple(capture_application_baseline(cluster, namespace) for namespace in namespaces),
        installed=True,
    )


def _application_needs_redeploy(environment: FastloopEnvironment, marker: AttemptMarker) -> Tier2Reason | None:
    """Tier 2 trigger: an unrecovered previous row, a warm app that fails the restore invariant, or a stuck pod."""
    from benchmarks.sregym.fastloop.admission import workload_identity
    from benchmarks.sregym.fastloop.clean_start import assert_clean_baseline
    from benchmarks.sregym.fastloop.wipe import await_healthy_pods, tier2_reason

    def invariant() -> None:
        workload_identity(environment.kubeconfig, environment.namespace)
        assert_clean_baseline(environment.kubeconfig, namespace=environment.namespace)

    def stuck_pods() -> list[str]:
        namespaces = application_namespaces(environment.baseline_path, environment.kubeconfig, environment.namespace)
        return await_healthy_pods(
            lambda: application_pod_problems(environment.kubeconfig, namespaces),
            timeout_seconds=wait_for(WaitKind.MINUTE),
            poll_seconds=wait_for(WaitKind.POLL),
        )

    return tier2_reason(marker, invariant, stuck_pods)


def deploy_with_setup_proof(
    environment: FastloopEnvironment,
    worker: SregymWorker,
    results_dir: Path,
    *,
    allow_reuse: bool = True,
    redeploy: bool = False,
) -> dict[str, Any]:
    """Reuse only the deploy/warm-up evidence; per-injection admission still runs."""
    from benchmarks.sregym.fastloop.setup_proof import check_reuse

    if allow_reuse:
        _, decision = check_reuse(environment)
    else:
        decision = {"reused": False, "receipt": None, "changed": ["setup_reuse_requires_sdo_admission"]}
    results_dir.mkdir(parents=True, exist_ok=True)
    (results_dir / "setup-proof.json").write_text(json.dumps(decision, indent=2) + "\n")
    if decision["reused"]:
        logger.info("Setup proof reused %s; injection admission remains required", decision["receipt"])
    else:
        logger.info("Full setup required: %s", ", ".join(decision["changed"]))
        worker.request(
            "deploy",
            problem_id=environment.deploy_problem,
            baseline_path=str(environment.baseline_path),
            redeploy=redeploy,
            timeout=DEPLOY_OP_TIMEOUT_SECONDS,
        )
    return decision


def _run(args: argparse.Namespace) -> int:
    run_started = time.monotonic()
    if args.sweep_service and (args.stream is None or args.agent != "sdo"):
        raise SystemExit("--sweep-service requires an SDO --stream run")
    environment = FastloopEnvironment.load(args.run_dir.resolve())
    if environment.image_delivery == "kind-load-fallback":
        from benchmarks.sregym.fastloop.image_delivery import warn_fallback

        warn_fallback(
            environment.image_delivery_reason, environment.image_delivery_images, environment.image_delivery_seconds
        )
    tier1 = _wipe_sdo_and_harness(args, environment) if _wipes_at_row_start(args, environment) else None
    if args.agent == "sdo":
        from benchmarks.sregym.adapter import control_namespace_for

        secret = "sdo-agent-credentials"
        checked = kubectl(
            ["get", "secret", secret],
            namespace=control_namespace_for(environment.namespace),
            kubeconfig=environment.kubeconfig,
            check=False,
        )
        if checked.returncode != 0:
            raise SystemExit(
                f"agent credentials Secret {secret!r} is missing or inaccessible in "
                f"{control_namespace_for(environment.namespace)!r}: {checked.stderr.strip()}"
            )
    if environment.dev_mount is not None:
        from benchmarks.sregym.fastloop.dev import prepare_dev_mount

        mount_args = (
            environment.dev_mount,
            None if environment.app == "toy" else environment.sregym_dir,
            environment.run_dir,
        )
        if environment.parallel_root is None:
            prepare_dev_mount(*mount_args)
        else:
            prepare_dev_mount(*mount_args, environment.parallel_root)
        os.environ["SDO_DEV_MOUNT"] = "1"
        os.environ["SDO_OPERATOR_BIN"] = str(environment.dev_mount / ".fastloop/sdo-operator")
    os.environ["KUBECONFIG"] = str(environment.kubeconfig)
    run_id = args.run_id or f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{args.agent}"
    results_dir = environment.run_dir / "results" / run_id
    stream_config = load_stream_config(args.stream) if args.stream else None
    stream: tuple[tuple[str, ...], ...] = stream_config.incidents if stream_config else ()
    grading_mode = args.diagnosis_grading or (stream_config.diagnosis_grading if stream_config else "off")
    judge_model_id = args.judge_model_id or (stream_config.judge_model_id if stream_config else "codex-gpt-6-luna")
    from benchmarks.sregym.fastloop.grading import SregymDiagnosisGrader

    grader = SregymDiagnosisGrader(environment.sregym_dir, judge_model_id) if grading_mode != "off" else None
    problems = tuple(args.problem or [environment.deploy_problem]) if not stream else tuple(ids[0] for ids in stream)
    from benchmarks.sregym.fastloop.fault_tracker import describe_sregym_problems, stream_faults
    from benchmarks.sregym.fastloop.lane_guard import LaneGuard, assert_faults_installable
    from benchmarks.sregym.faults.description import check_composites, validate_suite

    # Discovery and capability checks must finish before a worker can deploy or launch an agent.
    description_started = time.monotonic()
    selected_ids = tuple(dict.fromkeys(problem for ids in (stream or tuple((p,) for p in problems)) for problem in ids))
    descriptions = (
        describe_sregym_problems(selected_ids, sregym_dir=environment.sregym_dir, app_source_dir=environment.workspace)
        if environment.app != "toy"
        else {}
    )
    check_composites(descriptions)
    assert_faults_installable(
        descriptions, environment.sregym_dir, LaneGuard(environment.cluster, environment.kubeconfig, "row-start")
    )
    capability_mode = stream_config.capability_mode if stream_config else "strict"
    capability_evidence = validate_suite(
        descriptions if args.agent == "sdo" else {},
        require_tracking=args.require_fault_tracking,
        capability_mode=capability_mode,
    )
    args.permissive_responder = bool(stream_config and stream_config.permissive_responder)
    args.responder_namespaces = stream_config.responder_namespaces if stream_config else ()
    if args.agent != "sdo" and args.require_fault_tracking:
        # Capability scope is specific to SDO; tracking is common to both arms.
        unsupported = [key for key, item in descriptions.items() if not item.tracking_supported]
        if unsupported:
            raise ValueError(f"per-fault tracking unsupported for {unsupported}")
    catalog = {key: item.ordered_faults() for key, item in descriptions.items()}
    incidents: tuple[tuple[str, ...], ...] = stream or tuple((problem,) for problem in problems)
    faults_by_incident: dict[str, tuple[FaultSpec, ...]] = {}
    for ids in incidents:
        first_id = next(iter(ids))
        if len(ids) > 1 or (descriptions and descriptions[first_id].composite):
            faults_by_incident["+".join(ids)] = stream_faults(ids, catalog=catalog)
    if args.require_fault_tracking:
        for key, faults in faults_by_incident.items():
            if len({fault.name for fault in faults}) != len(faults):
                raise ValueError(f"{key}: per-fault tracking has duplicate probe identities")
    registered_composites = bool(faults_by_incident)
    description_seconds = time.monotonic() - description_started
    results_dir.mkdir(parents=True, exist_ok=True)
    (results_dir / "fault_descriptions.json").write_text(
        json.dumps({key: item.model_dump(mode="json") for key, item in descriptions.items()}, indent=2) + "\n",
        encoding="utf-8",
    )
    config = LoopConfig(
        delivery_record_path=environment.run_dir / "image_delivery.jsonl",
        image_delivery=environment.image_delivery,
        image_delivery_reason=environment.image_delivery_reason,
        run_id=run_id,
        problems=problems,
        incidents=len(stream) if stream else args.incidents,
        results_path=results_dir / INCIDENTS_FILENAME,
        stream=stream,
        continue_on_error=args.continue_on_error,
        capability_evidence=capability_evidence,
        permissive_responder=args.permissive_responder,
        responder_namespaces=args.responder_namespaces,
        slot_offset=args.slot_offset,
        cleanup_passes=args.cleanup_passes,
        lane_id=environment.parallel_lane or None,
        accepted_repository=environment.workspace if environment.shared_accepted_id else None,
    )
    write_loop_manifest(config)
    marker = AttemptMarker(environment.run_dir)
    redeploy_reason = _application_needs_redeploy(environment, marker) if tier1 is not None else None
    if redeploy_reason is not None:
        logger.info(
            "Tier-2 recreate of the application: %s (%s)", redeploy_reason.trigger.value, redeploy_reason.detail
        )
    if tier1 is not None:
        marker.begin()
    with _worker(environment) as worker:
        worker_ready = time.monotonic()
        if tier1 is not None:

            def redeploy_application() -> None:
                deploy_with_setup_proof(environment, worker, results_dir, allow_reuse=False, redeploy=True)
                _recapture_application_baseline(environment)

            run_tier2(
                redeploy_reason,
                tier1,
                cluster=KubectlLaneCluster(environment.kubeconfig),
                namespaces=(
                    application_namespaces(environment.baseline_path, environment.kubeconfig, environment.namespace)
                    if redeploy_reason is not None
                    else ()
                ),
                deploy=redeploy_application,
                timeout_seconds=wait_for(WaitKind.ROLLOUT),
            ).write(results_dir)
        if redeploy_reason is None:
            deploy_with_setup_proof(
                environment, worker, results_dir, allow_reuse=args.agent == "sdo" and environment.app != "toy"
            )
        deployed = time.monotonic()
        described = deployed
        baseline = load_application_baseline(environment.baseline_path, environment.namespace)
        if baseline is not None:
            from benchmarks.sregym.fastloop.recovery_closure import complete_recovery_baselines

            baseline = complete_recovery_baselines(AcceptedSource.of(environment), baseline)
        if baseline is None and args.require_full_cleanup:
            raise ValueError("full cleanup requires an application baseline")
        if baseline is None:
            logger.warning(
                "application baseline absent in %s; recovery will use the older fault-only path",
                environment.baseline_path,
            )
        before_inject = None
        if args.sweep_service:
            from benchmarks.sregym.fastloop.hotel_latent import sweep_preparer

            before_inject = sweep_preparer(
                tuple(args.sweep_service), catalog, namespace=environment.namespace, kubeconfig=environment.kubeconfig
            )
        driver = SregymFaultDriver(
            worker,
            namespace=environment.namespace,
            baseline_problem_id=environment.deploy_problem if baseline is not None else None,
            baseline=baseline,
            cluster=KubectlApplicationCluster(environment.kubeconfig) if baseline is not None else None,
            health_timeout_seconds=args.health_timeout,
            health_poll_seconds=wait_for(WaitKind.POLL_FAST),
            before_inject=before_inject,
            require_full_cleanup=args.require_full_cleanup,
            require_faulty_oracle=getattr(args, "require_faulty_oracle", False),
            required_faulty_components=tuple(getattr(args, "require_faulty_component", [])),
            pre_repair_oracle_path=results_dir / "pre-repair-oracle.json"
            if getattr(args, "require_faulty_oracle", False)
            else None,
        )
        config = replace(
            config,
            run_prelude_phase_seconds={
                "worker_start": worker_ready - run_started,
                "worker_deploy": deployed - worker_ready,
                "fault_description": description_seconds,
                "baseline_and_driver_setup": time.monotonic() - described,
                "total_before_first_incident": time.monotonic() - run_started,
            },
        )

        def reset_failed() -> None:
            _reset_failed_incident(environment, worker)

        if args.agent == "sdo":
            records = run_incidents(
                _sdo_agent(
                    args,
                    environment,
                    results_dir,
                    composite=registered_composites,
                    stream_faults=faults_by_incident,
                    worker=worker,
                ),
                driver,
                config,
                grader=grader,
                diagnosis_grading=grading_mode,
                on_failed_incident=reset_failed if args.continue_on_error else None,
            )
        else:
            records = _run_codex(
                args,
                environment,
                results_dir,
                worker,
                driver,
                config,
                grader,
                grading_mode,
                faults_by_incident,
                reset_failed if args.continue_on_error else None,
            )
    marker.end(unrecovered_rows(records))
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
    on_failed_incident: Callable[[], None] | None = None,
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
        registered = bool(stream_faults)
        if registered:
            from benchmarks.sregym.fastloop.composite import TrackedAgent, reader_for

            agent = TrackedAgent(  # type: ignore[assignment]
                agent,
                reader=reader_for(environment.namespace, environment.kubeconfig),
                report_dir=results_dir,
                settings=_composite_settings(args),
                stream_faults=stream_faults,
            )
        return run_incidents(
            agent, driver, config, grader=grader, diagnosis_grading=grading_mode, on_failed_incident=on_failed_incident
        )


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
        from benchmarks.sregym.fastloop.image_delivery import remove_registry

        remove_registry(environment.cluster)
    return 1 if errors else 0


def _reset(args: argparse.Namespace) -> int:
    from benchmarks.sregym.adapter import control_namespace_for
    from benchmarks.sregym.fastloop.reset import KubectlResetCluster, RestoreChecks, reset_environment

    environment = FastloopEnvironment.load(args.run_dir.resolve())
    if environment.shared_accepted_id:
        raise ValueError("shared accepted memory cannot use lane reset")
    if environment.seed_commit is None:
        raise ValueError("reset requires a seed commit captured by fastloop up")
    retarget = getattr(args, "retarget_dev_mount", False)
    if args.dev_mount is not None and environment.dev_mount != args.dev_mount.resolve() and not retarget:
        raise ValueError("reset --dev-mount must match the worktree mounted by up")
    RestoreChecks(environment).validate()
    mount = args.dev_mount.resolve() if retarget and args.dev_mount else environment.dev_mount
    if mount is not None:
        from benchmarks.sregym.fastloop.dev import prepare_dev_mount

        prepare_dev_mount(mount, None if environment.app == "toy" else environment.sregym_dir, environment.run_dir)
    saved_toy = None
    if environment.app == "toy":
        from benchmarks.sregym.fastloop.toy_reset import ToyControllerTemplate

        saved_toy = ToyControllerTemplate.capture(environment)
    cluster = KubectlResetCluster(environment.kubeconfig, control_namespace_for(environment.namespace))
    with _worker(environment) as worker:
        reset_environment(environment, cluster, worker, checks=RestoreChecks(environment))
    _restore_seed(environment)
    if retarget or environment.dev_mount_snapshot:
        from benchmarks.sregym.fastloop.dev import retarget_dev_mount

        if mount is None:
            raise ValueError("retarget requires --dev-mount WORKTREE")
        retarget_dev_mount(environment.cluster, mount)
        environment.dev_mount = mount
        environment.dev_mount_snapshot = True
        environment.save()
    if environment.app == "toy":
        if saved_toy is not None:
            saved_toy.restart(environment)
        else:
            from benchmarks.sregym.fastloop.toy import install_controller, record_controller
            from benchmarks.sregym.fastloop.toy_reset import clear_runtime_state

            clear_runtime_state(environment, control_namespace_for(environment.namespace))
            record_controller(environment, install_controller(environment))
    print(f"fastloop environment reset: {environment.path}")
    return 0


def _restore_seed(environment: FastloopEnvironment) -> None:
    if environment.seed_commit is None:
        raise ValueError("reset requires a seed commit captured by fastloop up")
    subprocess.run(["git", "-C", str(environment.workspace), "reset", "--hard", environment.seed_commit], check=True)
    subprocess.run(["git", "-C", str(environment.workspace), "clean", "-fd"], check=True)


def _retire_app(args: argparse.Namespace) -> int:
    from benchmarks.sregym.fastloop.app_retirement import (
        KubectlRetirementOps,
        RetirementRequest,
        retire_application,
    )
    from benchmarks.sregym.fastloop.lane_guard import LaneGuard

    environment = FastloopEnvironment.load(args.run_dir.resolve())
    request = RetirementRequest(
        cluster=environment.cluster,
        namespace=environment.namespace,
        next_namespace=args.next_namespace,
        lease=args.lease,
        coord=args.coord,
    )
    if args.run_dir.resolve() == args.next_run_dir.resolve():
        raise ValueError("app retirement requires a new run directory for the next memory lineage")
    ops = KubectlRetirementOps(request, environment.kubeconfig)
    ops.check_lease()
    LaneGuard(environment.cluster, environment.kubeconfig, args.lease, args.coord).install()
    prefix, separator, worker_id = environment.cluster.rpartition("-")
    if not separator or not worker_id.isdigit():
        raise ValueError("app retirement requires a kind lane name ending in its worker ID")
    command = [
        "up",
        "--run-dir",
        str(args.next_run_dir),
        "--seed",
        str(args.seed),
        "--problem",
        args.next_problem,
        "--cluster-prefix",
        prefix + "-",
        "--worker-id",
        worker_id,
        "--sregym-dir",
        str(args.sregym_dir),
        "--no-dev-mount",
        "--reuse-cluster-only",
    ]
    for role in ("controller", "responder", "validator"):
        command.extend([f"--{role}-image", getattr(args, f"{role}_image")])
    up_args = build_parser().parse_args(command)
    # Check source/image/host prerequisites before retiring the old application.
    up_preflight(up_args)
    args.next_run_dir.mkdir(parents=True, exist_ok=True)
    (args.next_run_dir / "app-retirement-started.json").write_text(
        json.dumps(
            {
                "cluster": request.cluster,
                "lease": request.lease,
                "retired_namespaces": sorted(request.namespaces),
                "next_namespace": request.next_namespace,
                "cluster_recreated": False,
            },
            indent=2,
        )
        + "\n"
    )

    def deploy_next() -> None:
        _up(up_args)
        next_environment = FastloopEnvironment.load(args.next_run_dir.resolve())
        if next_environment.cluster != environment.cluster or next_environment.namespace != args.next_namespace:
            raise ValueError("eval: next application deployment differs from the requested warm lane")

    receipt = retire_application(request, ops, deploy_next=deploy_next)
    args.next_run_dir.mkdir(parents=True, exist_ok=True)
    (args.next_run_dir / "app-retirement.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt))
    return 0


def _parallel(args: argparse.Namespace) -> int:
    from benchmarks.sregym.fastloop.parallel import ParallelPlan, run_parallel

    plan = ParallelPlan.model_validate_json(args.plan.read_text())
    if args.prepare:
        from benchmarks.sregym.fastloop.shared_memory import prepare_shared_lanes

        prepare_shared_lanes(plan, args.output.resolve().with_suffix(".attachment.json"))
    run_parallel(plan, args.output.resolve())
    return 0


def _parallel_attach(args: argparse.Namespace) -> int:
    from benchmarks.sregym.fastloop.parallel import ParallelPlan
    from benchmarks.sregym.fastloop.shared_memory import attach_shared_memory

    plan = ParallelPlan.model_validate_json(args.plan.read_text())
    attach_shared_memory(plan, args.report.resolve())
    return 0


def _check_plan(args: argparse.Namespace) -> int:
    from benchmarks.sregym.fastloop.probe_coverage import check_plan

    report = check_plan(args.plan, sregym_dir=args.sregym_dir)
    print(json.dumps({"tracking_supported": True, **report}))
    return 0


def _preflight(args: argparse.Namespace) -> int:
    if args.plan is not None:
        _check_plan(args)
    up_preflight(args)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="fastloop", description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    commands = parser.add_subparsers(dest="command", required=True)

    preflight = commands.add_parser("preflight", help="fail fast on host prerequisites before building images")
    preflight.add_argument("--dev-mount", type=Path, nargs="?", const=REPO_ROOT, default=REPO_ROOT)
    preflight.add_argument("--kind-worker-nodes", type=int, default=1)
    for role in ("controller", "responder", "validator"):
        preflight.add_argument(f"--{role}-image", default=None)
    preflight.add_argument(
        "--plan", type=Path, help="require attributable probes for every problem in this plan before host checks"
    )
    preflight.add_argument("--sregym-dir", type=Path, default=DEFAULT_SREGYM_DIR)
    preflight.set_defaults(handler=_preflight)

    check_plan = commands.add_parser(
        "check-plan", help="offline per-fault tracking validation for an entire plan or stream"
    )
    check_plan.add_argument("--plan", type=Path, required=True)
    check_plan.add_argument("--sregym-dir", type=Path, default=DEFAULT_SREGYM_DIR)
    check_plan.set_defaults(handler=_check_plan)

    up = commands.add_parser("up", help="create or reuse the cluster and deploy the application once")
    up.add_argument("--run-dir", type=Path, required=True)
    up.add_argument("--app", choices=("sregym", "toy"), default="sregym")
    up.add_argument("--toy-node-image", default="kindest/node:v1.34.0")
    up.add_argument("--seed", type=Path, help="git repository cloned as the application workspace (first time only)")
    up.add_argument("--problem", default=DEFAULT_PROBLEM, help="problem whose application is deployed")
    up.add_argument("--cluster-prefix", default="fastloop-w")
    up.add_argument("--worker-id", type=int, default=0)
    up.add_argument(
        "--reuse-cluster-only",
        action="store_true",
        help="attach with KUBECONFIG or refuse; never create or recreate a kind cluster",
    )
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
    up.add_argument(
        "--dev-mount",
        type=Path,
        nargs="?",
        const=REPO_ROOT,
        default=REPO_ROOT,
        help="mount a worktree (default: current worktree) and host-build the Go operator",
    )
    up.add_argument("--no-dev-mount", dest="dev_mount", action="store_const", const=None)
    up.add_argument(
        "--parallel-root", type=Path, help="NVMe directory bind-mounted on every node for later shared attachment"
    )
    up.add_argument("--redeploy", action="store_true", help="redeploy even if the application is healthy")
    up.add_argument(
        "--no-sandbox",
        action="store_true",
        help="run the SREGym worker without a private /tmp (unsafe next to other SREGym runs)",
    )
    up.set_defaults(handler=_up)

    retire = commands.add_parser("retire-app", help="retire one app and deploy the next on its existing kind lane")
    retire.add_argument("--run-dir", type=Path, required=True)
    retire.add_argument("--next-run-dir", type=Path, required=True)
    retire.add_argument("--next-namespace", required=True)
    retire.add_argument("--next-problem", required=True)
    retire.add_argument("--seed", type=Path, required=True)
    retire.add_argument("--lease", required=True)
    retire.add_argument("--coord", default="/mnt/data/shli/coord/coord")
    retire.add_argument("--sregym-dir", type=Path, default=DEFAULT_SREGYM_DIR)
    for role in ("controller", "responder", "validator"):
        retire.add_argument(f"--{role}-image", required=True)
    retire.set_defaults(handler=_retire_app)

    from benchmarks.sregym.fastloop.dry_run import budget, execute, setup_budget

    dry_run = commands.add_parser("eval-dry-run", help="scripted pre-injection proof on a real lane, without a fault")
    dry_run.add_argument("--run-dir", type=Path, required=True)
    dry_run.add_argument("--timeout", type=budget, default=300)
    dry_run.add_argument("--setup-timeout", type=setup_budget, default=900)
    dry_run.set_defaults(handler=lambda args: execute(args.run_dir, args.timeout, args.setup_timeout))

    reset = commands.add_parser("reset", help="restore a warm cluster to its application and controller baseline")
    reset.add_argument("--run-dir", type=Path, required=True)
    reset.add_argument(
        "--dev-mount", type=Path, nargs="?", const=REPO_ROOT, help="worktree mounted when this environment was created"
    )
    reset.add_argument(
        "--retarget-dev-mount",
        action="store_true",
        help="adopt an idle lane using node-local source snapshots without editing its old checkout",
    )
    reset.set_defaults(handler=_reset)

    run = commands.add_parser("run", help="loop incidents against the warm environment")
    run.add_argument("--run-dir", type=Path, required=True)
    run.add_argument("--agent", choices=("sdo", "codex"), required=True)
    run.add_argument("--no-checkpoints", action="store_true", help="skip optional offline replay checkpoint export")
    run.add_argument(
        "--credentials-hook",
        type=Path,
        help="executable the lane keeper provides; called with the SDO namespace to re-provision its credentials "
        "after the row-start wipe (required for non-toy SDO rows)",
    )
    run.add_argument(
        "--planned-commit",
        help="accepted commit this row starts from (seed for a first try, the previous row's accepted_after "
        "for a repeat); defaults to the seed captured by up",
    )
    run.add_argument("--agent-registrations", type=Path, help="SDO worker registration directory")
    run.add_argument("--problem", "--problems", action="append", help="problem id; repeat to cycle through several")
    run.add_argument(
        "--require-fault-tracking",
        action="store_true",
        help="reject faults without attributable probes before deployment",
    )
    run.add_argument("--stream", type=Path, help="JSON array or object with incidents and grading settings")
    run.add_argument(
        "--sweep-service",
        action="append",
        default=[],
        metavar="SERVICE",
        help="clear ownership immediately before this standalone targetPort target to test periodic sweeps",
    )
    run.add_argument("-n", "--incidents", type=int, default=1)
    run.add_argument("--slot-offset", type=int, default=0, help="global index of the first incident")
    run.add_argument(
        "--require-faulty-oracle", action="store_true", help="require a faulty official oracle before resuming repair"
    )
    run.add_argument(
        "--require-faulty-component",
        action="append",
        default=[],
        metavar="NAME",
        help="require this exact composite oracle component to grade faulty before repair (repeatable)",
    )
    run.add_argument("--require-full-cleanup", action="store_true", help="require v2 recovery and application baseline")
    run.add_argument(
        "--cleanup-passes",
        type=int,
        choices=(1, 2),
        default=1,
        help="verify full cleanup and the official recovery oracle twice when set to 2",
    )
    run.add_argument("--model", default="gpt-6-luna")
    run.add_argument("--provider", choices=("codex", "claude"), default="codex", help="SDO agent provider")
    run.add_argument("--repair-policy", choices=("recorded-actions", "commit"), default="recorded-actions")
    run.add_argument("--diagnosis-grading", choices=("off", "deferred", "inline"), default=None)
    run.add_argument("--judge-model-id", default=None, help="SREGym JUDGE_MODEL_ID for diagnosis grading")
    run.add_argument("--reflection-session", choices=("resume", "fresh"), default="resume")
    run.add_argument("--code-path", choices=("go", "shell"), default="go")
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
    run.add_argument(
        "--restart-controller-in-injection-pause",
        type=Path,
        default=None,
        metavar="EVIDENCE_JSONL",
        help="scripted-agent gate: delete the controller Pod inside every injection pause and record it here",
    )
    run.add_argument("--reasoning-effort", default=None, help="Codex baseline reasoning effort (default: Codex's)")
    run.add_argument("--timeout", type=int, default=3600, help="per-incident agent timeout in seconds")

    run.add_argument(
        "--detection-timeout",
        type=float,
        default=None,
        metavar="SECONDS",
        help="SDO: fail an incident if no detector opens it within this many seconds (default: disabled)",
    )
    run.add_argument("--health-timeout", type=float, default=300.0, help="seconds to wait for health after recovery")
    run.add_argument("--proxy-port", type=int, default=0, help="Codex baseline API proxy port (default: free port)")
    run.add_argument("--run-id", default=None)
    run.add_argument(
        "--continue-on-error", action="store_true", help="record a failed incident and continue after baseline recovery"
    )
    run.add_argument(
        "--composite-deadline",
        type=float,
        default=2400.0,
        help="composite problems: seconds after injection before SDO stops waiting for further incidents",
    )
    run.add_argument(
        "--composite-idle",
        type=float,
        default=600.0,
        help="composite problems: seconds SDO waits for another incident while faults remain",
    )
    run.set_defaults(handler=_run)

    parallel = commands.add_parser("parallel", help="run problem/lane pairs concurrently with one accepted memory")
    parallel.add_argument("--plan", type=Path, required=True, help="ParallelPlan JSON with lanes and encounters")
    parallel.add_argument(
        "--prepare", action="store_true", help="prepare native baselines without faults, then attach shared memory"
    )
    parallel.add_argument(
        "--output", type=Path, required=True, help="new external directory for aggregate rows and logs"
    )
    parallel.set_defaults(handler=_parallel)
    attach = commands.add_parser(
        "parallel-attach", help="bind accepted memory into idle prepared lanes; retains private PVC roots"
    )
    attach.add_argument("--plan", type=Path, required=True)
    attach.add_argument("--report", type=Path, required=True, help="external durable attachment/recovery report")
    attach.set_defaults(handler=_parallel_attach)

    replay = commands.add_parser("replay", help="re-execute one checkpointed stage without a cluster")
    replay.add_argument("--run-dir", type=Path, required=True, help="one fastloop results run directory")
    replay.add_argument("--incident", type=int, required=True)
    replay.add_argument("--stage", choices=("reflection", "responder", "broker"), required=True)
    replay.add_argument(
        "--agent",
        choices=("scripted", "live"),
        help="default: recorded model turns for reflection, scripted worker for responder",
    )
    replay.add_argument("--live-on-cache-miss", action="store_true")
    replay.set_defaults(handler=_replay)

    summary = commands.add_parser("summary", help="summarize incidents.jsonl files or run directories")
    summary.add_argument("paths", type=Path, nargs="+")
    summary.add_argument("--json", action="store_true")
    summary.set_defaults(handler=_summary)

    down = commands.add_parser("down", help="drain and remove the persistent SDO controller")
    down.add_argument("--run-dir", type=Path, required=True)
    down.add_argument("--delete-cluster", action="store_true")
    down.set_defaults(handler=_down)
    return parser


def _replay(args: argparse.Namespace) -> int:
    from benchmarks.sregym.fastloop.replay import CheckpointStore, replay_broker

    run_dir = args.run_dir.resolve()
    store = CheckpointStore(run_dir)
    environment = next(
        (parent / "fastloop.json" for parent in (run_dir, *run_dir.parents) if (parent / "fastloop.json").is_file()),
        None,
    )
    source = Path(json.loads(environment.read_text(encoding="utf-8"))["workspace"]) if environment else run_dir
    agent = args.agent or ("live" if args.stage == "reflection" else "scripted")
    if args.stage == "broker":
        result = replay_broker(store, args.incident, source=source, operator_root=REPO_ROOT / "sdo" / "operator")
    elif args.stage == "responder" and agent == "scripted":
        from benchmarks.sregym.fastloop.replay_worker import replay_scripted_responder

        result = replay_scripted_responder(store, args.incident, source=source)
    elif args.stage == "reflection" and agent == "scripted":
        from benchmarks.sregym.fastloop.replay_worker import replay_scripted_reflection

        result = replay_scripted_reflection(
            store, args.incident, source=source, operator_root=REPO_ROOT / "sdo" / "operator"
        )
    elif args.stage in {"responder", "reflection"} and agent == "live":
        from benchmarks.sregym.fastloop.replay_worker import replay_live_worker

        result = replay_live_worker(
            store, args.incident, args.stage, source=source, live_on_cache_miss=args.live_on_cache_miss
        )
    else:
        raise RuntimeError(f"{args.stage} replay requires a worker execution harness")
    print(json.dumps({"incident": args.incident, "stage": args.stage, **result}, sort_keys=True))
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = build_parser().parse_args(argv)
    if getattr(args, "slot_offset", 0) < 0:
        raise SystemExit("--slot-offset must be non-negative")
    if getattr(args, "incidents", 1) < 1:
        raise SystemExit("--incidents must be positive")
    return int(args.handler(args))
