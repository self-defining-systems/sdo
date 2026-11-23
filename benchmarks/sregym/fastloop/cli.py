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
from dataclasses import asdict, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.adapter import (
    HostMemoryPolicy,
    WaitKind,
    assert_host_disk_headroom,
    assert_host_headroom,
    assert_host_memory_admission,
    assert_lane_budget,
    host_env,
    kubectl,
    parse_memory_size,
    running_lane_caps,
    wait_for,
)
from benchmarks.sregym.fastloop.accepted_source import AcceptedSource, deployment_source_for_environment
from benchmarks.sregym.fastloop.baseline_recovery import (
    KubectlApplicationCluster,
    accepted_restore_baselines,
    application_namespaces,
    capture_application_baseline,
    load_application_baseline,
    save_application_baseline,
)
from benchmarks.sregym.fastloop.environment import DEFAULT_PROBLEM, FastloopEnvironment, Images, refuse_on_eks
from benchmarks.sregym.fastloop.evidence_sink import lane_evidence_run_id
from benchmarks.sregym.fastloop.fault_driver import SregymFaultDriver
from benchmarks.sregym.fastloop.gitlab_faults import add_faults_parser
from benchmarks.sregym.fastloop.gitlab_rehearsal import add_gitlab_rehearse_parser
from benchmarks.sregym.fastloop.gitlab_replay import add_gitlab_replay_parser
from benchmarks.sregym.fastloop.host_limits import DEFAULT_MEMORY_LIMIT, DEFAULT_PIDS_LIMIT, HostLimits, NodeLimits
from benchmarks.sregym.fastloop.lane_cgroup import (
    DEFAULT_LANE_CPUS,
    describe_mode,
    resolve_cgroup_parent,
    slice_memory_max,
)
from benchmarks.sregym.fastloop.loop import LoopConfig, run_incidents, write_loop_manifest
from benchmarks.sregym.fastloop.records import AgentSummary, IncidentRecord, load_records, summarize
from benchmarks.sregym.fastloop.stream import load_stream_config
from benchmarks.sregym.fastloop.toy_upstream import add_toy_parser
from benchmarks.sregym.fastloop.wipe import (
    AttemptMarker,
    KubectlLaneCluster,
    LaneInjectionState,
    Tier2Trigger,
    application_pod_problems,
    baseline_proof_owed,
    run_tier2,
    unrecovered_rows,
)
from benchmarks.sregym.fastloop.worker_client import SregymWorker, worker_argv
from benchmarks.sregym.repository_git import host_clone_command, host_git_command
from sdo.agents.provider import AGENT_EFFORTS

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from benchmarks.lanes.eks import EksLane
    from benchmarks.sregym.adapter import ClusterOps, RuntimeConfig
    from benchmarks.sregym.fastloop.composite import CompositeSettings
    from benchmarks.sregym.fastloop.evidence_queue import EvidenceExportQueue
    from benchmarks.sregym.fastloop.fault_tracker import FaultSpec
    from benchmarks.sregym.fastloop.loop import DiagnosisGrader, IncidentAgent
    from benchmarks.sregym.fastloop.wipe import Tier1Receipt, Tier2Reason

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SREGYM_DIR = REPO_ROOT / "third_party" / "sregym"
INCIDENTS_FILENAME = "incidents.jsonl"
STALE_IMAGES_HELP = (
    "skip the check that lane images were built from this tree's Dockerfiles and Go modules "
    "(deliberate frozen-image experiments only)"
)
CLUSTER_OP_TIMEOUT_SECONDS = wait_for(WaitKind.CLUSTER_OP)
DEPLOY_OP_TIMEOUT_SECONDS = wait_for(WaitKind.DEPLOY_OP)


def _worker(environment: FastloopEnvironment) -> SregymWorker:
    if environment.app == "toy":
        from benchmarks.sregym.fastloop.toy import ToyWorker

        return ToyWorker(environment)  # type: ignore[return-value]
    from benchmarks.sregym.fastloop.source_preflight import assert_harness_sources, assert_runtime_images

    assert_harness_sources(environment.sregym_dir)
    if not environment.allow_stale_images:
        assert_runtime_images(
            environment.images, environment.dev_mount or REPO_ROOT, dev_mount=environment.dev_mount is not None
        )
    if not lane_images_current(environment):
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
    for image in (images.controller, images.responder):
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


#: How often a worker start asks ECR whether it still holds the lane's images before it gives up: a throttled or
#: briefly unreachable API (six lanes ask at every row) is not a lost image.
ECR_ATTEMPTS = 3


def _ecr_holds(provider: Any, image: str, *, sleep: Callable[[float], None] = time.sleep) -> bool:
    from benchmarks.lanes.eks import EksLaneError

    for attempt in range(1, ECR_ATTEMPTS + 1):
        try:
            return bool(provider.holds(image))
        except EksLaneError as error:
            if attempt == ECR_ATTEMPTS:
                raise
            logger.warning("ECR did not answer for %s (attempt %d/%d): %s", image, attempt, ECR_ATTEMPTS, error)
            sleep(wait_for(WaitKind.POLL) * attempt)
    return False


def lane_images_current(environment: FastloopEnvironment) -> bool:
    """Whether every node of the lane still runs the images the lane recorded.

    A kind lane compares the node containers' images with the local ones; an EKS lane's images are digests, so they
    are current while ECR still stores them.
    """
    if environment.eks is not None:
        from benchmarks.lanes.eks import EksProvider

        provider = EksProvider(environment.eks, str(environment.kubeconfig))
        return all(
            _ecr_holds(provider, image) for image in (environment.images.controller, environment.images.responder)
        )
    return node_images_match(environment.cluster, environment.images)


def up_worker_environment(args: argparse.Namespace, *, workspace: Path) -> dict[str, str]:
    """The SREGym worker environment for ``up``: one reused kind lane deploying ``workspace`` from source."""

    resolve_node_limits(args)
    images = Images(controller=args.controller_image, responder=args.responder_image)
    worker_env = {
        "SREGYM_KIND_CLUSTER_PREFIX": args.cluster_prefix,
        "SREGYM_KIND_CLUSTER_NAME": f"{args.cluster_prefix}{args.worker_id}",
        "SREGYM_KIND_REQUIRE_NETWORK_POLICY": "1",
        "FASTLOOP_DELIVERY_RECORD": str(args.run_dir.resolve() / "image_delivery.jsonl"),
        "SREGYM_KIND_NETWORK_POLICY_CANARY_IMAGE": images.controller,
        "SREGYM_KIND_REQUIRED_IMAGES": json.dumps([images.controller, images.responder]),
        "SREGYM_REUSE_CLUSTER": "1",
        **({"SREGYM_WORKER_CPU_LIMIT": args.cpu_limit} if args.cpu_limit is not None else {}),
        "SDO_LANE_CPUS": args.lane_cpus,
        "SDO_LANE_CGROUP_PARENT": args.lane_cgroup or "",
        "SREGYM_WORKER_MEMORY_LIMIT": args.memory_limit,
        "SREGYM_WORKER_PIDS_LIMIT": str(args.pids_limit),
        **HostLimits(args.host_lock_dir.resolve(), args.host_redeploys).worker_env(),
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
        ["docker", "info", "--format", "{{.DockerRootDir}}"],
        check=True,
        capture_output=True,
        text=True,
        timeout=wait_for(WaitKind.SLOW_COMMAND),
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

    eks_platform = getattr(args, "platform", "kind") == "eks"
    if getattr(args, "reuse_cluster_only", False):
        if args.dev_mount is not None:
            raise ValueError(
                "an EKS lane has no node mounts; pass --no-dev-mount"
                if eks_platform
                else "attach-only up requires --no-dev-mount; existing node mounts cannot be changed"
            )
        if not os.environ.get("KUBECONFIG") or not Path(os.environ["KUBECONFIG"]).is_file():
            raise ValueError("attach-only up requires the existing lane KUBECONFIG")
    if eks_platform:
        if args.app != "sregym":
            raise ValueError("--platform eks supports SREGym applications only")
        if not getattr(args, "ecr_registry", None):
            raise ValueError(
                "--platform eks requires --ecr-registry <account>.dkr.ecr.<region>.amazonaws.com "
                "(localhost:<port> on a local kind host)"
            )
        if getattr(args, "parallel_root", None) is not None:
            raise ValueError("an EKS lane shares memory through the remote memory branch, not --parallel-root")
        for item in getattr(args, "ecr_tag", ()):
            if "=" not in item or not item.split("=", 1)[0]:
                raise ValueError(f"--ecr-tag must be KEY=VALUE, got {item!r}")
    if args.kind_worker_nodes < 0:
        raise SystemExit("--kind-worker-nodes must not be negative")
    if getattr(args, "parallel_root", None) is not None and (args.dev_mount is None or args.app in {"toy", "gitlab"}):
        raise ValueError("--parallel-root requires an SREGym --dev-mount lane")
    # An EKS lane builds images on this host and pushes them; it never runs kind.
    # (a local kind host's registry needs no AWS CLI)
    aws_needed = eks_platform and not str(getattr(args, "ecr_registry", "")).startswith("localhost:")
    for executable in ("docker", "kubectl", "go", *(("aws",) if aws_needed else () if eks_platform else ("kind",))):
        if shutil.which(executable) is None:
            raise ValueError(f"fastloop preflight: missing {executable}")
    subprocess.run(
        ["docker", "info", "--format", "{{.ServerVersion}}"],
        check=True,
        capture_output=True,
        text=True,
        timeout=wait_for(WaitKind.SLOW_COMMAND),
    )
    if not eks_platform:
        check_host_capacity(args)
    if args.dev_mount is None:
        if not eks_platform:
            warnings.warn("--no-dev-mount: code changes require image rebuilds", stacklevel=2)
    elif not (args.dev_mount.resolve() / "sdo/operator/go.mod").is_file():
        raise ValueError(f"fastloop preflight: not an SDO worktree: {args.dev_mount}")
    if getattr(args, "app", None) not in {"toy", "gitlab"}:
        from benchmarks.sregym.fastloop.source_preflight import assert_harness_sources

        assert_harness_sources(getattr(args, "sregym_dir", DEFAULT_SREGYM_DIR))
    from benchmarks.sregym.adapter import BENCHMARK_IMAGE_ENTRY_POINTS, assert_image_entrypoints

    lane_images = (
        (args.controller_image, ()),
        (args.responder_image, BENCHMARK_IMAGE_ENTRY_POINTS),
    )
    for image, _modules in lane_images:
        if image is not None:
            subprocess.run(
                ["docker", "image", "inspect", image],
                check=True,
                capture_output=True,
                text=True,
                timeout=wait_for(WaitKind.SLOW_COMMAND),
            )
    if not getattr(args, "allow_stale_images", False):
        # Label reads only: a stale image fails here in seconds, not after a live run's refresh budget.
        from benchmarks.sregym.fastloop.source_preflight import assert_images_fresh

        assert_images_fresh(
            {
                role: image
                for role, image in (("controller", args.controller_image), ("responder", args.responder_image))
                if image
            },
            args.dev_mount.resolve() if args.dev_mount is not None else REPO_ROOT,
            dev_mount=args.dev_mount is not None,
        )
    for image, modules in lane_images:
        if image is None:
            continue
        assert_image_entrypoints(image, modules, source_root=args.dev_mount if modules else None, run=subprocess.run)


# The GitLab chart's webservice, sidekiq and PostgreSQL need a node sized for
# them: on the 3 cpu default a rollout of the stamped workloads saturated the
# node and the lane failed in its steady window.
GITLAB_CPU_LIMIT = "8"
GITLAB_MEMORY_LIMIT = "20g"


_UNRESOLVED: Any = object()


def resolve_node_limits(args: argparse.Namespace) -> None:
    """Fill the node limits ``up`` left unset with the selected app's defaults."""

    gitlab = getattr(args, "app", None) == "gitlab"
    if args.cpu_limit is None and gitlab:
        args.cpu_limit = GITLAB_CPU_LIMIT  # else the lane cap below is split across the nodes
    if getattr(args, "lane_cpus", None) is None:
        args.lane_cpus = os.environ.get("SDO_LANE_CPUS") or DEFAULT_LANE_CPUS
    if getattr(args, "lane_cgroup", _UNRESOLVED) is _UNRESOLVED:
        args.lane_cgroup = resolve_cgroup_parent(
            getattr(args, "lane_cgroup_parent", None) or os.environ.get("SDO_LANE_CGROUP_PARENT")
        )
    if args.memory_limit is None:
        args.memory_limit = GITLAB_MEMORY_LIMIT if gitlab else DEFAULT_MEMORY_LIMIT


def up_host_memory_policy(args: argparse.Namespace) -> HostMemoryPolicy:
    """The host memory policy: ``SDO_HOST_*`` environment or host guard file, each overridable by its ``up`` flag."""
    base = HostMemoryPolicy.from_env(host_env(os.environ))
    reserve, pressure, budget = (
        getattr(args, name, None) for name in ("host_reserve_gib", "host_psi_max", "host_lane_budget_gib")
    )
    policy = HostMemoryPolicy(
        reserve_bytes=base.reserve_bytes if reserve is None else int(reserve * 1024**3),
        max_pressure_avg60=base.max_pressure_avg60 if pressure is None else pressure,
        lane_budget_bytes=base.lane_budget_bytes if budget is None else int(budget * 1024**3),
    )
    parent = getattr(args, "lane_cgroup", None)
    if policy.lane_budget_bytes is None and parent is not None:
        # The parent slice already caps the fleet: its MemoryMax is the budget when none was configured.
        policy = replace(policy, lane_budget_bytes=slice_memory_max(parent))
    return policy


def assert_lane_host_memory(args: argparse.Namespace, *, running: dict[str, int] | None = None) -> None:
    """Refuse to create or start a lane the host has no memory for, or past the host's lane budget.

    The lane cap is the node cap times its kind nodes. A lane that already runs holds its memory, so only the
    reserve and the pressure gate apply to it; foreign clusters are neither counted nor touched.
    """
    node = parse_memory_size(args.memory_limit)
    cap = node * (1 + getattr(args, "kind_worker_nodes", 1))
    prefix = (
        "fastloop-dev-w" if args.dev_mount is not None and args.cluster_prefix == "fastloop-w" else args.cluster_prefix
    )
    cluster = f"{prefix}{args.worker_id}"
    policy = up_host_memory_policy(args)
    if running is None:
        # Our lanes are every cluster under the host-wide prefix (lane clusters are named <prefix><lane>, so the
        # lane's own prefix would see only itself).
        scope = getattr(args, "host_lane_prefix", None) or host_env(os.environ).get("SDO_HOST_LANE_PREFIX") or prefix
        running = running_lane_caps(scope, default_node_bytes=node)
    assert_host_memory_admission(0 if cluster in running else cap, policy)
    assert_lane_budget(cluster, cap, running, policy)


def _admit_lane(args: argparse.Namespace) -> None:
    """The host memory gate ``up`` passes before it touches anything (a seam so tests need not read /proc)."""
    assert_lane_host_memory(args)


def eks_worker_environment(args: argparse.Namespace, *, workspace: Path, lane: EksLane) -> dict[str, str]:
    """The SREGym worker environment of an EKS lane: attach only, build the app from source, publish by mirror."""

    return {
        # A virtual lane's StorageClasses are its host's: SREGym installs no OpenEBS there.
        **({"SREGYM_MANAGED_STORAGE": "1"} if lane.virtual else {}),
        "SREGYM_KIND_CLUSTER_PREFIX": args.cluster_prefix,
        "SREGYM_KIND_CLUSTER_NAME": f"{args.cluster_prefix}{args.worker_id}",
        "SREGYM_CLUSTER_PLATFORM": "eks",
        "SREGYM_IMAGE_DELIVERY": "build",
        # Checked at attach (the VPC CNI policy agent), so the lane topology records the feature as on kind.
        "SREGYM_KIND_REQUIRE_NETWORK_POLICY": "1",
        "FASTLOOP_EKS_LANE": lane.model_dump_json(),
        "FASTLOOP_DELIVERY_RECORD": str(args.run_dir.resolve() / "image_delivery.jsonl"),
        "SREGYM_REUSE_CLUSTER": "1",
        **HostLimits(args.host_lock_dir.resolve(), args.host_redeploys).worker_env(),
        "SREGYM_DEPLOY_FROM_SOURCE": "1",
        "SREGYM_APP_SOURCE_DIR": str(workspace),
        "SREGYM_PRESERVE_INFRASTRUCTURE": "1",
        "SREGYM_SOURCE_BUILD_CACHE": "1",
    }


def attach_eks_lane(args: argparse.Namespace) -> tuple[EksLane, Images]:
    """Attach the EKS lane provider (fence, NetworkPolicy, node agent) and pin SDO's images in ECR; idempotent."""

    from benchmarks.lanes.eks import EksLaneError, EksProvider

    provider = EksProvider.attach(
        str(Path(os.environ["KUBECONFIG"]).resolve()),
        registry=args.ecr_registry,
        repository_tags=dict(item.split("=", 1) for item in args.ecr_tag),
        # The lanes platform sets this on a vcluster lane (kube-system and the nodes are the host cluster's).
        virtual=os.environ.get("BENCH_LANE_VIRTUAL") == "1",
    )
    prior = FastloopEnvironment.load(args.run_dir.resolve()) if (args.run_dir / "fastloop.json").is_file() else None
    if (
        prior is not None
        and prior.eks is not None
        and (prior.eks.server, prior.eks.cluster_uid) != (provider.lane.server, provider.lane.cluster_uid)
    ):
        raise EksLaneError("KUBECONFIG reaches a different cluster than the one this lane was created on")
    pinned = []
    for image in (args.controller_image, args.responder_image):
        pinned.append(provider.pin(image))
        # The lane's nodes pull the image in the background while the next one is pushed and the application builds,
        # so the first SDO install finds both on the node. Never a reason to fail or wait: the install pulls anyway.
        try:
            provider.register_prepull([pinned[-1]])
        except EksLaneError as error:
            logger.warning("prepull of %s not registered (the nodes pull it at first use): %s", pinned[-1], error)
    return provider.lane, Images(controller=pinned[0], responder=pinned[1])


def _up(args: argparse.Namespace) -> int:
    eks_platform = getattr(args, "platform", "kind") == "eks"
    if eks_platform:
        # An EKS lane only attaches: no kind cluster, node caps or host memory admission apply.
        args.reuse_cluster_only = True
    else:
        resolve_node_limits(args)
        NodeLimits.from_args(args)
        logger.info(describe_mode(args.lane_cgroup))
    HostLimits(args.host_lock_dir.resolve(), args.host_redeploys)
    if not eks_platform:
        _admit_lane(args)
    up_preflight(args)
    if args.app == "toy":
        from benchmarks.sregym.fastloop.toy import up

        return up(args)
    if args.app == "gitlab":
        from benchmarks.sregym.fastloop.gitlab_lane import up as gitlab_up

        return gitlab_up(args)
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
    evidence_root = args.evidence_root.resolve() if getattr(args, "evidence_root", None) is not None else None
    if evidence_root is not None:
        if args.dev_mount is None and not eks_platform:
            raise ValueError("--evidence-root requires a --dev-mount lane (the node mount is set at cluster creation)")
        from benchmarks.sregym.fastloop.dev import prepare_evidence_root

        prepare_evidence_root(evidence_root)
    if prior is not None and prior.evidence_root != evidence_root:
        raise ValueError("--evidence-root must match the root mounted when this lane was created")
    if prior is not None and prior.dev_mount != args.dev_mount:
        raise ValueError(
            "up --dev-mount must match the worktree used to create this environment; "
            "use --no-dev-mount for a lane created without a dev mount"
        )
    workspace = run_dir / "application_workspace"
    if not workspace.exists():
        if args.seed is None:
            raise SystemExit("--seed is required the first time (a git repository with the application source)")
        subprocess.run(
            host_clone_command(args.seed, workspace, "--quiet"), check=True, timeout=wait_for(WaitKind.OPERATION)
        )
    private_tmp = None if args.no_sandbox else run_dir / "tmp"
    if private_tmp is not None:
        private_tmp.mkdir(exist_ok=True)
    images = Images(controller=args.controller_image, responder=args.responder_image)
    eks_lane: EksLane | None = None
    host_operator: Path | None = None
    if eks_platform:
        from benchmarks.sregym.fastloop.dev import build_host_operator

        eks_lane, images = attach_eks_lane(args)
        from benchmarks.sregym.fastloop.eks_lane import apply_lane_env

        apply_lane_env(eks_lane, os.environ)
        host_operator = build_host_operator(REPO_ROOT)
    if args.dev_mount is not None:
        from benchmarks.sregym.fastloop.dev import prepare_dev_mount

        prepare_dev_mount(
            args.dev_mount, args.sregym_dir.resolve(), run_dir, parallel_root, evidence_root=evidence_root
        )
    seed_commit = prior.seed_commit if prior is not None else None
    if seed_commit is None and (workspace / ".git").exists():
        seed_commit = subprocess.run(
            host_git_command(workspace, "rev-parse", "HEAD"),
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    worker_env = (
        eks_worker_environment(args, workspace=workspace, lane=eks_lane)
        if eks_lane is not None
        else up_worker_environment(args, workspace=workspace)
    )
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
            and lane_images_current(previous)
        ):
            if previous.namespace == "astronomy-shop":
                from benchmarks.sregym.adapter import prepare_oracle_observability

                prepare_oracle_observability(
                    kubeconfig=previous.kubeconfig,
                    sregym_dir=previous.sregym_dir,
                    cluster=previous.cluster,
                )
            with _worker(previous) as cached_worker:
                health = cached_worker.request(
                    "health", namespace=previous.namespace, timeout=wait_for(WaitKind.SLOW_COMMAND)
                )
            if (
                health.get("healthy")
                and LaneInjectionState(previous.kubeconfig, previous.namespace).reason() is None
                and AttemptMarker(previous.run_dir).pending() is None
            ):
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
        platform="eks" if eks_platform else "kind",
        eks=eks_lane,
        host_operator=host_operator,
        pinned_tools=eks_platform,
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
        allow_stale_images=getattr(args, "allow_stale_images", False),
        worker_env=worker_env,
        parallel_root=parallel_root,
        evidence_root=evidence_root,
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
    app_recreated = False
    with worker:
        cluster = worker.request(
            "attach_existing" if args.reuse_cluster_only else "cluster",
            worker_id=args.worker_id,
            log_dir=str(run_dir),
            timeout=CLUSTER_OP_TIMEOUT_SECONDS,
        )
        history = (
            LaneInjectionState(prior.kubeconfig, prior.namespace).reason() or AttemptMarker(prior.run_dir).pending()
            if prior is not None
            else None
        )
        up_revision: str | None = None
        if prior is not None and cluster["cluster"] == prior.cluster and (args.redeploy or history is not None):
            from benchmarks.sregym.adapter import control_namespace_for
            from benchmarks.sregym.fastloop.reset import KubectlResetCluster, require_terminal_reflection
            from benchmarks.sregym.fastloop.wipe import Tier1Receipt, Tier2Reason, Tier2Trigger

            require_terminal_reflection(prior)
            started = time.monotonic()
            KubectlResetCluster(prior.kubeconfig, control_namespace_for(prior.namespace)).reset_controller(
                prior.seed_commit or "HEAD",
                preserve_repository=True,
            )
            reason = (
                Tier2Reason(Tier2Trigger.EXPLICIT_REDEPLOY, "explicit redeploy from accepted source")
                if args.redeploy
                else history
            )
            assert reason is not None
            deployed = recreate_application(
                prior,
                worker,
                reason,
                Tier1Receipt(time.monotonic() - started, (), False, ()),
                run_dir,
            )
            app_recreated = True
        else:
            from benchmarks.sregym.fastloop.deployed_application import workspace_revision

            # The revision `up` deploys is fixed before the deploy, as in every later recreation.
            up_revision = workspace_revision(placeholder.workspace)
            deployed = worker.request(
                "deploy",
                problem_id=args.problem,
                baseline_path=str(placeholder.baseline_path),
                redeploy=args.redeploy,
                revision=up_revision,
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
    from benchmarks.sregym.fastloop.lane_installability import capture_topology

    environment.lane_topology = capture_topology(
        environment.kubeconfig,
        network_policy=environment.worker_env.get("SREGYM_KIND_REQUIRE_NETWORK_POLICY") == "1",
    )
    cluster = KubectlApplicationCluster(environment.kubeconfig)
    namespaces = application_namespaces(environment.baseline_path, environment.kubeconfig, environment.namespace)
    if deployed.get("deployed") is True and not app_recreated:
        save_application_baseline(
            environment.baseline_path,
            tuple(capture_application_baseline(cluster, namespace) for namespace in namespaces),
            installed=True,
        )
        _record_fresh_deploy(environment, up_revision)
    elif not app_recreated:
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
            cluster_scoped(KubectlLaneCluster(environment.kubeconfig, environment.host_owned_kinds).list_objects()),
        )
    environment.save()
    if environment.eks is not None:
        from benchmarks.sregym.fastloop.eks_lane import verify_mirror_serves

        print(f"the lane's mirror serves {verify_mirror_serves(environment)}")
    if app_recreated:
        AttemptMarker(environment.run_dir).end(())
    if environment.parallel_root is not None:
        from benchmarks.sregym.fastloop.shared_memory import provision_parallel_volume

        provision_parallel_volume(environment)
    print(
        f"fastloop environment ready: cluster={environment.cluster} namespace={environment.namespace} "
        f"deployed={deployed.get('deployed')} ({float(deployed.get('seconds', 0.0)):.0f}s) -> {environment.path}"
    )
    return 0


def _repair_repository_bind(args: argparse.Namespace) -> int:
    from benchmarks.sregym.fastloop.shared_memory import ensure_repository_bind

    environment = FastloopEnvironment.load(args.run_dir.resolve())
    print(json.dumps(asdict(ensure_repository_bind(environment)), sort_keys=True))
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


def _runtime_config(args: argparse.Namespace, environment: FastloopEnvironment, results_dir: Path) -> RuntimeConfig:
    from benchmarks.sregym.adapter import RuntimeConfig, control_namespace_for, evidence_claim_volume
    from benchmarks.sregym.fastloop.dev import EVIDENCE_NODE_ROOT

    registrations = args.agent_registrations.resolve() if args.agent_registrations else None
    if registrations is None and environment.app == "toy":
        registrations = REPO_ROOT / "benchmarks/sregym/fastloop/agent_registrations"
    if registrations is None and args.provider == "codex":
        from benchmarks.sregym.fastloop.recording_worker import recording_registrations

        registrations = recording_registrations(results_dir, environment.images.responder)

    return RuntimeConfig(
        repository=environment.workspace,
        namespace=environment.namespace,
        application=environment.application,
        controller_image=environment.images.controller,
        responder_image=environment.images.responder,
        deployment_source=deployment_source_for_environment(environment),
        repository_pvc="sdo-application-repository",
        credentials_secret=getattr(args, "credentials_secret", "sdo-agent-credentials"),
        model=args.model,
        timeout_seconds=args.timeout,
        repair_policy=getattr(args, "repair_policy", None),
        agent_provider=args.provider,
        replay=getattr(args, "replay", "on"),
        agent_effort=args.reasoning_effort or "",
        lifecycle_timeout_seconds=getattr(args, "lifecycle_timeout", None),
        frozen_memory=getattr(args, "frozen_memory", False),
        external_incidents=getattr(args, "external_incidents", False),
        reflection_guidance=getattr(args, "reflection_guidance", None),
        code_path=getattr(args, "code_path", "go"),
        code_path_overrides=getattr(args, "code_path_overrides", "") or "",
        healthy_baseline=args.healthy_baseline or None,
        config_files=tuple(path.resolve() for path in getattr(args, "config", None) or ()),
        controller_namespace=control_namespace_for(environment.namespace),
        kind_cluster_name=environment.cluster,
        persistent=True,
        agent_registrations=registrations,
        permissive_responder=getattr(args, "permissive_responder", False),
        responder_namespaces=getattr(args, "responder_namespaces", ()),
        shared_accepted_id=environment.shared_accepted_id,
        reuse_existing=bool(environment.shared_accepted_id),
        evidence_sink_host_path=(
            EVIDENCE_NODE_ROOT if environment.evidence_root is not None and environment.platform == "kind" else ""
        ),
        # An EKS lane's sink is a claim in the control namespace that fastloop pulls before deleting it.
        evidence_sink_volume=(
            evidence_claim_volume() if environment.evidence_root is not None and environment.platform == "eks" else ""
        ),
        evidence_run_id=lane_evidence_run_id(environment, results_dir) if environment.evidence_root is not None else "",
        lane_platform=environment.platform,
    )


def _sdo_agent(
    args: argparse.Namespace,
    environment: FastloopEnvironment,
    results_dir: Path,
    *,
    composite: bool = False,
    stream_faults: dict[str, tuple[FaultSpec, ...]] | None = None,
    worker: SregymWorker | None = None,
    evidence_queue: EvidenceExportQueue | None = None,
) -> IncidentAgent:
    from benchmarks.sregym.adapter import (
        control_namespace_for,
        deployed_lifecycle,
        launch_native_lifecycle,
    )
    from benchmarks.sregym.fastloop.sdo_agent import SdoAgentSettings, SdoPersistentAgent

    runtime_config = _runtime_config(args, environment, results_dir)
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
        closure_bound_seconds=args.closure_bound,
        checkpoint_enabled=not args.no_checkpoints,
    )
    if environment.app == "toy":
        from benchmarks.sregym.fastloop.toy_ops import ToyClusterOps

        ops = _cluster_ops(args, environment.namespace, ops=ToyClusterOps(environment.kubeconfig))
    else:
        ops = _cluster_ops(args, environment.namespace)

    from benchmarks.sregym.adapter import PersistentState

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
            pre_onboarding_hook=getattr(args, "pre_onboarding_hook", None),
        )
        from benchmarks.sregym.fastloop.admission import CarriedProof
        from benchmarks.sregym.fastloop.setup_proof import seal_after_admission

        warm_uid = getattr(args, "warm_proven_controller_uid", None)
        ops = EvalAdmissionOps(  # type: ignore[assignment]
            ops,
            admission,
            fresh=fresh_memory_start,
            on_proven=lambda proof: seal_after_admission(environment, proof),
            carried=CarriedProof(warm_uid, unchanged=lambda: not getattr(args, "warm_application_repaired", True))
            if warm_uid
            else None,
        )

    def start_native(context: object) -> bool:
        nonlocal fresh_memory_start
        if environment.shared_accepted_id:
            from benchmarks.sregym.adapter import inspect_existing, native_install_config

            inspect_existing(native_install_config(runtime_config))
            return True
        if getattr(args, "ablation_arm", None):
            from benchmarks.sregym.adapter import install_persistent_controller

            install_persistent_controller(runtime_config)
            return False
        if environment.app == "toy":
            # Setup runs only for a from-scratch install on the planned commit, so a retained checkpoint or
            # health-refresh record from an earlier install would describe memory this install no longer has.
            from benchmarks.sregym.fastloop.toy import retire_control_state

            retire_control_state(environment, control_namespace_for(environment.namespace))
        fresh_memory_start = False
        if environment.app == "toy":
            from benchmarks.sregym.fastloop.toy import install_controller

            install_controller(environment, runtime_config)
        else:
            objective = lifecycle_objective(context.health_objective, getattr(args, "goal", None))  # type: ignore[attr-defined]
            launch_native_lifecycle(runtime_config, objective, "")
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
    return SdoPersistentAgent(
        settings,
        **agent_arguments,
        keep_running=environment.app == "toy",
        check_continuity=environment.app == "toy",
        evidence_queue=evidence_queue,
    )


def lifecycle_objective(generated: str, goal: str | None) -> str:
    """The human goal a fresh-memory install publishes: the operator's own goal when one is given
    (the deployer prompt budgets it at 256 characters), else the adapter's generated objective."""
    return goal.strip() if goal and goal.strip() else generated


def _wipes_at_row_start(args: argparse.Namespace, environment: FastloopEnvironment) -> bool:
    return args.agent == "sdo" and environment.app != "toy" and not environment.shared_accepted_id


def row_start_tier2(args: argparse.Namespace, *, wiped_at_start: bool, incidents: int) -> bool:
    """Whether the row leaves its fault recovery to the next row's start (Tier-2) instead of paying it at its end.

    Only a one-incident SDO row that wiped at start defers it; ``--end-of-row-recovery`` makes it pay the same
    recovery and cleanup a vanilla row pays, inside the row. So does ``--warm-lane``: the deferral assumes the next row
    wipes the lane and recreates the app, and a warm lane's next row does neither (its transaction refuses a lane
    whose attempt marker says a row was not verified recovered), so the row leaves the lane recovered.
    """

    return (
        wiped_at_start
        and incidents == 1
        and not getattr(args, "end_of_row_recovery", False)
        and not getattr(args, "warm_lane", False)
    )


def baseline_proof_at_next_start(args: argparse.Namespace, *, marked: bool, row_start_tier2: bool) -> bool:
    """Whether the run's last row leaves the proof of its restored baseline to the lane's next start.

    Only a row that pays its recovery at its end (``--end-of-row-recovery`` or ``--warm-lane``) on a lane whose attempt
    marker the next start reads (``marked``: it wiped at start or reseeded warm) defers: that start proves the baseline
    before it injects (the warm reset grades the restored app; otherwise the first no-fault rehearsal grades it afresh),
    and every reader that does not prove it sees the marker pending. Repeated cleanup passes keep their oracles.
    """

    return (
        marked
        and not row_start_tier2
        and getattr(args, "cleanup_passes", 1) == 1
        and (getattr(args, "end_of_row_recovery", False) or getattr(args, "warm_lane", False))
    )


def _warm_reset_args(args: argparse.Namespace, environment: FastloopEnvironment) -> argparse.Namespace:
    # A warm caller retains accepted knowledge by default. An explicitly
    # selected planned revision still owns the ordinary reset semantics.
    planned = args.planned_commit or AcceptedSource.of(environment).revision
    return argparse.Namespace(**{**vars(args), "planned_commit": planned})


def _replace_trace_stores_for_next_row(environment: FastloopEnvironment) -> None:
    """A row's end replaces the app's in-memory trace stores without waiting: the next row on the lane then finds them
    young and Ready for longer than its baseline check's sustained-health window (a warm start replaces only an old
    one). Best effort: a failure leaves the next start's own replacement to do it."""
    from benchmarks.sregym.fastloop.app_observability import restart_in_memory_trace_stores

    try:
        namespaces = application_namespaces(environment.baseline_path, environment.kubeconfig, environment.namespace)
        restart_in_memory_trace_stores(environment.kubeconfig, namespaces, reason="row end")
    except Exception:
        logger.warning("row end: trace store replacement failed; the next start replaces it", exc_info=True)


def _wipe_sdo_and_harness(args: argparse.Namespace, environment: FastloopEnvironment) -> Tier1Receipt:
    """Tier 1: delete SDO's control namespace and every harness leftover, then re-provision and re-seed SDO's inputs."""
    from benchmarks.sregym.fastloop.wipe import credentials_hook, wipe_environment

    if args.credentials_hook is None:
        raise SystemExit("eval: a row-start wipe deletes the SDO namespace; pass --credentials-hook to re-provision it")
    planned = args.planned_commit or environment.seed_commit
    if not planned:
        raise ValueError("eval: the row-start wipe needs --planned-commit or a seed commit captured by fastloop up")
    from benchmarks.sregym.fastloop.app_observability import await_trace_stores_ready, restart_in_memory_trace_stores

    # The app's own trace store grows while a lane idles; replace it before SDO's install records the namespace's Pods
    # (the replacement overlaps the wipe).
    namespaces = application_namespaces(environment.baseline_path, environment.kubeconfig, environment.namespace)
    restart_in_memory_trace_stores(environment.kubeconfig, namespaces)
    receipt = wipe_environment(
        environment,
        KubectlLaneCluster(environment.kubeconfig, environment.host_owned_kinds),
        applications=application_namespaces(environment.baseline_path, environment.kubeconfig, environment.namespace),
        provision_credentials=credentials_hook(args.credentials_hook.resolve(), environment.kubeconfig),
        planned_commit=planned,
        provenance_path=environment.run_dir / "results" / args.run_id / "reset-generation.json",
        timeout_seconds=wait_for(WaitKind.ROLLOUT),
    )
    await_trace_stores_ready(environment.kubeconfig, namespaces)
    logger.info(
        "Tier-1 wipe: %.1fs, removed %s, force-finalized %s, helpers removed %s",
        receipt.seconds,
        ", ".join(receipt.removed) or "nothing",
        ", ".join(receipt.force_finalized) or "nothing",
        ", ".join(receipt.helpers_removed) or "nothing",
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


def _record_deployed_source(environment: FastloopEnvironment, revision: str | None) -> None:
    """After a completed deploy of ``revision`` and its baseline capture, stamp the baseline with that source."""
    from benchmarks.sregym.fastloop.deployed_application import forget_deployed_source, record_deployed_source

    if revision is None:
        forget_deployed_source(environment)
    else:
        record_deployed_source(environment, revision)


def _deployed_source_change(environment: FastloopEnvironment, revision: str | None) -> Tier2Reason | None:
    """Why the running app is not the deploy of the row's application revision; unknown facts recreate."""
    from benchmarks.sregym.fastloop.deployed_application import deployed_application_change
    from benchmarks.sregym.fastloop.wipe import Tier2Reason, Tier2Trigger

    if revision is None:
        return Tier2Reason(Tier2Trigger.ACCEPTED_SOURCE_CHANGED, "the row's application revision is unknown")
    return deployed_application_change(environment, revision)


def _application_revision(args: argparse.Namespace, environment: FastloopEnvironment) -> str | None:
    """The source this row's application must be: its arm's planned commit, else the workspace head.

    Fixed once, before any deploy: another lane's publication may move the head while this row deploys.
    """
    from benchmarks.sregym.fastloop.deployed_application import workspace_revision

    return getattr(args, "planned_commit", None) or workspace_revision(environment.workspace)


def recreate_application(
    environment: FastloopEnvironment,
    worker: SregymWorker,
    reason: Tier2Reason,
    tier1: Tier1Receipt,
    results_dir: Path,
    *,
    revision: str | None = None,
) -> dict[str, Any]:
    """The reset transaction shared by explicit redeploy and row-start recreation.

    It deploys ``revision`` (default: the head, read before the deploy) and stamps the new baseline with it
    only after the whole transaction completed.
    """
    from benchmarks.sregym.fastloop.deployed_application import forget_deployed_source, workspace_revision

    revision = revision or workspace_revision(environment.workspace)
    deployed: dict[str, Any] = {}

    def deploy() -> None:
        deployed.update(
            worker.request(
                "deploy",
                problem_id=environment.deploy_problem,
                baseline_path=str(environment.baseline_path),
                redeploy=True,
                revision=revision,
                timeout=DEPLOY_OP_TIMEOUT_SECONDS,
            )
        )
        if deployed.get("deployed") is not True or not deployed.get("health", {}).get("healthy"):
            raise RuntimeError("eval: recreated application did not prove healthy deployment")
        if environment.namespace == "social-network":
            # A recreated namespace has empty stores; rows read the same fixture `reset` restores.
            from benchmarks.sregym.fastloop.social_fixture import restore_social_fixture

            restore_social_fixture(environment.kubeconfig)
        _recapture_application_baseline(environment)

    forget_deployed_source(environment)
    run_tier2(
        reason,
        tier1,
        cluster=KubectlLaneCluster(environment.kubeconfig, environment.host_owned_kinds),
        namespaces=application_namespaces(environment.baseline_path, environment.kubeconfig, environment.namespace)
        if environment.baseline_path.is_file()
        else (environment.namespace,),
        deploy=deploy,
        host_limits=HostLimits.from_env(environment.worker_env),
        injection_state=LaneInjectionState(environment.kubeconfig, environment.namespace),
        timeout_seconds=wait_for(WaitKind.ROLLOUT),
    ).write(results_dir)
    _record_deployed_source(environment, revision)
    return deployed


class ApplicationRepair:
    """Whether this row already recreated its application because the traffic probe failed (once per row)."""

    def __init__(self) -> None:
        self.done = False


def rehearse_with_repair(
    environment: FastloopEnvironment,
    worker: SregymWorker,
    results_dir: Path,
    problem_id: str,
    index: int,
    repair: ApplicationRepair,
    *,
    revision: str | None,
    fresh: bool = False,
) -> None:
    """Run the no-fault rehearsal; a failing traffic probe recreates the application once, then re-checks.

    The rehearsal is the setup health gate: pods Ready does not prove the app serves traffic, and a warm
    lane can carry a broken app from the previous row. Every arm gets the same rule, and
    ``setup-repair.json`` records what was repaired. Only an app that still fails after the recreate rejects the row.
    ``fresh`` grades the live app even when a healthy rehearsal is cached: the proof a previous row left to this start.
    """
    from benchmarks.sregym.fastloop.wipe import Tier1Receipt, Tier2Reason, Tier2Trigger

    def rehearse() -> dict[str, Any]:
        return worker.request(
            "rehearse_oracles", problem_id=problem_id, fresh=fresh, timeout=wait_for(WaitKind.LONG_OPERATION)
        )

    rehearsal = rehearse()
    if rehearsal.get("success") is not True and not repair.done:
        repair.done = True
        failed = rehearsal
        reason = Tier2Reason(
            Tier2Trigger.TRAFFIC_PROBE_FAILED, f"the application failed the traffic probe before {problem_id}"
        )
        logger.warning("Tier-2 recreate of the application: %s (%s)", reason.trigger.value, reason.detail)
        recreate_application(
            environment, worker, reason, Tier1Receipt(0.0, (), False, ()), results_dir, revision=revision
        )
        rehearsal = rehearse()
        (results_dir / "setup-repair.json").write_text(
            json.dumps(
                {
                    "trigger": reason.trigger.value,
                    "problem_id": problem_id,
                    "failed_probe": failed,
                    "recreated": True,
                    "healthy_after": rehearsal.get("success") is True,
                },
                indent=2,
                default=str,
            )
            + "\n"
        )
    rehearsal_dir = results_dir / "oracle-rehearsals"
    rehearsal_dir.mkdir(exist_ok=True)
    (rehearsal_dir / f"{index:03d}.json").write_text(
        json.dumps({"problem_id": problem_id, "verdict": rehearsal}, indent=2) + "\n"
    )
    if rehearsal.get("success") is not True:
        rejection = rehearsal.get("details", {}).get("rejection")
        cause = f" ({rejection['cause']}: {'; '.join(rejection['named'])})" if rejection else ""
        raise ValueError(f"eval: no-fault rehearsal has unhealthy collateral/traffic for {problem_id}{cause}")


def restore_lane_identities(environment: FastloopEnvironment, results_dir: Path) -> None:
    """Row start, after the SDO namespace wipe: the app holds only identities its gateway can carry.

    An identity the gateway rounds onto another user's id outlives rows and fails every
    signed-in probe step at install; repairing it here, while no prober runs, keeps that
    lane state from rejecting each later install, and the receipt names what was repaired.
    """
    if environment.namespace != "social-network":
        return
    from benchmarks.sregym.fastloop.social_fixture import restore_social_identities

    receipt = results_dir / "social-identities.json"
    try:
        repair = restore_social_identities(environment.kubeconfig)
    except Exception as exc:  # recorded; admission stays the gate
        # A failed repair leaves the lane as it was without this step; the install's admission still
        # judges it, and the receipt names this cause instead of the row crashing the same way each time.
        receipt.write_text(json.dumps({"error": f"{type(exc).__name__}: {exc}"}, indent=2) + "\n")
        logger.error("Social identity restore failed at row start; the row proceeds: %s", exc)
        return
    receipt.write_text(json.dumps(repair.to_json(), indent=2) + "\n")
    if repair:
        logger.warning("Social identities repaired at row start: %s", repair.describe())


def _setup_application(
    environment: FastloopEnvironment,
    worker: SregymWorker,
    results_dir: Path,
    args: argparse.Namespace,
    *,
    tier1: Tier1Receipt | None,
    redeploy_reason: Tier2Reason | None,
    warm: bool,
) -> None:
    """Every row path leaves the app the deploy of its arm's application revision, or recreates it.

    The Tier-1 path decided ``redeploy_reason`` (including the deployed-source check); a warm lane proved it in
    its transaction. Rows that do not wipe at start (vanilla, shared-accepted and parallel lanes) check here.
    Only the deployed application follows the revision: the workspace, and everything an agent can read from
    it, is never moved by this setup.
    """
    from benchmarks.sregym.fastloop.wipe import Tier1Receipt

    revision = _application_revision(args, environment) if environment.app != "toy" else None
    if tier1 is not None and redeploy_reason is not None:
        recreate_application(environment, worker, redeploy_reason, tier1, results_dir, revision=revision)
    else:
        if tier1 is not None:
            run_tier2(
                None,
                tier1,
                cluster=KubectlLaneCluster(environment.kubeconfig, environment.host_owned_kinds),
                namespaces=(),
                deploy=lambda: None,
                timeout_seconds=wait_for(WaitKind.ROLLOUT),
            ).write(results_dir)
        changed = (
            _deployed_source_change(environment, revision)
            if tier1 is None and not warm and environment.app != "toy"
            else None
        )
        if changed is not None:
            logger.info("Tier-2 recreate of the application: %s (%s)", changed.trigger.value, changed.detail)
            recreate_application(
                environment, worker, changed, Tier1Receipt(0.0, (), False, ()), results_dir, revision=revision
            )
        elif not warm:
            deploy_with_setup_proof(
                environment,
                worker,
                results_dir,
                allow_reuse=args.agent == "sdo" and environment.app != "toy",
                revision=revision,
                external=args.agent != "sdo" and environment.app != "toy",
            )
    if environment.app != "toy":
        _write_ran_source(environment, results_dir, revision)


def _write_ran_source(environment: FastloopEnvironment, results_dir: Path, revision: str | None) -> None:
    """Record which application source this row runs: set only when the live app is verifiably its deploy."""
    from benchmarks.sregym.fastloop.deployed_application import application_source_identity

    source = None
    if revision is not None and _deployed_source_change(environment, revision) is None:
        source = application_source_identity(environment.workspace, revision)
    results_dir.mkdir(parents=True, exist_ok=True)
    (results_dir / "app-source.json").write_text(
        json.dumps({"revision": revision, "source": source}) + "\n", encoding="utf-8"
    )


def _record_fresh_deploy(environment: FastloopEnvironment, revision: str | None) -> None:
    """A deploy `up` just performed is a whole-application recreation of ``revision`` (read before it)."""
    LaneInjectionState(environment.kubeconfig, environment.namespace).recreated()
    _record_deployed_source(environment, revision)


def _application_needs_redeploy(
    environment: FastloopEnvironment,
    marker: AttemptMarker,
    current_data_faults: tuple[str, ...] = (),
    *,
    revision: str | None = None,
    start_proves_baseline: bool = False,
) -> Tier2Reason | None:
    """Decide tier 2 from incoming faults, lane injection facts, live application health and its source."""
    from benchmarks.sregym.fastloop.clean_start import assert_clean_baseline
    from benchmarks.sregym.fastloop.wipe import await_healthy_pods, tier2_reason
    from benchmarks.sregym.fastloop.workload_readiness import workload_identity

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

    return tier2_reason(
        marker,
        invariant,
        stuck_pods,
        lane_state=LaneInjectionState(environment.kubeconfig, environment.namespace),
        current_data_faults=current_data_faults,
        source_change=lambda: _deployed_source_change(environment, revision),
        start_proves_baseline=start_proves_baseline,
    )


def deploy_with_setup_proof(
    environment: FastloopEnvironment,
    worker: SregymWorker,
    results_dir: Path,
    *,
    allow_reuse: bool = True,
    redeploy: bool = False,
    revision: str | None = None,
    external: bool = False,
) -> dict[str, Any]:
    """Reuse the application deploy while it matches its last passing admission; per-injection admission still runs.

    An ``external`` (non-SDO) row has no admission receipt; it reuses the app only when the previous row on the
    lane ended clean and the deployed source is ``revision`` (``external_reuse``), after which the usual
    pre-injection health gate still runs. The decision and its reasons are the row's ``setup-proof.json``.

    The row-start wipe has removed SDO's install, so the decision binds only what the deploy establishes. A deploy
    that actually deployed (the app was unhealthy) captures the baseline and stamps it with ``revision``; a skipped
    deploy changes nothing, so the existing stamp keeps (or does not keep) vouching.
    """
    from benchmarks.sregym.fastloop.setup_proof import check_application_reuse

    if allow_reuse:
        decision = check_application_reuse(environment)
    elif external:
        from benchmarks.sregym.fastloop.external_reuse import external_reuse_decision

        decision = external_reuse_decision(
            environment.run_dir, results_dir.name, lambda: _deployed_source_change(environment, revision)
        ).to_json()
    else:
        decision = {"reused": False, "receipt": None, "changed": ["setup_reuse_requires_sdo_admission"]}
    results_dir.mkdir(parents=True, exist_ok=True)
    (results_dir / "setup-proof.json").write_text(json.dumps(decision, indent=2) + "\n")
    if decision["reused"]:
        logger.info("Setup proof reused %s; injection admission remains required", decision["receipt"])
        if external:
            worker.request("clean_start", problem_id=environment.deploy_problem, timeout=wait_for(WaitKind.ROLLOUT))
    else:
        logger.info("Full setup required: %s", ", ".join(decision["changed"]))
        reply = worker.request(
            "deploy",
            problem_id=environment.deploy_problem,
            baseline_path=str(environment.baseline_path),
            redeploy=redeploy,
            revision=revision,
            timeout=DEPLOY_OP_TIMEOUT_SECONDS,
        )
        if isinstance(reply, dict) and reply.get("deployed") is True:
            _recapture_application_baseline(environment)
            _record_deployed_source(environment, revision)
    return decision


def _check_external_args(args: argparse.Namespace) -> None:
    """The ``--agent-*`` flags belong to ``--agent external`` and need a command; fail before touching the lane."""
    flags = tuple(
        getattr(args, name, None)
        for name in ("agent_command", "agent_label", "agent_state_map", "agent_closing_timeout", "agent_env")
    )
    if args.agent != "external":
        if any(flags):
            raise SystemExit("--agent-* flags apply to --agent external only")
        return
    if not args.agent_command or not args.agent_command.strip():
        raise SystemExit("--agent external requires --agent-command")
    if bad := [item for item in args.agent_env if "=" not in item or item.startswith("=")]:
        raise SystemExit(f"--agent-env expects KEY=VALUE, got {bad}")
    if args.agent_state_map is not None and not args.agent_state_map.is_file():
        raise SystemExit(f"--agent-state-map {args.agent_state_map} is not a file")


def _run(args: argparse.Namespace) -> int:
    from benchmarks.sregym.fastloop.warm_lane import own_lane

    _check_external_args(args)
    if getattr(args, "sweep_service", None) and (args.stream is None or args.agent != "sdo"):
        raise SystemExit("--sweep-service requires an SDO --stream run")
    # A full-reset caller must obey the same ownership as a warm caller; otherwise
    # it could wipe the controller while the warm transaction proves continuity.
    if getattr(args, "reasoning_effort", None) is None:
        from benchmarks.sregym.adapter import configured_agent_effort

        # Records and the Codex baseline name the effort the SDO agents run at.
        args.reasoning_effort = configured_agent_effort(tuple(path.resolve() for path in getattr(args, "config", ())))
    environment = FastloopEnvironment.load(args.run_dir.resolve())
    limits = HostLimits.from_env(environment.worker_env)
    with own_lane(limits.directory, environment.cluster):
        if not getattr(args, "warm_lane", False):
            return _run_owned(args)
        from benchmarks.sregym.fastloop.evidence_queue import lane_queue

        # The warm lane keeps its PVC across rows, so evidence delivery may outlive
        # the row: the earlier row's archive resumes here, beside this row's setup.
        queue = lane_queue(environment)
        try:
            return _run_owned(args, evidence_queue=queue)
        finally:
            # A caller that reads the row's results the moment this returns asks for the delivery to finish first.
            queue.close(
                drain=bool(getattr(args, "deliver_evidence_before_exit", False)), timeout=wait_for(WaitKind.OPERATION)
            )


DEFAULT_CLAUDE_MODEL = "claude-sonnet-5-5"


def opus_pool_missing(model: str, environ: Mapping[str, str]) -> str | None:
    """Why SDO's workers would refuse every Opus session of this row, or None.

    The worker admits an Opus session only through a filesystem pool (``libs.agent_cli.provider_capacity``); without
    one the incident opens and every dispatch fails after the fault is injected.
    """
    if "opus" not in model.lower():
        return None
    mode = environ.get("SDO_OPUS_POOL_MODE", "shared")
    if mode in {"per_host", "per_account"} and environ.get("SDO_OPUS_POOL_HOST_PATH", "").strip():
        return None
    if environ.get("SDO_OPUS_POOL_NFS_SERVER", "").strip() or environ.get("SDO_OPUS_POOL_DIR", "").strip():
        return None
    return f"model {model} needs an Opus session pool (SDO_OPUS_POOL_HOST_PATH, _NFS_SERVER or _DIR)"


def _agent_model(args: argparse.Namespace) -> str:
    """``--model``, else the agent's default: stock Claude Code runs Sonnet 5.5, everything else gpt-6-luna.

    The external command picks its own model; the row records ``--model`` as declared, else ``unspecified``.
    """
    if getattr(args, "model", None):
        return str(args.model)
    if args.agent == "external":
        return "unspecified"
    return DEFAULT_CLAUDE_MODEL if args.agent == "claudecode" else "gpt-6-luna"


def _run_owned(args: argparse.Namespace, *, evidence_queue: EvidenceExportQueue | None = None) -> int:
    """Persist failures before the incident loop owns canonical rows."""
    from benchmarks.sregym.fastloop.records import NotInjected, append_record

    args.run_id = args.run_id or f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{args.agent}"
    args.model = _agent_model(args)
    environment = FastloopEnvironment.load(args.run_dir.resolve())
    path = environment.run_dir / "results" / args.run_id / INCIDENTS_FILENAME
    started = time.monotonic()
    try:
        _apply_row_operator_env(args, environment)
        return _execute_owned(args, evidence_queue=evidence_queue)
    except (Exception, SystemExit) as exc:
        if not path.exists() or not path.read_text().strip():
            stream = load_stream_config(args.stream).incidents if args.stream else ()
            problems = args.problem or [environment.deploy_problem]
            slots = stream or tuple((problems[i % len(problems)],) for i in range(args.incidents))
            for offset, ids in enumerate(slots):
                append_record(
                    path,
                    IncidentRecord(
                        run_id=args.run_id,
                        index=args.slot_offset + offset,
                        agent=args.agent,
                        problem_id="+".join(ids),
                        model=args.model,
                        provider=args.provider,
                        reasoning_effort=args.reasoning_effort,
                        injection=NotInjected(stage="pre_injection"),
                        error=f"{type(exc).__name__}: {exc}",
                        run_prelude_phase_seconds={"failed_prelude": time.monotonic() - started},
                    ),
                )
        raise


def _apply_row_operator_env(args: argparse.Namespace, environment: FastloopEnvironment) -> None:
    """Before any row work: the lane's host operator binary and lane environment for every Go operator call.

    The warm transaction (inspect, sync, install-existing) and the row-start wipe call the Go operator and install the
    controller too, so they run the same binary and environment (the lane's facts, the Opus pool) as a fresh install;
    otherwise the first call builds the operator from source and a reseeded controller misses the row's pool.
    """
    from benchmarks.sregym.fastloop.eks_lane import VirtualLaneError

    try:
        environment.apply_operator_env(os.environ, model=args.model)
    except VirtualLaneError as error:
        raise SystemExit(f"eval: {error}; the row was refused before it deployed or injected") from error
    if args.agent == "sdo" and (missing := opus_pool_missing(args.model, os.environ)):
        raise SystemExit(f"eval: {missing}; configure the pool before the row deploys or injects")


def _operator_build(environment: Any) -> str | None:
    if environment.dev_mount is None:
        return None
    from benchmarks.sregym.fastloop.dev import operator_build

    return operator_build(environment.dev_mount)


def _execute_owned(args: argparse.Namespace, *, evidence_queue: EvidenceExportQueue | None = None) -> int:
    run_started = time.monotonic()
    environment = FastloopEnvironment.load(args.run_dir.resolve())
    if environment.shared_accepted_id:
        from benchmarks.sregym.fastloop.shared_memory import prepare_shared_lane

        prepare_shared_lane(environment)
    run_id = args.run_id or f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{args.agent}"
    args.run_id = run_id
    args.model = _agent_model(args)
    results_dir = environment.run_dir / "results" / run_id
    if args.agent == "sdo":
        from benchmarks.sregym.fastloop.evidence_sink import EvidenceSinkError, require_evidence_sink

        try:
            require_evidence_sink(environment, results_dir, waived=getattr(args, "allow_no_evidence_sink", False))
        except EvidenceSinkError as exc:
            raise SystemExit(str(exc)) from exc
    arm_memory = None
    if getattr(args, "ablation_arm", None):
        from benchmarks.sregym.fastloop.ablation import configure_arm

        arm_memory = configure_arm(args, environment)
        if getattr(args, "warm_lane", False):
            raise ValueError("ablation arms require a fresh install of their pinned memory")
        results_dir.mkdir(parents=True, exist_ok=True)
        (results_dir / "ablation-memory.json").write_text(json.dumps(asdict(arm_memory), indent=2) + "\n")
        if environment.app == "toy":
            from benchmarks.sregym.adapter import control_namespace_for
            from benchmarks.sregym.fastloop.toy import retire_control_state

            retire_control_state(environment, control_namespace_for(environment.namespace))
            subprocess.run(host_git_command(environment.workspace, "reset", "--hard", arm_memory.commit), check=True)
    stream_config = load_stream_config(args.stream) if args.stream else None
    stream: tuple[tuple[str, ...], ...] = stream_config.incidents if stream_config else ()
    grading_mode = args.diagnosis_grading or (stream_config.diagnosis_grading if stream_config else "off")
    judge_model_id = args.judge_model_id or (stream_config.judge_model_id if stream_config else "codex-gpt-6-luna")
    from benchmarks.sregym.fastloop.grading import SregymDiagnosisGrader

    grader = SregymDiagnosisGrader(environment.sregym_dir, judge_model_id) if grading_mode != "off" else None
    problems = tuple(args.problem or [environment.deploy_problem]) if not stream else tuple(ids[0] for ids in stream)
    from benchmarks.sregym.fastloop.fault_tracker import describe_sregym_problems, stream_faults
    from benchmarks.sregym.fastloop.lane_installability import require_runnable
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
    require_runnable(descriptions, environment, report_path=results_dir / "lane-plan.json")
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
    if environment.image_delivery == "kind-load-fallback":
        from benchmarks.sregym.fastloop.image_delivery import warn_fallback

        warn_fallback(
            environment.image_delivery_reason, environment.image_delivery_images, environment.image_delivery_seconds
        )
    data_faults = {key: item.data_level_faults() for key, item in descriptions.items()}
    incoming = stream[0] if stream else (problems[0],)
    incoming_data_faults = tuple(f"{key}: {fault}" for key in incoming for fault in data_faults.get(key, ()))
    warm_preparation = None
    tier1 = None
    if getattr(args, "warm_lane", False):
        if not _wipes_at_row_start(args, environment):
            raise ValueError("--warm-lane requires a private non-toy SDO lane")
        from benchmarks.sregym.fastloop.warm_environment import WarmEnvironment
        from benchmarks.sregym.fastloop.warm_lane import prepare_lane

        def full_reset() -> None:
            nonlocal tier1
            if evidence_queue is not None:
                evidence_queue.drain()
            tier1 = _wipe_sdo_and_harness(_warm_reset_args(args, environment), environment)

        os.environ["KUBECONFIG"] = str(environment.kubeconfig)
        with _worker(environment) as warm_worker:
            warm_preparation = WarmEnvironment(
                environment,
                _cluster_ops(args, environment.namespace),
                warm_worker,
                _runtime_config(args, environment, results_dir),
                results_dir / "warm",
                incoming_data_faults=incoming_data_faults,
                full_reset=full_reset,
                planned_commit=args.planned_commit,
            )
            warm_receipt = prepare_lane(warm_preparation, results_dir / "warm-transaction.json")
        if not warm_receipt.reused:
            warm_preparation = None
        elif warm_receipt.identity is not None:
            # The row's injection gate may stand on this proof while the proven controller still runs and no
            # rehearsal repaired the application (set once the rehearsals ran).
            args.warm_proven_controller_uid = warm_receipt.identity.controller_uid
    elif _wipes_at_row_start(args, environment):
        tier1 = _wipe_sdo_and_harness(args, environment)

    if args.agent == "sdo":
        from benchmarks.sregym.adapter import control_namespace_for

        secret = "sdo-agent-credentials"
        checked = kubectl(
            ["get", "secret", secret, "-o", "name"],
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
        prepare_dev_mount(*mount_args, environment.parallel_root, evidence_root=environment.evidence_root)
    if environment.eks is not None and args.agent == "sdo":
        from benchmarks.sregym.fastloop.eks_lane import ensure_opus_pool

        # Nodes may have been replaced since the last row (scale to zero and back).
        ensure_opus_pool(environment, args.model)
    os.environ["KUBECONFIG"] = str(environment.kubeconfig)
    results_dir.mkdir(parents=True, exist_ok=True)
    (results_dir / "fault_descriptions.json").write_text(
        json.dumps({key: item.model_dump(mode="json") for key, item in descriptions.items()}, indent=2) + "\n",
        encoding="utf-8",
    )
    deferred_tier2 = row_start_tier2(
        args, wiped_at_start=tier1 is not None, incidents=len(stream) if stream else args.incidents
    )
    config = LoopConfig(
        ablation_arm=arm_memory.arm if arm_memory else None,
        row_start_tier2=deferred_tier2,
        baseline_proof_at_next_start=baseline_proof_at_next_start(
            args, marked=tier1 is not None or warm_preparation is not None, row_start_tier2=deferred_tier2
        ),
        delivery_record_path=environment.run_dir / "image_delivery.jsonl",
        image_delivery=environment.image_delivery,
        image_delivery_reason=environment.image_delivery_reason,
        operator_build=_operator_build(environment),
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
    if args.agent != "sdo" and environment.app != "toy":
        # A vanilla arm does not own the workspace: its app is deployed, restored and compared from the arm's
        # revision while the head (and everything readable there) stays where it is.
        environment.app_revision = _application_revision(args, environment)
    marker = AttemptMarker(environment.run_dir)
    # The previous row recovered its fault and left the proof of the restored baseline to this start. A warm reset
    # proved it (its restore grades the app afresh); any other start grades it in its first no-fault rehearsal.
    prove_owed_baseline = (
        tier1 is not None
        and warm_preparation is None
        and (owed := marker.pending()) is not None
        and owed.trigger is Tier2Trigger.BASELINE_PROOF_OWED
    )
    redeploy_reason = (
        _application_needs_redeploy(
            environment,
            marker,
            incoming_data_faults,
            revision=_application_revision(args, environment),
            start_proves_baseline=True,
        )
        if tier1 is not None
        else None
    )
    if redeploy_reason is not None:
        logger.info(
            "Tier-2 recreate of the application: %s (%s)", redeploy_reason.trigger.value, redeploy_reason.detail
        )
    if tier1 is not None or warm_preparation is not None:
        marker.begin()
    with _worker(environment) as worker:
        worker_ready = time.monotonic()
        _setup_application(
            environment,
            worker,
            results_dir,
            args,
            tier1=tier1,
            redeploy_reason=redeploy_reason,
            warm=warm_preparation is not None,
        )
        if tier1 is not None:
            restore_lane_identities(environment, results_dir)
        if environment.app != "toy":
            repair = ApplicationRepair()
            rehearsal_problems = dict.fromkeys(problem for ids in incidents for problem in ids)
            for rehearsal_index, problem_id in enumerate(rehearsal_problems):
                rehearse_with_repair(
                    environment,
                    worker,
                    results_dir,
                    problem_id,
                    rehearsal_index,
                    repair,
                    revision=_application_revision(args, environment),
                    fresh=prove_owed_baseline and rehearsal_index == 0,
                )
            args.warm_application_repaired = repair.done
        deployed = time.monotonic()
        described = deployed
        baseline = load_application_baseline(environment.baseline_path, environment.namespace)
        if baseline is not None:
            from benchmarks.sregym.fastloop.recovery_closure import complete_recovery_baselines

            source = AcceptedSource.of(environment)
            baseline = complete_recovery_baselines(source, accepted_restore_baselines(source, baseline))
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
            accepted_source=lambda: AcceptedSource.of(environment),
            cluster=KubectlApplicationCluster(environment.kubeconfig) if baseline is not None else None,
            health_timeout_seconds=args.health_timeout,
            health_poll_seconds=wait_for(WaitKind.POLL_FAST),
            before_inject=before_inject,
            injection_state=LaneInjectionState(environment.kubeconfig, environment.namespace),
            data_faults=data_faults,
            require_full_cleanup=args.require_full_cleanup,
            row_start_tier2=config.row_start_tier2,
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

        try:
            if args.agent == "sdo":
                records = run_incidents(
                    _sdo_agent(
                        args,
                        environment,
                        results_dir,
                        composite=registered_composites,
                        stream_faults=faults_by_incident,
                        worker=worker,
                        evidence_queue=evidence_queue,
                    ),
                    driver,
                    config,
                    grader=grader,
                    diagnosis_grading=grading_mode,
                    on_failed_incident=reset_failed if args.continue_on_error else None,
                )
                if getattr(args, "warm_lane", False) or getattr(args, "end_of_row_recovery", False):
                    _replace_trace_stores_for_next_row(environment)
            elif args.agent in ("claudecode", "external"):
                run_baseline = _run_claudecode if args.agent == "claudecode" else _run_external
                records = run_baseline(
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
        except BaseException:
            # A crashed or timed-out run still keeps its transcripts and stamps the rows it cannot vouch for.
            try:
                _finish_evidence(args, environment, results_dir)
            except Exception:
                logger.exception("evidence copy and gate after a failed run")
            raise
    marker.end(unrecovered_rows(records), baseline_proof_owed=baseline_proof_owed(records))
    _print_summary(records)
    print(f"results: {config.results_path}")
    evidence_copied = _finish_evidence(args, environment, results_dir)
    return (
        0
        if records
        and evidence_copied
        and all(
            record.error is None
            and (grading_mode == "off" or (record.diagnosis_grade is not None and record.diagnosis_grade.error is None))
            for record in records
        )
        else 1
    )


def _finish_evidence(args: argparse.Namespace, environment: FastloopEnvironment, results_dir: Path) -> bool:
    """End of a run, and of a crashed one: copy the transcripts out of the lane's sink into the results, then refuse to
    grade any SDO row whose transcripts are missing. False when the copy failed or a row was refused."""
    from benchmarks.sregym.fastloop import evidence_gate, evidence_sink

    if args.agent != "sdo":
        return True
    copied = True
    if environment.evidence_root is not None:
        try:
            copy = evidence_sink.copy_out_run(environment, results_dir)
        except evidence_sink.EvidenceSinkError as exc:
            logger.error("evidence copy: %s", exc)
            print(f"evidence copy FAILED: {exc}", file=sys.stderr)
            copied = False
        else:
            print(f"evidence: {copy.incidents} incidents, {copy.tasks} tasks, {copy.files} files -> {copy.destination}")
    return evidence_gate.announce(evidence_gate.apply(results_dir)) and copied


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


def _run_claudecode(
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
    """SREGym's stock Claude Code agent, no SDO in the lane; auth from ``$CLAUDE_CONFIG_DIR`` (ClaudeGCP)."""
    from benchmarks.sregym.fastloop.claudecode_agent import (
        ClaudeCodeBaselineAgent,
        ClaudeCodeSettings,
        stock_prompt,
    )
    from benchmarks.sregym.fastloop.codex_agent import SubmissionStub

    results_dir.mkdir(parents=True, exist_ok=True)
    proxy = worker.request(
        "proxy",
        port=args.proxy_port or _free_port(),
        hide=[f"{environment.namespace}-sdo"],
        kubeconfig=str(results_dir / "agent.kubeconfig"),
    )
    settings = ClaudeCodeSettings(
        kubeconfig=Path(str(proxy["kubeconfig"])),
        results_dir=results_dir,
        model=args.model,
        timeout_seconds=float(args.timeout),
        reasoning_effort=args.reasoning_effort,
    )
    app_info = {
        "app_name": environment.application,
        "namespace": environment.namespace,
        "descriptions": environment.description,
    }
    with SubmissionStub(app_info=app_info) as stub:
        agent = ClaudeCodeBaselineAgent(
            settings,
            stub=stub,
            prompt_for=lambda _problem_id, api_base: stock_prompt(environment.sregym_dir, app_info, api_base),
        )
        if stream_faults:
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


def _external_settings(args: argparse.Namespace, kubeconfig: Path, results_dir: Path) -> Any:
    """The external arm's settings from ``--agent-*`` flags (a shell-quoted command, ``KEY=VALUE`` environment)."""
    import shlex

    from benchmarks.sregym.fastloop.external_agent import DEFAULT_CLOSING_PROMPT, ExternalAgentSettings

    state_map: dict[str, Path] = {}
    if args.agent_state_map:
        state_map = {str(k): Path(str(v)) for k, v in json.loads(args.agent_state_map.read_text()).items()}
    closing_prompt = (
        args.agent_closing_prompt_file.read_text() if args.agent_closing_prompt_file else DEFAULT_CLOSING_PROMPT
    )
    return ExternalAgentSettings(
        command=tuple(shlex.split(args.agent_command)),
        label=args.agent_label or Path(shlex.split(args.agent_command)[0]).name,
        results_dir=results_dir,
        kubeconfig=kubeconfig,
        incident_timeout_seconds=float(args.timeout),
        closing_timeout_seconds=args.agent_closing_timeout,
        closing_prompt=closing_prompt,
        evidence_grace_seconds=args.agent_evidence_grace,
        state_map=state_map,
        model=args.model,
        extra_env=dict(item.split("=", 1) for item in args.agent_env),
    )


def _run_external(
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
    """Any external agent command, no SDO in the lane: SREGym's own stock instruction, the command does the rest."""
    from benchmarks.sregym.fastloop.claudecode_agent import stock_prompt
    from benchmarks.sregym.fastloop.codex_agent import SubmissionStub
    from benchmarks.sregym.fastloop.external_agent import ExternalBaselineAgent

    results_dir.mkdir(parents=True, exist_ok=True)
    proxy = worker.request(
        "proxy",
        port=args.proxy_port or _free_port(),
        hide=[f"{environment.namespace}-sdo"],
        kubeconfig=str(results_dir / "agent.kubeconfig"),
    )
    settings = _external_settings(args, Path(str(proxy["kubeconfig"])), results_dir)
    app_info = {
        "app_name": environment.application,
        "namespace": environment.namespace,
        "descriptions": environment.description,
    }
    with SubmissionStub(app_info=app_info) as stub:
        agent = ExternalBaselineAgent(
            settings,
            stub=stub,
            prompt_for=lambda _problem_id, api_base: stock_prompt(environment.sregym_dir, app_info, api_base),
        )
        if stream_faults:
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
    from benchmarks.sregym.fastloop.evidence_queue import settle_lane_exports
    from benchmarks.sregym.fastloop.evidence_sink import settle_lane_evidence

    environment = FastloopEnvironment.load(args.run_dir.resolve())
    os.environ["KUBECONFIG"] = str(environment.kubeconfig)
    failed_exports = settle_lane_exports(environment)
    if evidence_failures := settle_lane_evidence(environment):
        for failure in evidence_failures:
            logger.error("evidence not copied out of the sink: %s", failure)
        print("down refused: the evidence sink is not copied out (nothing was deleted)", file=sys.stderr)
        return 1
    errors = teardown(environment.state_path, ops=KubectlClusterOps()) if environment.state_path.exists() else []
    if environment.app in {"toy", "gitlab"}:
        from benchmarks.sregym.fastloop.toy_upstream import UpstreamLayout, delete_upstream

        # The harness-owned git server lives outside the control namespace, so teardown() does not reach it.
        if (error := delete_upstream(environment.kubeconfig, UpstreamLayout(environment.namespace))) is not None:
            errors.append(error)
    errors.extend(f"evidence export for {job.incident_id} failed: {job.error}" for job in failed_exports)
    # Stopping the controller ran the sink's final pass; copy that out before the cluster can go.
    errors.extend(f"evidence copy failed: {failure}" for failure in settle_lane_evidence(environment))
    for error in errors:
        logger.error("persistent controller teardown: %s", error)
    if args.delete_cluster and any(error.startswith("evidence copy failed") for error in errors):
        print("cluster kept: the evidence sink is not copied out", file=sys.stderr)
    elif args.delete_cluster:
        # An EKS lane's cluster belongs to the lanes provider: `kind delete` on its name succeeds and deletes nothing.
        refuse_on_eks("deleting the lane's cluster", environment.platform)
        subprocess.run(["kind", "delete", "cluster", "--name", environment.cluster], check=True)
        from benchmarks.sregym.fastloop.image_delivery import remove_registry

        remove_registry(environment.cluster)
    return 1 if errors else 0


def _reset(args: argparse.Namespace) -> int:
    from benchmarks.sregym.adapter import control_namespace_for
    from benchmarks.sregym.fastloop.reset import KubectlResetCluster, RestoreChecks, reset_environment

    environment = FastloopEnvironment.load(args.run_dir.resolve())
    if getattr(args, "retarget_dev_mount", False):
        refuse_on_eks("retargeting a dev mount", environment.platform)
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

        prepare_dev_mount(
            mount,
            None if environment.app == "toy" else environment.sregym_dir,
            environment.run_dir,
            environment.parallel_root,
            evidence_root=environment.evidence_root,
        )
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
    subprocess.run(host_git_command(environment.workspace, "reset", "--hard", environment.seed_commit), check=True)
    subprocess.run(host_git_command(environment.workspace, "clean", "-fd"), check=True)


def _retire_app(args: argparse.Namespace) -> int:
    from benchmarks.sregym.fastloop.app_retirement import (
        KubectlRetirementOps,
        RetirementRequest,
        retire_application,
    )
    from benchmarks.sregym.fastloop.lane_guard import LaneGuard

    environment = FastloopEnvironment.load(args.run_dir.resolve())
    refuse_on_eks("app retirement", environment.platform)
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
    for role in ("controller", "responder"):
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
    if args.output.exists():
        # Checked again by run_parallel; here it saves a whole preparation from a reused path.
        raise ValueError(f"eval: output already exists: {args.output}")
    if args.prepare:
        from benchmarks.sregym.fastloop.shared_memory import prepare_shared_lanes

        prepare_shared_lanes(plan, args.output.resolve().with_suffix(".attachment.json"))
    run_parallel(plan, args.output.resolve())
    return 0


def _parallel_prepare_lane(args: argparse.Namespace) -> int:
    from benchmarks.sregym.fastloop.parallel import ParallelPlan
    from benchmarks.sregym.fastloop.shared_memory import prepare_lane

    prepare_lane(ParallelPlan.model_validate_json(args.plan.read_text()), args.lane)
    return 0


def _parallel_attach(args: argparse.Namespace) -> int:
    from benchmarks.sregym.fastloop.parallel import ParallelPlan
    from benchmarks.sregym.fastloop.shared_memory import attach_shared_memory

    plan = ParallelPlan.model_validate_json(args.plan.read_text())
    attach_shared_memory(plan, args.report.resolve())
    return 0


def _check_plan(args: argparse.Namespace) -> int:
    from benchmarks.sregym.fastloop.probe_coverage import check_plan

    report = check_plan(
        args.plan,
        sregym_dir=args.sregym_dir,
        environment=FastloopEnvironment.load(args.lane_environment)
        if getattr(args, "lane_environment", None)
        else None,
    )
    print(json.dumps({"tracking_supported": True, **report}))
    return 0


def _preflight(args: argparse.Namespace) -> int:
    from benchmarks.sregym.adapter import run_context_preflight

    run_context_preflight()
    if args.plan is not None:
        _check_plan(args)
    up_preflight(args)
    return 0


class _Parser(argparse.ArgumentParser):
    """Resolves ``run``'s agent-dependent ``--model`` default once the agent is known."""

    def parse_known_args(self, args=None, namespace=None):  # type: ignore[no-untyped-def,override]
        parsed, rest = super().parse_known_args(args, namespace)
        if hasattr(parsed, "agent") and hasattr(parsed, "model"):
            parsed.model = _agent_model(parsed)
        return parsed, rest


def build_parser() -> argparse.ArgumentParser:
    parser = _Parser(prog="fastloop", description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)

    preflight = commands.add_parser("preflight", help="fail fast on host prerequisites before building images")
    preflight.add_argument("--app", choices=("sregym", "toy", "gitlab"), default="sregym")
    preflight.add_argument("--dev-mount", type=Path, nargs="?", const=REPO_ROOT, default=REPO_ROOT)
    preflight.add_argument("--kind-worker-nodes", type=int, default=1)
    for role in ("controller", "responder"):
        preflight.add_argument(f"--{role}-image", default=None)
    preflight.add_argument(
        "--plan", type=Path, help="require attributable probes for every problem in this plan before host checks"
    )
    preflight.add_argument("--allow-stale-images", action="store_true", help=STALE_IMAGES_HELP)
    preflight.add_argument("--sregym-dir", type=Path, default=DEFAULT_SREGYM_DIR)
    preflight.set_defaults(handler=_preflight)

    check_plan = commands.add_parser(
        "check-plan", help="offline per-fault tracking validation for an entire plan or stream"
    )
    check_plan.add_argument("--plan", type=Path, required=True)
    check_plan.add_argument("--sregym-dir", type=Path, default=DEFAULT_SREGYM_DIR)
    check_plan.add_argument("--lane-environment", type=Path, help="filter against this saved lane before setup")
    check_plan.set_defaults(handler=_check_plan)

    up = commands.add_parser("up", help="create or reuse the cluster and deploy the application once")
    up.add_argument("--run-dir", type=Path, required=True)
    up.add_argument("--app", choices=("sregym", "toy", "gitlab"), default="sregym")
    up.add_argument("--frozen-memory", action="store_true", help="toy: install the supplied seed without judge refresh")
    up.add_argument(
        "--fresh-memory",
        action="store_true",
        help="toy: start with source only and onboard it with SDO's lifecycle agents (needs live agents)",
    )
    up.add_argument("--goal", help="toy --fresh-memory, gitlab: the human goal to publish instead of the default")
    up.add_argument(
        "--variant",
        choices=("raw", "kustomize", "helm", "compose"),
        default="raw",
        help="toy: deployment variant of the source (non-raw variants need --fresh-memory and build from source)",
    )
    up.add_argument(
        "--chart-release",
        help="gitlab: start the lane at this upstream chart release of apps/gitlab/releases.json (default: pins.json)",
    )
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
    up.add_argument(
        "--platform",
        choices=("kind", "eks"),
        default="kind",
        help="eks: attach to the existing EKS cluster KUBECONFIG reaches (implies --reuse-cluster-only; needs "
        "--no-dev-mount and --ecr-registry); images go to ECR, app images keep their names through a node mirror",
    )
    up.add_argument(
        "--ecr-registry",
        help="eks: ECR registry host <account>.dkr.ecr.<region>.amazonaws.com (a local kind host: localhost:<port>)",
    )
    up.add_argument(
        "--ecr-tag", action="append", default=[], help="eks: KEY=VALUE tag on every ECR repository the lane creates"
    )
    up.add_argument(
        "--cpu-limit",
        default=None,
        help="docker --cpus per kind node; default: --lane-cpus split across the lane's nodes (gitlab: 8)",
    )
    up.add_argument(
        "--lane-cpus",
        default=None,
        help="CPU cap of the whole lane, split across its kind nodes "
        f"(default env SDO_LANE_CPUS or {DEFAULT_LANE_CPUS})",
    )
    up.add_argument(
        "--lane-cgroup-parent",
        default=None,
        help="systemd slice for the kind nodes' cgroups (default env SDO_LANE_CGROUP_PARENT or auto: sdo-eval.slice "
        "when the host has it, else none; 'none' disables)",
    )
    up.add_argument(
        "--memory-limit",
        default=None,
        help=f"Docker memory cap per kind node (default {DEFAULT_MEMORY_LIMIT}; gitlab: 20g)",
    )
    up.add_argument("--pids-limit", type=int, default=DEFAULT_PIDS_LIMIT, help="Docker PID cap per kind node")
    up.add_argument(
        "--host-lock-dir",
        type=Path,
        default=HostLimits.from_env().directory,
        help="host-wide lock directory shared by ALL lanes, outside private /tmp",
    )
    up.add_argument(
        "--host-redeploys",
        type=int,
        default=HostLimits.from_env().redeploys,
        help="host-wide concurrent app redeploys (same value for every lane)",
    )
    up.add_argument(
        "--kind-worker-nodes",
        type=int,
        default=1,
        help="kind worker nodes beside the control plane (default 1, as every experiment lane)",
    )
    up.add_argument(
        "--host-reserve-gib",
        type=float,
        default=None,
        help="refuse unless MemAvailable >= the lane's cap + this reserve "
        "(default env SDO_HOST_MEMORY_RESERVE_GIB or 32)",
    )
    up.add_argument(
        "--host-psi-max",
        type=float,
        default=None,
        help="refuse while memory PSI some avg60 is at or above this (default env SDO_HOST_MEMORY_PSI_MAX or 10)",
    )
    up.add_argument(
        "--host-lane-budget-gib",
        type=float,
        default=None,
        help="refuse a lane that takes the summed caps of our running lanes past this "
        "(default env SDO_HOST_LANE_BUDGET_GIB; unset: unchecked)",
    )
    up.add_argument(
        "--host-lane-prefix",
        default=None,
        help="cluster-name prefix that marks OUR lanes for the lane budget (default env SDO_HOST_LANE_PREFIX, "
        "else --cluster-prefix)",
    )
    up.add_argument("--builder", default="fastloop", help="private buildx builder for the application image")
    up.add_argument(
        "--agent-registrations", type=Path, help="toy: SDO worker registration directory (default scripted)"
    )
    up.add_argument(
        "--credentials-hook",
        type=Path,
        help="toy: executable the lane keeper provides; called with the SDO namespace to provision its credentials",
    )
    up.add_argument("--allow-stale-images", action="store_true", help=STALE_IMAGES_HELP)
    up.add_argument("--controller-image", default=Images().controller)
    up.add_argument("--responder-image", default=Images().responder)
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
    up.add_argument(
        "--evidence-root",
        type=Path,
        help="host directory bind-mounted on every node as the controller's evidence sink (outlives the namespace)",
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
    for role in ("controller", "responder"):
        retire.add_argument(f"--{role}-image", required=True)
    retire.set_defaults(handler=_retire_app)

    from benchmarks.sregym.fastloop.dry_run import budget, execute, setup_budget

    dry_run = commands.add_parser("eval-dry-run", help="scripted pre-injection proof on a real lane, without a fault")
    dry_run.add_argument("--run-dir", type=Path, required=True)
    dry_run.add_argument("--timeout", type=budget, default=300)
    dry_run.add_argument("--setup-timeout", type=setup_budget, default=900)
    dry_run.set_defaults(handler=lambda args: execute(args.run_dir, args.timeout, args.setup_timeout))

    rebind = commands.add_parser(
        "repair-repository-bind", help="verify or repair the attached repository bind on its owning node"
    )
    rebind.add_argument("--run-dir", type=Path, required=True)
    rebind.set_defaults(handler=_repair_repository_bind)

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
    run.add_argument("--agent", choices=("sdo", "codex", "claudecode", "external"), required=True)
    run.add_argument(
        "--warm-lane",
        action="store_true",
        help="try persistent lane preparation; fall back to the full row reset on uncertainty",
    )
    run.add_argument(
        "--deliver-evidence-before-exit",
        action="store_true",
        help="with --warm-lane: finish delivering the sealed evidence into the results before returning, for a caller "
        "that copies the results out when this exits (default: delivery stays queued for the lane's next row)",
    )
    run.add_argument("--no-checkpoints", action="store_true", help="skip optional offline replay checkpoint export")
    run.add_argument(
        "--credentials-hook",
        type=Path,
        help="executable the lane keeper provides; called with the SDO namespace to re-provision its credentials "
        "after the row-start wipe (required for non-toy SDO rows)",
    )
    run.add_argument(
        "--goal",
        help="fresh-memory installs only: the human goal to publish instead of the generated one (for example "
        "one naming write journeys the health judge must check); at most 256 characters",
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
        "--end-of-row-recovery",
        action="store_true",
        help="pay the harness fault recovery and cleanup at the row's end even for a row that wiped at start "
        "(an arm-neutral end of row; without it a one-incident SDO row leaves it to the next row's start)",
    )
    run.add_argument(
        "--cleanup-passes",
        type=int,
        choices=(1, 2),
        default=1,
        help="verify full cleanup and the official recovery oracle twice when set to 2",
    )
    run.add_argument(
        "--model",
        default=None,
        help="agent model (default: gpt-6-luna; claude-sonnet-5-5 for --agent claudecode, SREGym's stock Claude Code; "
        "recorded as declared for --agent external, whose command picks its own)",
    )
    external = run.add_argument_group("external agent (--agent external)", "see fastloop/README.md for the contract")
    external.add_argument("--agent-command", help="the command to run per cell and phase (shell-quoted)")
    external.add_argument("--agent-label", help="the arm's label on every row (default: the command's name)")
    external.add_argument(
        "--agent-env", action="append", default=[], metavar="KEY=VALUE", help="extra environment for the command"
    )
    external.add_argument(
        "--agent-state-map",
        type=Path,
        help="JSON file {problem_id: state dir}: that cell starts from a copy of the state a previous pass kept",
    )
    external.add_argument(
        "--agent-closing-timeout",
        type=float,
        help="seconds for the closing phase after the oracle grade (default: no closing phase)",
    )
    external.add_argument("--agent-closing-prompt-file", type=Path, help="the closing phase's text (default: neutral)")
    external.add_argument(
        "--agent-evidence-grace",
        type=float,
        default=1.5 * wait_for(WaitKind.MINUTE),
        help="seconds past a phase's deadline before the arm kills the command's process group",
    )
    run.add_argument("--provider", choices=("codex", "claude"), default="codex", help="SDO agent provider")
    run.add_argument(
        "--config",
        type=Path,
        action="append",
        default=[],
        help="operator config overlay file passed after benchmarks/fuzzops/sdo-bench.yaml (repeatable; docs/config.md)",
    )
    run.add_argument(
        "--repair-policy", choices=("recorded-actions", "commit"), default=None, help="default: the configured value"
    )
    run.add_argument("--diagnosis-grading", choices=("off", "deferred", "inline"), default=None)
    run.add_argument("--judge-model-id", default=None, help="SREGym JUDGE_MODEL_ID for diagnosis grading")
    run.add_argument("--replay", choices=("on", "off"), default="on", help="corpus capture and memory replay")
    run.add_argument("--ablation-arm", choices=("full", "detectors-only", "playbooks-only"))
    run.add_argument(
        "--ablation-branch", help="variant branch to fetch (default ablate/20261103-<arm>); the branch carries its date"
    )
    run.add_argument(
        "--ablation-repository", help="override the per-app variant repository (toy proof uses a local repo)"
    )
    run.add_argument("--frozen-memory", action="store_true", help="keep closure evidence private and disable learning")
    run.add_argument("--code-path", choices=("go", "shell"), default="go")
    run.add_argument(
        "--code-path-overrides",
        default="",
        metavar="JSON",
        help='named code-path seam overrides passed to the controller, for example \'{"response_stages": ["agent"]}\' '
        "to score detection without automatic responses",
    )
    run.add_argument(
        "--pre-onboarding-hook",
        type=Path,
        help="executable run once per admission before SDO onboards the fresh app: argv is the app namespace, "
        "KUBECONFIG is the lane's; use it to create probe fixtures such as seeded users",
    )
    run.add_argument(
        "--reflection-guidance",
        choices=("baseline", "generalize", "generalize-spec"),
        default=None,
        help="default: the configured value",
    )
    run.add_argument(
        "--healthy-baseline",
        action="store_true",
        help="record the healthy namespace before each injection and reject learned detectors that fire on it",
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
    run.add_argument(
        "--reasoning-effort",
        choices=AGENT_EFFORTS,
        default=None,
        help="reasoning effort of every agent in the run: each SDO agent and the Codex baseline "
        "(default: the configured agent.effort)",
    )
    run.add_argument(
        "--lifecycle-timeout",
        type=int,
        default=None,
        help="seconds one SDO lifecycle (detection designer) session may run; raise it with the effort or app size "
        "(default: the configured agent.lifecycle_timeout)",
    )
    run.add_argument("--timeout", type=int, default=3600, help="per-incident agent timeout in seconds")

    run.add_argument(
        "--detection-timeout",
        type=float,
        default=None,
        metavar="SECONDS",
        help="SDO: fail an incident if no detector opens it within this many seconds (default: disabled)",
    )
    run.add_argument(
        "--closure-bound",
        type=float,
        default=None,
        metavar="SECONDS",
        help=(
            "SDO: end the row, named incident_not_closed, when its incident is still open this long after "
            "injection (the row's cap); default: wait for the reflection deadline"
        ),
    )
    run.add_argument("--health-timeout", type=float, default=300.0, help="seconds to wait for health after recovery")
    run.add_argument("--proxy-port", type=int, default=0, help="Codex baseline API proxy port (default: free port)")
    run.add_argument("--run-id", default=None)
    run.add_argument(
        "--allow-no-evidence-sink",
        action="store_true",
        help="run the SDO arm on a lane without an evidence sink (no transcripts are kept; its cells are not gradable)",
    )
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
        "parallel-attach",
        help="bind accepted memory into idle prepared lanes (a later cohort once live); retains private PVC roots",
    )
    attach.add_argument("--plan", type=Path, required=True)
    attach.add_argument("--report", type=Path, required=True, help="external durable attachment/recovery report")
    attach.set_defaults(handler=_parallel_attach)
    prepare_lane = commands.add_parser(
        "parallel-prepare-lane", help="one lane's no-fault native preparation; parallel --prepare runs one per lane"
    )
    prepare_lane.add_argument("--plan", type=Path, required=True)
    prepare_lane.add_argument("--lane", required=True)
    prepare_lane.set_defaults(handler=_parallel_prepare_lane)

    replay = commands.add_parser("replay", help="re-execute one checkpointed stage without a cluster")
    replay.add_argument("--run-dir", type=Path, required=True, help="one fastloop results run directory")
    replay.add_argument("--incident", type=int, required=True)
    replay.add_argument("--stage", choices=("reflection", "responder", "broker"), required=True)
    replay.add_argument("--script", type=Path, help="explicit agent script instead of the exported dispatch script")
    replay.set_defaults(handler=_replay)

    summary = commands.add_parser("summary", help="summarize incidents.jsonl files or run directories")
    summary.add_argument("paths", type=Path, nargs="+")
    summary.add_argument("--json", action="store_true")
    summary.set_defaults(handler=_summary)

    add_toy_parser(commands)

    gitlab = commands.add_parser("gitlab", help="gitlab lane maintenance commands")
    gitlab_commands = gitlab.add_subparsers(dest="gitlab_command", required=True)
    reonboard = gitlab_commands.add_parser(
        "reonboard", help="rerun only the onboarding step on an existing gitlab lane (not a clean install)"
    )
    reonboard.add_argument("--run-dir", type=Path, required=True)
    reonboard.add_argument("--goal", help="the human goal to publish instead of the default")
    reonboard.add_argument("--agent-registrations", type=Path, default=None)
    reonboard.set_defaults(handler=_gitlab_reonboard)
    add_faults_parser(gitlab_commands)
    add_gitlab_replay_parser(gitlab_commands)
    add_gitlab_rehearse_parser(gitlab_commands)

    down = commands.add_parser("down", help="drain and remove the persistent SDO controller")
    down.add_argument("--run-dir", type=Path, required=True)
    down.add_argument("--delete-cluster", action="store_true")
    down.set_defaults(handler=_down)
    return parser


def _gitlab_reonboard(args: argparse.Namespace) -> int:
    from benchmarks.sregym.fastloop.gitlab_lane import reonboard

    return reonboard(args)


def _replay(args: argparse.Namespace) -> int:
    from benchmarks.sregym.fastloop.replay import CheckpointStore, replay_broker

    run_dir = args.run_dir.resolve()
    store = CheckpointStore(run_dir)
    environment = next(
        (parent / "fastloop.json" for parent in (run_dir, *run_dir.parents) if (parent / "fastloop.json").is_file()),
        None,
    )
    source = Path(json.loads(environment.read_text(encoding="utf-8"))["workspace"]) if environment else run_dir
    if args.stage == "broker":
        result = replay_broker(store, args.incident, source=source, operator_root=REPO_ROOT / "sdo" / "operator")
    else:
        from benchmarks.sregym.fastloop.replay_worker import replay_script_worker

        result = replay_script_worker(store, args.incident, args.stage, source=source, script=args.script)
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
