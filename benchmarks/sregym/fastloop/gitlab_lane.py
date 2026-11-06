"""The GitLab lane: a kind cluster where SDO deploys and operates GitLab from its own Helm chart.

The harness provides the deployment source (the umbrella chart of ``apps/gitlab``: the unmodified upstream chart,
fetched at lane setup, plus app-owned PostgreSQL, Redis and S3-compatible storage). SDO's trusted deployer
deploys it; nothing here authors Kubernetes configuration. Cluster prerequisites that are not part of the app
(an ingress controller) are installed by this module. Everything GitLab-specific lives here and under
``apps/gitlab``; production code stays application-generic.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import time
import urllib.request
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from apps.gitlab import assemble
from benchmarks.sregym.adapter import WaitKind, deployment_plan, helm_source, prebuilt_image, wait_for
from benchmarks.sregym.repository_git import host_git_command

if TYPE_CHECKING:
    from pathlib import Path

    from benchmarks.sregym.fastloop.accepted_source import RenderedWorkloads

NAMESPACE = "gitlab"
APPLICATION = "gitlab"
CONTROL_NAMESPACE = NAMESPACE + "-sdo"
#: The objective a fresh install publishes unless ``--goal`` overrides it (the deployer budgets 256 characters).
DEFAULT_GOAL = (
    "Keep GitLab usable: the web UI answers HTTP 200 through its ingress, a user can sign in, Git over HTTP "
    "works, and Sidekiq processes background jobs."
)


@dataclass(frozen=True)
class ResourceClaim:
    """What the coordination board should be asked for before ``up`` (measured requests plus headroom)."""

    cores: int
    memory_gib: int
    disk_gib: int
    note: str


#: App requests are about 2.5 CPU / 4 GiB (upstream chart ~0.55 CPU / 1.9 GiB, datastores ~0.3 CPU / 0.6 GiB, ingress
#: and kube-system ~1 CPU / 1 GiB); a cold start (migrations, ~11 large images) peaks near twice that.
RESOURCE_CLAIM = ResourceClaim(
    cores=6,
    memory_gib=12,
    disk_gib=40,
    note="single-node kind, one replica per component; SDO controller and agent workers included",
)


def deployment_source() -> dict[str, Any]:
    """The provided DeploymentSource: the helm renderer over the umbrella chart with the kind values."""
    return helm_source("chart", assemble.VALUE_FILES)


def plan_from_workloads(workloads: RenderedWorkloads) -> dict[str, Any]:
    """The provided ``DeploymentPlan``: the lane's helm source, prebuilt upstream images, the rendered workloads.

    Nothing here is maintained by hand: images and workload IDs come from SDO's own render of the chart (Go decides
    which kinds are workloads), so a chart bump or a values change moves the plan with it. Hooks are not in the
    rendered inventory (SDO renders them as ephemeral install phases).
    """
    return deployment_plan(
        deployment_source(),
        images=[prebuilt_image(image) for image in workloads.images],
        expected_workload_ids=list(workloads.workload_ids),
    )


def provided_plan(workspace: Path, namespace: str = NAMESPACE) -> dict[str, Any]:
    """Plan for the committed ``workspace``, rendered through SDO's production ``render-source``."""
    from benchmarks.sregym.fastloop.accepted_source import AcceptedSource

    source = AcceptedSource(workspace, namespace, ("chart",), renderer_source=deployment_source())
    return plan_from_workloads(source.workloads)


def prepare_workspace(workspace: Path, *, archive: Path | None = None, pins: dict[str, Any] | None = None) -> str:
    """Assemble the lane source into the empty ``workspace``, commit it, and return the commit SDO starts from."""
    assemble.assemble(workspace, archive=archive, pins=pins)
    for git_args in (
        ["init", "-q"],
        ["add", "."],
        [
            "-c",
            "user.name=gitlab-lane",
            "-c",
            "user.email=gitlab-lane@invalid",
            "commit",
            "-qm",
            "feat: provide the GitLab deployment source",
        ],
    ):
        subprocess.run(host_git_command(workspace, *git_args), check=True, capture_output=True)
    return subprocess.run(
        host_git_command(workspace, "rev-parse", "HEAD"), check=True, capture_output=True, text=True
    ).stdout.strip()


def ingress_manifest(pins: dict[str, Any] | None = None) -> bytes:
    """The pinned ingress controller manifest, verified against its digest."""
    pin = (pins or assemble.load_pins())["prerequisites"]["ingress_nginx"]
    with urllib.request.urlopen(pin["url"], timeout=wait_for(WaitKind.COMMAND)) as response:
        data = response.read()
    actual = hashlib.sha256(data).hexdigest()
    if actual != pin["sha256"]:
        raise ValueError(f"ingress controller manifest sha256 {actual} does not match the pin {pin['sha256']}")
    return data


def prerequisite_images(pins: dict[str, Any] | None = None) -> list[str]:
    """Images the prerequisites need on every node (delivered like the SDO images, not pulled by workloads)."""
    return list((pins or assemble.load_pins())["prerequisites"]["ingress_nginx"]["images"])


def install_prerequisites(kubeconfig: Path, *, pins: dict[str, Any] | None = None) -> None:
    """Install the ingress controller (cluster-scoped, outside the app) and wait until it admits Ingresses."""
    manifest = ingress_manifest(pins)
    subprocess.run(
        ["kubectl", "--kubeconfig", str(kubeconfig), "apply", "-f", "-"],
        input=manifest,
        check=True,
        capture_output=True,
    )
    subprocess.run(
        [
            "kubectl",
            "--kubeconfig",
            str(kubeconfig),
            "-n",
            "ingress-nginx",
            "wait",
            "--for=condition=Available",
            "deployment/ingress-nginx-controller",
            f"--timeout={int(wait_for(WaitKind.OPERATION))}s",
        ],
        check=True,
        capture_output=True,
    )


def lane_runtime_config(environment: Any, plan: dict[str, Any], *, registrations: Path | None = None) -> Any:
    """The controller install for the lane: the provided plan, no manifest paths, persistent, recorded actions."""
    from benchmarks.sregym.adapter import RuntimeConfig

    return RuntimeConfig(
        repository=environment.workspace,
        namespace=environment.namespace,
        application=environment.application,
        controller_namespace=CONTROL_NAMESPACE,
        controller_image=environment.images.controller,
        responder_image=environment.images.responder,
        repository_pvc="sdo-application-repository",
        credentials_secret="sdo-agent-credentials",
        model="gpt-6-luna",
        timeout_seconds=int(wait_for(WaitKind.RESPONSE, 2)),
        repair_policy="recorded-actions",
        persistent=True,
        kind_cluster_name=environment.cluster,
        deployment_source=deployment_source(),
        deployment_plan=plan,
        agent_registrations=registrations.resolve() if registrations else None,
    )


def onboard(environment: Any, config: Any, objective: str) -> dict[str, Any]:
    """Run SDO's host lifecycle with the provided plan: no deployer agent; the judge still runs."""
    from benchmarks.sregym.adapter import launch_native_lifecycle

    if config.deployment_plan is None:
        raise ValueError(f"the GitLab lane needs a provided deployment plan for {environment.namespace}")
    return launch_native_lifecycle(config, objective, "")


def up(args: Any) -> int:
    """``fastloop up --app gitlab``: cluster, prerequisites, source workspace, SDO install with the provided source."""
    from benchmarks.sregym.fastloop import toy
    from benchmarks.sregym.fastloop.dev import prepare_dev_mount
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment, Images
    from benchmarks.sregym.fastloop.host_limits import HostLimits, NodeLimits, apply_node_limits
    from benchmarks.sregym.fastloop.toy_images import load_images

    if args.dev_mount is None:
        raise ValueError("gitlab lanes require a development mount; omit --no-dev-mount")
    run_dir = args.run_dir.resolve()
    if (run_dir / "fastloop.json").exists():
        raise ValueError(f"{run_dir} already holds a lane; run fastloop down first (gitlab lanes are not re-upped)")
    from benchmarks.sregym.fastloop.cli import lifecycle_objective

    objective = lifecycle_objective(DEFAULT_GOAL, getattr(args, "goal", None))
    host_limits = HostLimits(args.host_lock_dir.resolve(), args.host_redeploys)
    cluster = toy.cluster_name(args)
    if (
        cluster
        in subprocess.run(["kind", "get", "clusters"], check=True, capture_output=True, text=True).stdout.split()
    ):
        raise ValueError(f"{cluster} already exists without this run directory; refusing to adopt it")
    started = time.monotonic()
    run_dir.mkdir(parents=True, exist_ok=True)
    worktree = args.dev_mount.resolve()
    config = prepare_dev_mount(worktree, None, run_dir)
    kubeconfig = run_dir / "kubeconfig"
    subprocess.run(
        [
            "kind", "create", "cluster", "--name", cluster, "--image", args.toy_node_image,
            "--config", str(config), "--kubeconfig", str(kubeconfig), "--wait", "0s",
        ],
        check=True,
    )  # fmt: skip
    apply_node_limits(cluster, NodeLimits(args.cpu_limit, args.memory_limit, args.pids_limit))
    images = Images(controller=args.controller_image, responder=args.responder_image)
    load_images(
        cluster,
        [images.controller, images.responder, *prerequisite_images()],
        record_path=run_dir / "image_delivery.jsonl",
        host_limits=host_limits,
    )
    install_prerequisites(kubeconfig)
    workspace = run_dir / "application_workspace"
    workspace.mkdir()
    seed = prepare_workspace(workspace)
    toy.command(str(kubeconfig), "default", "create", "namespace", NAMESPACE)
    toy.command(str(kubeconfig), "default", "create", "namespace", CONTROL_NAMESPACE)
    hook = getattr(args, "credentials_hook", None)
    if hook is None:
        toy.command(str(kubeconfig), CONTROL_NAMESPACE, "create", "secret", "generic", "sdo-agent-credentials")
    else:
        from benchmarks.sregym.fastloop.wipe import credentials_hook

        credentials_hook(hook.resolve(), kubeconfig)(CONTROL_NAMESPACE)
    env = FastloopEnvironment(
        run_dir=run_dir,
        app="gitlab",
        cluster=cluster,
        kubeconfig=kubeconfig,
        namespace=NAMESPACE,
        application=APPLICATION,
        workspace=workspace,
        sregym_dir=toy.ROOT / "third_party/sregym",
        private_tmp=None,
        deploy_problem="",
        seed_commit=seed,
        dev_mount=worktree,
        images=images,
        worker_env=host_limits.worker_env(),
    )
    env.baseline_path.write_text("{}\n", encoding="utf-8")
    env.save()
    runtime = lane_runtime_config(
        env, provided_plan(workspace, NAMESPACE), registrations=getattr(args, "agent_registrations", None)
    )
    onboard(env, runtime, objective)
    toy.record_controller(env, runtime)
    print(json.dumps({"up_total": time.monotonic() - started, "workspace": str(workspace), "commit": seed}))
    return 0
