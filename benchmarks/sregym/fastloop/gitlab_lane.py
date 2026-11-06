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
import os
import subprocess
import time
import urllib.request
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from apps.gitlab import assemble
from benchmarks.sregym.adapter import (
    DeploymentPolicyConfig,
    WaitKind,
    benchmark_registry_host,
    deployment_plan,
    helm_source,
    prebuilt_image,
    wait_for,
)
from benchmarks.sregym.fastloop.gitlab_releases import load_releases
from benchmarks.sregym.fastloop.toy_upstream import UpstreamLayout, serve_upstream, source_config
from benchmarks.sregym.repository_git import host_git_command

if TYPE_CHECKING:
    from collections.abc import Sequence
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


#: Registries the lane's deployment may pull from: the upstream chart's images (registry.gitlab.com), the app-owned
#: datastore images the lane pins by digest (Docker Hub), and the lane's own in-cluster registry.
IMAGE_REGISTRIES = ("registry.gitlab.com", "docker.io", benchmark_registry_host(CONTROL_NAMESPACE))


def deployment_policy() -> DeploymentPolicyConfig:
    """The install-time policy of the lane: images only from ``IMAGE_REGISTRIES``; scope is not blocked.

    The umbrella chart deploys into its own namespace only, but ingress and prerequisites are cluster-level and
    installed by the lane, not by SDO, so scope blocking stays at the default (notes) until a grant list is measured.
    """
    return DeploymentPolicyConfig(allowed_registries=IMAGE_REGISTRIES)


def plan_from_workloads(workloads: RenderedWorkloads) -> dict[str, Any]:
    """The provided ``DeploymentPlan``: the lane's helm source, prebuilt upstream images, the rendered workloads.

    Nothing here is maintained by hand: Go proposes the plan's entries from SDO's own render of the chart. They are
    the release-independent forms (workload identities with generated name suffixes as ``*``, image repositories
    without tags), so an upstream release that only changes image tags, and with them a hash-named Job, still matches
    the plan and advances without an agent. Hooks are not in the rendered inventory (SDO renders them as ephemeral
    install phases).
    """
    return deployment_plan(
        deployment_source(),
        images=[prebuilt_image(repository) for repository in workloads.repositories],
        expected_workload_ids=list(workloads.identities),
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


def pull_missing_images(images: Sequence[str], run: Any = subprocess.run) -> list[str]:
    """Pull the images the host does not hold yet (delivery to the nodes only loads local images); return the pulled."""
    pulled = []
    for image in images:
        if run(["docker", "image", "inspect", image], capture_output=True).returncode != 0:
            run(["docker", "pull", image], check=True)
            pulled.append(image)
    return pulled


def install_prerequisites(kubeconfig: Path, *, pins: dict[str, Any] | None = None) -> None:
    """Install the ingress controller (cluster-scoped, outside the app) and wait until its Pod is Ready.

    Available alone is not enough: it is true with zero Pods because the manifest's maxUnavailable is 1.
    """
    manifest = ingress_manifest(pins)
    # The kind provider manifest schedules the controller only on nodes labelled ingress-ready=true.
    subprocess.run(
        ["kubectl", "--kubeconfig", str(kubeconfig), "label", "nodes", "--all", "ingress-ready=true", "--overwrite"],
        check=True,
        capture_output=True,
    )
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
            "rollout",
            "status",
            "deployment/ingress-nginx-controller",
            f"--timeout={int(wait_for(WaitKind.OPERATION))}s",
        ],
        check=True,
        capture_output=True,
    )


def lane_upstream(environment: Any) -> UpstreamLayout:
    """The Git remote the lane's SDO follows: served in ``<namespace>-upstream`` and seeded with the workspace commit.

    The release replay pushes successive chart releases to its ref; nothing else writes it.
    """
    return UpstreamLayout(environment.namespace)


def lane_runtime_config(environment: Any, plan: dict[str, Any], *, registrations: Path | None = None) -> Any:
    """The controller install for the lane: the provided plan, no manifest paths, persistent, recorded actions.

    SDO follows the lane's upstream (``--source-kind git``) so a pushed release advances the accepted source.
    """
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
        deployment_policy=deployment_policy(),
        source_kind="git",
        source_config=json.dumps(source_config(lane_upstream(environment))),
        agent_registrations=registrations.resolve() if registrations else None,
    )


def onboard(environment: Any, config: Any, objective: str) -> dict[str, Any]:
    """Run SDO's host lifecycle with the provided plan: no deployer agent; the judge still runs."""
    from benchmarks.sregym.adapter import launch_native_lifecycle

    if config.deployment_plan is None:
        raise ValueError(f"the GitLab lane needs a provided deployment plan for {environment.namespace}")
    os.environ["KUBECONFIG"] = str(environment.kubeconfig)
    environment.apply_operator_env(os.environ)
    return launch_native_lifecycle(config, objective, "")


ONBOARDING_EVIDENCE_DIRNAME = "onboarding_evidence"


def onboard_retaining_evidence(environment: Any, config: Any, objective: str, ops: Any = None) -> dict[str, Any]:
    """``onboard``, then export the lifecycle's runtime evidence into the run output, also when it fails.

    Every judge and deployer task persists its Codex rollout JSONL and request/result under
    ``sdo_runtime/tasks/<task-key>/`` on the repository volume, keyed by task ID, so failed and retried
    attempts survive their pods. A failed or slow onboarding would otherwise leave nothing to audit. The
    export reads through the running operate Pod, so it also works while the lifecycle is still running.
    """
    from dataclasses import replace

    if ops is None:
        from benchmarks.sregym.adapter import KubectlClusterOps

        ops = KubectlClusterOps()
    directory = environment.run_dir / ONBOARDING_EVIDENCE_DIRNAME
    try:
        return onboard(environment, config, objective)
    finally:
        directory.mkdir(parents=True, exist_ok=True)
        exported = ops.export_runtime_artifacts(replace(config, artifacts_dir=directory), directory)
        (directory / "export.json").write_text(json.dumps(exported, sort_keys=True) + "\n", encoding="utf-8")


def reonboard(args: Any) -> int:
    """``fastloop gitlab reonboard``: run only the onboarding step again on an existing gitlab lane.

    ``up`` refuses an existing lane, so a failed or abandoned onboarding on a Ready cluster would
    otherwise cost a full cluster rebuild. This is the same ``onboard_retaining_evidence`` ``up`` ends with,
    on the saved environment; it is not a clean install.
    """
    from benchmarks.sregym.fastloop import toy
    from benchmarks.sregym.fastloop.cli import lifecycle_objective
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

    environment = FastloopEnvironment.load(args.run_dir.resolve())
    if environment.app != "gitlab":
        raise ValueError(f"{environment.run_dir} is a {environment.app} lane; reonboard handles gitlab lanes only")
    runtime = lane_runtime_config(
        environment,
        provided_plan(environment.workspace, NAMESPACE),
        registrations=getattr(args, "agent_registrations", None),
    )
    onboard_retaining_evidence(environment, runtime, lifecycle_objective(DEFAULT_GOAL, getattr(args, "goal", None)))
    toy.record_controller(environment, runtime)
    print(json.dumps({"reonboarded": environment.cluster, "run_dir": str(environment.run_dir)}))
    return 0


def up(args: Any) -> int:
    """``fastloop up --app gitlab``: cluster, prerequisites, source workspace, SDO install with the provided source."""
    from benchmarks.sregym.fastloop import toy
    from benchmarks.sregym.fastloop.dev import prepare_dev_mount
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment, Images
    from benchmarks.sregym.fastloop.host_limits import HostLimits, NodeLimits, apply_node_limits
    from benchmarks.sregym.fastloop.toy_cni import CALICO_IMAGES, install_calico
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
    pull_missing_images([*prerequisite_images(), *CALICO_IMAGES])
    load_images(
        cluster,
        [images.controller, images.responder, *prerequisite_images(), *CALICO_IMAGES],
        record_path=run_dir / "image_delivery.jsonl",
        host_limits=host_limits,
    )
    install_calico(kubeconfig, run_dir / "calico.yaml")
    install_prerequisites(kubeconfig)
    workspace = run_dir / "application_workspace"
    workspace.mkdir()
    release = getattr(args, "chart_release", None)
    pins = None if release is None else load_releases().get(release).pins(assemble.load_pins())
    seed = prepare_workspace(workspace, pins=pins)
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
    serve_upstream(kubeconfig, workspace, lane_upstream(env), images.controller)
    runtime = lane_runtime_config(
        env, provided_plan(workspace, NAMESPACE), registrations=getattr(args, "agent_registrations", None)
    )
    onboard_retaining_evidence(env, runtime, objective)
    toy.record_controller(env, runtime)
    print(json.dumps({"up_total": time.monotonic() - started, "workspace": str(workspace), "commit": seed}))
    return 0
