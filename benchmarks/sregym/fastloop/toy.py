"""Real-kind toy lane; no external SREGym checkout or models required."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from apps.toy.faults import IMAGE, SERVICES, command, fault_spec
from apps.toy.variants.assemble import VARIANTS, assemble
from benchmarks.sregym.adapter import WaitKind, buildkit_image, deployment_plan, helm_source, raw_source, wait_for
from benchmarks.sregym.fastloop.environment import FastloopEnvironment, Images
from benchmarks.sregym.fastloop.host_limits import HostLimits, NodeLimits, apply_node_limits
from benchmarks.sregym.fastloop.toy_upstream import UpstreamLayout, serve_upstream, source_config
from benchmarks.sregym.repository_git import host_git_command

logger = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parents[3]
APP = ROOT / "apps/toy"
NAMESPACE = "sdo-toy"
CONTROLLER_REPOSITORY = "/workspace/application"  # the controller Job's --app
#: The goal a fresh-memory install publishes unless ``--goal`` overrides it (the deployer budgets 256 characters).
DEFAULT_GOAL = (
    "Keep the frontend, backend and catalog Deployments available with ready Service endpoints, "
    "and the frontend /journey endpoint answering HTTP 200 through the backend."
)


class ToyWorker:
    """The same fault-driver protocol, implemented against a real Kubernetes API."""

    def __init__(self, environment: FastloopEnvironment) -> None:
        self.environment = environment
        self.active: list[str] = []

    def __enter__(self) -> ToyWorker:
        return self

    def __exit__(self, *args: object) -> None:
        pass

    def request(self, op: str, *, timeout: float = int(wait_for(WaitKind.SLOW_COMMAND)), **args: Any) -> dict[str, Any]:
        env = self.environment
        config = str(env.kubeconfig)
        if op == "clean_start":
            from benchmarks.sregym.fastloop.clean_start import remove_injected_objects

            remove_injected_objects(env.kubeconfig)
            return {"success": True}
        if op == "deploy":
            return {"deployed": False, "seconds": 0}
        if op == "health":
            deployments = json.loads(command(config, env.namespace, "get", "deployments", "-o", "json"))["items"]
            unready = [
                item["metadata"]["name"]
                for item in deployments
                if item.get("status", {}).get("readyReplicas", 0) != item["spec"].get("replicas", 1)
            ]
            return {"healthy": len(deployments) == 3 and not unready, "unready": unready}
        if op == "inject":
            spec = fault_spec(args["problem_id"])
            if not args.get("compose"):
                self.active.clear()
            started = time.time()
            spec.inject(config, env.namespace)
            self.active.append(args["problem_id"])
            return {"started_at": started, "finished_at": time.time(), "fault": len(self.active) - 1}
        if op in {"oracle", "baseline_oracle"}:
            ids = self.active if op == "oracle" else [args["problem_id"]]
            if "fault" in args:
                ids = [ids[args["fault"]]]
            checks = {
                problem: fault_spec(problem).oracle(
                    config, env.namespace, image=env.worker_env.get("FASTLOOP_TOY_IMAGE", IMAGE)
                )
                for problem in ids
            }
            return {"kind": "health-check", "success": bool(checks) and all(checks.values()), "details": checks}
        if op == "recover":
            started = time.monotonic()
            apply_accepted_source(env.workspace, env.namespace, env.kubeconfig)
            return {
                "seconds": time.monotonic() - started,
                "protocol_version": 2,
                "success": True,
                "recovery_contract": "seed-compatible/v1",
            }
        if op == "refresh":
            # Commands create fresh kubectl clients; the toy retains no API or
            # credential clients across recovery.
            return {"protocol_version": 2, "success": True, "recovery_contract": "seed-compatible/v1"}
        raise ValueError(f"unsupported toy worker operation: {op}")


def apply_accepted_source(workspace: Path, namespace: str, kubeconfig: Path) -> None:
    from benchmarks.sregym.fastloop.accepted_source import AcceptedSource
    from benchmarks.sregym.fastloop.baseline_recovery import KubectlApplicationCluster

    source = AcceptedSource(workspace, namespace, ("kubernetes",))
    manifests = source.manifests
    cluster = KubectlApplicationCluster(kubeconfig)
    for manifest in manifests:
        cluster.apply(namespace, manifest)


def cluster_name(args: Any) -> str:
    return f"{args.cluster_prefix}{args.worker_id}"


def toy_variant(args: Any) -> str:
    """The deployment variant of this install; non-raw variants are onboarded fresh (SDO authors the deployment)."""
    variant = getattr(args, "variant", None) or "raw"
    if variant not in VARIANTS:
        raise ValueError(f"unknown toy variant {variant!r}; expected one of {VARIANTS}")
    if variant != "raw" and not getattr(args, "fresh_memory", False):
        raise ValueError(f"--variant {variant} needs --fresh-memory: only the raw variant has a hand-authored seed")
    return variant


def provided_deployment_plan(variant: str, namespace: str) -> dict[str, Any] | None:
    """The deployment the harness provides for a variant that ships one (helm); SDO deploys it, no agent authors it.

    The raw lane is seeded and the remaining variants are still authored by the deployer agent.
    """
    if variant != "helm":
        return None
    return deployment_plan(
        helm_source("chart"),
        images=(buildkit_image(IMAGE, ".", "Dockerfile"),),
        expected_workload_ids=[f"Deployment/{namespace}/{service}" for service in SERVICES],
    )


def prepare_workspace(workspace: Path, image: str, *, fresh: bool, variant: str = "raw") -> str:
    """Commit the toy source into ``workspace``; return the commit SDO starts from.

    Seeded (default): the hand-authored ``.sdo`` seed is committed on top of the source. Fresh: the workspace
    carries source only, so SDO's own lifecycle (source deployment, health judge) onboards it.

    A non-raw ``variant`` is assembled by ``apps/toy/variants/assemble.py`` (its own deployment files and a
    Dockerfile that builds ``main.go`` from source). Its image is built by the deployment source, so ``image``
    (the raw lane's prebuilt, kind-loaded tag) is not substituted into it.
    """
    if variant != "raw":
        if not fresh:
            raise ValueError(f"toy variant {variant} has no seed memory; prepare it fresh")
        assemble(variant, workspace)
    else:
        shutil.copytree(APP / "kubernetes", workspace / "kubernetes")
        for manifest in (workspace / "kubernetes").glob("*.yaml"):
            manifest.write_text(manifest.read_text().replace(IMAGE, image))
        for name in ("main.go", "Dockerfile"):
            shutil.copy(APP / name, workspace / name)

    def head() -> str:
        return subprocess.run(
            host_git_command(workspace, "rev-parse", "HEAD"), check=True, capture_output=True, text=True
        ).stdout.strip()

    for git_args in (
        ["init", "-q"],
        ["add", "."],
        ["-c", "user.name=toy", "-c", "user.email=toy@invalid", "commit", "-qm", "feat: seed toy application"],
    ):
        subprocess.run(host_git_command(workspace, *git_args), check=True)
    source_commit = head()
    if fresh:
        return source_commit
    shutil.copytree(APP / "seed/.sdo", workspace / ".sdo")
    arch = workspace / ".sdo/arch.md"
    topology = hashlib.sha256(
        b"".join(path.read_bytes() for path in sorted((workspace / "kubernetes").glob("*.yaml")))
    ).hexdigest()
    arch.write_text(arch.read_text().replace("SOURCE_COMMIT", source_commit).replace("TOPOLOGY_FINGERPRINT", topology))
    subprocess.run(host_git_command(workspace, "add", ".sdo"), check=True)
    subprocess.run(
        host_git_command(
            workspace,
            "-c",
            "user.name=toy",
            "-c",
            "user.email=toy@invalid",
            "commit",
            "-qm",
            "feat: seed toy operational memory",
        ),
        check=True,
    )
    return head()


def fresh_objective(args: Any) -> str | None:
    """The goal a fresh-memory toy install publishes, or None for the seeded default.

    Fresh onboarding needs real lifecycle agents, so it excludes the frozen seed install.
    """
    goal = getattr(args, "goal", None)
    fresh = bool(getattr(args, "fresh_memory", False))
    if goal is not None and not fresh:
        raise ValueError("--goal applies to fresh-memory installs only; add --fresh-memory")
    if not fresh:
        return None
    if getattr(args, "frozen_memory", False):
        raise ValueError("--fresh-memory onboards with agents; it cannot be combined with --frozen-memory")
    from benchmarks.sregym.fastloop.cli import lifecycle_objective

    return lifecycle_objective(DEFAULT_GOAL, goal)


#: Executables the scripted agent runner invokes inside the responder image.
SCRIPTED_WORKER_EXECUTABLES = ("bwrap",)


def up(args: Any) -> int:
    from benchmarks.sregym.fastloop.baseline_recovery import (
        KubectlApplicationCluster,
        capture_application_baseline,
        save_application_baseline,
    )
    from benchmarks.sregym.fastloop.dev import prepare_dev_mount
    from benchmarks.sregym.fastloop.toy_cni import CALICO_IMAGES, install_calico

    if args.dev_mount is None:
        raise ValueError("toy lanes require a development mount; omit --no-dev-mount")
    objective = fresh_objective(args)
    variant = toy_variant(args)
    if objective is None and not getattr(args, "frozen_memory", False):
        # A seed carries no judge provenance, so the install runs the scripted health refresh in the responder image.
        from benchmarks.sregym.adapter import assert_image_executables

        assert_image_executables(args.responder_image, SCRIPTED_WORKER_EXECUTABLES, run=subprocess.run)
    prebuilt = variant == "raw"  # other variants are built from source by SDO's deployment source
    host_limits = HostLimits(args.host_lock_dir.resolve(), args.host_redeploys)
    cluster = cluster_name(args)
    image = f"{IMAGE}-{cluster}"
    started = time.monotonic()
    run_dir = args.run_dir.resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    worktree = (args.dev_mount or ROOT).resolve()
    if (run_dir / "fastloop.json").exists():
        env = FastloopEnvironment.load(run_dir)
        apply_node_limits(env.cluster, NodeLimits(args.cpu_limit, args.memory_limit, args.pids_limit))
        if env.dev_mount is None:
            raise ValueError("toy environment requires a development mount")
        if not ToyWorker(env).request("health")["healthy"]:
            raise ValueError("toy environment is unhealthy; run fastloop reset")
        from benchmarks.sregym.fastloop.cli import node_images_match
        from benchmarks.sregym.fastloop.toy_images import load_images

        desired = Images(controller=args.controller_image, responder=args.responder_image)
        if env.images != desired or not node_images_match(env.cluster, desired):
            load_images(
                env.cluster,
                [desired.controller, desired.responder],
                record_path=run_dir / "image_delivery.jsonl",
                host_limits=host_limits,
            )
            env = FastloopEnvironment.load(run_dir)
            env.images = desired
            env.save()
        if env.dev_mount is None:
            raise ValueError("toy environment requires a development mount")
        env.worker_env.update(host_limits.worker_env())
        env.save()
        prepare_dev_mount(env.dev_mount, None, run_dir)
        config = install_controller(
            env,
            registrations=getattr(args, "agent_registrations", None),
            frozen_memory=getattr(args, "frozen_memory", False),
            variant=variant,
        )
        record_controller(env, config)
        print(f"toy environment ready: {env.path}")
        return 0
    clusters = subprocess.run(
        ["kind", "get", "clusters"], check=True, capture_output=True, text=True
    ).stdout.splitlines()
    if cluster in clusters:
        raise ValueError(f"{cluster} already exists without this run directory; refusing to adopt it")
    config = prepare_dev_mount(worktree, None, run_dir)
    if prebuilt:
        build = run_dir / "image"
        build.mkdir(exist_ok=True)
        subprocess.run(
            ["go", "build", "-buildvcs=false", "-trimpath", "-o", str(build / "toy"), str(APP / "main.go")],
            check=True,
            env={**os.environ, "CGO_ENABLED": "0", "GOMAXPROCS": "2", "GOFLAGS": "-p=2"},
        )
        shutil.copy(APP / "Dockerfile", build / "Dockerfile")
        subprocess.run(
            ["docker", "build", "--load", "--network=none", "--provenance=false", "-t", image, str(build)], check=True
        )
    kubeconfig = run_dir / "kubeconfig"
    create_start = time.monotonic()
    subprocess.run(
        [
            "kind",
            "create",
            "cluster",
            "--name",
            cluster,
            "--image",
            args.toy_node_image,
            "--config",
            str(config),
            "--kubeconfig",
            str(kubeconfig),
            "--wait",
            "0s",
        ],
        check=True,
    )
    apply_node_limits(cluster, NodeLimits(args.cpu_limit, args.memory_limit, args.pids_limit))
    created = time.monotonic()
    images = Images(controller=args.controller_image, responder=args.responder_image)
    from benchmarks.sregym.fastloop.toy_images import load_images

    load_images(
        cluster,
        [*([image] if prebuilt else []), images.controller, images.responder, *CALICO_IMAGES],
        record_path=run_dir / "image_delivery.jsonl",
        host_limits=host_limits,
    )
    images_loaded = time.monotonic()
    install_calico(kubeconfig, run_dir / "calico.yaml")
    loaded = time.monotonic()
    workspace = run_dir / "application_workspace"
    workspace.mkdir()
    seed = prepare_workspace(workspace, image, fresh=objective is not None, variant=variant)
    command(str(kubeconfig), "default", "create", "namespace", NAMESPACE)
    if prebuilt:
        with host_limits.admit("redeploy"):
            apply_accepted_source(workspace, NAMESPACE, kubeconfig)
            command(str(kubeconfig), NAMESPACE, "rollout", "status", "deployment", "--timeout=60s")
    env = FastloopEnvironment(
        run_dir=run_dir,
        app="toy",
        cluster=cluster,
        kubeconfig=kubeconfig,
        namespace=NAMESPACE,
        application="toy",
        workspace=workspace,
        sregym_dir=ROOT / "third_party/sregym",
        private_tmp=None,
        deploy_problem="toy_target_port_frontend",
        seed_commit=seed,
        dev_mount=worktree,
        images=images,
        worker_env={
            "FASTLOOP_TOY_IMAGE": image,
            **host_limits.worker_env(),
        },
    )
    from benchmarks.sregym.fastloop.image_delivery import delivery_state

    delivery = delivery_state(run_dir / "image_delivery.jsonl")
    env.image_delivery, env.image_delivery_reason = delivery["path"], delivery["reason"]
    env.image_delivery_seconds, env.image_delivery_images = delivery["seconds"], delivery["images"]
    env.baseline_path.write_text("{}\n", encoding="utf-8")
    save_application_baseline(
        env.baseline_path, (capture_application_baseline(KubectlApplicationCluster(kubeconfig), NAMESPACE),)
    )
    command(str(kubeconfig), "default", "create", "namespace", NAMESPACE + "-sdo")
    hook = getattr(args, "credentials_hook", None)
    if hook is None:
        command(str(kubeconfig), NAMESPACE + "-sdo", "create", "secret", "generic", "sdo-agent-credentials")
    else:
        from benchmarks.sregym.fastloop.wipe import credentials_hook

        credentials_hook(hook.resolve(), kubeconfig)(NAMESPACE + "-sdo")
    env.save()
    if variant == "helm":
        # The provided helm deployment follows an upstream the harness serves (seeded with the accepted source SHA).
        serve_upstream(kubeconfig, workspace, UpstreamLayout(NAMESPACE), images.controller)
    deployed = time.monotonic()
    config = install_controller(
        env,
        registrations=getattr(args, "agent_registrations", None),
        frozen_memory=getattr(args, "frozen_memory", False),
        objective=objective,
        variant=variant,
    )
    record_controller(env, config)
    timing = {
        "prepare": create_start - started,
        "cluster_create": created - create_start,
        "image_load": images_loaded - created,
        "cni": loaded - images_loaded,
        "application": deployed - loaded,
        "controller": time.monotonic() - deployed,
        "up_total": time.monotonic() - started,
    }
    (run_dir / "toy_timings.json").write_text(json.dumps(timing, indent=2) + "\n")
    print(json.dumps(timing))
    return 0


def install_controller(
    environment: FastloopEnvironment,
    config: Any = None,
    *,
    registrations: Path | None = None,
    frozen_memory: bool = False,
    objective: str | None = None,
    variant: str = "raw",
) -> Any:
    """Install the production controller and its registrations (scripted unless given), without benchmark resources.

    With ``objective`` the memory is fresh: the production Go host lifecycle (source deployment, health judge,
    goal publication) onboards the workspace, with the production worker registrations unless given.
    Only the raw variant declares a raw deployment source over ``kubernetes``; the others are rendered by a
    deployment source, so they declare none.
    """
    from dataclasses import replace

    from benchmarks.sregym.adapter import (
        RuntimeConfig,
        install_existing,
        native_install_config,
        publish_agent_registrations,
    )

    os.environ["KUBECONFIG"] = str(environment.kubeconfig)
    if environment.dev_mount is None:
        raise ValueError("toy controller requires a development mount")
    environment.apply_operator_env(os.environ)
    if config is None:
        config = RuntimeConfig(
            repository=environment.workspace,
            namespace=environment.namespace,
            application=environment.application,
            controller_namespace=environment.namespace + "-sdo",
            controller_image=environment.images.controller,
            responder_image=environment.images.responder,
            repository_pvc="sdo-application-repository",
            credentials_secret="sdo-agent-credentials",
            model="gpt-6-luna",
            timeout_seconds=int(wait_for(WaitKind.RESPONSE, 2)),
            repair_policy="recorded-actions",
            frozen_memory=frozen_memory,
            persistent=True,
            kind_cluster_name=environment.cluster,
            deployment_source=raw_source(("kubernetes",)) if variant == "raw" else None,
            deployment_plan=provided_deployment_plan(variant, environment.namespace),
            source_kind="git" if variant == "helm" else "",
            source_config=json.dumps(source_config(UpstreamLayout(environment.namespace))) if variant == "helm" else "",
            agent_registrations=registrations.resolve() if registrations else None,
        )
    if frozen_memory and not config.frozen_memory:
        config = replace(config, frozen_memory=True)
    install = replace(native_install_config(config), registration_configmap="sdo-agent-registrations")
    resources = [
        {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": install.control_namespace}},
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "sdo-controller-maintenance", "namespace": install.control_namespace},
            "data": {"state": "active", "generation": f"toy-install-{time.time_ns()}"},
        },
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": install.credentials_secret, "namespace": install.control_namespace},
        },
    ]
    subprocess.run(
        ["kubectl", "--kubeconfig", str(environment.kubeconfig), "apply", "-f", "-"],
        input=json.dumps({"apiVersion": "v1", "kind": "List", "items": resources}),
        text=True,
        check=True,
        capture_output=True,
    )
    if objective is not None:
        from benchmarks.sregym.adapter import launch_native_lifecycle

        receipt = launch_native_lifecycle(config, objective, "")
        wait_for_baseline(environment, receipt["judge_commit"])
        return config
    publish_agent_registrations(
        config.agent_registrations or ROOT / "benchmarks/sregym/fastloop/agent_registrations",
        install.control_namespace,
        environment.kubeconfig,
        environment.images.responder,
        {environment.images.controller, environment.images.responder},
    )
    receipt = install_existing(install)
    wait_for_baseline(environment, receipt.accepted_commit)
    return config


def record_controller(environment: FastloopEnvironment, config: Any) -> None:
    """Record the controller this up invocation installed, using its observed Pod UID."""
    from datetime import datetime, timezone

    from benchmarks.sregym.adapter import (
        ControllerRecord,
        KubectlClusterOps,
        PersistentState,
        deployed_lifecycle,
        runtime_config_payload,
    )

    os.environ["KUBECONFIG"] = str(environment.kubeconfig)
    pod = KubectlClusterOps().controller_pod(config.control_namespace)
    if pod is None:
        raise RuntimeError("toy controller has no ready Pod after baseline establishment")
    state = PersistentState.load(environment.state_path)
    state.controllers[environment.namespace] = ControllerRecord(
        application=environment.application,
        namespace=environment.namespace,
        control_namespace=config.control_namespace,
        lifecycle_fingerprint=deployed_lifecycle(environment.namespace).fingerprint,
        controller_pod_uid=pod.uid,
        controller_pod_name=pod.name,
        installed_by_stage="fastloop-up-toy",
        installed_at=datetime.now(timezone.utc),
        kubeconfig=str(environment.kubeconfig),
        runtime_config=runtime_config_payload(config),
    )
    state.save(environment.state_path)


def wait_for_baseline(environment: FastloopEnvironment, accepted_commit: str) -> None:
    """Require real detector evaluations and accepted-source adoption before reporting ready."""
    wait = float(os.environ.get("SDO_TOY_BASELINE_WAIT", str(wait_for(WaitKind.OPERATION))))
    deadline = time.monotonic() + wait
    while time.monotonic() < deadline:
        completed = subprocess.run(
            [
                "kubectl",
                "--kubeconfig",
                str(environment.kubeconfig),
                "-n",
                environment.namespace + "-sdo",
                "get",
                "configmap",
                "sdo-controller-state",
                "-o",
                "json",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode == 0:
            state = json.loads(json.loads(completed.stdout)["data"]["runtime-state.json"])
            pods = json.loads(
                command(
                    str(environment.kubeconfig),
                    environment.namespace + "-sdo",
                    "get",
                    "pods",
                    "-l",
                    "job-name=sdo-controller-run",
                    "-o",
                    "json",
                )
            )["items"]
            ready = [
                pod
                for pod in pods
                if not pod["metadata"].get("deletionTimestamp")
                and pod.get("status", {}).get("phase") == "Running"
                and any(
                    item.get("type") == "Ready" and item.get("status") == "True"
                    for item in pod.get("status", {}).get("conditions", [])
                )
            ]
            head = len(ready) == 1 and accepted_head(environment, ready[0]["metadata"]["name"], accepted_commit)
            if head and baseline_ready(state, head, ready[0]["metadata"]["creationTimestamp"]):
                break
        time.sleep(0.2)
    else:
        raise RuntimeError(f"toy controller did not establish an adopted, clear baseline in {wait:.0f}s")


def retire_control_state(environment: FastloopEnvironment, control: str) -> None:
    """Stop the controller and discard every control-state record of the previous install."""
    from benchmarks.sregym.adapter import reset_control_state, stop_runtime

    kubeconfig = str(environment.kubeconfig)
    stop_runtime(control, kubeconfig=kubeconfig)
    receipt = reset_control_state(control, kubeconfig=kubeconfig)
    names = command(kubeconfig, control, "get", "configmap", "-o", "name").split()
    stale = [
        name
        for name in names
        if name.startswith("configmap/sdo-health-refresh-") or name == "configmap/sdo-external-incident"
    ]
    if stale:
        command(kubeconfig, control, "delete", *stale, "--ignore-not-found=true")
    logger.info("Toy control state reset: %s", json.dumps({**receipt, "health_refresh": stale}, sort_keys=True))


def accepted_head(environment: FastloopEnvironment, pod_name: str, install_commit: str) -> str | None:
    """The controller's accepted HEAD if it contains the installed commit, else None.

    Install runs the judge refresh, which commits health detectors on top of the installed commit; the
    controller then serves and adopts that head, so adoption is judged against it, not the install commit.
    """
    try:
        return (
            command(
                str(environment.kubeconfig),
                environment.namespace + "-sdo",
                "exec",
                pod_name,
                "-c",
                "controller",
                "--",
                "sh",
                "-c",
                'git -C "$1" merge-base --is-ancestor "$2" HEAD && git -C "$1" rev-parse HEAD',
                "sh",
                CONTROLLER_REPOSITORY,
                install_commit,
            ).strip()
            or None
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return None


def baseline_ready(state: dict[str, Any], accepted_head: str, observed_after: str | None = None) -> bool:
    """Closed historical firings do not invalidate a currently clear controller."""
    latest = {item["detector_id"]: item for item in state.get("history", [])}
    return bool(
        state.get("evaluation_iteration", 0) > 0
        and not state.get("incident_open")
        and latest
        and (
            observed_after is None
            or any(
                datetime.fromisoformat(item["evaluated_at"].replace("Z", "+00:00"))
                >= datetime.fromisoformat(observed_after.replace("Z", "+00:00"))
                for item in latest.values()
                if item.get("evaluated_at")
            )
        )
        and all(item.get("status") == "clear" for item in latest.values())
        and state.get("ownership_adoption", {}).get("source_commit") == accepted_head
    )


@dataclass(frozen=True)
class OnboardingEvidence:
    """What a fresh variant onboarding must show, read from the accepted evidence plan and controller Job."""

    renderer_id: str
    image_builders: tuple[str, ...]
    controller_args: tuple[str, ...]


def onboarding_violations(evidence: OnboardingEvidence, variant: str) -> list[str]:
    """Why ``evidence`` does not show a ``variant`` onboarding built from source; empty when it does.

    The renderer id of each non-raw variant is the variant name. The raw variant renders ``raw``.
    """
    violations: list[str] = []
    if evidence.renderer_id != variant:
        violations.append(f"accepted evidence plan renderer_id is {evidence.renderer_id!r}, expected {variant!r}")
    if not evidence.image_builders:
        violations.append("accepted evidence plan declares no images")
    elif other := sorted({builder for builder in evidence.image_builders if builder != "buildkit"}):
        violations.append(f"images not built by buildkit: {', '.join(other)}")
    if "--deployment-source" not in evidence.controller_args:
        violations.append("controller args carry no --deployment-source")
    return violations


class RestartOps(Protocol):
    """The cluster and clock edge of the controller restart check (injected so the check is deterministic)."""

    def clock(self) -> float: ...
    def sleep(self, seconds: float) -> None: ...
    def controller_pod(self) -> str | None: ...
    def delete_pod(self, name: str) -> None: ...
    def verify_accepted(self) -> bool: ...
    def incident_open(self) -> bool: ...
    def drift_observed(self) -> bool: ...


@dataclass
class RestartResult:
    old_pod: str
    new_pod: str | None
    violations: list[str] = field(default_factory=list)


def check_controller_restart(
    ops: RestartOps, *, steady_seconds: float, restart_timeout: float, poll: float
) -> RestartResult:
    """Delete the controller pod; require a replacement, accepted-state verification, and a quiet steady window.

    A failure is reported in ``violations``; nothing here retries or softens a check.
    """
    old = ops.controller_pod()
    if old is None:
        raise RuntimeError("no controller pod to restart")
    ops.delete_pod(old)
    result = RestartResult(old_pod=old, new_pod=None)
    deadline = ops.clock() + restart_timeout
    while True:
        pod = ops.controller_pod()
        if pod is not None and pod != old:
            result.new_pod = pod
            break
        if ops.clock() >= deadline:
            result.violations.append(f"controller pod was not replaced within {restart_timeout:g}s")
            return result
        ops.sleep(poll)
    if not ops.verify_accepted():
        result.violations.append("accepted state did not verify after the restart")
    steady_end = ops.clock() + steady_seconds
    incident = drift = False
    while ops.clock() < steady_end:
        incident = incident or ops.incident_open()
        drift = drift or ops.drift_observed()
        ops.sleep(poll)
    incident = incident or ops.incident_open()
    drift = drift or ops.drift_observed()
    if incident:
        result.violations.append("an incident opened during the steady window")
    if drift:
        result.violations.append("drift was observed during the steady window")
    return result
