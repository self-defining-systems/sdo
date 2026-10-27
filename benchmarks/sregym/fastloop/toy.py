"""Real-kind toy lane; no external SREGym checkout or models required."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from apps.toy.faults import IMAGE, command, fault_spec
from benchmarks.sregym.adapter import WaitKind, wait_for
from benchmarks.sregym.fastloop.environment import FastloopEnvironment, Images
from benchmarks.sregym.fastloop.host_limits import HostLimits, NodeLimits, apply_node_limits

ROOT = Path(__file__).resolve().parents[3]
APP = ROOT / "apps/toy"
NAMESPACE = "sdo-toy"
CONTROLLER_REPOSITORY = "/workspace/application"  # the controller Job's --app
#: The kind cluster name; a scratch run sets ``SDO_TOY_CLUSTER`` to avoid the shared default.
CLUSTER = os.environ.get("SDO_TOY_CLUSTER", "sdo-toy")


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
            command(config, env.namespace, "apply", "-f", str(env.workspace / "kubernetes"))
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


def cluster_name(args: Any) -> str:
    return f"{args.cluster_prefix}{args.worker_id}"


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

        desired = Images(
            controller=args.controller_image, responder=args.responder_image, validator=args.validator_image
        )
        if env.images != desired or not node_images_match(env.cluster, desired):
            load_images(
                env.cluster,
                [desired.controller, desired.responder, desired.validator],
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
        config = install_controller(env)
        record_controller(env, config)
        print(f"toy environment ready: {env.path}")
        return 0
    clusters = subprocess.run(
        ["kind", "get", "clusters"], check=True, capture_output=True, text=True
    ).stdout.splitlines()
    if cluster in clusters:
        raise ValueError(f"{cluster} already exists without this run directory; refusing to adopt it")
    config = prepare_dev_mount(worktree, None, run_dir)
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
    images = Images(controller=args.controller_image, responder=args.responder_image, validator=args.validator_image)
    from benchmarks.sregym.fastloop.toy_images import load_images

    load_images(
        cluster,
        [image, images.controller, images.responder, images.validator, *CALICO_IMAGES],
        record_path=run_dir / "image_delivery.jsonl",
        host_limits=host_limits,
    )
    images_loaded = time.monotonic()
    install_calico(kubeconfig, run_dir / "calico.yaml")
    loaded = time.monotonic()
    workspace = run_dir / "application_workspace"
    workspace.mkdir()
    shutil.copytree(APP / "kubernetes", workspace / "kubernetes")
    for manifest in (workspace / "kubernetes").glob("*.yaml"):
        manifest.write_text(manifest.read_text().replace(IMAGE, image))
    for name in ("main.go", "Dockerfile"):
        shutil.copy(APP / name, workspace / name)
    for git_args in (
        ["init", "-q"],
        ["add", "."],
        ["-c", "user.name=toy", "-c", "user.email=toy@invalid", "commit", "-qm", "feat: seed toy application"],
    ):
        subprocess.run(["git", "-C", str(workspace), *git_args], check=True)
    seed = subprocess.run(
        ["git", "-C", str(workspace), "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()
    source_commit = seed
    shutil.copytree(APP / "seed/.sdo", workspace / ".sdo")
    arch = workspace / ".sdo/arch.md"
    topology = hashlib.sha256(
        b"".join(path.read_bytes() for path in sorted((workspace / "kubernetes").glob("*.yaml")))
    ).hexdigest()
    arch.write_text(arch.read_text().replace("SOURCE_COMMIT", source_commit).replace("TOPOLOGY_FINGERPRINT", topology))
    subprocess.run(["git", "-C", str(workspace), "add", ".sdo"], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(workspace),
            "-c",
            "user.name=toy",
            "-c",
            "user.email=toy@invalid",
            "commit",
            "-qm",
            "feat: seed toy operational memory",
        ],
        check=True,
    )
    seed = subprocess.run(
        ["git", "-C", str(workspace), "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()
    command(str(kubeconfig), "default", "create", "namespace", NAMESPACE)
    with host_limits.admit("redeploy"):
        command(str(kubeconfig), NAMESPACE, "apply", "-f", str(workspace / "kubernetes"))
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
    command(str(kubeconfig), NAMESPACE + "-sdo", "create", "secret", "generic", "sdo-agent-credentials")
    env.save()
    deployed = time.monotonic()
    config = install_controller(env)
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


def install_controller(environment: FastloopEnvironment, config: Any = None) -> Any:
    """Install the production controller and scripted registrations, without benchmark registry resources."""
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
    os.environ["SDO_DEV_MOUNT"] = "1"
    os.environ["SDO_OPERATOR_BIN"] = str(environment.dev_mount / ".fastloop/sdo-operator")
    if config is None:
        config = RuntimeConfig(
            repository=environment.workspace,
            namespace=environment.namespace,
            application=environment.application,
            controller_namespace=environment.namespace + "-sdo",
            controller_image=environment.images.controller,
            responder_image=environment.images.responder,
            validator_image=environment.images.validator,
            repository_pvc="sdo-application-repository",
            credentials_secret="sdo-agent-credentials",
            model="gpt-6-luna",
            timeout_seconds=int(wait_for(WaitKind.RESPONSE, 2)),
            repair_policy="recorded-actions",
            persistent=True,
            kind_cluster_name=environment.cluster,
            deployed_manifest_paths=("kubernetes",),
        )
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
    publish_agent_registrations(
        config.agent_registrations or ROOT / "benchmarks/sregym/adapter/scripted_registrations",
        install.control_namespace,
        environment.kubeconfig,
        environment.images.responder,
        {environment.images.controller, environment.images.responder, environment.images.validator},
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
