"""The pool's real-app preparation path, shared by hotel, social and astronomy."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, ConfigDict, Field

from benchmarks.sregym.fastloop.environment import Images  # noqa: TC001 - Pydantic resolves this at runtime.
from benchmarks.sregym.pool.relay import RelayConfig, ensure_relay

SEED_RECEIPT = "sdo-pool-seed.json"

if TYPE_CHECKING:
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment


def persist_image_settings(setup: RealAppSetup, environment: FastloopEnvironment) -> None:
    """Persist lane policy across worker restarts, including no-fault preparation and recovery."""
    configured = setup.process_environment()
    for name in ("FASTLOOP_APP_IMAGE_REGISTRY", "FASTLOOP_APP_IMAGE_PULL_ONLY"):
        environment.worker_env[name] = configured[name]
    environment.save()


def admit_load(lease: str, timeout: float = 300) -> None:
    deadline = time.monotonic() + timeout
    while True:
        if (
            subprocess.run(
                ["/mnt/data/shli/coord/coord", "gate", "40", "--lease", lease], capture_output=True
            ).returncode
            == 0
        ):
            return
        if time.monotonic() >= deadline:
            raise TimeoutError("pool: host load stayed at or above 40 for five minutes; preparation is resumable")
        time.sleep(5)


class RealAppSetup(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    code_root: Path
    seed: Path
    seed_revision: str = Field(pattern=r"^[0-9a-f]{40}$")
    registry: str = Field(min_length=1)
    images: Images
    credentials_hook: Path
    relay: RelayConfig | None = None
    cpu_limit: float = Field(default=2, gt=0)

    def process_environment(self) -> dict[str, str]:
        return {**os.environ, "FASTLOOP_APP_IMAGE_REGISTRY": self.registry, "FASTLOOP_APP_IMAGE_PULL_ONLY": "1"}

    def verify_seed(self) -> None:
        receipt = self.seed / ".git" / SEED_RECEIPT
        if not receipt.is_file():
            raise ValueError("pool: seed needs a freshly rendered source revision receipt")
        facts = json.loads(receipt.read_text())
        revision = subprocess.check_output(["git", "-C", str(self.code_root), "rev-parse", "HEAD"], text=True).strip()
        head = subprocess.check_output(["git", "-C", str(self.seed), "rev-parse", "HEAD"], text=True).strip()
        if facts.get("build_revision") != revision or facts.get("seed_revision") != head or head != self.seed_revision:
            raise ValueError("pool: seed source revision differs from the build being run")
        if subprocess.check_output(["git", "-C", str(self.seed), "status", "--porcelain"], text=True).strip():
            raise ValueError("pool: seed has uncommitted edits")
        if any((self.seed / ".sdo" / name).exists() for name in ("corpus", "outcomes.jsonl")):
            raise ValueError("pool: a fresh seed cannot contain incident history")
        if list((self.seed / ".sdo/outcomes").glob("*.json")):
            raise ValueError("pool: a fresh seed cannot contain incident outcomes")


def prepare_real_app(
    setup: RealAppSetup,
    *,
    run_dir: Path,
    lane: str,
    problem: str,
    lease: str,
    repo: str,
    branch: str,
    registrations: Path | None = None,
    provider: Literal["codex", "claude"] = "claude",
    model: str = "claude-opus-5-5",
    reasoning_effort: Literal["low", "medium", "high", "xhigh", "max"] = "medium",
) -> None:
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment
    from benchmarks.sregym.fastloop.lane_guard import LaneGuard
    from benchmarks.sregym.fastloop.parallel import Encounter, ParallelPlan
    from benchmarks.sregym.fastloop.remote_memory import MemoryEndpoint, bind_lane_to_branch
    from benchmarks.sregym.fastloop.shared_memory import prepare_lane
    from benchmarks.sregym.pool.endpoints import MemoryEndpoint as PoolMemoryEndpoint
    from benchmarks.sregym.pool.guard import guarded_module
    from benchmarks.sregym.pool.scheduler import PairSpec
    from benchmarks.sregym.pool.worker import worker_registrations

    setup.verify_seed()
    if (
        provider == "claude"
        and "opus" in model.lower()
        and not all(os.environ.get(name) for name in ("SDO_OPUS_POOL_NFS_SERVER", "SDO_OPUS_POOL_NFS_PATH"))
    ):
        raise ValueError("pool: Opus lanes require RATE1's shared NFS pool before preparation")
    environment = setup.process_environment()
    if setup.relay is not None:
        ensure_relay(setup.relay, setup.code_root, run_dir.parent / "registry-relay")
    prefix = f"pool2-{lane}-"
    cluster = prefix + "0"
    kubeconfig = run_dir / "kubeconfigs/worker_0.kubeconfig"
    guard = LaneGuard(cluster, kubeconfig, lease, "/mnt/data/shli/coord/coord")
    guard.install()
    command = [
        sys.executable,
        "-m",
        "benchmarks.sregym.fastloop",
        "up",
        "--run-dir",
        str(run_dir),
        "--seed",
        str(setup.seed),
        "--problem",
        problem,
        "--cluster-prefix",
        prefix,
        "--worker-id",
        "0",
        "--cpu-limit",
        str(setup.cpu_limit),
        "--dev-mount",
        str(setup.code_root),
        "--sregym-dir",
        str(setup.code_root / "third_party/sregym"),
    ]
    for role, image in setup.images.model_dump().items():
        command.extend([f"--{role}-image", image])
    command = guarded_module(
        command, cluster=cluster, kubeconfig=kubeconfig, lease=lease, coord=Path("/mnt/data/shli/coord/coord")
    )
    if (run_dir / "fastloop.json").is_file():
        existing = FastloopEnvironment.load(run_dir)
        if (existing.cluster, existing.deploy_problem, existing.dev_mount, existing.images, existing.seed_commit) != (
            cluster,
            problem,
            setup.code_root,
            setup.images,
            setup.seed_revision,
        ):
            raise ValueError("pool: completed deployment differs from this preparation; retire it explicitly")
    else:
        admit_load(lease)
        subprocess.run(command, env={**environment, "KUBECONFIG": str(kubeconfig)}, check=True)
    if os.environ.get("SDO_OPUS_POOL_NFS_SERVER"):
        nodes = subprocess.check_output(["kind", "get", "nodes", "--name", cluster], text=True).splitlines()
        for node in nodes:
            admit_load(lease)
            subprocess.run(
                [
                    "docker",
                    "exec",
                    node,
                    "sh",
                    "-ec",
                    "command -v mount.nfs >/dev/null || (apt-get update -qq && apt-get install -y -qq nfs-common)",
                ],
                check=True,
            )
    lane_env = FastloopEnvironment.load(run_dir)
    persist_image_settings(setup, lane_env)
    subprocess.run(
        [str(setup.credentials_hook), lane_env.namespace + "-sdo"],
        env={**environment, "KUBECONFIG": str(lane_env.kubeconfig)},
        check=True,
    )
    settings = PairSpec(
        run_id=f"prepare-{lane}",
        lane=lane,
        problem=problem,
        app=lane_env.problem_application,
        encounters=["first"],
        run_dir=run_dir,
        output=run_dir / "prepare",
        provider=provider,
        model=model,
        reasoning_effort=reasoning_effort,
        timeout_seconds=2400,
        diagnosis_grading="off",
        capability_mode="permissive",
        lane_memory="shared",
        memory=PoolMemoryEndpoint(repo=repo, branch=branch),
        agent_registrations=registrations,
        lease=lease,
    )
    prepared_registrations = worker_registrations(settings, lane_env)
    plan = ParallelPlan(
        run_id=f"prepare-{lane}",
        accepted_repository=run_dir.parent / "unused-accepted",
        lanes={lane: run_dir},
        encounters=[Encounter(problem=problem, lane=lane)],
        provider=provider,
        model=model,
        reasoning_effort=reasoning_effort,
        agent_registrations=prepared_registrations,
        capability_mode="permissive",
        permissive_responder=True,
    )
    admit_load(lease)
    prepare_lane(plan, lane)
    lane_env = FastloopEnvironment.load(run_dir)
    lane_env.parallel_lane = lane
    bind_lane_to_branch(lane_env, MemoryEndpoint(repo, branch), seed=True)
    preflight_memory(lane_env, repo, branch)


def preflight_memory(environment: FastloopEnvironment, repo: str, branch: str) -> str:
    """Prove this controller's mounted host key can fetch and push its exact branch."""
    from benchmarks.sregym.adapter import control_namespace_for, kubectl
    from benchmarks.sregym.fastloop.remote_memory import SSH_DIR, MemoryEndpoint, shared_identity

    endpoint = MemoryEndpoint(repo, branch)
    if (environment.memory_repo, environment.memory_branch, environment.shared_accepted_id) != (
        repo,
        branch,
        shared_identity(endpoint),
    ):
        raise ValueError("pool: lane is not bound to the run's app memory branch")
    # Push exactly the fetched SHA to the same branch: checks write permission without a synthetic memory commit.
    script = (
        "scratch=$(mktemp -d); trap 'rm -rf \"$scratch\"' EXIT; "
        f'cp {SSH_DIR}/identity "$scratch/identity"; chmod 600 "$scratch/identity"; '
        f'export GIT_SSH_COMMAND="ssh -F /dev/null -i $scratch/identity -o IdentitiesOnly=yes '
        f'-o BatchMode=yes -o UserKnownHostsFile={SSH_DIR}/known_hosts -o StrictHostKeyChecking=yes"; '
        'git init --quiet "$scratch/repo"; cd "$scratch/repo"; '
        'git fetch --quiet --no-tags "$1" "refs/heads/$2" && '
        'git push --porcelain "$1" "FETCH_HEAD:refs/heads/$2" && git rev-parse FETCH_HEAD'
    )
    result = kubectl(
        [
            "exec",
            "job/sdo-controller-run",
            "-c",
            "controller",
            "--",
            "sh",
            "-ec",
            script,
            "sh",
            endpoint.url,
            endpoint.branch,
        ],
        namespace=control_namespace_for(environment.namespace),
        kubeconfig=environment.kubeconfig,
    )
    head = result.stdout.strip().splitlines()[-1]
    (environment.run_dir / "memory-preflight.json").write_text(
        json.dumps({"repo": repo, "branch": branch, "head": head}) + "\n"
    )
    return head


def configure_workers(
    environment: FastloopEnvironment, registrations: Path, provider: str, model: str, effort: str = "medium"
) -> None:
    """Switch idle prepared lanes between scripted and live workers without reseeding memory."""
    from benchmarks.sregym import adapter
    from benchmarks.sregym.fastloop.shared_memory import JOB_NAME, _json, lane_environment, shared_controller_job

    control = adapter.control_namespace_for(environment.namespace)
    job = _json(environment, "get", "job", JOB_NAME, "-o", "json", namespace=control)
    pod = job["spec"]["template"]["spec"]
    [volume] = [v for v in pod["volumes"] if v["name"] == "registrations"]
    name = volume["configMap"]["name"]
    current = _json(environment, "get", "configmap", name, "-o", "json", namespace=control)
    desired = {path.name: path.read_text() for path in registrations.glob("*.json")}
    args = pod["containers"][0]["args"]
    if (
        current["data"] == desired
        and args[args.index("--model") + 1] == model
        and args[args.index("--responder-backend") + 1] == provider
        and "--agent-effort" in args
        and args[args.index("--agent-effort") + 1] == effort
    ):
        return
    with lane_environment(environment):
        adapter.pause_for_recovery(adapter.KubectlClusterOps(), control)
    recreated = shared_controller_job(job, environment.application)
    arguments = recreated["spec"]["template"]["spec"]["containers"][0]["args"]
    for flag, value in (("--model", model), ("--responder-backend", provider), ("--agent-effort", effort)):
        if flag in arguments:
            arguments[arguments.index(flag) + 1] = value
        else:
            arguments.extend([flag, value])
    adapter.kubectl(
        ["delete", "job", JOB_NAME, "--cascade=foreground", "--wait=true", "--timeout=120s"],
        namespace=control,
        kubeconfig=environment.kubeconfig,
    )
    current["data"] = desired
    adapter.kubectl(
        ["replace", "-f", "-"], namespace=control, kubeconfig=environment.kubeconfig, input_text=json.dumps(current)
    )
    adapter.kubectl(
        ["create", "-f", "-"], namespace=control, kubeconfig=environment.kubeconfig, input_text=json.dumps(recreated)
    )
    adapter.kubectl(
        ["wait", "--for=condition=Ready", "pod", "--selector", f"job-name={JOB_NAME}", "--timeout=120s"],
        namespace=control,
        kubeconfig=environment.kubeconfig,
    )
    environment.state_path.unlink(missing_ok=True)


def sync_memory(environment: FastloopEnvironment, code_root: Path) -> str:
    """Final pool drain through the real broker lock, then prove controller and remote heads agree."""
    from benchmarks.sregym.adapter import control_namespace_for, kubectl
    from benchmarks.sregym.fastloop.remote_memory import SSH_DIR, MemoryEndpoint, remote_head
    from benchmarks.sregym.fastloop.shared_memory import _json

    endpoint = MemoryEndpoint(environment.memory_repo, environment.memory_branch)
    control = control_namespace_for(environment.namespace)
    pods = _json(
        environment, "get", "pods", "--selector", "job-name=sdo-controller-run", "-o", "json", namespace=control
    )
    [pod] = [p["metadata"]["name"] for p in pods["items"] if p.get("status", {}).get("phase") == "Running"]
    binary = environment.run_dir / "sdo-pool-memory"
    subprocess.run(
        ["go", "build", "-ldflags=-s -w", "-o", str(binary), str(code_root / "benchmarks/sregym/pool/seed_render.go")],
        cwd=code_root / "sdo/operator/runtime",
        check=True,
    )
    kubectl(
        ["cp", str(binary), f"{pod}:/tmp/sdo-pool-memory", "-c", "controller"],
        namespace=control,
        kubeconfig=environment.kubeconfig,
    )
    kubectl(
        [
            "exec",
            pod,
            "-c",
            "controller",
            "--",
            "/tmp/sdo-pool-memory",
            "sync",
            "/workspace/application",
            "/workspace/pool-sync-workspaces",
            endpoint.url,
            endpoint.branch,
            SSH_DIR,
        ],
        namespace=control,
        kubeconfig=environment.kubeconfig,
    )
    head = kubectl(
        ["exec", pod, "-c", "controller", "--", "git", "-C", "/workspace/application", "rev-parse", "HEAD"],
        namespace=control,
        kubeconfig=environment.kubeconfig,
    ).stdout.strip()
    upstream = remote_head(endpoint)
    if head != upstream:
        raise ValueError(f"pool: lane head {head} differs from remote {upstream} after broker sync")
    (environment.run_dir / "memory-convergence.json").write_text(
        json.dumps({"repo": endpoint.repo, "branch": endpoint.branch, "head": head, "remote_head": upstream}) + "\n"
    )
    return head
