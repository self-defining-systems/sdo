"""Run a real Claude health-worker turn with an actual rehearsal health Job's confinement.

The caller explicitly opts into model cost and supplies an existing credentials
Secret. This probe never reads Secret values and writes only its private tmpfs.
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import subprocess
from pathlib import Path
from typing import Any

from benchmarks.sregym.adapter import WaitKind, wait_for
from benchmarks.sregym.fastloop.environment import FastloopEnvironment
from benchmarks.sregym.fastloop.toy_images import load_images
from libs.agent_cli.provider_capacity import prepare_host_pool

JOB_NAME = "sdo-claude-worker-probe"


def probe_job(
    template: dict[str, Any],
    image: str,
    credentials_secret: str,
    *,
    pool_host_path: str | None = None,
    pool_capacity: int = 8,
    host_id: str = "",
) -> dict[str, Any]:
    """Keep the real worker's uid, capabilities, seccomp, mounts and resource limits."""
    spec = copy.deepcopy(template["spec"]["template"]["spec"])
    agent = next(container for container in spec["containers"] if container["name"] == "agent")
    agent.update(image=image, command=["python3", "/usr/local/lib/sdo/check_claude_worker.py"], workingDir="/tmp")
    agent.pop("args", None)
    # Use normal Python paths, rather than the scripted executable projection.
    agent["env"] = [
        item
        for item in agent.get("env", [])
        if item["name"]
        not in {
            "PYTHONPATH",
            "SDO_ACTOR",
            "SDO_PHASE",
            "SDO_BASE_COMMIT",
            "SDO_SESSION_DIR",
            "SDO_STARTUP_TIMING_PATH",
            "SDO_TURN_USAGE_LOG",
        }
    ]
    agent["env"].extend(
        [{"name": "CLAUDE_CONFIG_DIR", "value": "/tmp/.claude"}, {"name": "PYTHONPATH", "value": "/opt/sdo"}]
    )
    agent.pop("envFrom", None)
    credentials = next(volume for volume in spec["volumes"] if volume["name"] == "credentials")
    credentials["secret"] = {"secretName": credentials_secret}
    if pool_host_path:
        spec["volumes"] = [volume for volume in spec["volumes"] if volume["name"] != "opus-pool"]
        spec["volumes"].append({"name": "opus-pool", "hostPath": {"path": pool_host_path, "type": "Directory"}})
        agent["volumeMounts"] = [mount for mount in agent["volumeMounts"] if mount["name"] != "opus-pool"]
        agent["volumeMounts"].append({"name": "opus-pool", "mountPath": "/sdo/opus-pool"})
        names = {
            "SDO_OPUS_POOL_DIR": "/sdo/opus-pool",
            "SDO_OPUS_POOL_MODE": "per_host",
            "SDO_OPUS_POOL_HOST_ID": host_id,
            "SDO_OPUS_MAX_SESSIONS": str(pool_capacity),
        }
        agent["env"] = [
            item
            for item in agent["env"]
            if item["name"] not in names and item["name"] not in {"SDO_OPUS_POOL_NFS_SERVER", "SDO_OPUS_POOL_NFS_PATH"}
        ]
        agent["env"].extend({"name": name, "value": value} for name, value in names.items())
    # The normal init containers are part of the production launch path.
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": JOB_NAME},
        "spec": {"backoffLimit": 0, "activeDeadlineSeconds": 240, "template": {"spec": spec}},
    }


def run(run_dir: Path, image: str, credentials_secret: str) -> None:
    environment = FastloopEnvironment.load(run_dir)
    kube = ["kubectl", "--kubeconfig", str(environment.kubeconfig), "-n", environment.namespace + "-sdo"]
    jobs = json.loads(subprocess.check_output([*kube, "get", "jobs", "-o", "json"], text=True))["items"]
    health = [
        job
        for job in jobs
        if any(
            item.get("name") == "SDO_ACTOR" and item.get("value") == "health_judge"
            for container in job["spec"]["template"]["spec"]["containers"]
            for item in container.get("env", [])
        )
    ]
    if not health:
        raise RuntimeError("no real health-judge Job template exists in the rehearsal lane")
    load_images(environment.cluster, [image], record_path=run_dir / "claude-image-delivery.jsonl")
    if environment.dev_mount is None:
        raise ValueError("live rehearsal probe requires the freeze development mount")
    pool = Path(os.environ["SDO_OPUS_POOL_DIR"])
    capacity = int(os.environ["SDO_OPUS_MAX_SESSIONS"])
    host_id = os.environ["SDO_OPUS_POOL_HOST_ID"]
    alias = environment.dev_mount / ".fastloop/claude-probe-opus-pool"
    prepare_host_pool(pool, [alias], capacity=capacity)
    job = probe_job(
        health[-1],
        image,
        credentials_secret,
        pool_host_path="/opt/sdo-dev/.fastloop/claude-probe-opus-pool",
        pool_capacity=capacity,
        host_id=host_id,
    )
    (run_dir / "claude-worker-job.json").write_text(json.dumps(job, indent=2) + "\n")
    subprocess.run([*kube, "create", "-f", "-"], input=json.dumps(job), text=True, capture_output=True, check=True)
    try:
        deadline = job["spec"]["activeDeadlineSeconds"]
        allowance = wait_for(WaitKind.COMMAND)
        waited = subprocess.run(
            [*kube, "wait", "--for=condition=complete", f"job/{JOB_NAME}", f"--timeout={deadline + allowance:g}s"],
            capture_output=True,
            text=True,
            check=False,
            timeout=deadline + 2 * allowance,
        )
        logs = subprocess.check_output([*kube, "logs", f"job/{JOB_NAME}", "-c", "agent"], text=True)
        (run_dir / "claude-worker.log").write_text(logs)
        print(logs, end="")
        if waited.returncode:
            raise RuntimeError("real Claude health worker failed; inspect claude-worker.log")
        result = json.loads(logs.strip().splitlines()[-1])
        if not result.get("passed") or result.get("access") != "danger-full-access":
            raise RuntimeError("real Claude worker did not report a danger-full-access health-worker proof")
        (run_dir / "claude-worker.json").write_text(json.dumps(result, indent=2) + "\n")
    finally:
        subprocess.run(
            [*kube, "delete", "job", JOB_NAME, "--wait=true", f"--timeout={allowance:g}s"],
            check=True,
            capture_output=True,
            timeout=2 * allowance,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--credentials-secret", required=True)
    args = parser.parse_args()
    run(args.run_dir, args.image, args.credentials_secret)


if __name__ == "__main__":
    main()
