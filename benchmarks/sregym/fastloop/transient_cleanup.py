"""Retire benchmark-owned disposable Pods within one actual-deletion budget."""

from __future__ import annotations

import json
import math
from typing import TYPE_CHECKING

from benchmarks.sregym.adapter import kubectl

if TYPE_CHECKING:
    from pathlib import Path

    from benchmarks.sregym.adapter import Deadline


def wait_deleted(config: Path, namespace: str, resources: list[str], deadline: Deadline) -> None:
    if resources:
        kubectl(
            ["wait", "--for=delete", *resources, f"--timeout={math.ceil(deadline.remaining())}s"],
            namespace=namespace,
            kubeconfig=config,
        )
        deadline.remaining()


def retire_pods(
    config: Path,
    namespace: str,
    selector: str,
    *,
    owner_kind: str,
    deadline: Deadline,
    owner_name: str | None = None,
    owner_uid: str | None = None,
    wait: bool = True,
) -> None:
    """Validate the entire selected set before requesting any retirement."""
    key, value = selector.split("=", 1)
    pods = json.loads(
        kubectl(["get", "pods", "-l", selector, "-o", "json"], namespace=namespace, kubeconfig=config).stdout
    )["items"]
    for pod in pods:
        metadata = pod["metadata"]
        if (
            metadata.get("namespace") != namespace
            or metadata.get("labels", {}).get(key) != value
            or not metadata.get("uid")
            or not any(
                owner.get("kind") == owner_kind
                and (owner_name is None or owner.get("name") == owner_name)
                and (owner_uid is None or owner.get("uid") == owner_uid)
                for owner in metadata.get("ownerReferences", [])
            )
        ):
            raise ValueError("eval: disposable Pod ownership mismatch; refusing retirement")
    for pod in pods:
        deadline.remaining()
        kubectl(
            ["delete", f"pod/{pod['metadata']['name']}", "--ignore-not-found=true", "--grace-period=0", "--wait=false"],
            namespace=namespace,
            kubeconfig=config,
        )
    # Wait on the whole selector, so a replacement writer cannot evade quiescence.
    if wait:
        wait_deleted(config, namespace, ["pods", f"--selector={selector}"], deadline)


def retire_harness_workload(config: Path, deadline: Deadline) -> None:
    """Only the isolated wrk2 Job can use disposable workload retirement."""
    result = kubectl(
        ["get", "job/wrk2-job", "--ignore-not-found=true", "-o", "json"], namespace="sregym", kubeconfig=config
    )
    if not result.stdout.strip():
        return
    metadata = json.loads(result.stdout)["metadata"]
    if (
        metadata.get("namespace") != "sregym"
        or metadata.get("name") != "wrk2-job"
        or metadata.get("labels", {}).get("job") != "workload"
        or not metadata.get("uid")
    ):
        raise ValueError("eval: harness Job ownership mismatch; refusing retirement")
    kubectl(
        ["patch", "job/wrk2-job", "--type=merge", "-p", '{"spec":{"suspend":true}}'],
        namespace="sregym",
        kubeconfig=config,
    )
    retire_pods(
        config,
        "sregym",
        "job-name=wrk2-job",
        owner_kind="Job",
        owner_name="wrk2-job",
        owner_uid=metadata["uid"],
        deadline=deadline,
    )
    kubectl(
        ["delete", "job/wrk2-job", "--cascade=foreground", "--wait=false", "--ignore-not-found=true"],
        namespace="sregym",
        kubeconfig=config,
    )
    wait_deleted(config, "sregym", ["job/wrk2-job"], deadline)
