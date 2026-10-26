"""Reuse the toy fixture in a private namespace/PVC on an existing warm lane."""

from __future__ import annotations

import hashlib
import json
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

from benchmarks.sregym.adapter import kubectl
from benchmarks.sregym.fastloop import toy
from benchmarks.sregym.fastloop.environment import FastloopEnvironment


def namespace_for(run_dir: Path) -> str:
    return "sdo-proof-" + hashlib.sha256(str(run_dir.resolve()).encode()).hexdigest()[:10]


def prepare(parent: FastloopEnvironment, worktree: Path) -> FastloopEnvironment:
    """Never reset the application's namespace, repository PVC, or pending ledger."""
    directory = parent.run_dir / "learning-loop-proof/environment"
    namespace = namespace_for(parent.run_dir)
    environment = None
    if (directory / "fastloop.json").is_file():
        environment = FastloopEnvironment.load(directory)
        if (
            environment.cluster != parent.cluster
            or environment.namespace != namespace
            or environment.images != parent.images
            or environment.dev_mount != worktree
        ):
            raise ValueError("learning-loop: isolated fixture identity changed; select a new rehearsal directory")
    collision = kubectl(
        ["get", "namespace", namespace, "--ignore-not-found", "-o", "name"],
        namespace=None,
        kubeconfig=parent.kubeconfig,
    )
    if environment is None and collision.stdout.strip():
        raise ValueError(f"learning-loop: refusing to adopt unrecorded namespace {namespace}")
    from benchmarks.sregym.adapter import control_namespace_for

    pods = kubectl(
        ["get", "pods", "-o", "json"],
        namespace=control_namespace_for(parent.namespace),
        kubeconfig=parent.kubeconfig,
        check=False,
    )
    if pods.returncode == 0 and any(
        item.get("status", {}).get("phase") not in {"Succeeded", "Failed"}
        for item in json.loads(pods.stdout).get("items", [])
    ):
        raise ValueError("learning-loop: stop the application runtime before preparing the isolated dev mount")
    directory.mkdir(parents=True, exist_ok=True)
    from benchmarks.sregym.fastloop.dev import prepare_dev_mount, retarget_dev_mount
    from benchmarks.sregym.fastloop.toy_images import load_images

    # This is an explicit idle-lane operation. Reuse the production dev mount;
    # no role image or pending operational repository is rebuilt or rewritten.
    os.environ["KUBECONFIG"] = str(parent.kubeconfig)
    prepare_dev_mount(worktree, None, directory)
    retarget_dev_mount(parent.cluster, worktree)
    if environment is not None:
        return environment
    image = "sdo-toy-app:proof-" + hashlib.sha256((parent.cluster + str(parent.run_dir)).encode()).hexdigest()[:12]
    toy.build_image(directory, image)
    load_images(parent.cluster, [image], record_path=directory / "image_delivery.jsonl")
    environment = toy.prepare_application(
        directory, parent.cluster, parent.kubeconfig, worktree, parent.images, image, namespace=namespace
    )
    (directory / "isolation.json").write_text(
        json.dumps(
            {
                "lane": parent.cluster,
                "application_namespace": parent.namespace,
                "proof_namespace": namespace,
                "proof_control_namespace": namespace + "-sdo",
                "application_workspace": str(parent.workspace),
                "proof_workspace": str(environment.workspace),
            },
            indent=2,
        )
        + "\n"
    )
    return environment
