"""Prepare source mounts and a host-built operator for a kind development lane."""

from __future__ import annotations

import os
import platform
import subprocess
import tempfile
from pathlib import Path

import yaml


def write_kind_config(source: Path, destination: Path, worktree: Path) -> Path:
    config = yaml.safe_load(source.read_text(encoding="utf-8"))
    for node in config["nodes"]:
        node.setdefault("extraMounts", []).append({"hostPath": str(worktree), "containerPath": "/opt/sdo-dev"})
    config.setdefault("containerdConfigPatches", []).extend(
        [
            '[plugins."io.containerd.grpc.v1.cri".registry]\n  config_path = "/etc/containerd/certs.d"',
            '[plugins."io.containerd.cri.v1.images".registry]\n  config_path = "/etc/containerd/certs.d"',
        ]
    )
    destination.write_text(yaml.safe_dump(config), encoding="utf-8")
    return destination


def prepare_dev_mount(worktree: Path, sregym_dir: Path | None, run_dir: Path) -> Path:
    if not (worktree / "sdo" / "operator" / "go.mod").is_file():
        raise ValueError(f"--dev-mount is not an SDO worktree: {worktree}")
    output = worktree / ".fastloop" / "sdo-operator"
    output.parent.mkdir(exist_ok=True)
    # Each lane owns its build output; publication stays atomic for running binaries.
    with tempfile.NamedTemporaryFile(dir=output.parent, prefix=f"{output.name}.", suffix=".new", delete=False) as temp:
        temporary_output = Path(temp.name)
    try:
        subprocess.run(
            ["go", "build", "-buildvcs=false", "-o", str(temporary_output), "./cmd/sdo-operator"],
            cwd=worktree / "sdo" / "operator",
            check=True,
        )
        os.replace(temporary_output, output)
    finally:
        temporary_output.unlink(missing_ok=True)
    if sregym_dir is None:
        destination = run_dir / "kind-dev.yaml"
        destination.write_text(
            yaml.safe_dump(
                {
                    "kind": "Cluster",
                    "apiVersion": "kind.x-k8s.io/v1alpha4",
                    "containerdConfigPatches": [
                        '[plugins."io.containerd.grpc.v1.cri".registry]\n  config_path = "/etc/containerd/certs.d"',
                        '[plugins."io.containerd.cri.v1.images".registry]\n  config_path = "/etc/containerd/certs.d"',
                    ],
                    "networking": {"disableDefaultCNI": True, "podSubnet": "192.168.0.0/16"},
                    "nodes": [
                        {
                            "role": "control-plane",
                            "extraMounts": [{"hostPath": str(worktree), "containerPath": "/opt/sdo-dev"}],
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        return destination
    arch = platform.machine().lower()
    base = (
        sregym_dir / "kind" / ("kind-config-arm.yaml" if "arm" in arch or "aarch" in arch else "kind-config-x86.yaml")
    )
    return write_kind_config(base, run_dir / "kind-dev.yaml", worktree)


def _copy_dev_snapshot(worktree: Path, node: str, target: str) -> None:
    """Copy the mounted code allowlist to node storage, never into another checkout."""
    command = [
        "tar",
        "-C",
        str(worktree),
        "--exclude=__pycache__",
        "--exclude=.git",
        "-cf",
        "-",
        "sdo",
        "libs",
        "examples",
        "benchmarks",
        "apps/toy/seed",
        "apps/toy/kubernetes",
        ".fastloop/sdo-operator",
    ]
    producer = subprocess.Popen(command, stdout=subprocess.PIPE)
    try:
        subprocess.run(
            ["docker", "exec", "-i", node, "tar", "-xf", "-", "-C", target],
            stdin=producer.stdout,
            check=True,
            timeout=60,
        )
    finally:
        if producer.stdout:
            producer.stdout.close()
        if producer.wait(timeout=10):
            raise RuntimeError("failed to snapshot the dev worktree")


def retarget_dev_mount(cluster: str, worktree: Path) -> None:
    """Rebind an idle warm lane without changing its old host checkout or images.

    Kind cannot change Docker bind specifications. A node-local snapshot covers
    the original mount. Reset refreshes it; node recreation still uses normal up.
    The caller stops controller writers before this explicit lane adoption.
    """
    import hashlib

    target = "/opt/sdo-fastloop-" + hashlib.sha256(str(worktree.resolve()).encode()).hexdigest()[:16]
    nodes = subprocess.run(
        ["kind", "get", "nodes", "--name", cluster], capture_output=True, text=True, check=True, timeout=10
    ).stdout.splitlines()
    if not nodes:
        raise ValueError(f"dev retarget found no nodes for {cluster}")
    for node in nodes:
        subprocess.run(["docker", "exec", node, "mkdir", "-p", target], check=True, timeout=10)
        _copy_dev_snapshot(worktree, node, target)
        subprocess.run(["docker", "exec", node, "mount", "--bind", target, "/opt/sdo-dev"], check=True, timeout=10)
