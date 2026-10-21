"""Prepare source mounts and a host-built operator for a kind development lane."""

from __future__ import annotations

import platform
import subprocess
from typing import TYPE_CHECKING

import yaml

if TYPE_CHECKING:
    from pathlib import Path


def write_kind_config(source: Path, destination: Path, worktree: Path) -> Path:
    config = yaml.safe_load(source.read_text(encoding="utf-8"))
    for node in config["nodes"]:
        node.setdefault("extraMounts", []).append({"hostPath": str(worktree), "containerPath": "/opt/sdo-dev"})
    destination.write_text(yaml.safe_dump(config), encoding="utf-8")
    return destination


def prepare_dev_mount(worktree: Path, sregym_dir: Path | None, run_dir: Path) -> Path:
    if not (worktree / "sdo" / "operator" / "go.mod").is_file():
        raise ValueError(f"--dev-mount is not an SDO worktree: {worktree}")
    output = worktree / ".fastloop" / "sdo-operator"
    output.parent.mkdir(exist_ok=True)
    subprocess.run(
        ["go", "build", "-buildvcs=false", "-o", str(output), "./cmd/sdo-operator"],
        cwd=worktree / "sdo" / "operator",
        check=True,
    )
    if sregym_dir is None:
        destination = run_dir / "kind-dev.yaml"
        destination.write_text(
            yaml.safe_dump(
                {
                    "kind": "Cluster",
                    "apiVersion": "kind.x-k8s.io/v1alpha4",
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
