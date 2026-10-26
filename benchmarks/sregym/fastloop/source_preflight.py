"""Cheap source prerequisites, checked before importing external app factories."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

FIRST_PARTY_APPS = ("hotel-reservation", "astronomy-shop", "social-network")


def assert_harness_sources(directory: Path) -> None:
    def git(path: Path, *args: str) -> str:
        result = subprocess.run(["git", "-C", str(path), *args], capture_output=True, text=True, timeout=5)
        if result.returncode:
            raise ValueError(f"eval: initialize pinned SREGym source prerequisite: {path}")
        return result.stdout.strip()

    if Path(git(directory, "rev-parse", "--show-toplevel")).resolve() != directory.resolve():
        raise ValueError(f"eval: initialize SREGym submodule prerequisite: {directory}")
    row = git(directory, "ls-tree", "HEAD", "SREGym-applications").split()
    applications = directory / "SREGym-applications"
    if len(row) != 4 or row[0] != "160000" or git(applications, "rev-parse", "HEAD") != row[2]:
        raise ValueError("eval: pinned applications revision is missing or differs from SREGym gitlink")
    for app in FIRST_PARTY_APPS:
        metadata = directory / "sregym/service/metadata" / f"{app}.json"
        if not metadata.is_file():
            raise ValueError(f"eval: application source prerequisite missing: {metadata}")
        value = json.loads(metadata.read_text())
        if value.get("K8S Deploy Path"):
            source = applications / value["K8S Deploy Path"]
        else:
            source = applications / value["Helm Config"]["chart_path"] / "Chart.yaml"
        if not source.exists():
            raise ValueError(f"eval: application source prerequisite missing for {app}: {source}")


if TYPE_CHECKING:
    from benchmarks.sregym.fastloop.environment import Images


def assert_runtime_images(images: Images, worktree: Path) -> None:
    """Reject saved lane images from other code before resetting or injecting."""
    revision = subprocess.run(
        ["git", "-C", str(worktree), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    ).stdout.strip()
    dirty = subprocess.run(
        ["git", "-C", str(worktree), "diff", "--quiet", "HEAD", "--", "sdo/operator", "scripts/sdo-images.hcl"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    if dirty.returncode:
        raise ValueError(
            "eval: image source has uncommitted changes; commit and rebuild with scripts/build_sdo_images.sh"
        )
    for image in (images.controller, images.validator):
        inspected = subprocess.run(
            ["docker", "image", "inspect", image],
            check=True,
            capture_output=True,
            text=True,
            timeout=20,
        )
        labels = json.loads(inspected.stdout)[0].get("Config", {}).get("Labels") or {}
        if labels.get("org.opencontainers.image.revision") != revision:
            raise ValueError(
                f"eval: image {image} source differs from {revision}; rebuild with scripts/build_sdo_images.sh"
            )
    try:
        subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "--network",
                "none",
                "--read-only",
                "--tmpfs",
                "/tmp",
                "--user",
                "65532:65532",
                images.validator,
                "go",
                "version",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=20,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        raise ValueError(
            f"eval: validator {images.validator} Go toolchain cannot run; rebuild with scripts/build_sdo_images.sh"
        ) from exc
