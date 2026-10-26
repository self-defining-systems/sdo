"""Cheap source prerequisites, checked before importing external app factories."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

from benchmarks.sregym.fastloop.image_provenance import (
    REVISION_LABEL,
    SOURCE_TREE_LABEL,
    ImageSource,
    changed_build_paths,
    image_source,
)

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


def _image_labels(image: str) -> dict[str, str]:
    inspected = subprocess.run(
        ["docker", "image", "inspect", image], check=True, capture_output=True, text=True, timeout=20
    )
    return json.loads(inspected.stdout)[0].get("Config", {}).get("Labels") or {}


def _assert_image_source(image: str, current: ImageSource, worktree: Path) -> None:
    """Accept an image whose built paths have the current tree, not only the current HEAD."""
    labels = _image_labels(image)
    if labels.get(SOURCE_TREE_LABEL) == current.identity:
        return
    revision = labels.get(REVISION_LABEL)
    if not revision:
        raise ValueError(f"eval: image {image} has no source label; rebuild with scripts/build_sdo_images.sh")
    try:
        built = image_source(worktree, revision, current.target)
    except ValueError as exc:
        raise ValueError(
            f"eval: image {image} source {revision} is not in {worktree}; fetch that commit or rebuild "
            "with scripts/build_sdo_images.sh"
        ) from exc
    if built != current:
        changed = changed_build_paths(worktree, built, current, revision, "HEAD")
        raise ValueError(
            f"eval: image {image} source differs from {revision} in {', '.join(changed[:5])}; "
            "rebuild with scripts/build_sdo_images.sh"
        )


def assert_runtime_images(images: Images, worktree: Path, *, dev_mount: bool = True) -> None:
    """Reject saved lane images from other code before resetting or injecting."""
    revision = subprocess.run(
        ["git", "-C", str(worktree), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    ).stdout.strip()
    sources = {
        image: image_source(worktree, "HEAD", target)
        for image, target in (
            (images.controller, "controller"),
            (images.validator, "validator"),
        )
    }
    built_paths = sorted({path for source in sources.values() for path in source.paths})
    dirty = subprocess.run(
        ["git", "-C", str(worktree), "status", "--porcelain", "--untracked-files=normal", "--", *built_paths],
        capture_output=True,
        text=True,
        timeout=10,
    )
    if dirty.returncode or dirty.stdout.strip():
        raise ValueError(
            "eval: image source has uncommitted changes; commit and rebuild with scripts/build_sdo_images.sh"
        )
    for image, source in sources.items():
        _assert_image_source(image, source, worktree)
    # Controller and validator remain source-bound above. A retained responder
    # may be from an older compatible build, but its complete runtime import
    # closure must load before a worker (and therefore injection) can start.
    if _image_labels(images.responder).get(REVISION_LABEL) != revision:
        from benchmarks.sregym.adapter import BENCHMARK_IMAGE_ENTRY_POINTS, assert_image_entrypoints

        assert_image_entrypoints(
            images.responder,
            BENCHMARK_IMAGE_ENTRY_POINTS,
            source_root=worktree if dev_mount else None,
            run=subprocess.run,
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
