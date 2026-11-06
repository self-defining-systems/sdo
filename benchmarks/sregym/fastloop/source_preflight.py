"""Cheap source prerequisites, checked before importing external app factories."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

from benchmarks.sregym.adapter import WaitKind, wait_for
from benchmarks.sregym.fastloop.image_provenance import (
    DEFAULT_WORKER_TARGET,
    IMAGE_TARGET_LABEL,
    IMAGE_TARGETS,
    MANIFEST_TREE_LABEL,
    REVISION_LABEL,
    SCRIPTED_WORKER_TARGET,
    SOURCE_TREE_LABEL,
    WORKER_TARGET_LABEL,
    ImageSource,
    changed_build_paths,
    image_source,
    uncommitted_build_files,
)
from benchmarks.sregym.repository_git import host_git_command

FIRST_PARTY_APPS = ("hotel-reservation", "astronomy-shop", "social-network")


def assert_harness_sources(directory: Path) -> None:
    def git(path: Path, *args: str) -> str:
        result = subprocess.run(
            host_git_command(path, *args), capture_output=True, text=True, timeout=wait_for(WaitKind.PROBE)
        )
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
    from collections.abc import Mapping

    from benchmarks.sregym.fastloop.environment import Images


def _image_labels(image: str) -> dict[str, str]:
    inspected = subprocess.run(
        ["docker", "image", "inspect", image],
        check=True,
        capture_output=True,
        text=True,
        timeout=wait_for(WaitKind.COMMAND),
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
        changed = changed_build_paths(built, current)
        raise ValueError(
            f"eval: image {image} source differs from {revision} in {', '.join(changed[:5])}; "
            "rebuild with scripts/build_sdo_images.sh"
        )


def assert_runtime_images(images: Images, worktree: Path, *, dev_mount: bool = True) -> None:
    """Reject saved lane images from other code before resetting or injecting."""
    revision = subprocess.run(
        host_git_command(worktree, "rev-parse", "HEAD"),
        check=True,
        capture_output=True,
        text=True,
        timeout=wait_for(WaitKind.PROBE),
    ).stdout.strip()
    sources = {image: image_source(worktree, "HEAD", target) for image, target in ((images.controller, "controller"),)}
    if uncommitted_build_files(worktree, list(sources.values())):
        raise ValueError(
            "eval: image source has uncommitted changes; commit and rebuild with scripts/build_sdo_images.sh"
        )
    for image, source in sources.items():
        _assert_image_source(image, source, worktree)
    # The controller remains source-bound above. A retained responder
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
                images.controller,
                "go",
                "version",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=wait_for(WaitKind.COMMAND),
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        raise ValueError(
            f"eval: controller {images.controller} Go toolchain cannot run; rebuild with scripts/build_sdo_images.sh"
        ) from exc


ALLOW_STALE_FLAG = "--allow-stale-images"


def rebuild_command(target: str, image: str, worker_target: str | None = None) -> str:
    """The build line that replaces ``image`` with one built from the current tree."""
    name = image.rpartition("/")[2]
    tag = name.partition(":")[2] or "latest"
    scripted = "SDO_SCRIPTED_WORKER=1 " if worker_target == SCRIPTED_WORKER_TARGET else ""
    return f"{scripted}SDO_IMAGES={target} SDO_IMAGE_TAG={tag} bash scripts/build_sdo_images.sh"


def assert_images_fresh(images: Mapping[str, str], worktree: Path, *, dev_mount: bool = True) -> None:
    """Fail within seconds when a lane image was built from other inputs than the tree it will run.

    Only labels are read, no container runs. Each SDO-built image names the Bake target and the source
    identity it was built from. With a development mount the Go and Python code come from the mount, so
    only the inputs the mount cannot replace must match: Dockerfiles, Bake blocks and Go module manifests
    (they decide the baked tools and the offline module cache). Without a mount every build input must match.
    ``images`` maps a role (controller, responder) to its image reference.
    """
    for role, image in images.items():
        labels = _image_labels(image)
        target = labels.get(IMAGE_TARGET_LABEL)
        if target not in IMAGE_TARGETS:
            rebuild = rebuild_command("controller" if role == "controller" else "benchmark", image)
            raise ValueError(
                f"fastloop up: {role} image {image} records no source inputs (built before image provenance); "
                f"rebuild: {rebuild} (or pass {ALLOW_STALE_FLAG} for a deliberate frozen image)"
            )
        worker_target = labels.get(WORKER_TARGET_LABEL, DEFAULT_WORKER_TARGET) if target == "benchmark" else None
        current = image_source(worktree, "HEAD", target, worker_target)
        _assert_inputs_match(image, role, labels, current, worktree, dev_mount=dev_mount, worker_target=worker_target)


def _assert_inputs_match(
    image: str,
    role: str,
    labels: Mapping[str, str],
    current: ImageSource,
    worktree: Path,
    *,
    dev_mount: bool,
    worker_target: str | None,
) -> None:
    def identity(source: ImageSource) -> str:
        return source.manifest_identity if dev_mount else source.identity

    rebuild = (
        f"rebuild: {rebuild_command(current.target, image, worker_target)} "
        f"(or pass {ALLOW_STALE_FLAG} for a deliberate frozen image)"
    )
    dirty = uncommitted_build_files(worktree, [current])
    if dev_mount:
        dirty = [path for path in dirty if current.is_manifest_path(path)]
    if dirty:
        raise ValueError(
            f"fastloop up: {role} image inputs have uncommitted changes ({', '.join(dirty[:5])}); "
            f"commit, then {rebuild}"
        )
    if labels.get(MANIFEST_TREE_LABEL if dev_mount else SOURCE_TREE_LABEL) == identity(current):
        return
    revision = labels.get(REVISION_LABEL)
    try:
        built = image_source(worktree, revision, current.target, worker_target) if revision else None
    except ValueError:
        built = None
    if built is None:
        raise ValueError(
            f"fastloop up: {role} image {image} was built from a source this tree cannot compare; {rebuild}"
        )
    if identity(built) == identity(current):
        return
    changed = changed_build_paths(built, current)
    if dev_mount:
        changed = [path for path in changed if current.is_manifest_path(path)] or changed
    raise ValueError(
        f"fastloop up: {role} image {image} was built from different inputs than {worktree} "
        f"(differs in {', '.join(changed[:5])}); {rebuild}"
    )
