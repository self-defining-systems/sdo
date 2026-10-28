"""Registry-first lane delivery, with a loud and recorded archive fallback."""

from __future__ import annotations

import platform
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

from benchmarks.sregym.adapter import (
    ImageRetentionError,
    LaneResourcePolicy,
    WaitKind,
    assert_host_disk_headroom,
    retain_lane_images,
    wait_for,
)
from benchmarks.sregym.fastloop.host_limits import HostLimits
from benchmarks.sregym.fastloop.image_delivery import registry_delivery, save_delivery, warn_fallback


def load_images(
    cluster: str,
    images: list[str],
    *,
    record_path: Path | None = None,
    resource_policy: LaneResourcePolicy | None = None,
    host_limits: HostLimits | None = None,
) -> dict[str, dict[str, str]]:
    with (host_limits or HostLimits.from_env()).admit("image-delivery"):
        try:
            disk = assert_host_disk_headroom(policy=resource_policy)
        except ValueError as exc:
            raise ValueError(f"eval: image-delivery disk floor: {exc}") from exc
        return _deliver_images(cluster, images, record_path=record_path, disk=disk)


def _deliver_images(
    cluster: str,
    images: list[str],
    *,
    record_path: Path | None,
    disk: dict[str, Any],
) -> dict[str, dict[str, str]]:
    started = time.monotonic()
    reason = ""
    path = "registry"
    evidence: dict[str, dict[str, str]] = {}
    registry: dict[str, Any] = {}
    try:
        evidence = registry_delivery(cluster, images, metrics=registry)
    except (subprocess.SubprocessError, RuntimeError, ValueError, KeyError, OSError) as error:
        reason = str(error)
        if isinstance(error, subprocess.CalledProcessError):
            reason += ": " + str(error.stderr or error.stdout or "").strip()[:1000]
        import sys

        print(
            f"WARNING: registry delivery failed: {reason}; images: {', '.join(images)}; starting kind-load-fallback",
            file=sys.stderr,
        )
        path = "kind-load-fallback"
        fallback_started = time.monotonic()
        try:
            _load_archive_images(cluster, images)
        except BaseException:
            seconds = time.monotonic() - fallback_started
            warn_fallback(reason, images, seconds)
            if record_path is not None:
                save_delivery(
                    record_path,
                    {
                        "path": path,
                        "reason": reason,
                        "seconds": seconds,
                        "images": images,
                        "cluster": cluster,
                        "success": False,
                        "disk": disk,
                        "registry": registry,
                    },
                )
            raise
        warn_fallback(reason, images, time.monotonic() - fallback_started)
    record = {
        "path": path,
        "reason": reason,
        "seconds": time.monotonic() - started,
        "images": images,
        "cluster": cluster,
        "success": True,
        "disk": disk,
        "registry": registry,
    }
    try:
        record["retention"] = retain_lane_images(cluster, images)
    except (subprocess.SubprocessError, RuntimeError, ValueError, KeyError, OSError, TypeError) as exc:
        record["retention"] = {"error": str(exc), **(exc.evidence if isinstance(exc, ImageRetentionError) else {})}
        if record_path is not None:
            save_delivery(record_path, record)
        raise RuntimeError(f"eval: image retention failed after delivery to {cluster}: {exc}") from exc
    if record_path is not None:
        save_delivery(record_path, record)
    return evidence


def _load_archive_images(cluster: str, images: list[str]) -> None:
    """Use content-complete manifest identities; Docker and CRI IDs can name different objects."""
    machine = platform.machine().lower()
    architecture = "arm64" if machine in {"aarch64", "arm64"} else "amd64"
    image_platform = "linux/" + architecture
    nodes = subprocess.run(
        ["kind", "get", "nodes", "--name", cluster], check=True, capture_output=True, text=True
    ).stdout.splitlines()
    if not nodes:
        raise ValueError(f"no kind nodes found for {cluster}")
    missing: set[str] = set()
    for node in nodes:
        rows = subprocess.run(
            ["docker", "exec", node, "ctr", "--namespace", "k8s.io", "images", "check"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.splitlines()
        complete = {
            fields[0]: (fields[2], fields[-1] == "true")
            for row in rows
            if len(fields := row.split()) >= 4 and fields[3] == "complete"
        }
        for image in dict.fromkeys(images):
            expected = subprocess.run(
                ["docker", "image", "inspect", "--platform", image_platform, "--format", "{{.Id}}", image],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
            first = image.split("/", 1)[0]
            if "/" not in image:
                ref = "docker.io/library/" + image
            elif "." not in first and ":" not in first and first != "localhost":
                ref = "docker.io/" + image
            else:
                ref = image
            cached = complete.get(ref)
            if cached is None or cached[0] != expected:
                missing.add(image)
                continue
            subprocess.run(
                [
                    "docker",
                    "exec",
                    node,
                    "ctr",
                    "--namespace",
                    "k8s.io",
                    "images",
                    "label",
                    ref,
                    "io.cri-containerd.image=managed",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            if not cached[1]:
                target = "/tmp/sdo-toy-image-unpack"
                subprocess.run(["docker", "exec", node, "mkdir", "-p", target], check=True)
                subprocess.run(
                    ["docker", "exec", node, "ctr", "--namespace", "k8s.io", "images", "mount", ref, target],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                try:
                    subprocess.run(
                        ["docker", "exec", node, "ctr", "--namespace", "k8s.io", "images", "unmount", target],
                        check=True,
                    )
                finally:
                    subprocess.run(["docker", "exec", node, "rmdir", target], check=False)
    if missing:
        with tempfile.TemporaryDirectory(prefix="sdo-toy-images-") as temporary:
            archive = Path(temporary) / "images.tar"
            subprocess.run(
                ["docker", "image", "save", "--platform", image_platform, "-o", str(archive), *sorted(missing)],
                check=True,
                timeout=wait_for(WaitKind.ROLLOUT),
            )
            subprocess.run(
                ["kind", "load", "image-archive", "--name", cluster, str(archive)],
                check=True,
                timeout=wait_for(WaitKind.ROLLOUT),
            )
