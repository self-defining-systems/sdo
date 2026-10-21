"""Verify prepared kind image content before spending time transferring archives."""

from __future__ import annotations

import platform
import subprocess
import tempfile
from pathlib import Path


def load_images(cluster: str, images: list[str]) -> None:
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
            )
            subprocess.run(["kind", "load", "image-archive", "--name", cluster, str(archive)], check=True)
