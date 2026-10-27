"""Lane-owned registry delivery, retaining original image names on every node.

Follows https://kind.sigs.k8s.io/docs/user/local-registry/ . No shared registry
or Docker storage is pruned; an existing container must prove lane ownership.
"""

from __future__ import annotations

import hashlib
import json
import platform
import re
import subprocess
import time
import urllib.request
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.adapter import WaitKind, wait_for

if TYPE_CHECKING:
    from pathlib import Path


def _run(argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv, check=True, capture_output=True, text=True, timeout=wait_for(WaitKind.ROLLOUT), **kwargs
    )


def image_ref(image: str) -> str:
    first = image.split("/", 1)[0]
    if "/" not in image:
        return "docker.io/library/" + image
    if "." not in first and ":" not in first and first != "localhost":
        return "docker.io/" + image
    return image


def _wait_registry(host: str) -> None:
    for _ in range(50):
        try:
            with urllib.request.urlopen(f"http://{host}/v2/", timeout=wait_for(WaitKind.PROBE)) as response:
                if response.status == 200:
                    return
        except OSError:
            pass
        time.sleep(0.1)
    raise RuntimeError(f"lane registry {host} did not become ready")


def registry_delivery(cluster: str, images: list[str]) -> dict[str, dict[str, str]]:
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]*", cluster):
        raise ValueError(f"invalid kind lane name: {cluster!r}")
    name = cluster + "-registry"
    inspected = subprocess.run(["docker", "container", "inspect", name], capture_output=True, text=True, check=False)
    if inspected.returncode == 0:
        info = json.loads(inspected.stdout)[0]
        if (info.get("Config", {}).get("Labels") or {}).get("sdo.fastloop.lane") != cluster:
            raise RuntimeError(f"refusing unowned registry container {name}")
        if not info["State"]["Running"]:
            _run(["docker", "start", name])
    else:
        _run(
            [
                "docker",
                "run",
                "-d",
                "--restart=unless-stopped",
                "--label",
                "sdo.fastloop.lane=" + cluster,
                "-p",
                "127.0.0.1::5000",
                "--name",
                name,
                "registry:3",
            ]
        )
    info = json.loads(_run(["docker", "container", "inspect", name]).stdout)[0]
    if "kind" not in info["NetworkSettings"]["Networks"]:
        _run(["docker", "network", "connect", "kind", name])
    # Docker may reassign an ephemeral published port when a network is added.
    info = json.loads(_run(["docker", "container", "inspect", name]).stdout)[0]
    port = info["NetworkSettings"]["Ports"]["5000/tcp"][0]["HostPort"]
    nodes = _run(["kind", "get", "nodes", "--name", cluster]).stdout.splitlines()
    if not nodes:
        raise RuntimeError(f"no kind nodes for {cluster}")
    host = "127.0.0.1:" + port
    _wait_registry(host)
    directory = "/etc/containerd/certs.d/" + host
    for node in nodes:
        _run(["docker", "exec", node, "mkdir", "-p", directory])
        _run(
            ["docker", "exec", "-i", node, "cp", "/dev/stdin", directory + "/hosts.toml"],
            input=f'[host."http://{name}:5000"]\n  capabilities = ["pull", "resolve"]\n',
        )
    evidence: dict[str, dict[str, str]] = {node: {} for node in nodes}
    for image in dict.fromkeys(images):
        # One repository per original name, independent of tag. Stable paths let
        # the registry reuse layers across tags and roles without name collisions.
        repository = image_ref(image).split("@", 1)[0].rsplit(":", 1)[0]
        key = hashlib.sha256(repository.encode()).hexdigest()[:16]
        local = f"{host}/sdo/{key}:current"
        _run(["docker", "tag", image, local])
        architecture = "arm64" if platform.machine().lower() in {"aarch64", "arm64"} else "amd64"
        pushed = _run(["docker", "push", "--platform", "linux/" + architecture, local])
        match = re.search(r"digest: (sha256:[0-9a-f]{64})", pushed.stdout + pushed.stderr)
        if match is None:
            raise RuntimeError(f"registry push returned no manifest digest for {image}")
        digest = match[1]
        for node in nodes:
            # CRI uses hosts.toml and unpacks the image. Tag under the original
            # ref so existing manifests and IfNotPresent semantics keep working.
            _run(["docker", "exec", node, "crictl", "pull", local])
            rows = _run(
                ["docker", "exec", node, "ctr", "-n", "k8s.io", "images", "ls", "-q", "target.digest==" + digest]
            ).stdout.splitlines()
            if local not in rows:
                raise RuntimeError(f"{node} pulled unexpected registry content for {image}")
            _run(["docker", "exec", node, "ctr", "-n", "k8s.io", "images", "tag", "--force", local, image_ref(image)])
            _run(["docker", "exec", node, "crictl", "inspecti", image])
            evidence[node][image] = digest
    return evidence


def remove_registry(cluster: str) -> None:
    """Remove only the registry container whose label proves lane ownership."""
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]*", cluster):
        raise ValueError(f"invalid kind lane name: {cluster!r}")
    name = cluster + "-registry"
    inspected = subprocess.run(["docker", "container", "inspect", name], capture_output=True, text=True, check=False)
    if inspected.returncode != 0:
        if "No such container" in inspected.stderr:
            return
        inspected.check_returncode()
    info = json.loads(inspected.stdout)[0]
    if (info.get("Config", {}).get("Labels") or {}).get("sdo.fastloop.lane") != cluster:
        raise RuntimeError(f"refusing to remove unowned registry container {name}")
    _run(["docker", "container", "rm", "--force", name])


def warn_fallback(reason: str, images: list[str], seconds: float) -> None:
    import sys

    print(
        f"WARNING: image_delivery: kind-load-fallback; registry error: {reason}; "
        f"images: {', '.join(images)}; fallback took {seconds:.2f}s",
        file=sys.stderr,
    )


def delivery_state(record_path: Path) -> dict[str, Any]:
    """A partial successful delivery cannot hide another image's fallback."""
    state: dict[str, Any] = {}
    pending: set[str] = set()
    for line in record_path.read_text().splitlines():
        record = json.loads(line)
        if record["path"] == "kind-load-fallback":
            pending.update(record["images"])
            state = record
        elif pending:
            pending.difference_update(record["images"])
            if not pending:
                state = record
        else:
            state = record
    if pending:
        state = {**state, "images": sorted(pending)}
    return state


def save_delivery(record_path: Path, record: dict[str, Any]) -> None:
    record_path.parent.mkdir(parents=True, exist_ok=True)
    with record_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record) + "\n")
    record = delivery_state(record_path)
    lane_path = record_path.parent / "fastloop.json"
    if lane_path.is_file():
        lane = json.loads(lane_path.read_text())
        lane.update(
            image_delivery=record["path"],
            image_delivery_reason=record["reason"],
            image_delivery_seconds=record["seconds"],
            image_delivery_images=record["images"],
        )
        temporary = lane_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(lane, indent=2) + "\n")
        temporary.replace(lane_path)


def main() -> int:
    import argparse
    from pathlib import Path

    from benchmarks.sregym.fastloop.toy_images import load_images

    parser = argparse.ArgumentParser(description="Deliver through a lane registry, with a recorded automatic fallback")
    parser.add_argument("--cluster", required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--min-free-gib", type=int, default=100)
    parser.add_argument("images", nargs="+")
    args = parser.parse_args()
    from benchmarks.sregym.adapter import LaneResourcePolicy

    load_images(
        args.cluster,
        args.images,
        record_path=args.run_dir / "image_delivery.jsonl",
        resource_policy=LaneResourcePolicy(host_disk_floor_bytes=args.min_free_gib * 1024**3),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
