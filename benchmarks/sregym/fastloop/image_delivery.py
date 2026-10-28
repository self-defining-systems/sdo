"""Host-owned registry delivery, retaining original image names on every node.

One publication per frozen image, shared by every lane on the kind network.
No shared registry or Docker storage is pruned by lane teardown.
"""

from __future__ import annotations

import hashlib
import json
import platform
import re
import subprocess
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
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
    raise RuntimeError(f"host registry {host} did not become ready")


HOST_REGISTRY_NAME = "sdo-fastloop-registry"
_REGISTRY_LABEL = "sdo.fastloop.registry"
_REGISTRY_OWNER = "host-v1"
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
_MANIFEST_TYPES = "application/vnd.oci.image.manifest.v1+json,application/vnd.docker.distribution.manifest.v2+json"


@dataclass(frozen=True)
class RegistryManifest:
    digest: str
    config: str


def _manifest(host: str, repository: str, selector: str) -> RegistryManifest | None:
    """Only absence is a cache miss; corrupt or unavailable content fails closed."""
    request = urllib.request.Request(
        f"http://{host}/v2/{repository}/manifests/{selector}", headers={"Accept": _MANIFEST_TYPES}
    )
    try:
        with urllib.request.urlopen(request, timeout=wait_for(WaitKind.PROBE)) as response:
            raw = response.read(4 * 1024 * 1024 + 1)
            digest = response.headers.get("Docker-Content-Digest", "")
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise
    if len(raw) > 4 * 1024 * 1024 or digest != "sha256:" + hashlib.sha256(raw).hexdigest():
        raise ValueError(f"registry manifest digest mismatch: {repository}@{selector}")
    if selector.startswith("sha256:") and digest != selector:
        raise ValueError(f"registry returned unexpected manifest digest: {repository}@{selector}")
    document = json.loads(raw)
    descriptor = document.get("config") if isinstance(document, dict) else None
    config = descriptor.get("digest") if isinstance(descriptor, dict) else None
    if not isinstance(config, str) or not _DIGEST.fullmatch(config):
        raise ValueError(f"registry manifest has no image config identity: {repository}@{selector}")
    return RegistryManifest(digest, config)


def _host_registry() -> str:
    name = HOST_REGISTRY_NAME
    inspected = subprocess.run(["docker", "container", "inspect", name], capture_output=True, text=True, check=False)
    if inspected.returncode == 0:
        info = json.loads(inspected.stdout)[0]
        if (info.get("Config", {}).get("Labels") or {}).get(_REGISTRY_LABEL) != _REGISTRY_OWNER:
            raise RuntimeError(f"refusing unowned registry container {name}")
        if not info["State"]["Running"]:
            _run(["docker", "start", name])
    else:
        if "No such container" not in inspected.stderr:
            inspected.check_returncode()
        _run(
            [
                "docker",
                "run",
                "-d",
                "--restart=unless-stopped",
                "--label",
                f"{_REGISTRY_LABEL}={_REGISTRY_OWNER}",
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
    host = "127.0.0.1:" + port
    _wait_registry(host)
    return host


def registry_delivery(
    cluster: str, images: list[str], *, metrics: dict[str, Any] | None = None
) -> dict[str, dict[str, str]]:
    """Called under load_images' host-wide image-delivery admission lock.

    Content-derived tags locate cached manifests; nodes always pull their exact
    digest. Both HTTP manifest bytes and CRI config IDs must match the frozen
    local image (its config or, on a containerd image store, manifest digest),
    including on a cache hit. Lane teardown retains this registry.
    """
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]*", cluster):
        raise ValueError(f"invalid kind lane name: {cluster!r}")
    host = _host_registry()
    nodes = _run(["kind", "get", "nodes", "--name", cluster]).stdout.splitlines()
    if not nodes:
        raise RuntimeError(f"no kind nodes for {cluster}")
    directory = "/etc/containerd/certs.d/" + host
    for node in nodes:
        _run(["docker", "exec", node, "mkdir", "-p", directory])
        _run(
            ["docker", "exec", "-i", node, "cp", "/dev/stdin", directory + "/hosts.toml"],
            input=f'[host."http://{HOST_REGISTRY_NAME}:5000"]\n  capabilities = ["pull", "resolve"]\n',
        )
    architecture = "arm64" if platform.machine().lower() in {"aarch64", "arm64"} else "amd64"
    # Freeze all identities before publication, and tag by ID rather than a
    # mutable source tag so a concurrent build cannot substitute other bytes.
    frozen = {
        image: _run(
            ["docker", "image", "inspect", "--platform", "linux/" + architecture, "--format", "{{.Id}}", image]
        ).stdout.strip()
        for image in dict.fromkeys(images)
    }
    for image, expected in frozen.items():
        if not _DIGEST.fullmatch(expected):
            raise ValueError(f"invalid frozen image identity for {image}: {expected!r}")
    stats = metrics if metrics is not None else {}
    stats.update(
        registry=HOST_REGISTRY_NAME,
        uploads=0,
        cache_hits=0,
        publication_seconds=0.0,
        pull_seconds=0.0,
        frozen_ids=frozen,
        manifest_digests={},
    )
    evidence: dict[str, dict[str, str]] = {node: {} for node in nodes}
    for image, expected in frozen.items():
        started = time.monotonic()
        # A single repository also deduplicates identical bytes published under
        # different source names; image identity, not a lane/tag, owns the cache.
        key = "sdo/images"
        tag = "sha256-" + expected.removeprefix("sha256:")
        local = f"{host}/{key}:{tag}"
        manifest = _manifest(host, key, tag)
        cached = manifest is not None
        if manifest is None:
            _run(["docker", "tag", expected, local])
            pushed = _run(["docker", "push", "--platform", "linux/" + architecture, local])
            match = re.search(r"digest: (sha256:[0-9a-f]{64})", pushed.stdout + pushed.stderr)
            if match is None:
                raise RuntimeError(f"registry push returned no manifest digest for {image}")
            manifest = _manifest(host, key, match[1])
            stats["uploads"] += 1
        # Docker's classic store reports the config digest as the image ID; its
        # containerd image store reports the manifest digest. Either pins the bytes.
        if manifest is None or expected not in {manifest.config, manifest.digest}:
            raise ValueError(f"registry image identity differs from frozen image {image}: expected {expected}")
        if cached:
            stats["cache_hits"] += 1
        digest = manifest.digest
        stats["manifest_digests"][image] = digest
        stats["publication_seconds"] += time.monotonic() - started
        immutable = f"{host}/{key}@{digest}"
        started = time.monotonic()
        for node in nodes:
            # CRI uses hosts.toml and unpacks shared layers. Preserve the source
            # ref for application manifests and IfNotPresent semantics.
            _run(["docker", "exec", node, "crictl", "pull", immutable])
            rows = _run(
                ["docker", "exec", node, "ctr", "-n", "k8s.io", "images", "ls", "-q", "target.digest==" + digest]
            ).stdout.splitlines()
            if immutable not in rows:
                raise ValueError(f"{node} pulled unexpected manifest digest for {image}")
            _run(
                ["docker", "exec", node, "ctr", "-n", "k8s.io", "images", "tag", "--force", immutable, image_ref(image)]
            )
            installed = json.loads(_run(["docker", "exec", node, "crictl", "inspecti", image]).stdout)
            if installed["status"]["id"] != manifest.config:
                raise ValueError(f"{node} image identity differs from frozen image {image}: expected {manifest.config}")
            evidence[node][image] = digest
        stats["pull_seconds"] += time.monotonic() - started
    return evidence


def remove_registry(cluster: str) -> None:
    """Remove a retired lane-owned registry; never remove the shared host cache."""
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

    parser = argparse.ArgumentParser(
        description="Deliver through the shared host registry, with a recorded automatic fallback"
    )
    parser.add_argument("--cluster", required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    from benchmarks.sregym.fastloop.host_limits import HostLimits

    parser.add_argument("--host-lock-dir", type=Path, default=HostLimits.from_env().directory)
    parser.add_argument("--min-free-gib", type=int, default=100)
    parser.add_argument("images", nargs="+")
    args = parser.parse_args()
    from benchmarks.sregym.adapter import LaneResourcePolicy

    load_images(
        args.cluster,
        args.images,
        record_path=args.run_dir / "image_delivery.jsonl",
        host_limits=HostLimits(args.host_lock_dir),
        resource_policy=LaneResourcePolicy(host_disk_floor_bytes=args.min_free_gib * 1024**3),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
