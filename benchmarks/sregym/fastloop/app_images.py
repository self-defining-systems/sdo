"""Source-built app images: one build per content identity, shared through one registry.

SREGym tags a source-built image ``<repository>:sregym-src-<identity>[-component]``
and labels it with the same identity (build context, Dockerfile, build
arguments, platform). Every host shares one app-image registry,
``FASTLOOP_APP_IMAGE_REGISTRY`` (``host:port``; default: this host's registry).
Another host reaches it through a loopback port that relays to the registry
host over the LAN. Before building, a lane uses the local image carrying the
identity, else pulls the identity from the shared registry, else builds and
publishes it there, so other lanes and hosts pull instead of building.

A pull enters the host store the way a build's ``--load`` does: the verified
manifest and blobs stream into ``docker load``. The host only stores and
re-delivers app images (kind nodes run them), so unlike ``docker pull`` this
does not fail on a host whose snapshotter cannot unpack a shared base layer.
"""

from __future__ import annotations

import fcntl
import hashlib
import io
import json
import os
import subprocess
import sys
import tarfile
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.adapter import WaitKind, wait_for
from benchmarks.sregym.fastloop.image_delivery import _MANIFEST_TYPES, _manifest
from benchmarks.sregym.fastloop.image_delivery import image_ref as qualified_ref

if TYPE_CHECKING:
    from pathlib import Path

APP_REPOSITORY = "sdo/app-src"
REGISTRY_ENV = "FASTLOOP_APP_IMAGE_REGISTRY"

LocalIdentity = Callable[[str], "str | None"]


def _run(argv: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(argv, check=True, capture_output=True, text=True, timeout=wait_for(WaitKind.ROLLOUT))


def _registry_tag(image_ref: str, identity: str) -> str:
    tag = image_ref.rpartition(":")[2]
    if identity not in tag:
        raise ValueError(f"source image {image_ref} is not tagged by its identity {identity}")
    return tag


class _VerifiedBlob(io.RawIOBase):
    """A registry blob read once, failing the load if its bytes are not its digest."""

    def __init__(self, response: Any, digest: str, size: int) -> None:
        self._response, self._digest, self._remaining = response, digest, size
        self._hash = hashlib.sha256()

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Any) -> int:
        data = self._response.read(min(len(buffer), self._remaining))
        self._hash.update(data)
        self._remaining -= len(data)
        if not self._remaining and "sha256:" + self._hash.hexdigest() != self._digest:
            raise ValueError(f"registry blob digest mismatch: {self._digest}")
        buffer[: len(data)] = data
        return len(data)


def _load_from_registry(registry: str, digest: str, image_ref: str) -> None:
    """Stream one verified manifest and its blobs into ``docker load`` under ``image_ref``."""
    base = f"http://{registry}/v2/{APP_REPOSITORY}"
    request = urllib.request.Request(f"{base}/manifests/{digest}", headers={"Accept": _MANIFEST_TYPES})
    with urllib.request.urlopen(request, timeout=wait_for(WaitKind.PROBE)) as response:
        raw = response.read(4 * 1024 * 1024 + 1)
    if "sha256:" + hashlib.sha256(raw).hexdigest() != digest:
        raise ValueError(f"registry manifest digest mismatch: {registry}/{APP_REPOSITORY}@{digest}")
    manifest = json.loads(raw)

    def path(blob: str) -> str:
        return "blobs/sha256/" + blob.removeprefix("sha256:")

    name = image_ref.removeprefix("docker.io/library/").removeprefix("docker.io/")
    # Both layouts docker save writes: index.json for the containerd store, manifest.json for the classic one.
    index = {
        "schemaVersion": 2,
        "manifests": [
            {
                "mediaType": manifest.get("mediaType", "application/vnd.oci.image.manifest.v1+json"),
                "digest": digest,
                "size": len(raw),
                "annotations": {
                    "io.containerd.image.name": qualified_ref(image_ref),
                    "org.opencontainers.image.ref.name": name,
                },
            }
        ],
    }
    legacy = [
        {
            "Config": path(manifest["config"]["digest"]),
            "RepoTags": [name],
            "Layers": [path(layer["digest"]) for layer in manifest["layers"]],
        }
    ]
    loader = subprocess.Popen(
        ["docker", "load"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=False
    )
    assert loader.stdin is not None
    try:
        with tarfile.open(fileobj=loader.stdin, mode="w|") as archive:

            def add(name: str, data: Any, size: int) -> None:
                member = tarfile.TarInfo(name)
                member.size = size
                archive.addfile(member, data)

            for name_, data in (
                ("oci-layout", b'{"imageLayoutVersion":"1.0.0"}'),
                ("index.json", json.dumps(index).encode()),
                ("manifest.json", json.dumps(legacy).encode()),
                (path(digest), raw),
            ):
                add(name_, io.BytesIO(data), len(data))
            for descriptor in (manifest["config"], *manifest["layers"]):
                with urllib.request.urlopen(
                    f"{base}/blobs/{descriptor['digest']}", timeout=wait_for(WaitKind.PROBE)
                ) as blob:
                    add(
                        path(descriptor["digest"]),
                        io.BufferedReader(_VerifiedBlob(blob, descriptor["digest"], descriptor["size"])),
                        descriptor["size"],
                    )
    except BaseException:
        loader.kill()
        loader.wait()
        raise
    loader.stdin.close()
    output = loader.stdout.read().decode(errors="replace") if loader.stdout else ""
    if loader.wait(timeout=wait_for(WaitKind.ROLLOUT)) != 0:
        raise subprocess.CalledProcessError(loader.returncode, ["docker", "load"], output)


def ensure_app_image(
    image_ref: str,
    identity: str,
    build: Callable[[], None],
    *,
    local_identity: LocalIdentity,
    registry: str,
    lock_dir: Path,
) -> dict[str, Any]:
    """Make ``image_ref`` local, building only when neither this host nor the shared registry holds ``identity``.

    Content pulled under an identity must carry that identity's label, or
    delivery fails rather than deploying other bytes. An unreachable registry
    is recorded and the lane builds; it never blocks the deploy.
    """
    tag = _registry_tag(image_ref, identity)
    started = time.monotonic()
    lock_dir.mkdir(parents=True, exist_ok=True)
    record: dict[str, Any] = {"image": image_ref, "identity": identity, "registry": registry, "published": False}
    with (lock_dir / f"app-image-{identity}.lock").open("a") as lock:
        # Two lanes deploying the same sources wait here; the second finds the first's image.
        fcntl.flock(lock, fcntl.LOCK_EX)
        record["lock_wait_seconds"] = time.monotonic() - started
        try:
            manifest = _manifest(registry, APP_REPOSITORY, tag)
        except (urllib.error.URLError, OSError) as exc:
            manifest = None
            record["registry_error"] = str(exc)
            print(f"WARNING: app image registry {registry} unavailable: {exc}", file=sys.stderr)
        record["source"] = "local"
        if local_identity(image_ref) != identity:
            if manifest is not None:
                pulled = time.monotonic()
                _load_from_registry(registry, manifest.digest, image_ref)
                record.update(source="registry", pull_seconds=time.monotonic() - pulled)
            else:
                build()
                record["source"] = "build"
            if local_identity(image_ref) != identity:
                raise ValueError(f"{record['source']} image {image_ref} does not carry identity {identity}")
        if manifest is None and "registry_error" not in record:
            target = f"{registry}/{APP_REPOSITORY}:{tag}"
            try:
                _run(["docker", "tag", image_ref, target])
                _run(["docker", "push", target])
                record["published"] = True
            except subprocess.SubprocessError as exc:
                # The image is local; a failed upload costs sharing, not this deploy.
                record["registry_error"] = str(exc)
                print(f"WARNING: app image publication to {registry} failed: {exc}", file=sys.stderr)
    record["seconds"] = time.monotonic() - started
    return record


def shared_registry(env: dict[str, str] | None = None) -> str:
    values = os.environ if env is None else env
    configured = values.get(REGISTRY_ENV, "").strip()
    if configured:
        return configured
    from benchmarks.sregym.fastloop.image_delivery import _host_registry

    return _host_registry()


def install_shared_builds(source_deploy: Any, *, record_path: Path | None, lock_dir: Path) -> None:
    """Route SREGym's source builds through the shared app-image registry."""
    build_image = source_deploy._build_image

    def shared_build(image_ref: str, command: list[str], *, identity: str) -> None:
        record = ensure_app_image(
            image_ref,
            identity,
            lambda: build_image(image_ref, command, identity=identity),
            local_identity=source_deploy.local_image_identity,
            registry=shared_registry(),
            lock_dir=lock_dir,
        )
        print(f"app image {image_ref}: {record['source']} in {record['seconds']:.1f}s", file=sys.stderr)
        if record_path is not None:
            record_path.parent.mkdir(parents=True, exist_ok=True)
            with record_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record) + "\n")

    source_deploy._build_image = shared_build
