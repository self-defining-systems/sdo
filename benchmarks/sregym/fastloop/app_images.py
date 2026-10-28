"""Source-built app images: one build per content identity, shared through registries.

SREGym tags a source-built image ``<repository>:sregym-src-<identity>[-component]``
and labels it with the same identity (build context, Dockerfile, build
arguments, platform). Before building, a lane looks for that identity in the
local Docker store, then in the host registry, then in any other registries in
``FASTLOOP_APP_IMAGE_REGISTRIES`` (comma-separated ``host:port``, for example a
tunnel to another host's registry). Only a miss everywhere builds, and every
build or remote pull is published to each registry that lacks it, so other
lanes and hosts pull instead of building.
"""

from __future__ import annotations

import fcntl
import json
import os
import subprocess
import sys
import time
import urllib.error
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.adapter import WaitKind, wait_for
from benchmarks.sregym.fastloop.image_delivery import _manifest

if TYPE_CHECKING:
    from pathlib import Path

APP_REPOSITORY = "sdo/app-src"
REGISTRIES_ENV = "FASTLOOP_APP_IMAGE_REGISTRIES"

LocalIdentity = Callable[[str], "str | None"]


def _run(argv: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(argv, check=True, capture_output=True, text=True, timeout=wait_for(WaitKind.ROLLOUT))


def _registry_tag(image_ref: str, identity: str) -> str:
    tag = image_ref.rpartition(":")[2]
    if identity not in tag:
        raise ValueError(f"source image {image_ref} is not tagged by its identity {identity}")
    return tag


def _lookup(registry: str, tag: str, *, required: bool) -> Any:
    """A manifest, or None on a miss; an unreachable optional registry is a miss."""
    try:
        return _manifest(registry, APP_REPOSITORY, tag)
    except (urllib.error.URLError, OSError) as exc:
        if required:
            raise
        print(f"WARNING: app image registry {registry} unavailable: {exc}", file=sys.stderr)
        return None


def ensure_app_image(
    image_ref: str,
    identity: str,
    build: Callable[[], None],
    *,
    local_identity: LocalIdentity,
    registries: Sequence[str],
    lock_dir: Path,
) -> dict[str, Any]:
    """Make ``image_ref`` local, building only when no store holds ``identity``.

    The first registry is this host's (lookup failures there fail closed);
    later ones are optional. Content pulled under an identity must carry that
    identity's label, or delivery fails rather than deploying other bytes.
    """
    tag = _registry_tag(image_ref, identity)
    started = time.monotonic()
    lock_dir.mkdir(parents=True, exist_ok=True)
    with (lock_dir / f"app-image-{identity}.lock").open("a") as lock:
        # Two lanes deploying the same sources wait here; the second finds the first's image.
        fcntl.flock(lock, fcntl.LOCK_EX)
        waited = time.monotonic() - started
        present = {registry: _lookup(registry, tag, required=index == 0) for index, registry in enumerate(registries)}
        source = "local"
        if local_identity(image_ref) != identity:
            source = "build"
            for registry, manifest in present.items():
                if manifest is None:
                    continue
                remote = f"{registry}/{APP_REPOSITORY}@{manifest.digest}"
                _run(["docker", "pull", remote])
                _run(["docker", "tag", remote, image_ref])
                if local_identity(image_ref) != identity:
                    raise ValueError(f"registry {registry} image {tag} does not carry identity {identity}")
                source = "registry:" + registry
                break
            else:
                build()
                if local_identity(image_ref) != identity:
                    raise ValueError(f"source build of {image_ref} does not carry identity {identity}")
        published = []
        for registry, manifest in present.items():
            if manifest is not None:
                continue
            target = f"{registry}/{APP_REPOSITORY}:{tag}"
            try:
                _run(["docker", "tag", image_ref, target])
                _run(["docker", "push", target])
            except subprocess.SubprocessError as exc:
                # The image is local; an unreachable registry costs sharing, not this deploy.
                print(f"WARNING: app image publication to {registry} failed: {exc}", file=sys.stderr)
                continue
            published.append(registry)
    return {
        "image": image_ref,
        "identity": identity,
        "source": source,
        "published": published,
        "lock_wait_seconds": waited,
        "seconds": time.monotonic() - started,
    }


def shared_registries(env: dict[str, str] | None = None) -> list[str]:
    from benchmarks.sregym.fastloop.image_delivery import _host_registry

    values = os.environ if env is None else env
    extra = [entry.strip() for entry in values.get(REGISTRIES_ENV, "").split(",") if entry.strip()]
    host = _host_registry()
    return [host, *(registry for registry in extra if registry != host)]


def install_shared_builds(source_deploy: Any, *, record_path: Path | None, lock_dir: Path) -> None:
    """Route SREGym's source builds through the shared app-image stores."""
    build_image = source_deploy._build_image

    def shared_build(image_ref: str, command: list[str], *, identity: str) -> None:
        record = ensure_app_image(
            image_ref,
            identity,
            lambda: build_image(image_ref, command, identity=identity),
            local_identity=source_deploy.local_image_identity,
            registries=shared_registries(),
            lock_dir=lock_dir,
        )
        print(f"app image {image_ref}: {record['source']} in {record['seconds']:.1f}s", file=sys.stderr)
        if record_path is not None:
            record_path.parent.mkdir(parents=True, exist_ok=True)
            with record_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record) + "\n")

    source_deploy._build_image = shared_build
