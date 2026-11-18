"""The ``sdo-operator`` binary of a source tree, built once per tree on a host and kept in a cache.

The pods of a dev-mounted lane run ``/usr/local/bin/sdo-operator`` from the lane's dev directory; a swap to a revision
whose operator source differs puts the cached binary of that revision there (``devtree.sync``).
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import subprocess
import time
from pathlib import Path
from typing import TYPE_CHECKING

from benchmarks.devswap import devtree
from benchmarks.devswap.kube import SwapError, run

if TYPE_CHECKING:
    from collections.abc import Callable

#: Overrides the cache directory (default ``~/.cache/sdo-operator-bins``).
CACHE_ENV = "SDO_BINARY_CACHE"
BUILD_TIMEOUT_S = 1200


def cache_root() -> Path:
    return Path(os.environ.get(CACHE_ENV) or Path.home() / ".cache" / "sdo-operator-bins")


#: How the binary is built (part of the cache key; the image build uses the same): static, no VCS stamp.
BUILD_RECIPE = "cgo0-buildvcs0-v1"
BUILD_ENV = {"CGO_ENABLED": "0"}
BUILD_FLAGS = ["-buildvcs=false"]


def uncommitted_digest(checkout: Path) -> str:
    """A digest of the uncommitted and untracked changes under the compiled paths; empty for a clean checkout."""
    paths = [devtree.BINARY_PATH, devtree.CONTRACTS_PATH]
    status = subprocess.run(
        ["git", "-C", str(checkout), "status", "--porcelain", "--untracked-files=all", "--", *paths],
        capture_output=True,
        text=True,
        check=False,
    ).stdout
    if not status.strip():
        return ""
    digest = hashlib.sha256(status.encode())
    for line in sorted(status.splitlines()):
        target = checkout / line[3:].strip().split(" -> ")[-1]
        if target.is_file():
            digest.update(target.read_bytes())
    return digest.hexdigest()


def operator_cache_key(checkout: Path, allow_dirty: bool = False) -> str:
    """The identity of the operator binary's source: the git tree ids of the compiled paths it is built from, how it is
    built and, with ``allow_dirty``, a digest of the uncommitted changes. A checkout with uncommitted changes under the
    compiled paths is refused otherwise: the build compiles the working tree, so the binary would be cached under the
    committed tree's key for every later claim of that revision."""
    dirty = uncommitted_digest(checkout)
    if dirty and not allow_dirty:
        raise SwapError(
            f"{checkout} has uncommitted changes under {devtree.BINARY_PATH} or {devtree.CONTRACTS_PATH}: commit them "
            "or allow a dirty build (then the binary is cached under a digest of them)"
        )
    ident = devtree.fingerprint(checkout)
    return hashlib.sha256(
        json.dumps(
            {
                "tree": {p: ident[p] for p in (devtree.BINARY_PATH, devtree.CONTRACTS_PATH)},
                "recipe": BUILD_RECIPE,
                "dirty": dirty,
            }
        ).encode()
    ).hexdigest()[:20]


def build_operator(checkout: Path, out: Path) -> Path:
    """``go build`` the checkout's ``sdo-operator`` into ``out`` (static, like the image's)."""
    out.parent.mkdir(parents=True, exist_ok=True)
    run(
        ["go", "build", *BUILD_FLAGS, "-o", str(out.resolve()), "./cmd/sdo-operator"],
        "go build sdo-operator",
        BUILD_TIMEOUT_S,
        cwd=checkout / "sdo" / "operator",
        env={**os.environ, **BUILD_ENV},
    )
    return out


def cached_operator_binary(
    checkout: Path, log: Callable[[str], object] = print, root: Path | None = None, allow_dirty: bool = False
) -> Path:
    """The ``sdo-operator`` binary of ``checkout``, built once per source tree on this host. Claims that need the same
    tree at the same moment build it once (the others wait on a lock and then copy it)."""
    key = operator_cache_key(checkout, allow_dirty)
    directory = (root or cache_root()) / key
    target = directory / "sdo-operator"
    if target.is_file():
        log(f"operator binary from the cache: {key}")
        return target
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / "build.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if target.is_file():
            log(f"operator binary from the cache: {key} (built by a concurrent swap)")
            return target
        started = time.monotonic()
        staged = build_operator(checkout, target.with_name(f"sdo-operator.{os.getpid()}"))
        staged.replace(target)
        log(f"built the operator binary in {time.monotonic() - started:.0f}s")
    return target
