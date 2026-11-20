"""The fuzzops workload adapter for the lane platform: the built-in workload, plus what an EKS lane needs of it.

``python -m benchmarks.fuzzops.workload <verb>`` is a lanes workload command (``benchmarks/lanes/docs/contract.md``).
Every verb the built-in fuzzops workload implements is handed back to it through ``WORKLOAD_BUILTIN``, unchanged, so a
spec that names this adapter behaves as a spec that names none, except on a lane with ``BENCH_LANE_PROVIDER=eks``:

``app-up images``
    The packs load their images into a kind node (``kind load docker-image``). An EKS lane has no kind node: this
    adapter puts a ``kind`` command first on the PATH of that one call that publishes the images to the lane's ECR
    mirror under the names the manifests declare (``benchmarks.lanes.eks.publish_app_image``; nodes try the mirror
    first and fall back to the upstream) and that has no nodes to list, so a pack that preloads through
    ``kind get nodes`` has nothing to do.
``run``
    The same shim, and ``FUZZOPS_IMAGE_PUBLISH`` naming it: a fault that builds a patched image (fuzzops
    ``build-and-deploy-patched-image``, which says ``ImageMirror``) publishes it to the mirror instead of loading it
    into a node, so such a fault is not excluded on EKS (``eks_faults`` reads the same field).
``footprint``
    A GitLab app wants 8 cores and 30Gi (the size the built-in workload gives it); other apps take the default.

The shim needs ``BENCH_IMAGE_REGISTRY`` (``benchmarks/fuzzops/eks.py``), ``aws`` and ``docker`` on the PATH.
"""

from __future__ import annotations

import json
import os
import re
import stat
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

from benchmarks.fuzzops import eks

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

UNSUPPORTED = 64
REPO_ROOT = Path(__file__).resolve().parents[2]


class WorkloadError(RuntimeError):
    """A verb the adapter cannot carry out; the message is what the platform records."""


def footprint(app: str) -> dict[str, Any] | None:
    """The lane size the built-in workload gives the app (``None``: the platform default)."""
    if "gitlab" in Path(app.rstrip("/")).name.lower():
        return {"cpus": 8, "memory": "30Gi"}
    return None


def kind_shim(
    argv: Sequence[str],
    env: Mapping[str, str],
    log: Callable[[str], None],
    run: Callable[..., Any] = subprocess.run,
) -> int:
    """What the ``kind`` command does on an EKS lane: ``load docker-image`` publishes, ``get nodes`` lists none."""
    words = list(argv)
    if words[:2] == ["get", "nodes"]:
        return 0
    if words[:2] == ["load", "docker-image"]:
        images = _load_images(words[2:])
        kubectl = _kubectl(env)
        eks.publish_images(eks.lane_from_env(env, kubectl, run), images, log, run)
        return 0
    log(f"kind {' '.join(words)}: not available on an EKS lane (no kind node)")
    return 2


def _load_images(args: Sequence[str]) -> list[str]:
    """The images of ``kind load docker-image [--name N] [--nodes L] IMAGE...``."""
    images: list[str] = []
    skip = False
    for arg in args:
        if skip:
            skip = False
        elif arg in ("--name", "-n", "--nodes"):
            skip = True
        elif re.match(r"^--(name|nodes)=", arg):
            continue
        elif not arg.startswith("-"):
            images.append(arg)
    return images


def _kubectl(env: Mapping[str, str]) -> Callable[..., list[str]]:
    def command(*args: str) -> list[str]:
        out = ["kubectl"]
        if env.get("KUBECONFIG"):
            out += ["--kubeconfig", env["KUBECONFIG"]]
        if env.get("FUZZOPS_KUBE_CONTEXT"):
            out += ["--context", env["FUZZOPS_KUBE_CONTEXT"]]
        return [*out, *args]

    return command


def write_shim(directory: Path) -> None:
    """A ``kind`` executable in ``directory`` that runs this module's shim with this interpreter."""
    path = directory / "kind"
    path.write_text(f'#!/bin/sh\nexec "{sys.executable}" -m benchmarks.fuzzops.workload kind "$@"\n')
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def handle(
    verb: str,
    env: Mapping[str, str],
    call: Callable[[Sequence[str], Mapping[str, str]], int],
) -> int:
    """One verb. ``call`` runs a command line with an environment and answers its exit code (stdout passes through)."""
    if verb == "footprint":
        size = footprint(env.get("BENCH_PACK", ""))
        if size is None:
            print(json.dumps({"unsupported": True}))
            return UNSUPPORTED
        print(json.dumps(size))
        return 0
    builtin = json.loads(env["WORKLOAD_BUILTIN"]) if env.get("WORKLOAD_BUILTIN") else []
    if not builtin:
        print(json.dumps({"unsupported": True}))
        return UNSUPPORTED
    if eks.is_eks(env) and (verb == "run" or (verb == "app-up" and env.get("WORKLOAD_ACTION") == "images")):
        with tempfile.TemporaryDirectory(prefix="fuzzops-eks-shim-") as shim:
            write_shim(Path(shim))
            patched = {
                **env,
                "PATH": f"{shim}{os.pathsep}{env.get('PATH', '')}",
                "PYTHONPATH": f"{REPO_ROOT}{os.pathsep}{env.get('PYTHONPATH', '')}".rstrip(os.pathsep),
                # a fault that builds an image (fuzzops build-and-deploy-patched-image) publishes it through this
                "FUZZOPS_IMAGE_PUBLISH": f"{shim}/kind load docker-image",
            }
            return call(builtin, patched)
    return call(builtin, env)


def _call(argv: Sequence[str], env: Mapping[str, str]) -> int:
    return subprocess.run(list(argv), env=dict(env), check=False).returncode


def main(argv: Sequence[str] | None = None, env: Mapping[str, str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    environ = os.environ if env is None else env
    if not args:
        print("usage: python -m benchmarks.fuzzops.workload VERB", file=sys.stderr)
        return 2
    if args[0] == "kind":
        try:
            return kind_shim(args[1:], environ, lambda line: print(line, file=sys.stderr))
        except (eks.EksHookError, WorkloadError) as exc:
            print(exc, file=sys.stderr)
            return 1
    return handle(args[0], environ, _call)


if __name__ == "__main__":
    raise SystemExit(main())
