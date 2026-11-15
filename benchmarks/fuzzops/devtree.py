"""The dev-mounted source tree of a fuzzops lane.

A lane started with ``settings.dev_mount`` runs the controller and responder pods from the base images and projects
the checkout's Python sources over them from a node directory (``devmount``, ``SDO_DEV_MOUNT_PATH``): a change to the
responder's prompt, skills or Python agent code reaches a lane by copying files, with no image build or push.
The directory lives under the lane's evidence mount, which every node already has.

Go code, protobuf contracts and image manifests are compiled or baked in: a swap onto a revision that changed them is
refused (``swap_problem``) and the lane takes the full build.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path  # noqa: TC003

#: Directories the pods mount from the tree (``devmount.Apply``/``ApplyWorker``); benchmarks only its sregym package.
PROJECTED = ("sdo", "libs", "examples", "benchmarks/__init__.py", "benchmarks/sregym")
#: Paths whose change needs a rebuilt binary or image: a swap cannot carry them.
COMPILED = (
    "sdo/operator",
    "sdo/contracts",
    "scripts/sdo-images.hcl",
    "scripts/build_sdo_images.sh",
    "docker",
    "uv.lock",
    "pyproject.toml",
)
_EXCLUDES = ("--exclude=.git", "--exclude=__pycache__", "--exclude=*.pyc", "--exclude=.venv", "--exclude=node_modules")


def sync(checkout: Path, dest: Path, binary: Path | None) -> None:
    """Make ``dest`` the projection of ``checkout`` plus the host-built operator binary (replacing what was there;
    ``None`` keeps the binary the lane has)."""
    dest.mkdir(parents=True, exist_ok=True)
    dest.chmod(0o755)
    for rel in PROJECTED:
        source = checkout / rel
        target = dest / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        if source.is_dir():
            cmd = ["rsync", "-a", "--delete", *_EXCLUDES, f"{source}/", f"{target}/"]
        else:
            cmd = ["rsync", "-a", str(source), str(target)]
        subprocess.run(cmd, check=True, capture_output=True)
    if binary is None:
        return
    operator = dest / ".fastloop" / "sdo-operator"
    operator.parent.mkdir(parents=True, exist_ok=True)
    staged = operator.with_name("sdo-operator.new")
    shutil.copy2(binary, staged)
    staged.chmod(0o755)
    staged.replace(operator)


def fingerprint(checkout: Path) -> dict[str, str]:
    """The git object ids of ``COMPILED`` at the checkout's HEAD (empty for a path the tree lacks): what a lane was
    built from. A shallow checkout has no older revision to diff against, so ``prepare`` records this instead."""
    out: dict[str, str] = {}
    for path in COMPILED:
        proc = subprocess.run(
            ["git", "-C", str(checkout), "rev-parse", "--verify", "-q", f"HEAD:{path}"],
            capture_output=True,
            text=True,
            check=False,
        )
        out[path] = proc.stdout.strip()
    return out


def swap_problem(checkout: Path, lane: dict[str, str]) -> str:
    """Why the lane built with fingerprint ``lane`` cannot run the checkout by a swap; empty when it can."""
    changed = [path for path, ident in fingerprint(checkout).items() if lane.get(path) != ident]
    if changed:
        return "compiled code changed since the lane was built: " + ", ".join(changed[:5])
    return ""
