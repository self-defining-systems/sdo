"""The dev-mounted source tree of a fuzzops lane.

A lane started with ``settings.dev_mount`` runs the controller and responder pods from the base images and projects
the checkout's Python sources over them from a node directory (``devmount``, ``SDO_DEV_MOUNT_PATH``): a change to the
responder's prompt, skills or Python agent code reaches a lane by copying files, with no image build or push.
The directory lives under the lane's evidence mount, which every node already has.

Image manifests and dependency files are baked in: a swap onto a revision that changed them is refused
(``swap_problem``) and the lane takes the full build. A change to the Go operator source is carried by a rebuilt
binary (built once per source tree, ``hooks.cached_operator_binary``) and a controller restart. The protobuf contracts
are not baked in (the Go ones are in that binary, the Python ``_gen`` modules are projected with ``sdo``): an *additive*
change to them is carried the same way; a breaking one (``contractdiff``) is refused.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path  # noqa: TC003

from benchmarks.fuzzops import contractdiff

#: Directories the pods mount from the tree (``devmount.Apply``/``ApplyWorker``); benchmarks only its sregym package.
PROJECTED = ("sdo", "libs", "examples", "benchmarks/__init__.py", "benchmarks/sregym")
#: Paths whose change needs a rebuilt binary or image: a swap carries only a changed ``sdo/operator`` (a rebuilt binary)
#: and an additive change to ``sdo/contracts``.
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


#: The contracts path: its change is compared field by field, not refused outright.
CONTRACTS_PATH = "sdo/contracts"
#: The compiled path a swap can carry by a rebuilt operator binary (the controller restarts on it).
BINARY_PATH = "sdo/operator"


def changed_compiled(checkout: Path, lane: dict[str, str]) -> list[str]:
    """The ``COMPILED`` paths whose content differs from what the lane was built from."""
    return [path for path, ident in fingerprint(checkout).items() if lane.get(path) != ident]


def binary_changed(checkout: Path, lane: dict[str, str]) -> bool:
    """Whether the operator binary the lane runs differs from the checkout's source (a swap must rebuild it)."""
    changed = changed_compiled(checkout, lane)
    return BINARY_PATH in changed or CONTRACTS_PATH in changed  # the binary embeds the Go contracts


def contracts_snapshot(checkout: Path) -> str:
    """The checkout's contract descriptors, in the form a lane keeps (``contractdiff.encode``)."""
    return contractdiff.encode(contractdiff.from_directory(checkout))


def contract_problems(checkout: Path, snapshot: str) -> list[str]:
    """How the checkout's contracts break the ones the lane was built with (``snapshot``); with no snapshot, that the
    lane cannot say."""
    if not snapshot:
        return ["the lane recorded no contract descriptors"]
    return contractdiff.breaking(contractdiff.decode(snapshot), contractdiff.from_directory(checkout))


def swap_problem(checkout: Path, lane: dict[str, str], rebuild_binary: bool = False, contracts: str = "") -> str:
    """Why the lane built with fingerprint ``lane`` cannot run the checkout by a swap; empty when it can. With
    ``rebuild_binary`` a changed operator source is not a problem: the swap builds the binary, and with it a changed
    contract that only adds to the lane's ``contracts`` snapshot (also built into that binary)."""
    changed = changed_compiled(checkout, lane)
    if rebuild_binary:
        changed = [path for path in changed if path != BINARY_PATH]
        if CONTRACTS_PATH in changed:
            if breaks := contract_problems(checkout, contracts):
                return "breaking contract change since the lane was built: " + "; ".join(breaks[:5])
            changed.remove(CONTRACTS_PATH)
    if changed:
        return "compiled code changed since the lane was built: " + ", ".join(changed[:5])
    return ""
