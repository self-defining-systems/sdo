"""Make a lane's accepted repository the tree an install of this build would start on.

A new operator binary generates files into the memory it installs (generic detectors and their template). A swapped lane
runs ``sdo-operator prepare-tree`` on a clone of its accepted repository with the new binary and, when the head moves,
puts the prepared head back on the lane with the controller stopped.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from contextlib import AbstractContextManager  # noqa: TC003
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from benchmarks.devswap.controller import (
    SWAP_STANDBY_WAIT_S,
    controller_stopped,
    repository_state,
    restore_repository,
    retire_standby,
    wait_standby,
)
from benchmarks.devswap.kube import APPLICATION_ROOT, Kubectl, SwapError, run

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping


@dataclass(frozen=True)
class TreeSteps:
    """What ``prepare_accepted_tree`` does, one callable per step (tests substitute them)."""

    export: Callable[[], Path]  # the accepted repository as a clone on the host
    prepare: Callable[[Path], str]  # ``sdo-operator prepare-tree`` on the clone: its head afterwards
    import_head: Callable[[Path, str], object]  # put the clone's head into the lane's accepted repository


def prepare_accepted_tree(steps: TreeSteps, head: str, out: Callable[[str], object] = print) -> str:
    """Make the lane's accepted repository the tree an install of this build would start on: the same tree
    preparation install-existing runs (generic detectors added, or regenerated to this build's template, committed).
    Returns the resulting head; a tree that is already prepared is left alone. Preparing the result again must
    change nothing, or the lane would differ from a fresh install."""
    clone = steps.export()
    prepared = steps.prepare(clone)
    if prepared == head:
        return head
    again = steps.prepare(clone)
    if again != prepared:
        raise SwapError(f"tree preparation is not stable: {prepared[:12]} prepares to {again}")
    out(f"tree prepared: {head[:12]} -> {prepared[:12]}")
    steps.import_head(clone, prepared)
    return prepared


@dataclass(frozen=True)
class ImportSteps:
    """What ``import_prepared_head`` does to the lane, one callable per step (tests substitute them)."""

    #: Stops the controller for a block and resumes it however the block ends.
    controller_stopped: Callable[[], AbstractContextManager[None]]
    retire_standby: Callable[[], object]
    restore: Callable[[], object]
    wait_standby: Callable[[], object]


def import_prepared_head(steps: ImportSteps) -> None:
    """Put a prepared tree on the lane's repository with the controller stopped, then start it and wait for its own
    standby. The controller is started again when a step fails. The standby is retired only once the controller is
    stopped: a running controller recreates the standby it lost at once, and that stale Ready standby would end the wait
    before the restarted controller exists (the next read then fails with ``no running controller pod``)."""
    with steps.controller_stopped():
        steps.retire_standby()
        steps.restore()
    steps.wait_standby()


def regenerate_tree(
    kube: Kubectl,
    control: str,
    work_root: Path,
    head: str,
    prepare: Callable[[Path, Path | None], str],
    wait_idle: Callable[[], str],
    out: Callable[[str], object] = print,
    binary: Path | None = None,
) -> str:
    """``prepare_accepted_tree`` on the lane: the accepted repository goes to the host as a bundle, ``prepare``
    (the caller runs ``sdo-operator prepare-tree`` with the settings of the lane's install, by ``binary``, the operator
    the swap placed) prepares the clone, and a changed
    head is restored into the volume with the controller stopped. Asserts the repository is the new head, clean, with
    the operator idle (``wait_idle`` returns the reason it is not, or empty)."""
    work = Path(tempfile.mkdtemp(prefix="sdo-prepare-tree-", dir=work_root))

    def export() -> Path:
        data = kube.exec_controller(
            control, f"cd {APPLICATION_ROOT} && git -c safe.directory='*' bundle create - HEAD 2>/dev/null"
        )
        bundle = work / "accepted.bundle"
        bundle.write_bytes(data)
        run(["git", "clone", "-q", str(bundle), str(work / "tree")], "clone the accepted repository")
        return work / "tree"

    def import_head(clone: Path, new_head: str) -> None:
        bundle = work / "prepared.bundle"
        run(["git", "-C", str(clone), "bundle", "create", str(bundle), "HEAD"], "bundle the prepared tree")
        import_prepared_head(
            ImportSteps(
                controller_stopped=lambda: controller_stopped(kube, control),
                retire_standby=lambda: retire_standby(kube, control),
                restore=lambda: restore_repository(kube, control, new_head, bundle),
                wait_standby=lambda: wait_standby(kube, control, SWAP_STANDBY_WAIT_S),
            )
        )
        now = repository_state(kube, control)
        if now["head"] != new_head or now["status"]:
            raise SwapError(f"the lane's repository is {now['head'][:12]} after tree preparation, not {new_head[:12]}")
        if why := wait_idle():
            raise SwapError(f"the operator is not idle after tree preparation: {why}")

    try:
        return prepare_accepted_tree(TreeSteps(export, lambda clone: prepare(clone, binary), import_head), head, out)
    finally:
        shutil.rmtree(work, ignore_errors=True)


def prepare_tree_argv(install_args: list[str], clone: Path, binary: Path | None = None) -> list[str]:
    """The ``prepare-tree`` command for a lane whose ``install-existing`` command line was kept: the same arguments,
    run on ``clone`` (the ``--repository``) by ``binary`` (the new operator build; default the install's own)."""
    if len(install_args) < 2 or install_args[1] != "install-existing":
        raise SwapError("the kept install command is not an sdo-operator install-existing command line")
    args = list(install_args)
    args[1] = "prepare-tree"
    if binary is not None:
        args[0] = str(binary)
    if "--repository" not in args:
        raise SwapError("the kept install command has no --repository")
    args[args.index("--repository") + 1] = str(clone)
    return args


def prepare_tree_from_install(
    install_args: list[str], install_env: Mapping[str, str], own_binary: Path | None = None
) -> Callable[[Path, Path | None], str]:
    """A ``regenerate_tree`` preparer from the lane's kept ``install-existing`` argv and environment: runs
    ``sdo-operator prepare-tree`` with them on a clone and returns the clone's head. It runs the binary it is given (the
    one the swap placed; the lane is prepared by the build it runs), else ``own_binary``, else the install's own."""

    def prepare(clone: Path, binary: Path | None) -> str:
        proc = run(
            prepare_tree_argv(install_args, clone, binary or own_binary),
            "sdo-operator prepare-tree",
            300,
            env={**os.environ, **install_env},
        )
        return str(json.loads(proc.stdout.strip().splitlines()[-1])["head"])

    return prepare
