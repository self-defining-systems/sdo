"""Import a learned memory (a seed) into a lane's accepted repository with one controller restart.

A lane's controller caches its accepted commit, so a seed is imported with the controller stopped. The memory the
controller must start on is the seed after tree preparation (``sdo-operator prepare-tree``: the generic detectors the
running build generates). That preparation needs no lane at all, only the seed's git objects, so it runs on the host
first, on a clone of the seed's bundle, while the controller keeps serving. The lane then goes through exactly one stop,
import and start of the controller (a fresh build imports the seed, restarts the controller, prepares the tree the
controller started on and imports again: two restarts).
"""

from __future__ import annotations

import json
import re
import shutil
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from benchmarks.devswap.kube import SwapError, run
from benchmarks.devswap.timeline import Timeline
from benchmarks.devswap.tree import TreeSteps, prepare_accepted_tree

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

_COMMIT = re.compile(r"^[0-9a-f]{40}$")


@dataclass(frozen=True)
class MemorySeed:
    """The memory to import: its accepted commit, the git bundle that holds it and the operator configuration laid over
    the lane's own for the run that uses it (empty: the lane's own)."""

    commit: str
    bundle: Path
    overlay: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not _COMMIT.match(self.commit):
            raise ValueError(f"seed commit must be 40 hex digits, got {self.commit!r}")


@dataclass(frozen=True)
class PreparedMemory:
    """What goes into the lane: the head to put the accepted repository at and the bundle that holds it."""

    head: str
    bundle: Path


def prepare_memory(
    seed: MemorySeed,
    prepare: Callable[[Path, Path | None], str] | None,
    binary: Path | None,
    work_root: Path,
    out: Callable[[str], object] = print,
) -> PreparedMemory:
    """The seed as the lane's controller must start on it. Nothing here touches a lane: the seed's commit is checked out
    from its bundle into a scratch clone, prepared (``prepare_accepted_tree``: the preparation must be stable) and
    bundled into ``work_root``. Without ``prepare`` the seed is imported as it is."""
    if prepare is None:
        return PreparedMemory(seed.commit, seed.bundle)
    work = Path(tempfile.mkdtemp(prefix="sdo-prepare-seed-", dir=work_root))
    clone = work / "tree"
    prepared_bundle = work / "prepared.bundle"

    def export() -> Path:
        run(["git", "init", "-q", str(clone)], "init the seed clone")
        # whatever its refs are named (a learned memory's bundle may have a branch and no HEAD, or only HEAD)
        heads = run(["git", "bundle", "list-heads", str(seed.bundle)], "list the seed bundle's refs").stdout.split("\n")
        refs = [line.split(" ", 1)[1] for line in heads if " " in line]
        specs = [f"+{ref}:refs/seed/{number}" for number, ref in enumerate(refs)]
        run(["git", "-C", str(clone), "fetch", "-q", str(seed.bundle), *specs], "fetch the seed bundle")
        run(["git", "-C", str(clone), "checkout", "-q", "--detach", seed.commit], "check out the seed commit")
        return clone

    def import_head(path: Path, head: str) -> None:
        run(
            ["git", "-C", str(path), "bundle", "create", "-q", str(prepared_bundle), "HEAD"], "bundle the prepared seed"
        )
        if run(["git", "-C", str(path), "rev-parse", "HEAD"], "git rev-parse").stdout.strip() != head:
            raise SwapError("the prepared seed's HEAD is not the head tree preparation reported")

    try:
        steps = TreeSteps(export, lambda path: prepare(path, binary), import_head)
        head = prepare_accepted_tree(steps, seed.commit, out)
        if head == seed.commit:
            return PreparedMemory(head, seed.bundle)
        # the bundle outlives the scratch clone beside it
        kept = work_root / f"prepared-{head[:12]}.bundle"
        prepared_bundle.replace(kept)
        return PreparedMemory(head, kept)
    finally:
        shutil.rmtree(work, ignore_errors=True)


@dataclass(frozen=True)
class SeedSteps:
    """What ``apply_seed`` does, one callable per step (tests substitute them)."""

    #: Host side, controller running, the lane untouched: the seed as the controller must start on it.
    prepare: Callable[[], PreparedMemory]
    reset_incidents: Callable[[], object]
    retire_standby: Callable[[], object]
    stop_controller: Callable[[], object]
    #: Put the seed's config overlay on the stopped controller's configuration (the lane's own when it has none).
    apply_config: Callable[[Mapping[str, Any]], object]
    import_memory: Callable[[PreparedMemory], object]  # the repository at the head, from the bundle
    start_controller: Callable[[], object]
    wait_standby: Callable[[], object]
    repository_state: Callable[[], Mapping[str, str]]
    controller_idle: Callable[[], str]  # why the operator is not idle, waiting for it; empty when it is
    is_ancestor: Callable[[str, str], bool]  # (ancestor, descendant) in the lane's accepted repository
    #: Called with the head the controller holds when its start-up upkeep (a schema migration) committed on top of the
    #: prepared seed: make that tree prepared again (``devswap.tree.regenerate_tree``: nothing when preparing it changes
    #: nothing, else one more import and restart) and return the resulting head.
    settle: Callable[[str], str]


def apply_seed(
    steps: SeedSteps, seed: MemorySeed, out: Callable[[str], object] = print, timeline: Timeline | None = None
) -> dict[str, Any]:
    """Make a prepared, idle lane's accepted repository the seed's memory, prepared as an install of this build would
    start it: prepare on the host first (the lane keeps running), then close any incident, stop the controller (it
    caches the accepted commit), import the prepared head, start it again (the same install, state and images) and
    assert the repository is that head, clean, with the operator idle. The controller is stopped and started once."""
    started = time.monotonic()
    t = timeline or Timeline(out, "seed.")
    with t.step("prepare"):
        prepared = steps.prepare()
    with t.step("reset_incidents"):
        steps.reset_incidents()
    try:
        with t.step("stop_controller"):
            steps.stop_controller()
        # With the controller stopped nothing recreates the standby: the next one starts from the new configuration.
        # Retired before the stop, a running controller would start a replacement from the old environment that the
        # restart adopts.
        with t.step("retire_standby"):
            steps.retire_standby()
        with t.step("apply_config"):
            steps.apply_config(seed.overlay)
        with t.step("import_memory"):
            steps.import_memory(prepared)
    finally:
        # the controller never stays stopped: a failed import must not leave the lane unable to answer a reset
        with t.step("start_controller"):
            steps.start_controller()
    with t.step("wait_standby"):
        steps.wait_standby()
    now = steps.repository_state()
    # The controller's startup upkeep (schema, module) may commit on top of the seed; anything else moved the head.
    if now["status"] or (now["head"] != prepared.head and not steps.is_ancestor(prepared.head, now["head"])):
        raise SwapError(f"the lane's repository is {now['head'][:12]} after the seed, not {prepared.head[:12]}")
    with t.step("controller_idle"):
        why = steps.controller_idle()
    if why:
        raise SwapError(f"the operator is not idle after the seed: {why}")
    head = now["head"]
    if head != prepared.head:
        # The upkeep ran after the preparation, the other way round from a fresh build (which migrates the seed and then
        # prepares it). A migration that rewrites what the preparation generated would leave a tree that is not the
        # prepared one: preparing it once more settles that, and costs nothing when it changes nothing.
        with t.step("settle"):
            head = steps.settle(head)
    receipt = {
        "seed": seed.commit,
        "prepared": prepared.head,
        "head": head,
        "seed_s": round(time.monotonic() - started, 1),
    }
    out(json.dumps(receipt))
    return receipt
