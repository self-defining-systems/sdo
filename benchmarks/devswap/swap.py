"""Run a revision's compiled code on a prepared lane without rebuilding its images.

``swap_compiled`` is the whole sequence and the one entry a benchmark runner calls (``python -m benchmarks.devswap`` is
the command line of it): refuse what a binary swap cannot carry, build the revision's ``sdo-operator`` once per source
tree, project the checkout and the binary where the lane's pods read them (the dev mount), end the idle responder
standby so the next one imports them, assert the lane is still what its preparation left, regenerate the Go-generated
files of the accepted memory with the new binary, and restart the controller on the new binary unless a step already
did. A benchmark's own steps (golden copy, seed import, configuration) run through the ``SwapPlan`` callbacks.
"""

from __future__ import annotations

import shutil
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from benchmarks.devswap import devtree
from benchmarks.devswap.binary import cached_operator_binary
from benchmarks.devswap.controller import (
    SWAP_STANDBY_WAIT_S,
    controller_pod_ids,
    controller_stopped,
    repository_state,
    retire_standby,
    wait_standby,
    wipe_problems,
)
from benchmarks.devswap.kube import SwapError
from benchmarks.devswap.timeline import Timeline
from benchmarks.devswap.tree import regenerate_tree
from benchmarks.devswap.warm import start_go_cache_warm

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping
    from pathlib import Path

    from benchmarks.devswap.kube import Kubectl


def _nothing(*_args: object) -> None:
    return None


class Phases:
    """Seconds spent in each named phase of a hook (a phase that fails is timed too): the swap receipt's ``phases``."""

    def __init__(self) -> None:
        self.seconds: dict[str, float] = {}

    @contextmanager
    def __call__(self, name: str) -> Iterator[None]:
        started = time.monotonic()
        try:
            yield
        finally:
            self.seconds[name] = round(self.seconds.get(name, 0.0) + time.monotonic() - started, 1)


class ControllerBinary:
    """Whether the lane's controller process started after the operator binary was placed on the dev mount. Several
    swap steps stop and start the controller anyway (a lane reset, a seed import, a tree import) and each start runs
    the binary on the mount, so the swap restarts it for the binary only when none of them did: the controller pod
    seen at placement is still the running one. ``running`` returns the ids of the controller pods that run and raises
    when it cannot tell (an unknown controller is never taken for a restarted one)."""

    def __init__(self, running: Callable[[], set[str]]) -> None:
        self._running = running
        self._placed_under: set[str] = set()

    def placed(self) -> None:
        self._placed_under = self._running()

    def started_since(self) -> bool:
        """Whether the controller that ran at placement is gone (a later one runs the binary on the mount)."""
        return bool(self._placed_under) and not self._placed_under & self._running()

    def restart_if_stale(self, restart: Callable[[], object]) -> bool:
        """Run ``restart`` when a controller that was running at placement still is; returns whether it did."""
        if not self._placed_under & self._running():
            self._placed_under = set()
            return False
        restart()
        self._placed_under = set()
        return True


def restart_controller(kube: Kubectl, control: str, wait_idle: Callable[[], str]) -> None:
    """Stop and start the controller Job (its state, repository and install kept), end the idle standby so the next one
    imports what is on the dev mount, and wait for an idle operator (``wait_idle`` returns the reason it is not). The
    standby is retired while the controller is stopped: a running one would recreate it at once."""
    with controller_stopped(kube, control):
        retire_standby(kube, control)
    wait_standby(kube, control, SWAP_STANDBY_WAIT_S)
    if why := wait_idle():
        raise SwapError(f"the operator is not idle after the controller restart: {why}")


@dataclass(frozen=True)
class SwapState:
    """What the lane is, as the caller records it: the compiled paths and contracts its binary was built from, and the
    accepted head its memory is at."""

    compiled: Mapping[str, str]
    contracts: str
    head: str


@dataclass(frozen=True)
class SwapPlan:
    """One swap: the lane, the revision, and the benchmark's own steps. Callbacks default to doing nothing."""

    kube: Kubectl
    control: str  # the control namespace (the controller Job and the accepted repository)
    checkout: Path  # the target revision's checkout (``HEAD`` is the revision)
    #: What the lane was built from: ``devtree.fingerprint`` and ``devtree.contracts_snapshot`` at its preparation.
    lane_compiled: Mapping[str, str]
    lane_contracts: str
    #: ``repository_state`` at preparation: the accepted head, no uncommitted files, the outcome files.
    prepared: Mapping[str, str]
    place: Callable[[Path | None], object]  # project the checkout (and the binary, when given) onto the dev mount
    idle: Callable[[], str]  # why the operator is not idle right now, or empty
    wait_idle: Callable[[], str]  # the same, waiting for it to become idle
    work_root: Path  # a directory for scratch clones and the binary kept aside
    #: A file holding the binary the lane runs now (the dev mount's own, or the runner's copy). It is kept aside before
    #: the binary is replaced, so a swap that fails puts it back; without it a failed swap cannot restore the binary.
    current_binary: Path | None = None
    #: ``sdo-operator prepare-tree`` on a clone of the accepted repository with the lane's install settings, by the
    #: given binary (the one the swap placed; None when it did not place one): its head. None leaves the memory alone
    #: (the caller imports memory itself and prepares it after).
    prepare_tree: Callable[[Path, Path | None], str] | None = None
    #: The binary on the mount is this one (built, cached, or put back by a failed swap): keep the runner's copy so.
    on_binary: Callable[[Path], object] = _nothing
    #: Record what the lane is, each time it changes (the binary placed, the memory head moved, a failed swap undone).
    persist: Callable[[SwapState], object] = _nothing
    restore_moved: Callable[[list[str]], object] | None = None  # the lane moved while held; None refuses it
    verify: Callable[[], object] = _nothing  # the caller's own lane checks (a golden copy), after the lane is asserted
    #: Import a seed memory (``devswap.seed.apply_seed``), recording its steps in the timeline it is given; returns the
    #: accepted head it leaves. The controller restarts once, on the prepared seed, so with a seed ``prepare_tree`` does
    #: not run (the seed is prepared before it goes in). The lane record keeps the lane's own head: a reset of the lane
    #: returns to it, never to a seed.
    seed: Callable[[Timeline], str] | None = None
    finish: Callable[[], object] = _nothing  # the caller's steps that restart the controller (a seed import)
    #: A glob, beside the checkout, of older checkouts that hold the contracts of a lane that recorded none.
    legacy_checkouts: str = ""
    allow_dirty: bool = False  # build from a checkout with uncommitted changes under the compiled paths
    #: With a new binary, compile the new tree's Go packages into the lane's build cache (``devswap.warm``) while the old
    #: controller still serves, so its restart only links. Never fails the swap.
    warm_go_cache: bool = True


@dataclass(frozen=True)
class SwapResult:
    head: str  # the accepted head the lane starts from
    phases: Phases
    binary: bool  # a new binary was placed
    restarted: bool  # the swap itself restarted the controller for it
    compiled: Mapping[str, str] = field(default_factory=dict)  # what the lane now runs, for the caller to record
    contracts: str = ""
    seeded: str = ""  # the accepted head after a seed import (``head`` stays the lane's own)


def _keep_aside(plan: SwapPlan) -> Path | None:
    """A copy of the binary the lane runs now, for a failed swap to put back."""
    if plan.current_binary is None or not plan.current_binary.is_file():
        return None
    kept = plan.work_root / "previous-sdo-operator"
    shutil.copy2(plan.current_binary, kept)
    return kept


def _roll_back(
    plan: SwapPlan,
    controller: ControllerBinary,
    previous: Path | None,
    head: str,
    contracts: str,
    out: Callable[[str], object],
) -> None:
    """Undo a failed swap's binary: put the previous one back on the mount and restart the controller if it already runs
    the new one, then record what the lane is. Without a previous binary the mount keeps the new one and the record says
    so. The Python tree is not undone: every swap projects its own. Failures here are reported, not raised (the swap's
    own error is the one to see)."""
    try:
        if previous is None:
            plan.persist(SwapState(devtree.fingerprint(plan.checkout), devtree.contracts_snapshot(plan.checkout), head))
            return
        plan.on_binary(previous)
        plan.place(previous)
        try:
            started = controller.started_since()
        except SwapError:
            started = True  # an unreadable controller is restarted rather than trusted
        plan.persist(SwapState(plan.lane_compiled, contracts, head))
        if started:
            restart_controller(plan.kube, plan.control, plan.wait_idle)
        out("the failed swap's binary was put back")
    except Exception as exc:
        out(f"could not undo the failed swap's binary: {exc}")


def swap_compiled(plan: SwapPlan, out: Callable[[str], object] = print) -> SwapResult:
    """Run ``plan.checkout`` on the lane (see the module docstring). Raises ``SwapError`` with the reason when the lane
    cannot take the revision by a swap or is not what its preparation left. A step that fails after the binary was
    placed puts the previous binary back (``_roll_back``); every step that stops the controller starts it again."""
    phases = Phases()
    contracts = plan.lane_contracts
    if not contracts and devtree.CONTRACTS_PATH in devtree.changed_compiled(plan.checkout, plan.lane_compiled):
        # a lane built before lanes recorded their descriptors: read them from the checkout it was built from
        contracts = devtree.legacy_snapshot(plan.checkout, plan.lane_compiled, plan.legacy_checkouts)
    if why := devtree.swap_problem(plan.checkout, plan.lane_compiled, rebuild_binary=True, contracts=contracts):
        raise SwapError(why)
    controller = ControllerBinary(lambda: controller_pod_ids(plan.kube, plan.control))
    binary = previous = None
    if devtree.binary_changed(plan.checkout, plan.lane_compiled):
        with phases("binary"):
            binary = cached_operator_binary(plan.checkout, out, allow_dirty=plan.allow_dirty)
            previous = _keep_aside(plan)
            controller.placed()  # the controller that runs now runs the old binary
    compiled, snapshot = dict(plan.lane_compiled), contracts
    head = plan.prepared["head"]
    seeded = ""
    warm = None
    try:
        with phases("project"):
            if binary is not None:
                plan.on_binary(binary)
            plan.place(binary)
        if binary is not None:
            compiled, snapshot = devtree.fingerprint(plan.checkout), devtree.contracts_snapshot(plan.checkout)
            plan.persist(SwapState(compiled, snapshot, head))
            if plan.warm_go_cache:
                # the new tree is on the mount and the old controller still runs: compile what its restart would
                with phases("warm_start"):
                    warm = start_go_cache_warm(plan.kube, plan.control, out)
        with phases("standby"):
            retire_standby(plan.kube, plan.control)
            wait_standby(plan.kube, plan.control, SWAP_STANDBY_WAIT_S)
        with phases("golden"):
            if problems := wipe_problems(plan.prepared, repository_state(plan.kube, plan.control)):
                # An idle lane's accepted memory moved (an incident opened and closed while it was held).
                if plan.restore_moved is None:
                    raise SwapError("the lane is not the one prepared: " + "; ".join(problems))
                out("the lane moved while held, restoring it: " + "; ".join(problems))
                if warm is not None:
                    warm.finish()  # the restore resets the lane, which stops the controller
                plan.restore_moved(problems)
            plan.verify()
        if why := plan.idle():
            raise SwapError(f"the operator is not idle: {why}")
        if warm is not None:
            # joined before the first step that stops the controller (a seed import, a tree import, the restart)
            with phases("warm"):
                warm.finish()
        if plan.seed is not None:
            seed_steps = Timeline(out, "seed.")
            try:
                with phases("seed"):
                    seeded = plan.seed(seed_steps)
            finally:
                phases.seconds.update(seed_steps.seconds())
        elif plan.prepare_tree is not None:
            with phases("tree"):
                head = regenerate_tree(
                    plan.kube, plan.control, plan.work_root, head, plan.prepare_tree, plan.wait_idle, out, binary
                )
            plan.persist(SwapState(compiled, snapshot, head))
        with phases("finish"):
            plan.finish()
        restarted = False
        with phases("restart"):
            if controller.restart_if_stale(lambda: restart_controller(plan.kube, plan.control, plan.wait_idle)):
                restarted = True
                out("restarted the controller on the new binary")
    except BaseException:
        if warm is not None:
            warm.abort()
        if binary is not None:
            _roll_back(plan, controller, previous, head, contracts, out)
        raise
    plan.persist(SwapState(compiled, snapshot, head))
    return SwapResult(head, phases, binary is not None, restarted, compiled, snapshot, seeded)
