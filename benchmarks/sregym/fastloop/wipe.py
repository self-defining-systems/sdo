"""Row-start reset in two tiers: SDO state is always wiped, the application only when it must be.

Tier 1 runs at every row start. It deletes every namespace and cluster-scoped object that is not the
lane's own (the harness leftovers of earlier attempts) and SDO's control namespace, then re-provisions
credentials by reference, re-seeds the accepted repository at the row's planned commit and forgets the
controller registry. SDO is reinstalled afterwards by the normal native install. Nothing SDO wrote
survives except the memory commit itself, so no leftover needs its own cleanup rule.

Tier 2 concerns the application namespaces. The warm app is kept when the previous attempt finished
cleanly and the restore invariant passes against the captured baseline; otherwise the app is redeployed
from the image cache.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Iterable

    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

# Kubectl resource names in deletion order: admission and API registrations first so nothing routes to a
# backend being removed, RBAC before the namespaces whose service accounts it binds, namespaces last.
CLUSTER_KINDS: tuple[str, ...] = (
    "validatingwebhookconfiguration",
    "mutatingwebhookconfiguration",
    "apiservice",
    "ingressclass",
    "customresourcedefinition",
    "clusterrolebinding",
    "clusterrole",
    "priorityclass",
    "storageclass",
    "namespace",
)
NAMESPACE_KIND = "namespace"
INVENTORY_SUFFIX = ".lane-inventory.json"
MARKER_NAME = "attempt-in-progress"
RECEIPT_NAME = "wipe-receipt.json"
#: Target for tier 1 (seconds); reported in the receipt, never a gate.
TIER1_TARGET_SECONDS = 90.0


class WipeError(RuntimeError):
    """Tier 1 could not remove, confirm gone or re-provision something the row depends on."""


def object_kind(identity: str) -> str:
    """``clusterrole.rbac.authorization.k8s.io/x`` -> ``clusterrole``."""
    return identity.partition("/")[0].partition(".")[0]


def object_name(identity: str) -> str:
    return identity.partition("/")[2]


def namespace_identity(name: str) -> str:
    return f"{NAMESPACE_KIND}/{name}"


class LaneCluster(Protocol):
    def list_objects(self) -> frozenset[str]:
        """Every object of ``CLUSTER_KINDS`` (namespaces included) as ``kind/name``; raises on failure."""
        ...

    def delete(self, identity: str) -> None:
        """Request deletion; an already absent object is not an error."""
        ...


def inventory_path(kubeconfig: Path) -> Path:
    return kubeconfig.with_name(kubeconfig.name + INVENTORY_SUFFIX)


def load_inventory(path: Path) -> frozenset[str] | None:
    try:
        return frozenset(json.loads(path.read_text(encoding="utf-8")))
    except FileNotFoundError:
        return None
    except (OSError, ValueError, TypeError) as exc:
        raise WipeError(f"eval: lane inventory {path} is unreadable: {exc}") from exc


def save_inventory(path: Path, objects: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(sorted(objects), indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def cluster_scoped(objects: Iterable[str]) -> frozenset[str]:
    """The lane's cluster-scoped baseline: everything of ``CLUSTER_KINDS`` except namespaces."""
    return frozenset(identity for identity in objects if object_kind(identity) != NAMESPACE_KIND)


def harness_leftovers(
    current: Iterable[str],
    baseline: Collection[str],
    *,
    keep_namespaces: Collection[str],
    control_namespace: str,
) -> frozenset[str]:
    """What tier 1 removes: SDO's control namespace, foreign namespaces and cluster objects new since the baseline.

    ``keep_namespaces`` are the lane's infrastructure and application namespaces; they are never removed.
    """
    doomed = set()
    for identity in current:
        name = object_name(identity)
        if object_kind(identity) == NAMESPACE_KIND:
            if name == control_namespace or name not in keep_namespaces:
                doomed.add(identity)
        elif identity not in baseline:
            doomed.add(identity)
    return frozenset(doomed)


def _deletion_order(identity: str) -> tuple[int, str]:
    return CLUSTER_KINDS.index(object_kind(identity)), identity


@dataclass(frozen=True)
class Tier1Receipt:
    seconds: float
    removed: tuple[str, ...]
    inventory_bootstrapped: bool


def run_tier1(
    cluster: LaneCluster,
    *,
    inventory: Path,
    keep_namespaces: Collection[str],
    control_namespace: str,
    provision_credentials: Callable[[str], None],
    reseed: Callable[[], None],
    forget_controller: Callable[[], None],
    timeout_seconds: float,
    poll_seconds: float = 1.0,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> Tier1Receipt:
    """Wipe what the harness and SDO own on this lane, then re-provision and re-seed SDO's inputs.

    A lane with no recorded inventory adopts its current cluster-scoped objects as its baseline after the
    namespaces are wiped; ``fastloop up`` records one for every new lane.
    """
    started = clock()
    current = cluster.list_objects()
    baseline = load_inventory(inventory)
    bootstrapped = baseline is None
    if baseline is None:
        baseline = cluster_scoped(current)
    removed = remaining = harness_leftovers(
        current, baseline, keep_namespaces=keep_namespaces, control_namespace=control_namespace
    )
    deadline = started + timeout_seconds
    requested = False
    error = ""
    while remaining:
        if not requested:
            error = ""
            for identity in sorted(remaining, key=_deletion_order):
                try:
                    cluster.delete(identity)
                except Exception as exc:  # retried until the deadline
                    error = f"delete {identity}: {type(exc).__name__}: {str(exc)[:200]}"
            requested = not error
        try:
            remaining &= cluster.list_objects()
        except Exception as exc:
            error = f"list cluster objects: {type(exc).__name__}: {str(exc)[:200]}"
        if not remaining or clock() >= deadline:
            break
        sleep(poll_seconds)
    if remaining:
        raise WipeError(
            "eval: tier-1 wipe could not remove: " + ", ".join(sorted(remaining)) + (f" ({error})" if error else "")
        )
    if bootstrapped:
        save_inventory(inventory, baseline)
    provision_credentials(control_namespace)
    reseed()
    forget_controller()
    return Tier1Receipt(round(clock() - started, 3), tuple(sorted(removed)), bootstrapped)


def wipe_environment(
    environment: FastloopEnvironment,
    cluster: LaneCluster,
    *,
    applications: Collection[str],
    provision_credentials: Callable[[str], None],
    planned_commit: str,
    timeout_seconds: float,
) -> Tier1Receipt:
    """Tier 1 for one fastloop lane: its SDO control namespace, harness leftovers, seed and controller registry."""
    from benchmarks.sregym.adapter import allowed_namespaces, control_namespace_for

    control = control_namespace_for(environment.namespace)
    return run_tier1(
        cluster,
        inventory=inventory_path(environment.kubeconfig),
        keep_namespaces=allowed_namespaces(set(applications)) - {control},
        control_namespace=control,
        provision_credentials=provision_credentials,
        reseed=lambda: reseed_workspace(environment.workspace, planned_commit),
        forget_controller=lambda: environment.state_path.unlink(missing_ok=True),
        timeout_seconds=timeout_seconds,
    )


class AttemptMarker:
    """Present from the moment a row's attempt starts until it ends cleanly; a survivor means an unclean attempt."""

    def __init__(self, run_dir: Path) -> None:
        self.path = run_dir / MARKER_NAME

    def unclean(self) -> bool:
        return self.path.exists()

    def begin(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(f"{os.getpid()}\n", encoding="utf-8")

    def end_cleanly(self) -> None:
        self.path.unlink(missing_ok=True)


def tier2_reason(marker: AttemptMarker, restore_invariant: Callable[[], None]) -> str | None:
    """Why the application must be redeployed, or ``None`` when the warm app stays.

    The invariant raises ``ValueError``/``RuntimeError`` when the app no longer matches its captured baseline.
    """
    if marker.unclean():
        return "previous attempt did not end cleanly"
    try:
        restore_invariant()
    except (ValueError, RuntimeError) as exc:
        return f"restore invariant failed: {exc}"
    return None


@dataclass(frozen=True)
class WipeReceipt:
    tier1_seconds: float
    tier1_removed: tuple[str, ...]
    tier1_inventory_bootstrapped: bool
    tier2_ran: bool
    tier2_seconds: float
    tier2_reason: str | None

    def write(self, directory: Path) -> Path:
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / RECEIPT_NAME
        path.write_text(json.dumps(asdict(self), indent=2) + "\n", encoding="utf-8")
        return path


def run_tier2(
    reason: str | None,
    redeploy: Callable[[], None],
    tier1: Tier1Receipt,
    *,
    clock: Callable[[], float] = time.monotonic,
) -> WipeReceipt:
    """Redeploy the application only when ``reason`` names a trigger; return the row's receipt."""
    started = clock()
    if reason is not None:
        redeploy()
    return WipeReceipt(
        tier1.seconds,
        tier1.removed,
        tier1.inventory_bootstrapped,
        reason is not None,
        round(clock() - started, 3) if reason is not None else 0.0,
        reason,
    )


class KubectlLaneCluster:
    def __init__(self, kubeconfig: Path) -> None:
        if not isinstance(kubeconfig, Path) or not kubeconfig.is_absolute():
            raise ValueError("eval: lane wipe requires an absolute kubeconfig path")
        self._kubeconfig = kubeconfig

    def list_objects(self) -> frozenset[str]:
        from benchmarks.sregym.adapter import kubectl

        output = kubectl(
            ["get", ",".join(CLUSTER_KINDS), "-o", "name"], namespace=None, kubeconfig=self._kubeconfig
        ).stdout
        return frozenset(line.strip() for line in output.splitlines() if line.strip())

    def delete(self, identity: str) -> None:
        from benchmarks.sregym.adapter import kubectl

        kubectl(
            ["delete", identity, "--ignore-not-found=true", "--wait=false"],
            namespace=None,
            kubeconfig=self._kubeconfig,
        )


def credentials_hook(command: Path, kubeconfig: Path) -> Callable[[str], None]:
    """Re-provision SDO credentials by reference: the lane keeper's executable gets the namespace, never a value."""

    def provision(control_namespace: str) -> None:
        completed = subprocess.run(
            [str(command), control_namespace],
            env={**os.environ, "KUBECONFIG": str(kubeconfig)},
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode:
            raise WipeError(
                f"eval: credentials hook {command} failed for {control_namespace} (exit {completed.returncode}): "
                f"{completed.stderr.strip()[-300:]}"
            )

    return provision


def reseed_workspace(workspace: Path, planned_commit: str) -> None:
    """Put the accepted repository at the row's planned commit; nothing newer survives."""
    for argv in (
        ["cat-file", "-e", f"{planned_commit}^{{commit}}"],
        ["reset", "--hard", planned_commit],
        ["clean", "-fd"],
    ):
        completed = subprocess.run(["git", "-C", str(workspace), *argv], capture_output=True, text=True, check=False)
        if completed.returncode:
            raise WipeError(
                f"eval: cannot seed {workspace} at {planned_commit}: git {argv[0]}: {completed.stderr.strip()}"
            )
