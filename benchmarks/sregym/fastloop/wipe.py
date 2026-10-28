"""Row-start reset in two tiers: SDO state is always wiped, the application only when it must be.

Tier 1 runs at every row start. It deletes every namespace and cluster-scoped object that is not the
lane's own (the harness leftovers of earlier attempts) and SDO's control namespace, then re-provisions
credentials by reference, re-seeds the accepted repository at the row's planned commit and forgets the
controller registry. SDO is reinstalled afterwards by the normal native install. Nothing SDO wrote
survives except the memory commit itself, so no leftover needs its own cleanup rule.

A namespace the wipe deletes may stay Terminating because an injected or harness object in it keeps a
finalizer nobody will ever run (a fault's cleanup token). Those objects are the wipe's own state, so the
wipe removes their finalizers, within the same deadline, and records each one in the receipt.

Tier 2 concerns the application namespaces. Incoming data faults, last lane-injected data faults and
unknown lane injection state always recreate the app. Otherwise the warm app is kept when the previous
row recovered cleanly, the restore invariant passes and every app pod is healthy.
Otherwise the app namespaces are deleted, PVCs and the data in them included, and the app is deployed
again from the image cache: fault state that lives in a database is not a Kubernetes object, so no
restore of objects can undo it.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
from dataclasses import asdict, dataclass
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from benchmarks.sregym.adapter import WaitKind, wait_for
from benchmarks.sregym.fastloop.host_limits import HostLimits
from benchmarks.sregym.fastloop.records import Injected, NotInjected

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Iterable, Mapping, Sequence

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

    def blocking_objects(self, namespace: str) -> tuple[str, ...]:
        """Objects (``kind/name``) holding finalizers in a namespace that is Terminating and reports remaining content.

        Empty for a namespace that is not stuck, so a namespace still draining pods is left alone.
        """
        ...

    def remove_finalizers(self, namespace: str, identity: str) -> None:
        """Clear the finalizers of one object in ``namespace``; an already absent object is not an error."""
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
    force_finalized: tuple[str, ...]


def release_stuck_namespaces(cluster: LaneCluster, remaining: Collection[str], released: list[str]) -> tuple[str, ...]:
    """Clear finalizers on the objects holding the remaining namespaces Terminating; return what still blocks.

    Only namespace identities in ``remaining`` are touched, and ``remaining`` holds only what this wipe asked to
    delete, so a namespace outside the lane's owned set is never reached. Each release is appended to ``released``
    as ``namespace/kind/name``.
    """
    blocking: list[str] = []
    for identity in sorted(remaining):
        if object_kind(identity) != NAMESPACE_KIND:
            continue
        namespace = object_name(identity)
        for item in cluster.blocking_objects(namespace):
            cluster.remove_finalizers(namespace, item)
            owned = f"{namespace}/{item}"
            if owned not in released:
                released.append(owned)
            blocking.append(owned)
    return tuple(blocking)


def remove_until_gone(
    cluster: LaneCluster,
    identities: Collection[str],
    *,
    timeout_seconds: float,
    poll_seconds: float,
    clock: Callable[[], float],
    sleep: Callable[[float], None],
) -> tuple[str, ...]:
    """Delete in webhook, API, RBAC, namespace order and wait until all are gone; fail closed at the deadline.

    A namespace that stays Terminating on objects with finalizers has them released. Returns the objects released
    (``namespace/kind/name``); the failure names whatever still blocks.
    """
    remaining = frozenset(identities)
    deadline = clock() + timeout_seconds
    requested = False
    error = ""
    released: list[str] = []
    blocking: tuple[str, ...] = ()
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
        if remaining and requested:
            try:
                blocking = release_stuck_namespaces(cluster, remaining, released)
            except Exception as exc:  # retried until the deadline
                error = f"release finalizers: {type(exc).__name__}: {str(exc)[:200]}"
        if not remaining or clock() >= deadline:
            break
        sleep(poll_seconds)
    if remaining:
        if blocking:
            error = (error + "; " if error else "") + "still blocking after finalizer removal: " + ", ".join(blocking)
        raise WipeError(
            "eval: wipe could not remove: " + ", ".join(sorted(remaining)) + (f" ({error})" if error else "")
        )
    return tuple(released)


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
    poll_seconds: float = wait_for(WaitKind.SAMPLE),
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
    removed = harness_leftovers(current, baseline, keep_namespaces=keep_namespaces, control_namespace=control_namespace)
    force_finalized = remove_until_gone(
        cluster, removed, timeout_seconds=timeout_seconds, poll_seconds=poll_seconds, clock=clock, sleep=sleep
    )
    if bootstrapped:
        save_inventory(inventory, baseline)
    provision_credentials(control_namespace)
    reseed()
    forget_controller()
    return Tier1Receipt(round(clock() - started, 3), tuple(sorted(removed)), bootstrapped, force_finalized)


def wipe_environment(
    environment: FastloopEnvironment,
    cluster: LaneCluster,
    *,
    applications: Collection[str],
    provision_credentials: Callable[[str], None],
    planned_commit: str,
    timeout_seconds: float,
    provenance_path: Path | None = None,
) -> Tier1Receipt:
    """Tier 1 for one fastloop lane: its SDO control namespace, harness leftovers, seed and controller registry."""
    from benchmarks.sregym.adapter import allowed_namespaces, control_namespace_for
    from benchmarks.sregym.fastloop.parallel_memory import memory_head
    from benchmarks.sregym.fastloop.reset_provenance import publish_reset_generation

    if provenance_path is not None and provenance_path.exists():
        raise WipeError("eval: reset generation already published; refusing to reset an existing attempt")
    before = memory_head(environment.workspace) if provenance_path is not None else None
    control = control_namespace_for(environment.namespace)
    receipt = run_tier1(
        cluster,
        inventory=inventory_path(environment.kubeconfig),
        keep_namespaces=allowed_namespaces(set(applications)) - {control},
        control_namespace=control,
        provision_credentials=provision_credentials,
        reseed=lambda: reseed_workspace(environment.workspace, planned_commit),
        forget_controller=lambda: environment.state_path.unlink(missing_ok=True),
        timeout_seconds=timeout_seconds,
    )
    if provenance_path is not None and before is not None:
        publish_reset_generation(provenance_path, environment.workspace, planned_commit, before)
    return receipt


class Tier2Trigger(StrEnum):
    UNCLEAN_ATTEMPT = "unclean_attempt"
    PREVIOUS_ROW_UNRECOVERED = "previous_row_unrecovered"
    CURRENT_ROW_DATA_FAULTS = "current_row_data_faults"
    LANE_LAST_INJECTION_DATA_FAULTS = "lane_last_injection_data_faults"
    LANE_INJECTION_UNKNOWN = "lane_injection_unknown"
    RESTORE_INVARIANT = "restore_invariant"
    PODS_UNHEALTHY = "pods_unhealthy"


@dataclass(frozen=True)
class Tier2Reason:
    trigger: Tier2Trigger
    detail: str
    # Catalog-classified data-level faults causing this decision.
    data_faults: tuple[str, ...] = ()


UNRECOVERED_PREFIX = "unrecovered: "


class AttemptMarker:
    """Present from the moment a row's attempt starts until it ends with every injected row recovered.

    A survivor with no content means the attempt died; one that names rows means they were injected and
    the harness could not verify their recovery. Either makes the next row recreate the application.
    Data-level fault history is owned separately by LaneInjectionState.
    """

    def __init__(self, run_dir: Path) -> None:
        self.path = run_dir / MARKER_NAME

    def pending(self) -> Tier2Reason | None:
        try:
            content = self.path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        lines = content.splitlines()
        rows = next((line[len(UNRECOVERED_PREFIX) :] for line in lines if line.startswith(UNRECOVERED_PREFIX)), "")
        if rows.strip():
            return Tier2Reason(
                Tier2Trigger.PREVIOUS_ROW_UNRECOVERED,
                f"previous row injected and not verified recovered: {rows.strip()}",
            )
        return Tier2Reason(Tier2Trigger.UNCLEAN_ATTEMPT, "previous attempt did not end cleanly")

    def begin(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(f"{os.getpid()}\n", encoding="utf-8")

    def end(self, unrecovered: Collection[str]) -> None:
        """The run ended: forget the attempt unless it left unrecovered rows."""
        if not unrecovered:
            self.path.unlink(missing_ok=True)
            return
        lines = [str(os.getpid())]
        if unrecovered:
            lines.append(f"{UNRECOVERED_PREFIX}{', '.join(unrecovered)}")
        self.path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _attempted(record: Any) -> bool:
    injection = record.injection
    return isinstance(injection, Injected) or (isinstance(injection, NotInjected) and injection.stage == "injection")


def unrecovered_rows(records: Iterable[Any]) -> tuple[str, ...]:
    """Problem ids of rows whose fault was injected, or may have been, without a verified full recovery."""
    return tuple(
        record.problem_id
        for record in records
        if _attempted(record) and (record.recovery_error is not None or not record.full_cleanup_verified)
    )


class LaneInjectionState:
    """Durable lane facts written at the injection boundary, never reconstructed from result journals.

    Missing, malformed and pending records are unknown. Write pending before the worker can mutate the
    app: a killed caller or a partial injection cannot leave a stale clean record behind.
    """

    def __init__(self, kubeconfig: Path, namespace: str) -> None:
        if not namespace or "/" in namespace or "\\" in namespace:
            raise ValueError("lane injection state requires an application namespace")
        self.path = kubeconfig.with_name(f"{kubeconfig.name}.{namespace}.last-injection.json")

    def _write(self, problems: Sequence[str], faults: Sequence[str], *, complete: bool) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(self.path.name + ".tmp")
        temporary.write_text(
            json.dumps(
                {
                    "version": 1,
                    "complete": complete,
                    "problems": list(problems),
                    "data_faults": list(faults),
                }
            )
            + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, self.path)

    def begin(self, problems: Sequence[str]) -> None:
        self._write(problems, (), complete=False)

    def finish(self, problems: Sequence[str], catalog: Mapping[str, Sequence[str]]) -> None:
        if not all(problem in catalog for problem in problems):
            return  # An undescribed injection has unknown data effects.
        self._write(
            problems, tuple(f"{problem}: {fault}" for problem in problems for fault in catalog[problem]), complete=True
        )

    def reason(self) -> Tier2Reason | None:
        unknown = Tier2Reason(Tier2Trigger.LANE_INJECTION_UNKNOWN, "lane's last injection state is unknown")
        try:
            document = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return unknown
        if (
            not isinstance(document, dict)
            or type(document.get("version")) is not int
            or document.get("version") != 1
            or document.get("complete") is not True
        ):
            return unknown
        problems, faults = document.get("problems"), document.get("data_faults")
        if (
            not isinstance(problems, list)
            or not problems
            or not all(isinstance(item, str) and item for item in problems)
            or not isinstance(faults, list)
            or not all(isinstance(item, str) and item for item in faults)
        ):
            return unknown
        if faults:
            return Tier2Reason(
                Tier2Trigger.LANE_LAST_INJECTION_DATA_FAULTS,
                "lane's last injection changed application data: " + ", ".join(faults),
                tuple(faults),
            )
        return None


def await_healthy_pods(
    unhealthy: Callable[[], Sequence[str]],
    *,
    timeout_seconds: float,
    poll_seconds: float,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> list[str]:
    """Pods still unhealthy when the short bound ends; empty as soon as every pod is healthy."""
    deadline = clock() + timeout_seconds
    while True:
        stuck = list(unhealthy())
        if not stuck or clock() >= deadline:
            return stuck
        sleep(poll_seconds)


def pod_problems(listing: dict[str, Any]) -> list[str]:
    """``namespace/pod (why)`` for each pod of a ``kubectl get pods -o json`` listing that is not serving.

    Finished pods (phase Succeeded) and pods already being deleted are not problems.
    """
    problems = []
    for item in listing["items"]:
        metadata, status = item["metadata"], item.get("status", {})
        phase = status.get("phase", "Unknown")
        if phase == "Succeeded" or metadata.get("deletionTimestamp"):
            continue
        ready = any(c.get("type") == "Ready" and c.get("status") == "True" for c in status.get("conditions", []))
        if phase == "Running" and ready:
            continue
        waiting = [
            c["state"]["waiting"].get("reason", "")
            for c in status.get("containerStatuses", [])
            if c.get("state", {}).get("waiting")
        ]
        why = next((reason for reason in waiting if reason), phase if phase != "Running" else "NotReady")
        problems.append(f"{metadata.get('namespace', '')}/{metadata['name']} ({why})")
    return sorted(problems)


def tier2_reason(
    marker: AttemptMarker,
    restore_invariant: Callable[[], None],
    unhealthy_pods: Callable[[], Sequence[str]],
    *,
    lane_state: LaneInjectionState | None = None,
    current_data_faults: Sequence[str] = (),
) -> Tier2Reason | None:
    """Why the application must be recreated, or ``None`` when the warm app stays.

    The invariant raises ``ValueError``/``RuntimeError`` when the app no longer matches its captured
    baseline. ``unhealthy_pods`` waits its own short bound and returns the pods still not serving.
    """
    if current_data_faults:
        return Tier2Reason(
            Tier2Trigger.CURRENT_ROW_DATA_FAULTS,
            "incoming row changes application data: " + ", ".join(current_data_faults),
            tuple(current_data_faults),
        )
    if lane_state is not None:
        history = lane_state.reason()
        if history is not None:
            return history
    pending = marker.pending()
    if pending is not None:
        return pending
    try:
        restore_invariant()
    except (ValueError, RuntimeError) as exc:
        return Tier2Reason(Tier2Trigger.RESTORE_INVARIANT, f"restore invariant failed: {exc}")
    stuck = unhealthy_pods()
    if stuck:
        return Tier2Reason(Tier2Trigger.PODS_UNHEALTHY, "pods unhealthy after the restore: " + ", ".join(stuck))
    return None


@dataclass(frozen=True)
class WipeReceipt:
    tier1_seconds: float
    tier1_removed: tuple[str, ...]
    tier1_inventory_bootstrapped: bool
    tier2_ran: bool
    tier2_seconds: float
    tier2_trigger: str | None
    tier2_reason: str | None
    tier2_namespaces: tuple[str, ...]
    force_finalized: tuple[str, ...]
    previous_row_data_faults: tuple[str, ...]
    current_row_data_faults: tuple[str, ...]

    def write(self, directory: Path) -> Path:
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / RECEIPT_NAME
        path.write_text(json.dumps(asdict(self), indent=2) + "\n", encoding="utf-8")
        return path


def run_tier2(
    reason: Tier2Reason | None,
    tier1: Tier1Receipt,
    *,
    cluster: LaneCluster,
    namespaces: Collection[str],
    deploy: Callable[[], None],
    host_limits: HostLimits | None = None,
    timeout_seconds: float,
    poll_seconds: float = wait_for(WaitKind.SAMPLE),
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> WipeReceipt:
    """Recreate the application only when ``reason`` names a trigger; return the row's receipt.

    The application namespaces are deleted whole, so PVCs and the data in them go too, and ``deploy``
    then installs the app from the image cache into a namespace that does not exist.
    """
    started = clock()
    recreated: tuple[str, ...] = ()
    force_finalized = tier1.force_finalized
    if reason is not None:
        with (host_limits or HostLimits.from_env()).admit("redeploy"):
            recreated = tuple(sorted(namespaces))
            force_finalized += remove_until_gone(
                cluster,
                [namespace_identity(name) for name in recreated],
                timeout_seconds=timeout_seconds,
                poll_seconds=poll_seconds,
                clock=clock,
                sleep=sleep,
            )
            deploy()
    return WipeReceipt(
        tier1.seconds,
        tier1.removed,
        tier1.inventory_bootstrapped,
        reason is not None,
        round(clock() - started, 3) if reason is not None else 0.0,
        reason.trigger.value if reason is not None else None,
        reason.detail if reason is not None else None,
        recreated,
        force_finalized,
        reason.data_faults
        if reason is not None and reason.trigger is Tier2Trigger.LANE_LAST_INJECTION_DATA_FAULTS
        else (),
        reason.data_faults if reason is not None and reason.trigger is Tier2Trigger.CURRENT_ROW_DATA_FAULTS else (),
    )


# Namespace conditions the API server sets while a deleted namespace still has content it cannot remove.
STUCK_CONDITIONS = frozenset(
    {"NamespaceFinalizersRemaining", "NamespaceContentRemaining", "NamespaceDeletionContentFailure"}
)


def namespace_is_stuck(namespace: dict[str, Any]) -> bool:
    """True for a Terminating namespace whose deletion reports remaining finalizers or content."""
    if not namespace.get("metadata", {}).get("deletionTimestamp"):
        return False
    return any(
        condition.get("type") in STUCK_CONDITIONS and condition.get("status") == "True"
        for condition in namespace.get("status", {}).get("conditions", [])
    )


def finalizer_holders(listing: dict[str, Any]) -> tuple[str, ...]:
    """``kind/name`` of each object of a ``kubectl get ... -o json`` listing that carries finalizers."""
    return tuple(
        sorted(
            f"{item['kind'].lower()}/{item['metadata']['name']}"
            for item in listing.get("items", [])
            if item.get("metadata", {}).get("finalizers")
        )
    )


class KubectlLaneCluster:
    def __init__(self, kubeconfig: Path) -> None:
        if not isinstance(kubeconfig, Path) or not kubeconfig.is_absolute():
            raise ValueError("eval: lane wipe requires an absolute kubeconfig path")
        self._kubeconfig = kubeconfig
        self._namespaced_kinds: str | None = None

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

    def blocking_objects(self, namespace: str) -> tuple[str, ...]:
        from benchmarks.sregym.adapter import kubectl

        described = kubectl(
            ["get", "namespace", namespace, "-o", "json", "--ignore-not-found=true"],
            namespace=None,
            kubeconfig=self._kubeconfig,
        ).stdout
        if not described.strip() or not namespace_is_stuck(json.loads(described)):
            return ()
        if self._namespaced_kinds is None:
            kinds = kubectl(
                ["api-resources", "--verbs=list", "--namespaced=true", "-o", "name"],
                namespace=None,
                kubeconfig=self._kubeconfig,
            ).stdout
            self._namespaced_kinds = ",".join(line.strip() for line in kinds.splitlines() if line.strip())
        # Some kinds (aggregated APIs) may not list; a partial listing still names the holders.
        listing = kubectl(
            ["get", self._namespaced_kinds, "-o", "json", "--ignore-not-found=true"],
            namespace=namespace,
            kubeconfig=self._kubeconfig,
            check=False,
        ).stdout
        return finalizer_holders(json.loads(listing)) if listing.strip() else ()

    def remove_finalizers(self, namespace: str, identity: str) -> None:
        from benchmarks.sregym.adapter import kubectl

        kubectl(
            ["patch", identity, "--type=merge", "-p", '{"metadata":{"finalizers":null}}'],
            namespace=namespace,
            kubeconfig=self._kubeconfig,
            check=False,
        )


def application_pod_problems(kubeconfig: Path, namespaces: Collection[str]) -> list[str]:
    """The pods of the application namespaces that are not serving right now."""
    from benchmarks.sregym.adapter import kubectl

    problems: list[str] = []
    for namespace in namespaces:
        listing = kubectl(["get", "pods", "-o", "json"], namespace=namespace, kubeconfig=kubeconfig).stdout
        problems.extend(pod_problems(json.loads(listing)))
    return problems


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
