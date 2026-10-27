"""Durable record of what a fault's setup put on a reused cluster, so the next row removes it.

A fault may install infrastructure the app baseline never covers: a Helm release in its own
namespace with its ClusterRoles, IngressClass and webhooks. The SREGym worker removes what an
injection created, but only from its own memory and only when full recovery runs. An attempt that
aborts before or during injection (a killed worker, a timeout, a stopped line) leaves those objects
on the lane, and the entry check then refuses every later row.

The window owner is this ledger. The worker opens it right before a fault's setup runs, records the
objects the setup created when it returns, and closes it only after full recovery. Any other exit
leaves it open on disk beside the lane's kubeconfig, and the next row's entry replays it before
the foreign-namespace check. Entries are kind/name strings from ``kubectl get -o name``; no value
is ever read.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from benchmarks.sregym.adapter import WaitKind, kubectl, wait_for

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Iterable

# kubectl resource names, in deletion order: admission and API registrations first so nothing routes
# to a backend being removed, RBAC before the namespaces whose service accounts it binds, and
# namespaces last, since deleting one removes everything inside it.
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
LEDGER_SUFFIX = ".setup-ledger.json"
REMOVAL_TIMEOUT_SECONDS = wait_for(WaitKind.ROLLOUT)


class SetupLedgerError(RuntimeError):
    """A fault's leftover objects could not be read, removed or confirmed gone."""


def object_kind(identity: str) -> str:
    """``clusterrole.rbac.authorization.k8s.io/x`` -> ``clusterrole``."""
    return identity.partition("/")[0].partition(".")[0]


def object_name(identity: str) -> str:
    return identity.partition("/")[2]


class ClusterInventory(Protocol):
    def list_objects(self) -> frozenset[str]:
        """Every object of ``CLUSTER_KINDS``; raises when any kind cannot be listed."""
        ...

    def delete(self, identity: str) -> None:
        """Request deletion; an already absent object is not an error."""
        ...


class KubectlClusterInventory:
    def __init__(self, kubeconfig: Path) -> None:
        if not isinstance(kubeconfig, Path) or not kubeconfig.is_absolute():
            raise ValueError("eval: setup ledger inventory requires an absolute kubeconfig path")
        self._kubeconfig = kubeconfig

    def list_objects(self) -> frozenset[str]:
        output = kubectl(
            ["get", ",".join(CLUSTER_KINDS), "-o", "name"], namespace=None, kubeconfig=self._kubeconfig
        ).stdout
        return frozenset(line.strip() for line in output.splitlines() if line.strip())

    def delete(self, identity: str) -> None:
        kubectl(
            ["delete", identity, "--ignore-not-found=true", "--wait=false"],
            namespace=None,
            kubeconfig=self._kubeconfig,
        )


def ledger_path(kubeconfig: Path) -> Path:
    return kubeconfig.with_name(kubeconfig.name + LEDGER_SUFFIX)


@dataclass(frozen=True)
class SetupWindow:
    """An open window: ``before`` is the lane's inventory when setup began.

    ``created`` is ``None`` while setup is running or was cut off, and the objects it created once
    it returned.
    """

    before: frozenset[str]
    created: frozenset[str] | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.before, frozenset):
            raise TypeError("before must be a frozenset of object identities")
        if self.created is not None and not isinstance(self.created, frozenset):
            raise TypeError("created must be a frozenset of object identities or None")

    def leftovers(self, current: Iterable[str], protected_namespaces: Collection[str]) -> frozenset[str]:
        """What to remove now: exactly ``created`` when known, else everything new since ``before``.

        A protected namespace (the lane's infrastructure, app and SDO namespaces) is never removed.
        """
        current = frozenset(current)
        candidates = (self.created if self.created is not None else current - self.before) & current
        return frozenset(
            identity
            for identity in candidates
            if not (object_kind(identity) == NAMESPACE_KIND and object_name(identity) in protected_namespaces)
        )


class SetupLedger:
    def __init__(self, path: Path) -> None:
        if not isinstance(path, Path) or not path.is_absolute():
            raise ValueError("eval: setup ledger needs an absolute path")
        self.path = path

    def window(self) -> SetupWindow | None:
        try:
            document = json.loads(self.path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except (OSError, ValueError) as exc:
            raise SetupLedgerError(f"eval: setup ledger {self.path} is unreadable: {exc}") from exc
        try:
            created = document["created"]
            return SetupWindow(
                before=frozenset(document["before"]), created=None if created is None else frozenset(created)
            )
        except (KeyError, TypeError) as exc:
            raise SetupLedgerError(f"eval: setup ledger {self.path} is malformed: {exc!r}") from exc

    def open(self, before: Iterable[str], *, extend: bool = False) -> None:
        """Start a window. With ``extend`` an open one keeps its original ``before`` (a composition);
        otherwise a window left by an earlier attempt is replaced by this one."""
        if not extend or self.window() is None:
            self._write(SetupWindow(before=frozenset(before)))

    def record_created(self, after: Iterable[str]) -> None:
        window = self.window()
        if window is None:
            raise SetupLedgerError("eval: no open setup window to record created objects in")
        self._write(SetupWindow(before=window.before, created=frozenset(after) - window.before))

    def close(self) -> None:
        self.path.unlink(missing_ok=True)

    def _write(self, window: SetupWindow) -> None:
        document: dict[str, Any] = {
            "before": sorted(window.before),
            "created": None if window.created is None else sorted(window.created),
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(self.path.name + ".tmp")
        temporary.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
        os.replace(temporary, self.path)


def _deletion_order(identity: str) -> tuple[int, str]:
    return CLUSTER_KINDS.index(object_kind(identity)), identity


def replay(
    ledger: SetupLedger,
    inventory: ClusterInventory,
    *,
    protected_namespaces: Collection[str],
    timeout_seconds: float = REMOVAL_TIMEOUT_SECONDS,
    poll_seconds: float = wait_for(WaitKind.SAMPLE),
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> frozenset[str]:
    """Remove what an open window's setup left, wait until it is gone, then close the window.

    Returns what was removed (empty without a window). Raises ``SetupLedgerError`` naming what
    could not be removed; the window then stays open for the next attempt.
    """
    window = ledger.window()
    if window is None:
        return frozenset()
    removed = remaining = window.leftovers(inventory.list_objects(), protected_namespaces)
    deadline = clock() + timeout_seconds
    error = ""
    requested = False
    while remaining:
        if not requested:
            error = ""
            for identity in sorted(remaining, key=_deletion_order):
                try:
                    inventory.delete(identity)
                except Exception as exc:  # retried until the deadline
                    error = f"delete {identity}: {type(exc).__name__}: {str(exc)[:200]}"
            requested = not error
        try:
            remaining &= inventory.list_objects()
        except Exception as exc:
            error = f"list cluster objects: {type(exc).__name__}: {str(exc)[:200]}"
        if not remaining or clock() >= deadline:
            break
        sleep(poll_seconds)
    if remaining:
        raise SetupLedgerError(
            "eval: objects an earlier fault setup left on this lane could not be removed: "
            + ", ".join(sorted(remaining))
            + (f" ({error})" if error else "")
        )
    ledger.close()
    return removed


def run_in_window(
    ledger: SetupLedger,
    inventory: ClusterInventory,
    setup: Callable[[], dict[str, Any]],
    *,
    compose: bool = False,
) -> dict[str, Any]:
    """Run one fault setup so that no exit path forgets what it installed.

    The window opens before ``setup`` and holds what it created once it returns, whether or not it
    reports a failed injection. If ``setup`` raises or the process dies, the window stays open with
    no ``created`` set and the next row's replay removes everything new since it opened.
    """
    ledger.open(inventory.list_objects(), extend=compose)
    reply = setup()
    ledger.record_created(inventory.list_objects())
    return reply


def close_after_full_recovery(ledger: SetupLedger, request: dict[str, Any], reply: dict[str, Any]) -> bool:
    """Close the window once the worker reports every fault recovered and every created object gone."""
    recovered = (
        request.get("op") == "recover"
        and request.get("fault") is None
        and reply.get("success") is True
        and not reply.get("pending")
        and not reply.get("errors")
    )
    if recovered:
        ledger.close()
    return recovered
