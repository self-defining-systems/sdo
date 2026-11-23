"""Start a row with fresh app-owned trace stores.

Several apps ship an in-memory trace store (Jaeger all-in-one) with no bound on the spans it keeps, so it grows for as
long as the app runs: a lane that sat idle before its first row measured 2.6 GB 38 minutes after the app came up (the
hotel app; about 70 MB a minute with its load generator), past the admission budget, and the row failed before it
injected. A freshly deployed app has an empty store, which is what every row is meant to start from, so the
row-start restore replaces these pods: it must happen before SDO's install, because onboarding records the
namespace's Pods by UID.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from benchmarks.sregym.adapter import WaitKind, is_in_memory_trace_store, kubectl, wait_for

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
    from pathlib import Path

logger = logging.getLogger(__name__)


#: A warm start replaces only a trace store older than this. The row's end replaces it (without waiting), so a lane
#: whose next row follows finds a young store that has been Ready for longer than the baseline check's sustained-health
#: window, instead of a fresh pod that makes that check wait the whole window; a store left growing on an idle lane
#: (about 70 MB/min against a 2 GiB budget) is older and still replaced.
TRACE_STORE_MAX_AGE_SECONDS = 600.0


def restart_in_memory_trace_stores(
    kubeconfig: Path,
    namespaces: Iterable[str],
    *,
    older_than: float | None = None,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    reason: str = "row start",
) -> list[str]:
    """Delete the pods of every in-memory trace store of the app namespaces (their controllers recreate them empty);
    with ``older_than``, only the pods created more than that many seconds ago.

    Returns the ``namespace/pod`` names. It does not wait: the row's own readiness waits (the restore's health loop, the
    setup's healthy-pods wait) cover the replacement, which overlaps the work that follows.
    """
    restarted: list[str] = []
    for namespace in namespaces:
        listing = json.loads(
            kubectl(["get", "pods", "-o", "json"], namespace=namespace, kubeconfig=kubeconfig).stdout or "{}"
        )
        for pod in listing.get("items", []):
            metadata, spec = pod.get("metadata", {}), pod.get("spec", {})
            if metadata.get("deletionTimestamp") or not any(
                is_in_memory_trace_store(str(container.get("image", ""))) for container in spec.get("containers", [])
            ):
                continue
            if older_than is not None:
                created = metadata.get("creationTimestamp")
                born = datetime.fromisoformat(str(created).replace("Z", "+00:00")) if created else None
                if born is not None and (now() - born).total_seconds() <= older_than:
                    continue
            kubectl(
                ["delete", "pod", metadata["name"], "--ignore-not-found=true", "--wait=false"],
                namespace=namespace,
                kubeconfig=kubeconfig,
            )
            restarted.append(f"{namespace}/{metadata['name']}")
    if restarted:
        logger.info("%s: replaced in-memory trace stores %s", reason, ", ".join(restarted))
    return restarted


def await_trace_stores_ready(
    kubeconfig: Path,
    namespaces: Iterable[str],
    *,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Wait until every in-memory trace store of the namespaces is Ready and none is being deleted.

    A path that checks the app right after its row-start work (the Tier-2 decision reads the pods) calls this first, so
    the replacement it started does not read as an unhealthy application."""
    deadline = monotonic() + wait_for(WaitKind.ROLLOUT)
    pending: list[str] = []
    while True:
        pending = []
        for namespace in namespaces:
            listing = json.loads(
                kubectl(["get", "pods", "-o", "json"], namespace=namespace, kubeconfig=kubeconfig).stdout or "{}"
            )
            for pod in listing.get("items", []):
                metadata = pod.get("metadata", {})
                if not any(
                    is_in_memory_trace_store(str(container.get("image", "")))
                    for container in pod.get("spec", {}).get("containers", [])
                ):
                    continue
                conditions = pod.get("status", {}).get("conditions", [])
                ready = any(row.get("type") == "Ready" and row.get("status") == "True" for row in conditions)
                if metadata.get("deletionTimestamp") or not ready:
                    pending.append(f"{namespace}/{metadata.get('name')}")
        if not pending:
            return
        if monotonic() >= deadline:
            raise TimeoutError(
                "in-memory trace stores not ready after the row-start replacement: " + ", ".join(pending)
            )
        sleep(wait_for(WaitKind.POLL))
