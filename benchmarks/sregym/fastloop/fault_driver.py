"""Fault injection, grading, and recovery through the SREGym fast-loop worker, one fault or a composition."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Protocol

from benchmarks.sregym.fastloop.baseline_recovery import ApplicationBaseline, RecoveryCleanup, reconcile_application
from benchmarks.sregym.fastloop.loop import InjectionWindow
from benchmarks.sregym.fastloop.records import OracleVerdict
from benchmarks.sregym.protocol.injection import FaultComponent, InvalidInjectionError, check_injection

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from benchmarks.sregym.fastloop.baseline_recovery import ApplicationCluster

HEALTH_POLL_SECONDS = 1.0
#: The oracle waits up to 60 s for rollouts itself; injection and recovery wait on rollouts too.
WORKER_OP_TIMEOUT_SECONDS = 900.0
SEED_RECOVERY_CONTRACT = "seed-compatible/v1"


class FastloopFaultError(RuntimeError):
    """Raised when a fault cannot be recovered to a healthy application."""


class WorkerRequester(Protocol):
    def request(self, op: str, *, timeout: float = ..., **args: Any) -> dict[str, Any]: ...


def _check_recovery(reply: dict[str, Any]) -> None:
    if reply.get("protocol_version") == 2 and reply.get("success") is not True:
        raise FastloopFaultError("full recovery failed; warm lane cannot be reused")


def _utc(epoch: object) -> datetime:
    if not isinstance(epoch, (int, float)):
        raise FastloopFaultError(f"worker returned a non-numeric timestamp {epoch!r}")
    return datetime.fromtimestamp(float(epoch), tz=timezone.utc)


def _window(reply: dict[str, Any]) -> InjectionWindow:
    return InjectionWindow(started_at=_utc(reply.get("started_at")), finished_at=_utc(reply.get("finished_at")))


@dataclass(frozen=True)
class InjectedFault:
    problem_id: str
    #: The fault's position in its composition; ``recover_fault`` takes it.
    index: int
    window: InjectionWindow


@dataclass(frozen=True)
class CompositeInjection:
    faults: tuple[InjectedFault, ...]

    def __post_init__(self) -> None:
        if not self.faults:
            raise ValueError("a composite injection has at least one fault")

    @property
    def window(self) -> InjectionWindow:
        """From the first fault's start to the last fault's end."""

        return InjectionWindow(
            started_at=self.faults[0].window.started_at, finished_at=self.faults[-1].window.finished_at
        )


class SregymFaultDriver:
    def __init__(
        self,
        worker: WorkerRequester,
        *,
        namespace: str,
        baseline: ApplicationBaseline | tuple[ApplicationBaseline, ...] | None = None,
        cluster: ApplicationCluster | None = None,
        health_timeout_seconds: float = 300.0,
        health_poll_seconds: float = HEALTH_POLL_SECONDS,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        before_inject: Callable[[str], None] | None = None,
        require_full_cleanup: bool = False,
    ) -> None:
        if health_timeout_seconds <= 0:
            raise ValueError("health_timeout_seconds must be positive")
        if health_poll_seconds <= 0:
            raise ValueError("health_poll_seconds must be positive")
        if require_full_cleanup and (not baseline or cluster is None):
            raise ValueError("full cleanup requires an application baseline and cluster")
        self._require_full_cleanup = require_full_cleanup
        self.last_full_cleanup_verified = False
        self._worker = worker
        self._namespace = namespace
        self._baselines = (baseline,) if isinstance(baseline, ApplicationBaseline) else baseline
        self._cluster = cluster
        self.last_cleanup = RecoveryCleanup()
        self.last_components: list[FaultComponent] = []
        self._health_timeout = health_timeout_seconds
        self._health_poll = health_poll_seconds
        self._monotonic = monotonic
        self._sleep = sleep
        self._before_inject = before_inject

    def inject(self, problem_id: str) -> InjectionWindow:
        if self._before_inject is not None:
            self._before_inject(problem_id)
        reply = self._worker.request("inject", problem_id=problem_id, timeout=WORKER_OP_TIMEOUT_SECONDS)
        self.last_components = [FaultComponent.model_validate(item) for item in reply.get("components", [])]
        check_injection(reply)
        return _window(reply)

    def inject_composite(self, problem_ids: Sequence[str]) -> CompositeInjection:
        """Inject several problems so their faults are live at once, in the given order.

        ``recover`` then reverts all of them, and ``recover_fault`` one of them.
        """

        problems = list(problem_ids)
        if not problems or not all(isinstance(problem, str) and problem for problem in problems):
            raise ValueError("a composite fault needs at least one non-empty problem id")
        if len(set(problems)) != len(problems):
            raise ValueError(f"a composite fault lists a problem more than once: {problems}")
        faults: list[InjectedFault] = []
        self.last_components = []
        for position, problem_id in enumerate(problems):
            try:
                reply = self._worker.request(
                    "inject", problem_id=problem_id, compose=position > 0, timeout=WORKER_OP_TIMEOUT_SECONDS
                )
                self.last_components.extend(FaultComponent.model_validate(item) for item in reply.get("components", []))
                check_injection(reply)
                indices = reply.get("faults", [reply.get("fault")])
                if (
                    not isinstance(indices, list)
                    or not indices
                    or indices != list(range(len(faults), len(faults) + len(indices)))
                ):
                    raise FastloopFaultError(f"worker returned invalid component indices for {problem_id}: {indices!r}")
                index = reply.get("fault")
                if index != len(faults):
                    raise FastloopFaultError(f"worker placed {problem_id} at fault {index!r}, expected {len(faults)}")
                faults.extend(
                    InjectedFault(problem_id=problem_id, index=index, window=_window(reply)) for index in indices
                )
            except InvalidInjectionError:
                # The loop owns terminal recording and cleanup, including the failed component.
                raise
            except Exception as exc:
                if faults:
                    try:
                        self.recover()
                    except Exception as recovery_error:
                        raise FastloopFaultError(
                            f"composite injection failed at {problem_id}: {exc}; recovery failed: {recovery_error}"
                        ) from recovery_error
                raise FastloopFaultError(f"composite injection failed at {problem_id}: {exc}") from exc
        return CompositeInjection(faults=tuple(faults))

    def oracle(self, *, fault: int | None = None) -> OracleVerdict:
        args: dict[str, Any] = {} if fault is None else {"fault": fault}
        reply = self._worker.request("oracle", timeout=WORKER_OP_TIMEOUT_SECONDS, **args)
        return OracleVerdict(kind=reply["kind"], success=reply["success"], details=reply.get("details", {}))

    def recover_fault(self, index: int) -> float:
        """Revert one fault of a composition; the others stay, so the application may stay unhealthy."""

        if index < 0:
            raise ValueError("fault index must not be negative")
        reply = self._worker.request("recover", mode="full", fault=index, timeout=WORKER_OP_TIMEOUT_SECONDS)
        _check_recovery(reply)
        seconds = reply.get("seconds")
        if not isinstance(seconds, (int, float)):
            raise FastloopFaultError(f"worker returned a non-numeric recovery time {seconds!r}")
        return float(seconds)

    def recover(self) -> float:
        """Reset faults for seed reuse, restore manifests, refresh clients, and verify health.

        Retrying this entire operation is safe after a partial manifest restore or client refresh.
        The harness owns credential resets; this adapter never reads credential material.
        """

        started = self._monotonic()
        self.last_cleanup = RecoveryCleanup()
        self.last_full_cleanup_verified = False
        reply = self._worker.request("recover", mode="full", timeout=WORKER_OP_TIMEOUT_SECONDS)
        if self._require_full_cleanup and reply.get("protocol_version") != 2:
            raise FastloopFaultError("full cleanup requires protocol v2 recovery")
        _check_recovery(reply)
        seed_compatible = (
            reply.get("protocol_version") == 2
            and reply.get("success") is True
            and reply.get("recovery_contract") == SEED_RECOVERY_CONTRACT
            and not reply.get("pending")
            and not reply.get("errors")
        )
        restoring_seed = self._baselines is not None and self._cluster is not None
        if (self._require_full_cleanup or restoring_seed) and not seed_compatible:
            raise FastloopFaultError("full cleanup requires seed-compatible/v1 recovery; update the SREGym harness")
        if self._baselines is not None and self._cluster is not None:
            for baseline in self._baselines:
                cleaned = reconcile_application(self._cluster, baseline, baseline.namespace)
                for kind, count in cleaned.deleted_counts.items():
                    self.last_cleanup.deleted_counts[kind] = self.last_cleanup.deleted_counts.get(kind, 0) + count
                self.last_cleanup.restored.extend(
                    f"{baseline.namespace}/{name}" if len(self._baselines) > 1 else name for name in cleaned.restored
                )
        else:
            self.last_cleanup.skipped_reason = "application baseline unavailable"
        if seed_compatible:
            refreshed = self._worker.request("refresh", timeout=WORKER_OP_TIMEOUT_SECONDS)
            if (
                refreshed.get("protocol_version") != 2
                or refreshed.get("recovery_contract") != SEED_RECOVERY_CONTRACT
                or refreshed.get("success") is not True
                or refreshed.get("pending")
                or refreshed.get("errors")
            ):
                raise FastloopFaultError("full cleanup client refresh failed; warm lane cannot be reused")
        deadline = started + self._health_timeout
        health: dict[str, Any] = {}
        unready_rollouts: list[str] = []
        while True:
            health = self._worker.request("health", namespace=self._namespace, timeout=60)
            if health.get("healthy"):
                unready_rollouts = self._unready_rollouts()
                if not unready_rollouts:
                    self.last_full_cleanup_verified = (
                        reply.get("protocol_version") == 2
                        and reply.get("success") is True
                        and seed_compatible
                        and self.last_cleanup.skipped_reason is None
                    )
                    return self._monotonic() - started
            if self._monotonic() >= deadline:
                raise FastloopFaultError(
                    f"{self._namespace} is not healthy {self._health_timeout:.0f}s after recovery; "
                    f"unready: {health.get('unready')}; rollouts: {unready_rollouts}"
                )
            self._sleep(self._health_poll)

    def _unready_rollouts(self) -> list[str]:
        if self._cluster is None:
            return []
        namespaces = (baseline.namespace for baseline in self._baselines) if self._baselines else (self._namespace,)
        unready: list[str] = []
        for namespace in namespaces:
            for value in self._cluster.list_objects(namespace):
                kind = value.get("kind")
                metadata = value.get("metadata", {})
                if kind == "Pod" and metadata.get("deletionTimestamp"):
                    unready.append(f"{namespace}/Pod/{metadata.get('name')}")
                if kind not in {"Deployment", "StatefulSet"}:
                    continue
                spec = value.get("spec", {})
                status = value.get("status", {})
                replicas = spec.get("replicas", 1)
                if (
                    status.get("observedGeneration", 0) < metadata.get("generation", 0)
                    or status.get("updatedReplicas", 0) != replicas
                    or status.get("readyReplicas", 0) != replicas
                    or status.get("replicas", 0) != replicas
                ):
                    unready.append(f"{namespace}/{kind}/{metadata.get('name')}")
        return unready
