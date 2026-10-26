"""One long-running SDO controller per application across SREGym problems.

SREGym redeploys the application for every problem: the conductor deletes the
application namespace after each problem and recreates it before the next.
The SDO controller is long-running, so in persistent mode the controller,
its repository PVC, state ConfigMap, Lease, credentials, and responder and
validator Jobs live in a dedicated per-application namespace
(``<application-namespace>-sdo``) that observes the application namespace
through namespace-scoped RBAC. Between problems the adapter pauses the
controller through its maintenance ConfigMap; the next problem re-grants
access to the recreated namespace and resumes it with a new generation.

Reflection is asynchronous: a problem ends as soon as the controller has
independently verified recovery, and the next problem (or pipeline teardown)
waits for that incident's reflection and detector rollout before injecting,
then writes the earlier problem's strict receipt. The wait is reported as a
pre-injection ``reflection_drain`` cost, never as resolution time.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, fields, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, get_args, get_type_hints

from google.protobuf import json_format
from pydantic import BaseModel, ConfigDict, Field

from benchmarks.sregym.adapter.cluster_resources import kubectl
from benchmarks.sregym.adapter.eval_guards import (
    INCIDENT_EXECUTION_LABEL,
    STANDBY_LABEL,
    idle_standby_jobs,
    incident_work_open,
    queued_reflection_count,
    queued_reflection_ids,
    terminal_incident_ids,
)
from benchmarks.sregym.adapter.eval_guards import reflection_attempt_seconds as default_reflection_attempt_seconds
from benchmarks.sregym.adapter.fault_gate import (
    BASELINE_TIMEOUT_SECONDS,
    RESUMED_BASELINE_TIMEOUT_SECONDS,
    inject_fault_after_resumed_baseline,
)
from benchmarks.sregym.adapter.native_operator import (
    NativeOperatorError,
    observe_controller_state,
)
from benchmarks.sregym.adapter.native_operator import (
    export_runtime_artifacts as go_export_runtime_artifacts,
)
from benchmarks.sregym.adapter.native_operator import (
    inspect_existing as go_inspect_existing,
)
from benchmarks.sregym.adapter.native_operator import (
    set_maintenance as go_set_maintenance,
)
from benchmarks.sregym.adapter.runtime import (
    RUNTIME_ARTIFACTS_DIRNAME,
    ControllerInstallError,
    RuntimeConfig,
    collect_production_receipt,
    export_controller_logs,
)
from benchmarks.sregym.protocol import ProductionReceiptValidationError, validate_production_receipt
from benchmarks.sregym.protocol.injection import InvalidInjectionError
from benchmarks.sregym.protocol.native_receipt import NativeReceiptValidationError
from sdo.contracts import (
    ControllerObservation,
    ControllerObservationFailure,
    ReceiptDrainResult,
    ReceiptFailure,
    RejectedProductionReceipt,
)

if TYPE_CHECKING:
    from collections.abc import Callable


logger = logging.getLogger(__name__)

#: Path of the pipeline-scoped JSON state file; set by the runner for every stage.
PERSISTENT_STATE_ENV = "SDO_PERSISTENT_CONTROLLER_STATE"
CONTROL_NAMESPACE_SUFFIX = "-sdo"
RESOLUTION_FILENAME = "sdo_incident_resolution.json"
STRICT_RECEIPT_FILENAME = "sdo_production_receipt_strict.json"
# A drained receipt that failed production validation, kept with its error for analysis.
REJECTED_RECEIPT_FILENAME = "sdo_rejected_production_receipt.json"
CONTROLLER_LOGS_SUBDIR = Path(RUNTIME_ARTIFACTS_DIRNAME) / "controller_logs"
POLL_SECONDS = 1.0
#: A state read at or past a deadline still gets this long to be observed, so it decides the outcome.
OBSERVATION_FLOOR_SECONDS = 5.0
CONTROLLER_JOB_NAME = "sdo-controller-run"
# The runtime storage edge's checkpoint identity (lifecycleio.StateRevisionAnnotation
# and StateDataKey): its CAS annotation plus typed data key, never its name.
STATE_REVISION_ANNOTATION = "sdo.dev/state-revision"
STATE_DATA_KEY = "runtime-state.json"
MAINTENANCE_ACK_TIMEOUT_SECONDS = 600.0
#: Recovery waits this long for SDO to close an incident that opened after the graded one closed.
#: Live P4 responder runtime p90 was 442 s (18 rows, 2026-10-26); the rest is closure margin, capped at 10 min.
POST_CLOSURE_WAIT_SECONDS = 600.0
DRAIN_TIMEOUT_SECONDS = 3600.0
# After the reflection drain returns, the controller's asynchronous close-out may
# still be settling the two volatile signals the strict receipt samples: the
# durable responder result reaching a terminal ``completed`` status, and the
# shared worktree directory draining. The relaunch-after-closure predicate can
# fire on an intermediate relaunch on composite incidents (closure
# retries), so the gate must wait for these to settle before it samples, or a
# solved incident is rejected. Bounded so a genuinely unsettled receipt still
# rejects promptly.
CLOSEOUT_SETTLE_TIMEOUT_SECONDS = 120.0
# Resume can require informer resync, state baseline startup and workload probes
# before reconciliation starts. Bound that path separately from receipt drain.
RENDERED_LOCK_SETTLE_TIMEOUT_SECONDS = 600.0
STAGE_END_EVIDENCE_SCOPE = "stage-end snapshot; the drained strict receipt supersedes it"
_ZERO_TIME = "0001-01-01T00:00:00Z"


class PersistentControllerError(RuntimeError):
    """Raised when a persistent controller cannot be installed, reused, or drained."""


class ClosureFailedError(PersistentControllerError):
    """Raised when the controller gave up committing an incident closure the broker kept rejecting."""


class ControllerJobFailedError(ClosureFailedError):
    """The controller Job reached a terminal condition before acknowledging the closure, so no owner can ACK it."""


class ReflectionDeadlineError(PersistentControllerError):
    """The controller was read and still owned the closure when the acknowledgment deadline passed.

    For a successful closure this covers its reflection and detector rollout;
    a non-success closure's queued reflection runs after the acknowledgment.
    """


class ControllerStateUnavailableError(PersistentControllerError):
    """The controller state could not be read before a deadline, so whether closure is over is unknown.

    Raised by the recovery drain and by admission's prior-closure wait; either
    way the row is not measured. ``evidence`` carries the last observation.
    """

    reason = "controller_state_unavailable"

    def __init__(self, message: str, evidence: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.evidence = evidence or {}


class DetectionMissError(PersistentControllerError):
    """Raised when no incident opened within the stage's detection deadline after injection.

    An agent outcome (the detectors missed the fault), reported as a detection
    miss censored at the deadline, not a measured TTD or TTM.
    """


def control_namespace_for(app_namespace: str) -> str:
    """Name the per-application SDO namespace after the application namespace."""

    name = f"{app_namespace}{CONTROL_NAMESPACE_SUFFIX}"
    if len(name) > 63:
        raise PersistentControllerError(f"application namespace {app_namespace!r} is too long for {name!r}")
    return name


class PendingIncident(BaseModel):
    """A verified incident whose reflection may still be running."""

    model_config = ConfigDict(extra="forbid")

    incident_id: str = Field(min_length=1)
    stage_label: str
    receipt_dir: Path
    repository: Path
    verified_at: datetime | None
    stage_receipt: dict[str, Any]
    #: The one reflection deadline every drain of this incident shares, set when the closure is recorded.
    #: Absent only on records written before it existed; their first drain sets it.
    reflection_deadline_at: datetime | None = None


class ControllerRecord(BaseModel):
    """One application's controller as installed by this pipeline."""

    model_config = ConfigDict(extra="forbid")

    application: str
    namespace: str
    control_namespace: str
    lifecycle_fingerprint: str
    controller_pod_uid: str
    controller_pod_name: str
    installed_by_stage: str
    installed_at: datetime
    kubeconfig: str | None = None
    runtime_config: dict[str, Any]
    served_stages: list[str] = Field(default_factory=list)
    pending: PendingIncident | None = None
    #: The stage receipt of the stage that served the current problem; follow-up incidents inherit it.
    served_stage_receipt: dict[str, Any] | None = None


class DeferredReceipt(BaseModel):
    """Where a stage's receipt is written after its drain, for publication into the results tree."""

    model_config = ConfigDict(extra="forbid")

    incident_id: str = Field(min_length=1)
    stage_label: str
    staging_dir: Path


class CleanupTarget(BaseModel):
    """Recorded before installation, including installations interrupted before readiness."""

    model_config = ConfigDict(extra="forbid")
    namespace: str
    control_namespace: str
    kubeconfig: str | None = None


class AbortedIncident(BaseModel):
    model_config = ConfigDict(extra="forbid")
    incident_id: str
    namespace: str
    stage_label: str
    aborted_at: datetime


class PersistentState(BaseModel):
    """Pipeline-scoped registry of persistent controllers, keyed by application namespace."""

    model_config = ConfigDict(extra="forbid")

    controllers: dict[str, ControllerRecord] = Field(default_factory=dict)
    deferred_receipts: list[DeferredReceipt] = Field(default_factory=list)
    cleanup_targets: dict[str, CleanupTarget] = Field(default_factory=dict)
    aborted_incidents: list[AbortedIncident] = Field(default_factory=list)

    @classmethod
    def load(cls, path: Path) -> PersistentState:
        if not path.is_file():
            return cls()
        return cls.model_validate_json(path.read_text(encoding="utf-8"))

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(self.model_dump_json(indent=2) + "\n", encoding="utf-8")
        temporary.replace(path)


def record_cleanup_target(namespace: str, control_namespace: str, kubeconfig: str | None) -> None:
    """Journal a native launch before any benchmark transport or installer side effect."""
    selected = os.getenv(PERSISTENT_STATE_ENV) or os.getenv("SDO_RUN_REGISTRY_STATE")
    if not selected:
        return
    path = Path(selected).resolve()
    state = PersistentState.load(path)
    state.cleanup_targets[namespace] = CleanupTarget(
        namespace=namespace,
        control_namespace=control_namespace,
        kubeconfig=kubeconfig,
    )
    state.save(path)


@dataclass(frozen=True)
class ControllerPod:
    name: str
    uid: str


@dataclass(frozen=True)
class VerifiedIncident:
    incident_id: str
    closure: dict[str, Any] | None
    #: Reflections SDO still owed ahead of this closure in the state that showed it verified.
    queued_reflections: int = 0


def runtime_config_payload(config: RuntimeConfig) -> dict[str, Any]:
    payload = {field.name: getattr(config, field.name) for field in fields(RuntimeConfig)}
    return {key: str(value) if isinstance(value, Path) else value for key, value in payload.items()}


#: Host-side paths and per-call install modes that do not change what the running controller does.
_NON_CONTROLLER_SETTINGS = frozenset({"repository", "artifacts_dir", "wait_for_completion", "reuse_existing"})


def controller_settings(payload: dict[str, Any]) -> dict[str, Any]:
    """The recorded settings a running controller was installed with, normalized as saved state."""

    return {key: value for key, value in json.loads(json.dumps(payload)).items() if key not in _NON_CONTROLLER_SETTINGS}


def runtime_config_from_payload(payload: dict[str, Any]) -> RuntimeConfig:
    values = dict(payload)
    for name in ("deployed_manifest_paths", "responder_namespaces"):
        values[name] = tuple(values.get(name, ()))
    hints = get_type_hints(RuntimeConfig)
    for field in fields(RuntimeConfig):
        value = values.get(field.name)
        hint = hints[field.name]
        if value is not None and Path in (hint, *get_args(hint)):
            values[field.name] = Path(value)
    return RuntimeConfig(**values)


class ClusterOps(Protocol):
    """Cluster side effects of persistent mode; replaced by a fake in tests."""

    def controller_pod(self, control_namespace: str) -> ControllerPod | None: ...

    def controller_job_failure(self, control_namespace: str) -> str | None:
        """The controller Job's terminal condition (``reason: message``), or None while it can still run pods."""
        ...

    def namespace_exists(self, namespace: str) -> bool: ...

    def delete_controller(self, control_namespace: str) -> None: ...

    def install(self, config: RuntimeConfig) -> bool: ...

    def set_maintenance(
        self, control_namespace: str, *, paused: bool, generation: str, preserve_baseline: bool = False
    ) -> None: ...

    def runtime_state(self, control_namespace: str) -> dict[str, Any]: ...

    def controller_logs(self, control_namespace: str) -> str: ...

    def inject_after_resume(
        self, control_namespace: str, generation: str, inject: Callable[[], None]
    ) -> dict[str, float]: ...

    def collect_receipt(self, config: RuntimeConfig, incident_id: str, artifacts_dir: Path) -> dict[str, Any]: ...

    def export_runtime_artifacts(self, config: RuntimeConfig, artifacts_dir: Path) -> dict[str, str | None]: ...

    def export_controller_logs(self, control_namespace: str, artifacts_dir: Path) -> None: ...


class KubectlClusterOps:
    """Production implementation over kubectl and the SDO controller installer."""

    def __init__(self, *, preserve_applications: bool = True) -> None:
        if not isinstance(preserve_applications, bool):
            raise TypeError("preserve_applications must be bool")
        self.preserve_applications = preserve_applications

    def controller_pod(self, control_namespace: str) -> ControllerPod | None:
        completed = kubectl(
            [
                "get",
                "pods",
                "--ignore-not-found",
                "--request-timeout=5s",
                "--selector",
                f"job-name={CONTROLLER_JOB_NAME}",
                "-o",
                "json",
            ],
            namespace=control_namespace,
            check=False,
        )
        if completed.returncode != 0:
            raise PersistentControllerError(
                f"cannot inspect controller pods in {control_namespace!r}: {completed.stderr.strip()}"
            )
        running = [
            item
            for item in json.loads(completed.stdout or "{}").get("items", [])
            if isinstance(item, dict) and item.get("status", {}).get("phase") == "Running"
        ]
        if len(running) != 1:
            return None
        metadata = running[0].get("metadata", {})
        return ControllerPod(name=str(metadata.get("name", "")), uid=str(metadata.get("uid", "")))

    def controller_job_failure(self, control_namespace: str) -> str | None:
        completed = kubectl(
            ["get", "job", CONTROLLER_JOB_NAME, "--ignore-not-found", "--request-timeout=5s", "-o", "json"],
            namespace=control_namespace,
            check=False,
        )
        if completed.returncode != 0:
            raise PersistentControllerError(
                f"cannot inspect the controller Job in {control_namespace!r}: {completed.stderr.strip()}"
            )
        return controller_job_failure(json.loads(completed.stdout)) if completed.stdout.strip() else None

    def namespace_exists(self, namespace: str) -> bool:
        return kubectl(["get", f"namespace/{namespace}"], namespace=None, check=False).returncode == 0

    def delete_controller(self, control_namespace: str) -> None:
        self.stop_runtime(CleanupTarget(namespace=control_namespace, control_namespace=control_namespace))

    def stop_runtime(self, target: CleanupTarget) -> None:
        from benchmarks.sregym.adapter.native_operator import stop_runtime

        result = stop_runtime(target.control_namespace, kubeconfig=target.kubeconfig)
        for name in result["removed"]:
            logger.info("stopped %s", name)

    def protect_incident_work(self, target: CleanupTarget, directory: Path) -> None:
        """Capture worker evidence and refuse automatic cleanup of unfinished work."""
        config = Path(target.kubeconfig) if target.kubeconfig else None
        listing = json.loads(
            kubectl(["get", "jobs,pods", "-o", "json"], namespace=target.control_namespace, kubeconfig=config).stdout
        )["items"]

        def worker(metadata: dict[str, Any]) -> bool:
            labels = metadata.get("labels", {})
            return (
                labels.get("app.kubernetes.io/name") == "sdo-worker"
                or bool(labels.get("sdo.dev/validator-isolation"))
                or bool(labels.get("sdo.dev/validator-canary"))
            )

        def labels(row: dict[str, Any]) -> dict[str, str]:
            return {
                **row.get("spec", {}).get("template", {}).get("metadata", {}).get("labels", {}),
                **row["metadata"].get("labels", {}),
            }

        def remove(resources: list[str]) -> None:
            kubectl(
                [
                    "delete",
                    *resources,
                    "--ignore-not-found=true",
                    "--cascade=foreground",
                    "--wait=true",
                    "--timeout=60s",
                ],
                namespace=target.control_namespace,
                kubeconfig=config,
            )

        def inspect_workers(listing: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], set[str]]:
            jobs = [
                row
                for row in listing
                if row["kind"] == "Job"
                and (worker(row["metadata"]) or worker(row.get("spec", {}).get("template", {}).get("metadata", {})))
            ]
            job_uids = {row["metadata"]["uid"] for row in jobs}
            pods = [
                row
                for row in listing
                if row["kind"] == "Pod"
                and (
                    worker(row["metadata"])
                    or any(owner.get("uid") in job_uids for owner in row["metadata"].get("ownerReferences", []))
                )
            ]
            directory.mkdir(parents=True, exist_ok=True)
            # Keep conditions and container states, without persisting environment values.
            inventory = {
                key: [{"metadata": row["metadata"], "status": row.get("status", {})} for row in rows]
                for key, rows in (("jobs", jobs), ("pods", pods))
            }
            (directory / "workloads.json").write_text(json.dumps(inventory, indent=2) + "\n")
            for pod in pods:
                name = pod["metadata"]["name"]
                logs = kubectl(
                    [
                        "logs",
                        f"pod/{name}",
                        "--all-containers=true",
                        "--prefix=true",
                        "--tail=2000",
                        "--limit-bytes=1048576",
                    ],
                    namespace=target.control_namespace,
                    kubeconfig=config,
                    check=False,
                )
                (directory / f"{name}.log").write_text(logs.stdout)
                if logs.returncode:
                    (directory / f"{name}.log-error").write_text(logs.stderr)

            idle_uids: set[str] = set()
            if any(labels(row).get(STANDBY_LABEL) == "true" for row in jobs):
                records = json.loads(
                    kubectl(
                        ["get", "configmaps", "-l", INCIDENT_EXECUTION_LABEL, "-o", "json"],
                        namespace=target.control_namespace,
                        kubeconfig=config,
                    ).stdout
                )["items"]
                try:
                    idle = idle_standby_jobs(jobs, records)
                except ValueError as exc:
                    raise PersistentControllerError(f"{exc}; cleanup blocked") from exc
                idle_uids = {row["metadata"]["uid"] for row in jobs if row["metadata"]["name"] in idle}

            active = [
                f"job/{row['metadata']['name']}"
                for row in jobs
                if row["metadata"]["uid"] not in idle_uids
                and not any(
                    condition.get("type") in {"Complete", "Failed"} and condition.get("status") == "True"
                    for condition in row.get("status", {}).get("conditions", [])
                )
            ]
            active.extend(
                f"pod/{row['metadata']['name']}"
                for row in pods
                if row.get("status", {}).get("phase") not in {"Succeeded", "Failed"}
                and not any(owner.get("uid") in idle_uids for owner in row["metadata"].get("ownerReferences", []))
            )
            if active:
                raise PersistentControllerError(
                    f"eval: cleanup preserves active incident work: {active}; evidence: {directory}"
                )
            return jobs, idle_uids

        # Protect existing work before shutdown, which can cancel validator tasks.
        jobs, idle_uids = inspect_workers(listing)
        dispatchers = [
            row for row in listing if labels(row).get("app.kubernetes.io/name") in {"sdo-controller", "sdo-operate"}
        ]
        dispatcher_uids = {row["metadata"]["uid"] for row in dispatchers if row["kind"] == "Job"}
        dispatchers.extend(
            row
            for row in listing
            if row["kind"] == "Pod"
            and row not in dispatchers
            and any(owner.get("uid") in dispatcher_uids for owner in row["metadata"].get("ownerReferences", []))
        )
        if dispatchers:
            for kind in ("Job", "Pod"):
                resources = [f"{kind.lower()}/{row['metadata']['name']}" for row in dispatchers if row["kind"] == kind]
                if resources:
                    remove(resources)
            listing = json.loads(
                kubectl(
                    ["get", "jobs,pods", "-o", "json"], namespace=target.control_namespace, kubeconfig=config
                ).stdout
            )["items"]
            if any(
                labels(row).get("app.kubernetes.io/name") in {"sdo-controller", "sdo-operate"}
                or row["metadata"].get("uid") in {item["metadata"].get("uid") for item in dispatchers}
                for row in listing
            ):
                raise PersistentControllerError("eval: cleanup could not quiesce standby dispatchers")

            # Claims or workers published during shutdown remain protected.
            jobs, idle_uids = inspect_workers(listing)

        idle = [f"job/{row['metadata']['name']}" for row in jobs if row["metadata"]["uid"] in idle_uids]
        if idle:
            # Go stop uses zero grace. Retire idle slots normally, including
            # their sidecars, and prove them gone before entering that path.
            remove(idle)
            remaining = json.loads(
                kubectl(
                    ["get", "jobs,pods", "-o", "json"],
                    namespace=target.control_namespace,
                    kubeconfig=config,
                ).stdout
            )["items"]
            if any(
                row["metadata"].get("uid") in idle_uids
                or any(owner.get("uid") in idle_uids for owner in row["metadata"].get("ownerReferences", []))
                for row in remaining
            ):
                raise PersistentControllerError("eval: idle standby deletion incomplete; cleanup blocked")

    def retire_controller_state(self, target: CleanupTarget, archive_dir: Path) -> list[str]:
        """Archive, then delete, the stopped controller's checkpoint of open incident and closure work.

        Onboarding authority, maintenance and the accepted repository survive; a
        later install starts from accepted memory, not an incident it did not open.
        """
        config = Path(target.kubeconfig) if target.kubeconfig else None

        def checkpoints() -> list[dict[str, Any]]:
            listing = kubectl(
                ["get", "configmaps", "-o", "json"], namespace=target.control_namespace, kubeconfig=config
            )
            return [
                item
                for item in json.loads(listing.stdout or "{}").get("items", [])
                if STATE_REVISION_ANNOTATION in (item.get("metadata", {}).get("annotations") or {})
                and STATE_DATA_KEY in (item.get("data") or {})
            ]

        retired = []
        for item in checkpoints():
            name = item["metadata"]["name"]
            _write_json(item, archive_dir / f"{name}.json")
            kubectl(
                ["delete", "configmap", name, "--ignore-not-found=true", "--wait=true"],
                namespace=target.control_namespace,
                kubeconfig=config,
            )
            retired.append(name)
        if survivors := [item["metadata"]["name"] for item in checkpoints()]:
            raise PersistentControllerError(f"controller state survived teardown: {survivors}")
        return retired

    def stop_transport(self, target: CleanupTarget) -> None:
        config = Path(target.kubeconfig) if target.kubeconfig else None
        # These resources belong to the benchmark, never to the production stop API.
        kubectl(
            [
                "delete",
                "deployment/sdo-sregym-bridge",
                "service/sdo-sregym-bridge",
                "deployment/sdo-benchmark-registry",
                "service/sdo-benchmark-registry",
                "--ignore-not-found=true",
                "--cascade=foreground",
                "--wait=true",
                "--timeout=60s",
            ],
            namespace=target.control_namespace,
            kubeconfig=config,
        )

    def abort_workload(self, target: CleanupTarget) -> None:
        self.stop_transport(target)
        if self.preserve_applications:
            return
        config = Path(target.kubeconfig) if target.kubeconfig else None
        if target.namespace == target.control_namespace:
            raise PersistentControllerError("refusing to delete an application namespace containing accepted memory")
        kubectl(
            ["delete", "namespace", target.namespace, "--ignore-not-found=true", "--wait=true", "--timeout=60s"],
            namespace=None,
            kubeconfig=config,
        )

    def abort_leftovers(self, target: CleanupTarget) -> list[str]:
        from benchmarks.sregym.adapter.native_operator import stop_runtime

        remaining = stop_runtime(target.control_namespace, kubeconfig=target.kubeconfig, inspect=True)["leftovers"]
        config = Path(target.kubeconfig) if target.kubeconfig else None
        for namespace, names in (
            (
                target.control_namespace,
                [
                    "deployment/sdo-sregym-bridge",
                    "service/sdo-sregym-bridge",
                    "deployment/sdo-benchmark-registry",
                    "service/sdo-benchmark-registry",
                ],
            ),
            (None, [f"namespace/{target.namespace}"]),
        ):
            if namespace is None and self.preserve_applications:
                continue
            response = kubectl(
                ["get", *names, "--ignore-not-found=true", "-o", "name"], namespace=namespace, kubeconfig=config
            )
            remaining.extend(response.stdout.splitlines())
        # Deployment deletion can return before its orphaned ReplicaSet pods disappear.
        response = kubectl(["get", "pods", "-o", "json"], namespace=target.control_namespace, kubeconfig=config)
        for pod in json.loads(response.stdout or "{}").get("items", []):
            if pod.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/name") in (
                "sdo-sregym-bridge",
                "sdo-benchmark-registry",
            ):
                remaining.append("pod/" + pod["metadata"]["name"])
        return remaining

    def install(self, config: RuntimeConfig) -> bool:
        from benchmarks.sregym.adapter.native_operator import install_existing, sync_existing
        from benchmarks.sregym.adapter.runtime import native_install_config

        native = native_install_config(config)
        if config.reuse_existing:
            go_inspect_existing(native)
            return True
        # Reset retained the accepted PVC but deleted its Job. Sync first so
        # reinstalling cannot replace learned memory with the stale host tree.
        sync_existing(native)
        return install_existing(native).reused

    def set_maintenance(
        self, control_namespace: str, *, paused: bool, generation: str, preserve_baseline: bool = False
    ) -> None:
        if paused and incident_work_open(self.runtime_state(control_namespace)):
            raise PersistentControllerError("eval: cannot pause SDO while incident closure work is pending")
        kubeconfig = os.getenv("KUBECONFIG", "").strip()
        go_set_maintenance(
            control_namespace,
            paused=paused,
            generation=generation,
            preserve_baseline=preserve_baseline,
            kubeconfig=Path(kubeconfig).resolve() if kubeconfig else None,
        )

    def runtime_state(self, control_namespace: str) -> dict[str, Any]:
        completed = kubectl(
            ["get", "configmap/sdo-controller-state", "-o", "json"], namespace=control_namespace, check=False
        )
        if completed.returncode != 0:
            return {}
        document = json.loads(completed.stdout)
        state = json.loads(document.get("data", {}).get("runtime-state.json", "{}"))
        return state if isinstance(state, dict) else {}

    def controller_logs(self, control_namespace: str) -> str:
        pod = self.controller_pod(control_namespace)
        if pod is None:
            return ""
        return kubectl(
            ["logs", f"pod/{pod.name}", "--container=controller"], namespace=control_namespace, check=False
        ).stdout

    def inject_after_resume(
        self, control_namespace: str, generation: str, inject: Callable[[], None]
    ) -> dict[str, float]:
        self.set_maintenance(control_namespace, paused=False, generation=generation)
        scale = float(os.getenv("SDO_TIME_SCALE", "1"))
        return inject_fault_after_resumed_baseline(
            control_namespace,
            generation,
            inject=lambda: inject_while_paused(self, control_namespace, generation, inject),
            kubectl_runner=kubectl,
            timeout_seconds=RESUMED_BASELINE_TIMEOUT_SECONDS * scale,
            startup_timeout_seconds=BASELINE_TIMEOUT_SECONDS * scale,
            poll_seconds=min(1.0, scale),
        )

    def collect_receipt(self, config: RuntimeConfig, incident_id: str, artifacts_dir: Path) -> dict[str, Any]:
        return collect_production_receipt(config, incident_id, artifacts_dir)

    def export_runtime_artifacts(self, config: RuntimeConfig, artifacts_dir: Path) -> dict[str, str | None]:
        """Ask the Go operator to export whitelisted runtime evidence."""
        try:
            from benchmarks.sregym.adapter.runtime import native_install_config

            directory = go_export_runtime_artifacts(native_install_config(config), artifacts_dir.resolve())
            return {"directory": str(directory), "error": None}
        except (NativeOperatorError, ControllerInstallError, OSError, subprocess.SubprocessError) as exc:
            return {"directory": None, "error": f"{type(exc).__name__}: {exc}"}

    def export_controller_logs(self, control_namespace: str, artifacts_dir: Path) -> None:
        export_controller_logs(control_namespace, artifacts_dir)


@dataclass(frozen=True)
class StageInputs:
    stage_label: str
    application: str
    namespace: str
    lifecycle_fingerprint: str
    runtime_config: RuntimeConfig
    receipt_dir: Path
    state_path: Path
    kubeconfig: str | None = None
    verification_timeout_seconds: float = 3900.0
    #: End the stage as a detection miss if no incident has opened this long
    #: after injection. None waits the whole verification budget.
    detection_timeout_seconds: float | None = None
    lifecycle_installs_controller: bool = False
    #: Budget from a verified closure to its reflection's acknowledgment, shared by every drain of the incident.
    reflection_wait_seconds: float = DRAIN_TIMEOUT_SECONDS
    #: SDO's worst case to finish or abandon one queued reflection; the deadline adds one per reflection ahead.
    reflection_attempt_seconds: float = default_reflection_attempt_seconds()

    def __post_init__(self) -> None:
        if self.detection_timeout_seconds is not None and self.detection_timeout_seconds <= 0:
            raise ValueError(f"detection_timeout_seconds must be positive, got {self.detection_timeout_seconds}")
        if self.reflection_wait_seconds <= 0:
            raise ValueError(f"reflection_wait_seconds must be positive, got {self.reflection_wait_seconds}")
        if self.reflection_attempt_seconds <= 0:
            raise ValueError(f"reflection_attempt_seconds must be positive, got {self.reflection_attempt_seconds}")


@dataclass(frozen=True)
class Clock:
    monotonic: Callable[[], float] = time.monotonic
    sleep: Callable[[float], None] = time.sleep
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc)


def run_persistent_stage(
    inputs: StageInputs,
    *,
    ops: ClusterOps,
    run_lifecycle: Callable[[], bool],
    inject: Callable[[], None],
    clock: Clock | None = None,
    pause_after_verified: bool = True,
) -> dict[str, Any]:
    """Run one benchmark problem against the application's persistent controller.

    ``pause_after_verified=False`` leaves the controller active after the first
    verified incident, for compositions whose other faults the controller must
    keep handling; the caller then pauses it (``set_maintenance``) itself.

    Returns the stage's resolution record. It ends at controller-verified
    recovery; the strict receipt, which needs the finished reflection, is
    written later by :func:`drain_pending_incident`.
    """

    clock = clock or Clock()
    started = clock.monotonic()
    config = replace(inputs.runtime_config, persistent=True, wait_for_completion=False)
    control = config.control_namespace
    if control != control_namespace_for(inputs.namespace):
        raise PersistentControllerError(f"controller namespace {control!r} is not owned by {inputs.namespace!r}")
    state = PersistentState.load(inputs.state_path)
    record = state.controllers.get(inputs.namespace)
    if record is not None and record.application != inputs.application:
        raise PersistentControllerError(
            f"namespace {inputs.namespace!r} is operated by the persistent controller of "
            f"{record.application!r}; refusing to reuse it for {inputs.application!r}"
        )
    state.cleanup_targets[inputs.namespace] = CleanupTarget(
        namespace=inputs.namespace,
        control_namespace=control,
        kubeconfig=inputs.kubeconfig,
    )
    state.save(inputs.state_path)
    pod = ops.controller_pod(control) if record is not None else None
    inventory_ready = clock.monotonic()
    drain_seconds = 0.0
    if record is not None and record.pending is not None:
        # The previous problem's learning must reach memory (and any learned
        # detector the controller) before this problem's fault is injected.
        drain_started = clock.monotonic()
        if pod is not None and pod.uid == record.controller_pod_uid:
            try:
                drained = drain_pending_incident(
                    record, ops=ops, repository=config.repository, drained_by=inputs.stage_label, clock=clock
                )
            except ClosureFailedError as exc:
                # The controller continues with later incidents after a permanent closure failure,
                # so this stage's fault must not wait on it. The failure stays on the previous stage's record.
                drained = record_failed_closure(record.pending, str(exc), clock.monotonic() - drain_started)
        else:
            # The recorded controller is unavailable. Reject its undrained evidence explicitly
            # before replacing it, so the loss cannot block all subsequent problems.
            drained = record_failed_closure(record.pending, _lost_controller_message(record, pod), 0.0)
        drain_seconds = drained.waited_seconds
        if drained.HasField("failure"):
            logger.warning("previous incident %s: %s", drained.incident_id, drained.failure.message)
        record.pending = None
        state.save(inputs.state_path)
        # Publish the drained receipt now, so a pipeline stopped during this
        # stage keeps the previous stage's strict receipt and token evidence.
        _publish_drained_receipts(inputs.state_path)
    # A controller serves this stage only if it was installed for the same topology
    # and the same controller settings (workers, credentials, model, responder scope).
    same_controller = (
        record is not None
        and record.lifecycle_fingerprint == inputs.lifecycle_fingerprint
        and controller_settings(record.runtime_config) == controller_settings(runtime_config_payload(config))
    )
    reusable = same_controller and pod is not None and record is not None and pod.uid == record.controller_pod_uid
    restart_after_reset = (
        same_controller and record is not None and record.controller_pod_uid == "" and ops.namespace_exists(control)
    )
    shared_adoption = bool(config.shared_accepted_id and not reusable and not restart_after_reset)
    lifecycle_started = clock.monotonic()
    if reusable or restart_after_reset:
        lifecycle_reused, lifecycle_revalidation_skipped = True, True
    else:
        if record is not None and record.pending is not None:
            raise PersistentControllerError(
                f"cannot replace the controller of {inputs.namespace!r}: incident {record.pending.incident_id!r} "
                "has not been drained into operational memory"
            )
        if config.shared_accepted_id:
            # Shared attachment has already prepared this exact accepted mount.
            # A lost host registry must never trigger a seed or destroy a peer's
            # live repository. Rebuild it only after native identity inspection.
            pod = ops.controller_pod(control)
            if pod is None or not ops.install(replace(config, reuse_existing=True)):
                raise PersistentControllerError("shared memory requires its prepared running controller")
            lifecycle_reused, lifecycle_revalidation_skipped = True, True
        else:
            if ops.namespace_exists(control):
                # Never adopt a controller this pipeline did not install and validate.
                ops.delete_controller(control)
            state.controllers.pop(inputs.namespace, None)
            state.save(inputs.state_path)
            lifecycle_reused, lifecycle_revalidation_skipped = run_lifecycle(), False
    lifecycle_ready = clock.monotonic()
    install_inspected = shared_adoption
    if not reusable:
        if restart_after_reset:
            # Reset deletes the Job while retaining the accepted repository.
            # No lifecycle runs on this path to recreate the controller.
            ops.install(replace(config, reuse_existing=False))
            install_inspected = True
        pod = (
            _wait_for_controller_pod(ops, control, clock)
            if inputs.lifecycle_installs_controller
            else ops.controller_pod(control)
        )
        if pod is None:
            ops.install(replace(config, reuse_existing=False))
            pod = _wait_for_controller_pod(ops, control, clock)
            install_inspected = True
    else:
        # Recreating the app namespace deletes its grants even when the
        # controller Pod and accepted repository survive. The Go inspector
        # owns restoring those grants; identity alone cannot establish access.
        assert pod is not None
        if not ops.install(replace(config, reuse_existing=True)):
            raise PersistentControllerError(
                "the running controller was not reused during application access restoration"
            )
        install_inspected = True
    install_inspection_ready = clock.monotonic()
    if restart_after_reset:
        assert record is not None
        record.controller_pod_uid = pod.uid
        record.controller_pod_name = pod.name
    elif record is None or not reusable:
        record = ControllerRecord(
            application=inputs.application,
            namespace=inputs.namespace,
            control_namespace=control,
            lifecycle_fingerprint=inputs.lifecycle_fingerprint,
            controller_pod_uid=pod.uid,
            controller_pod_name=pod.name,
            installed_by_stage=inputs.stage_label,
            installed_at=clock.now(),
            kubeconfig=inputs.kubeconfig,
            runtime_config=runtime_config_payload(config),
        )
    elif pod.uid != record.controller_pod_uid:
        raise PersistentControllerError("the persistent controller pod was replaced mid-pipeline")
    state.controllers[inputs.namespace] = record
    state.save(inputs.state_path)
    known = _known_incident_ids(ops.runtime_state(control))
    install_ready = clock.monotonic()
    generation = f"{inputs.stage_label}-{uuid.uuid4().hex[:8]}"
    try:
        gate_timings = ops.inject_after_resume(control, generation, inject)
    except InvalidInjectionError as exc:
        # No incident exists: settle maintenance without waiting for response, reflection,
        # acknowledgment, or a strict receipt, and retain a benchmark terminal record.
        try:
            paused = f"{generation}-injection-invalid"
            ops.set_maintenance(control, paused=True, generation=paused)
            wait_for_maintenance_ack(ops, control, paused, clock)
        except Exception as cleanup_exc:
            exc.cleanup_error = f"controller cleanup failed: {cleanup_exc}"
        inputs.receipt_dir.mkdir(parents=True, exist_ok=True)
        (inputs.receipt_dir / "sdo_infrastructure_outcome.json").write_text(
            json.dumps(
                {
                    "infra_error": exc.infra_error,
                    "injection_evidence": exc.details,
                    "cleanup_error": exc.cleanup_error,
                    "incident_credit": False,
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        raise
    injected = clock.monotonic()
    verified = _wait_for_verified_incident(
        ops,
        control,
        known,
        inputs.verification_timeout_seconds,
        clock,
        inputs.detection_timeout_seconds,
    )
    verified_ready = clock.monotonic()
    if pause_after_verified:
        pause_controller(ops, control, label=generation, clock=clock)
    paused_ready = clock.monotonic()
    stage_evidence = _export_stage_end_evidence(ops, control, config, inputs.receipt_dir, clock)
    closure = verified.closure or {}
    raw_request = closure.get("request")
    incident_request = raw_request if isinstance(raw_request, dict) else {}
    raw_result = closure.get("result")
    result: dict[str, Any] = raw_result if isinstance(raw_result, dict) else {}
    resolution: dict[str, Any] = {
        "schema_version": "sdo.sregym-incident-resolution/v1",
        "incident_id": verified.incident_id,
        "namespace": inputs.namespace,
        "confirmed_root_causes": result.get("confirmed_root_causes", []),
        "repair_actions": result.get("repair_actions", []),
        "usage": result.get("usage"),
        "playbook_coverage": incident_request.get("playbook_coverage"),
        "resolution_path": result.get("resolution_path"),
        # Names the responder's rollout in the stage-end evidence.
        "responder_session_id": result.get("responder_session_id"),
        "lifecycle_reused": lifecycle_reused,
        "fault_injection_deferred": True,
        "fault_gate_timings_seconds": gate_timings,
        "persistent_controller": {
            "control_namespace": control,
            "controller_pod_uid": pod.uid,
            "controller_pod_name": pod.name,
            "installed_this_stage": not (reusable or shared_adoption),
            "installed_by_stage": record.installed_by_stage,
            "lifecycle_revalidation_skipped": lifecycle_revalidation_skipped,
            "maintenance_generation": generation,
            "stage_label": inputs.stage_label,
        },
        "pre_injection_costs_seconds": {
            "previous_incident_reflection_drain": drain_seconds,
            "inventory_and_lifecycle": lifecycle_ready - started - drain_seconds,
            "controller_install_or_reuse": install_ready - lifecycle_ready,
            "controller_baseline_wait": gate_timings.get("controller_baseline_wait", 0.0),
        },
        "reflection_drain_seconds": drain_seconds,
        "driver_phase_timings_seconds": {
            "controller_inventory": inventory_ready - started,
            "reflection_drain": drain_seconds,
            "lifecycle": lifecycle_ready - lifecycle_started,
            "inventory_and_lifecycle": lifecycle_ready - started - drain_seconds,
            "controller_install_inspection": install_inspection_ready - lifecycle_ready if install_inspected else 0.0,
            "controller_install_or_reuse": install_ready - lifecycle_ready,
            "fault_gate": injected - install_ready,
            "injection_to_verified_recovery": verified_ready - injected,
            "pause_for_redeploy": paused_ready - verified_ready,
            "driver_total_before_submission": paused_ready - started,
        },
    }
    resolution["stage_end_runtime_artifacts"] = stage_evidence
    resolution.update(_resolution_timings(closure))
    record.pending = PendingIncident(
        incident_id=verified.incident_id,
        stage_label=inputs.stage_label,
        receipt_dir=inputs.receipt_dir,
        repository=config.repository,
        verified_at=_timestamp(closure.get("verified_at")),
        reflection_deadline_at=reflection_deadline(inputs, verified, clock),
        stage_receipt={
            key: value
            for key, value in resolution.items()
            if key
            not in {
                "schema_version",
                "confirmed_root_causes",
                "repair_actions",
                "namespace",
                "incident_id",
                "responder_session_id",
                "usage",
                "stage_end_runtime_artifacts",
            }
        },
    )
    record.served_stages.append(inputs.stage_label)
    record.served_stage_receipt = dict(record.pending.stage_receipt)
    state.deferred_receipts.append(
        DeferredReceipt(
            incident_id=verified.incident_id, stage_label=inputs.stage_label, staging_dir=inputs.receipt_dir
        )
    )
    state.save(inputs.state_path)
    persist_resolution(resolution, inputs.receipt_dir)
    return resolution


def _export_stage_end_evidence(
    ops: ClusterOps, control: str, config: RuntimeConfig, receipt_dir: Path, clock: Clock
) -> dict[str, Any]:
    """Export the stage-end snapshot and record how long each part took.

    Both exports are cumulative over the persistent controller's lifetime (its whole log, every retained
    runtime stream), so their cost grows with the incident index; the timings make that visible per stage.
    """

    started = clock.monotonic()
    ops.export_controller_logs(control, receipt_dir)
    logs_done = clock.monotonic()
    evidence: dict[str, Any] = dict(ops.export_runtime_artifacts(config, receipt_dir))
    artifacts_done = clock.monotonic()
    evidence["scope"] = STAGE_END_EVIDENCE_SCOPE
    evidence["export_seconds"] = {
        "controller_logs": round(logs_done - started, 3),
        "runtime_artifacts": round(artifacts_done - logs_done, 3),
    }
    logger.info(
        "stage-end evidence export: controller logs %.1fs, runtime artifacts %.1fs",
        logs_done - started,
        artifacts_done - logs_done,
    )
    return evidence


def pause_controller(ops: ClusterOps, control: str, *, label: str, clock: Clock | None = None) -> None:
    """Pause the controller and wait for its acknowledgement (used when a composition ends)."""

    clock = clock or Clock()
    if incident_work_open(ops.runtime_state(control)):
        logger.info("Leaving %s active: incident closure work is pending", control)
        return
    generation = f"{label}-{uuid.uuid4().hex[:8]}-paused"
    ops.set_maintenance(control, paused=True, generation=generation)
    wait_for_maintenance_ack(ops, control, generation, clock)


@dataclass(frozen=True)
class FollowupClosureWait:
    """Recovery's wait for SDO to close incidents that opened after the graded incident closed."""

    graded_incident_id: str
    followup_incident_ids: tuple[str, ...]
    waited_seconds: float
    bound_seconds: float
    closed: bool


class FollowupClosureTimeout(PersistentControllerError):
    def __init__(self, wait: FollowupClosureWait) -> None:
        super().__init__(
            f"eval: follow-up incident(s) {', '.join(wait.followup_incident_ids)} opened after "
            f"{wait.graded_incident_id!r} closed and stayed open past {wait.bound_seconds:.0f}s"
        )
        self.wait = wait


def _open_incident_id(snapshot: dict[str, Any]) -> str:
    request = snapshot.get("incident_request") or (snapshot.get("pending_closure") or {}).get("request")
    incident_id = request.get("incident_id") if isinstance(request, dict) else None
    return incident_id if isinstance(incident_id, str) and incident_id else "unidentified"


def pause_for_recovery(
    ops: ClusterOps,
    control: str,
    *,
    incident_id: str | None = None,
    clock: Clock | None = None,
    followup_wait_seconds: float = POST_CLOSURE_WAIT_SECONDS,
    reflection_attempt_seconds: float = default_reflection_attempt_seconds(),
    closure_open: bool = False,
) -> FollowupClosureWait | None:
    """Fence harness mutations behind a terminal incident and acknowledged intake pause.

    An incident that opened after the graded incident closed is SDO's work in progress, not a
    harness error: recovery waits a bounded time for SDO to close it and reports that wait. Each
    reflection queued behind a failed attempt's acknowledgment extends the bound by SDO's worst case
    for one reflection (``reflection_attempt_seconds``), since SDO may legitimately run it that long.
    ``closure_open`` is for a row past its reflection deadline: intake is still paused, but the
    still-running closure is left to SDO, so the injected fault does not outlive the row.
    """

    clock = clock or Clock()
    closed: set[str] = set()

    def require_closed() -> None:
        if closure_open:
            return
        snapshot = ops.runtime_state(control)
        if not snapshot or incident_work_open(snapshot):
            raise PersistentControllerError("eval: recovery requires a closed incident and available controller state")
        closed.update(terminal_incident_ids(snapshot))
        if incident_id is not None and incident_id not in closed:
            raise PersistentControllerError(f"eval: recovery incident {incident_id!r} is not durably closed")

    wait = _wait_for_followup_closure(
        ops, control, incident_id, closed, followup_wait_seconds, reflection_attempt_seconds, clock
    )
    require_closed()
    generation = f"benchmark-recovery-{uuid.uuid4().hex}-paused"
    ops.set_maintenance(control, paused=True, generation=generation)
    wait_for_maintenance_ack(ops, control, generation, clock)
    if not any(
        row.get("maintenance_generation") == generation and row.get("controller_maintenance") == "paused"
        for row in _log_records(ops.controller_logs(control))
    ):
        raise PersistentControllerError("eval: recovery requires an acknowledged paused intake")
    # Admission may have raced the pause request; no recovery write is safe until
    # the acknowledged pause is followed by a fresh, idle controller snapshot.
    require_closed()
    return wait


def _wait_for_followup_closure(
    ops: ClusterOps,
    control: str,
    incident_id: str | None,
    closed: set[str],
    bound: float,
    reflection_attempt_seconds: float,
    clock: Clock,
) -> FollowupClosureWait | None:
    """Wait while the graded incident is closed and a later incident or a queued reflection is still open.

    The bound is the largest ``bound + queued reflections x reflection_attempt_seconds`` seen, so a queue
    that drains never shortens a wait already in progress.
    """

    if incident_id is None:
        return None
    started = clock.monotonic()
    followups: list[str] = []
    budget = bound
    while True:
        snapshot = ops.runtime_state(control)
        if not snapshot or not incident_work_open(snapshot):
            break
        closed.update(terminal_incident_ids(snapshot))
        queued = queued_reflection_ids(snapshot)
        # A reflection queued behind a closure is SDO work even when it belongs to the graded incident.
        names = queued or [_open_incident_id(snapshot)]
        if incident_id not in closed or (not queued and names == [incident_id]):
            break
        followups.extend(name for name in names if name not in followups)
        budget = max(budget, bound + queued_reflection_count(snapshot) * reflection_attempt_seconds)
        waited = clock.monotonic() - started
        if waited >= budget:
            raise FollowupClosureTimeout(FollowupClosureWait(incident_id, tuple(followups), waited, budget, False))
        clock.sleep(POLL_SECONDS)
    if not followups:
        return None
    waited = clock.monotonic() - started
    logger.info("recovery waited %.1fs for follow-up incident(s) %s to close", waited, ", ".join(followups))
    return FollowupClosureWait(incident_id, tuple(followups), waited, budget, True)


def collect_followup_incident(
    inputs: StageInputs,
    *,
    ops: ClusterOps,
    known: set[str],
    timeout_seconds: float,
    clock: Clock | None = None,
) -> dict[str, Any] | None:
    """Wait for a further incident the running controller verifies, and register it as pending.

    For compositions: the first incident's stage leaves the controller active, the caller drains
    it, and this collects the next incident for the faults the first did not cover. Returns
    ``None`` when no new incident is verified within ``timeout_seconds``.
    """

    clock = clock or Clock()
    state = PersistentState.load(inputs.state_path)
    record = state.controllers.get(inputs.namespace)
    if record is None:
        raise PersistentControllerError(f"no persistent controller is recorded for {inputs.namespace!r}")
    if record.pending is not None:
        raise PersistentControllerError(
            f"incident {record.pending.incident_id!r} is undrained; drain it before collecting a follow-up"
        )
    control = record.control_namespace
    try:
        verified = _wait_for_verified_incident(ops, control, known, timeout_seconds, clock)
    except ClosureFailedError:
        raise
    except PersistentControllerError:
        return None
    config = replace(inputs.runtime_config, persistent=True, wait_for_completion=False)
    evidence = _export_stage_end_evidence(ops, control, config, inputs.receipt_dir, clock)
    closure = verified.closure or {}
    raw_request = closure.get("request")
    incident_request = raw_request if isinstance(raw_request, dict) else {}
    raw_result = closure.get("result")
    result: dict[str, Any] = raw_result if isinstance(raw_result, dict) else {}
    base = dict(record.served_stage_receipt) if record.served_stage_receipt else {}
    resolution: dict[str, Any] = {
        "schema_version": "sdo.sregym-incident-resolution/v1",
        "incident_id": verified.incident_id,
        "namespace": inputs.namespace,
        "confirmed_root_causes": result.get("confirmed_root_causes", []),
        "repair_actions": result.get("repair_actions", []),
        "usage": result.get("usage"),
        "playbook_coverage": incident_request.get("playbook_coverage"),
        "resolution_path": result.get("resolution_path"),
        "responder_session_id": result.get("responder_session_id"),
        "followup_incident": True,
        "stage_end_runtime_artifacts": evidence,
    }
    resolution.update(_resolution_timings(closure))
    record.pending = PendingIncident(
        incident_id=verified.incident_id,
        stage_label=inputs.stage_label,
        receipt_dir=inputs.receipt_dir,
        repository=config.repository,
        verified_at=_timestamp(closure.get("verified_at")),
        reflection_deadline_at=reflection_deadline(inputs, verified, clock),
        stage_receipt={
            **base,
            # Setup and gate costs were paid once, by the first incident.
            "pre_injection_costs_seconds": {},
            **{
                key: value
                for key, value in resolution.items()
                if key
                not in {
                    "schema_version",
                    "confirmed_root_causes",
                    "repair_actions",
                    "namespace",
                    "incident_id",
                    "responder_session_id",
                    "usage",
                    "stage_end_runtime_artifacts",
                }
            },
        },
    )
    record.served_stages.append(inputs.stage_label)
    state.deferred_receipts.append(
        DeferredReceipt(
            incident_id=verified.incident_id, stage_label=inputs.stage_label, staging_dir=inputs.receipt_dir
        )
    )
    state.save(inputs.state_path)
    persist_resolution(resolution, inputs.receipt_dir)
    return resolution


def _closeout_settled(receipt: dict[str, Any]) -> bool:
    """Whether the two volatile close-out signals the strict receipt samples have settled.

    ``completed`` is the durable responder result's terminal status and
    ``remaining_worktrees`` is the shared worktree directory the controller drains
    asynchronously at acknowledge. The reflection-drain predicate can return while
    either is still settling on composite incidents, so the gate must not sample
    until both are quiescent.
    """

    if receipt.get("schema_version") == "sdo.production-receipt/v2":
        ledger = receipt.get("native_ledger")
        return isinstance(ledger, dict) and ledger.get("acknowledged") is True
    return receipt.get("completed") is True and receipt.get("remaining_worktrees") == []


def _rendered_lock_settled(repository: Path) -> bool:
    """Read the synchronized accepted lock; agents never publish this evidence."""
    path = repository / ".sdo/rendered/lock.json"
    if not path.exists():
        return True  # Legacy repositories have no rendered-lock contract.
    try:
        source = json.loads(path.read_text())["source_commit"]
    except (OSError, ValueError, KeyError, TypeError):
        return False
    if not isinstance(source, str) or not source:
        return False
    result = subprocess.run(
        ["git", "diff", "--quiet", source, "HEAD", "--", ".", ":(exclude).sdo"],
        cwd=repository,
        capture_output=True,
        check=False,
        timeout=30,
    )
    return result.returncode == 0


def _collect_settled_receipt(
    ops: ClusterOps,
    config: RuntimeConfig,
    incident_id: str,
    receipt_dir: Path,
    clock: Clock,
    timeout: float | None = None,
) -> dict[str, Any]:
    """Collect the receipt once close-out has settled, or once the bounded wait elapses.

    Re-samples the receipt until its volatile close-out signals settle so a solved
    incident is not rejected because the asynchronous close-out had not finished.
    Accepted source repairs also wait for the controller-owned rendered lock,
    which is published after acknowledgment; collection synchronizes that Git state.
    A genuinely unsettled receipt (for example a responder that never reported
    ``completed``) still falls through after ``timeout`` and is validated as-is, so
    a real failure is reported promptly rather than hidden.
    """

    receipt = ops.collect_receipt(config, incident_id, receipt_dir)
    scale = float(os.getenv("SDO_TIME_SCALE", "1"))
    rendered_timeout = timeout if timeout is not None else RENDERED_LOCK_SETTLE_TIMEOUT_SECONDS * scale
    if timeout is None:
        timeout = CLOSEOUT_SETTLE_TIMEOUT_SECONDS * scale
    deadline = clock.monotonic() + timeout
    resumed = False
    control = config.control_namespace
    previous_lines: list[str] = []
    last_error = ""
    progress = "waiting for controller resume"
    generation = f"rendered-closeout-{uuid.uuid4().hex[:8]}"

    def reconciliation_error(lines: list[str]) -> str:
        for line in reversed(lines):
            marker = line.find("reconcile rendered lock:")
            if marker >= 0:
                return line[marker:][:2048]
        return ""

    try:
        while (
            not _closeout_settled(receipt) or not _rendered_lock_settled(config.repository)
        ) and clock.monotonic() < deadline:
            if _closeout_settled(receipt) and not _rendered_lock_settled(config.repository) and not resumed:
                # Maintenance permits closure but suppresses rendered reconciliation.
                # Recovery precedes fastloop learning. Let the acknowledged
                # controller publish source evidence, then pause it again.
                previous_lines = ops.controller_logs(control).splitlines()
                last_error = reconciliation_error(previous_lines)
                ops.set_maintenance(control, paused=False, generation=generation)
                resumed = True
                deadline = clock.monotonic() + rendered_timeout
            if resumed:
                lines = ops.controller_logs(control).splitlines()
                new_lines = lines[len(previous_lines) :] if lines[: len(previous_lines)] == previous_lines else lines
                previous_lines = lines
                error = reconciliation_error(new_lines)
                if error:
                    raise PersistentControllerError(f"rendered source lock publication failed: {error}")
                for record in _log_records("\n".join(new_lines)):
                    phase = progress
                    if "state_baseline_startup_ms" in record:
                        phase = "state baseline initialized"
                    if record.get("maintenance_generation") == generation:
                        phase = "controller resumed; waiting for rendered lock"
                    if phase != progress:
                        progress = phase
                        logger.info("rendered source lock closeout: %s", progress)
            clock.sleep(POLL_SECONDS)
            receipt = ops.collect_receipt(config, incident_id, receipt_dir)
        if not _rendered_lock_settled(config.repository):
            details = f"; controller progress: {progress}"
            if last_error:
                details += f"; last reported controller error: {last_error}"
            raise PersistentControllerError(
                "controller acknowledged closure but did not publish a current rendered source lock" + details
            )
        return receipt
    finally:
        if resumed:
            try:
                pause_controller(ops, control, label="rendered-closeout", clock=clock)
            finally:
                # Stage-end evidence predates this resume. Preserve its actual
                # diagnostics before teardown removes the acknowledged runtime.
                try:
                    ops.export_controller_logs(control, receipt_dir)
                except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
                    logger.warning("could not retain rendered closeout logs: %s", type(exc).__name__)


def record_failed_closure(pending: PendingIncident, message: str, waited_seconds: float) -> ReceiptDrainResult:
    """Record an incident whose closure the controller gave up on as that incident's rejected receipt."""

    failure = ReceiptFailure(
        incident_id=pending.incident_id, code=ReceiptFailure.CODE_REJECTED_EVIDENCE, message=message
    )
    rejection = RejectedProductionReceipt(validation_error=message, failure=failure, receipt_json="{}")
    _write_json(
        json_format.MessageToDict(rejection, preserving_proto_field_name=True),
        pending.receipt_dir / REJECTED_RECEIPT_FILENAME,
    )
    return ReceiptDrainResult(incident_id=pending.incident_id, waited_seconds=waited_seconds, failure=failure)


def _lost_controller_message(record: ControllerRecord, pod: ControllerPod | None) -> str:
    assert record.pending is not None
    now = "is absent" if pod is None else f"is now {pod.name!r} (uid {pod.uid!r})"
    return (
        f"controller pod {record.controller_pod_name!r} (uid {record.controller_pod_uid!r}) of "
        f"{record.control_namespace!r} was lost before incident {record.pending.incident_id!r} was drained "
        f"into operational memory; the controller {now}, so the adapter cannot collect its strict receipt"
    )


def reflection_deadline(inputs: StageInputs, verified: VerifiedIncident, clock: Clock) -> datetime:
    """The incident's one reflection deadline: its budget plus SDO's worst case for each reflection queued ahead.

    A closure waits behind the queue's FIFO (one reflection at a time under the broker lock), so a fixed
    budget from verification would expire while SDO is still legitimately working through earlier failed
    attempts.
    """

    seconds = inputs.reflection_wait_seconds + verified.queued_reflections * inputs.reflection_attempt_seconds
    return clock.now() + timedelta(seconds=seconds)


def drain_pending_incident(
    record: ControllerRecord,
    *,
    ops: ClusterOps,
    repository: Path,
    drained_by: str,
    clock: Clock | None = None,
) -> ReceiptDrainResult:
    """Wait for a verified incident's reflection and rollout, then write its strict receipt.

    ``repository`` receives the controller's operational repository, which is
    the source of truth across problems. Every drain of an incident waits only until the
    pending record's one reflection deadline, so a drain after a timed-out drain fails at once.
    Returns the elapsed wait and any typed receipt failure for this incident.
    """

    clock = clock or Clock()
    pending = record.pending
    if pending is None:
        return ReceiptDrainResult()
    if pending.reflection_deadline_at is None:
        pending.reflection_deadline_at = clock.now() + timedelta(seconds=DRAIN_TIMEOUT_SECONDS)
    started = clock.monotonic()
    phases: dict[str, float] = {}
    phase_timestamps = {"drain_started": clock.now().isoformat()}
    try:
        drained_state = _wait_for_reflection_drain(
            ops, record.control_namespace, pending.incident_id, pending.reflection_deadline_at, clock
        )
    except ClosureFailedError:
        # The controller log holds every broker rejection of the closure.
        ops.export_controller_logs(record.control_namespace, pending.receipt_dir)
        raise
    phases["wait_for_ack"] = clock.monotonic() - started
    phase_timestamps["ack_observed"] = clock.now().isoformat()
    config = replace(
        runtime_config_from_payload(record.runtime_config),
        repository=repository,
        artifacts_dir=pending.receipt_dir,
        persistent=True,
    )
    # Complete the teardown an abandoned incident left behind, so its leaked
    # worktree does not reject this solved incident's receipt. Safe-guarded: only
    # when the controller is positively quiescent.
    # The native Go broker releases abandoned proposals and acknowledged
    # incident worktrees; the benchmark must not mutate its workspace.
    del drained_state
    # The stage-end export ran before reflection; re-export so the reflection rollout,
    # usage and checkpoints are complete. The Go export overwrites files in place.
    runtime_artifacts = ops.export_runtime_artifacts(config, pending.receipt_dir)
    receipt: dict[str, Any] = {}
    try:
        receipt = _collect_settled_receipt(ops, config, pending.incident_id, pending.receipt_dir, clock)
        phases["collect_settled_receipt"] = clock.monotonic() - started - phases["wait_for_ack"]
        phase_timestamps["receipt_collected"] = clock.now().isoformat()
        # The reflection-drain cost includes the wait for close-out to settle; neither
        # is resolution time (both are excluded below).
        waited = clock.monotonic() - started
        receipt.update(pending.stage_receipt)
        recovery = receipt.get("phase_timings_seconds", {}).get("operational_recovery")
        if "incident_resolution_seconds" not in receipt and isinstance(recovery, (int, float)):
            receipt["incident_resolution_seconds"] = float(recovery)
            receipt["incident_resolution_scope"] = "detected_to_independently_verified_health"
        # Resolution ends at verified health. Setup before injection and learning
        # after verification (including this drain) are reported, never included.
        receipt["excluded_from_incident_resolution_seconds"] = {
            **pending.stage_receipt.get("pre_injection_costs_seconds", {}),
            "post_recovery_learning_and_receipt": receipt.get("phase_timings_seconds", {}).get(
                "post_recovery_learning_and_receipt"
            ),
        }
        receipt["reflection_drain"] = {
            "drained_by": drained_by,
            "waited_seconds": waited,
            "phase_seconds": phases,
            "phase_timestamps": phase_timestamps,
            "runtime_artifacts": runtime_artifacts,
        }
        validate_production_receipt(receipt, allow_test_lifecycle=config.allow_test_lifecycle)
    except ProductionReceiptValidationError as exc:
        if isinstance(exc, NativeReceiptValidationError):
            receipt = exc.receipt
        ops.export_controller_logs(record.control_namespace, pending.receipt_dir)
        failure = (
            exc.failure
            if isinstance(exc, NativeReceiptValidationError)
            else ReceiptFailure(
                incident_id=pending.incident_id,
                code=ReceiptFailure.CODE_REJECTED_EVIDENCE,
                message=str(exc),
            )
        )
        rejection = RejectedProductionReceipt(
            validation_error=str(exc), failure=failure, receipt_json=json.dumps(receipt)
        )
        try:
            json_format.ParseDict(receipt, rejection.receipt)
            rejected_document = json_format.MessageToDict(rejection, preserving_proto_field_name=True)
        except (json_format.Error, ValueError):
            rejection.ClearField("receipt")
            rejected_document = json_format.MessageToDict(rejection, preserving_proto_field_name=True)
        _write_json(rejected_document, pending.receipt_dir / REJECTED_RECEIPT_FILENAME)
        return ReceiptDrainResult(
            incident_id=pending.incident_id,
            waited_seconds=clock.monotonic() - started,
            failure=failure,
        )
    # The controller log covering reflection is evidence whether or not the receipt validates.
    ops.export_controller_logs(record.control_namespace, pending.receipt_dir)
    persist_strict_receipt(receipt, pending.receipt_dir)
    return ReceiptDrainResult(incident_id=pending.incident_id, waited_seconds=waited)


def teardown(state_path: Path, *, ops: ClusterOps, clock: Clock | None = None) -> list[str]:
    """Drain every pending incident, stop every persistent controller, and return errors."""

    state = PersistentState.load(state_path)
    errors: list[str] = []
    for namespace, record in list(state.controllers.items()):
        try:
            pod = ops.controller_pod(record.control_namespace)
        except (PersistentControllerError, ControllerInstallError, OSError, ValueError) as exc:
            # A failed inventory cannot authorize deleting a possibly healthy controller.
            errors.append(f"{namespace}: {exc}")
            continue
        try:
            if record.pending is not None:
                if pod is None or pod.uid != record.controller_pod_uid:
                    drained = record_failed_closure(record.pending, _lost_controller_message(record, pod), 0.0)
                else:
                    drained = drain_pending_incident(
                        record,
                        ops=ops,
                        repository=record.pending.repository,
                        drained_by="pipeline-teardown",
                        clock=clock,
                    )
                if drained.HasField("failure"):
                    logger.warning("incident %s: %s", drained.incident_id, drained.failure.message)
                record.pending = None
                state.save(state_path)
        except (PersistentControllerError, ControllerInstallError, OSError, ValueError) as exc:
            errors.append(f"{namespace}: {exc}")
        finally:
            try:
                if isinstance(ops, KubectlClusterOps):
                    target = CleanupTarget(
                        namespace=record.namespace,
                        control_namespace=record.control_namespace,
                        kubeconfig=record.kubeconfig,
                    )
                    ops.stop_runtime(target)
                    ops.stop_transport(target)
                    ops.retire_controller_state(
                        target, state_path.parent / f"{state_path.name}.teardown-state" / record.control_namespace
                    )
                else:
                    ops.delete_controller(record.control_namespace)
            except (PersistentControllerError, NativeOperatorError, OSError, ValueError, RuntimeError) as exc:
                errors.append(f"{namespace}: stop failed: {exc}")
            else:
                state.controllers.pop(namespace, None)
            state.save(state_path)
    return errors


def abort(state_path: Path, *, ops: AbortClusterOps) -> list[str]:
    """Clean recorded resources only after capturing and protecting incident workers."""
    from benchmarks.sregym.adapter.processes import terminate_run

    try:
        errors = terminate_run(state_path)
    except (OSError, ValueError, RuntimeError) as exc:
        errors = [f"host helper cleanup failed: {exc}"]
    state_path = state_path.resolve()
    state = PersistentState.load(state_path)
    try:
        with state_path.with_name(state_path.name + ".abort-evidence.json").open("x", encoding="utf-8") as evidence:
            evidence.write(state.model_dump_json(indent=2) + "\n")
    except FileExistsError:
        pass  # A retry must retain the original pending-incident evidence.
    targets = dict(state.cleanup_targets)
    for namespace, record in state.controllers.items():
        targets[namespace] = CleanupTarget(
            namespace=record.namespace, control_namespace=record.control_namespace, kubeconfig=record.kubeconfig
        )
    state.cleanup_targets = targets
    state.save(state_path)
    for namespace, target in targets.items():
        try:
            ops.protect_incident_work(
                target, state_path.parent / (state_path.name + ".abort-workers") / target.control_namespace
            )
        except (RuntimeError, OSError, ValueError, KeyError, subprocess.SubprocessError) as exc:
            errors.append(f"{namespace}: worker cleanup blocked: {exc}")
            continue
        record = state.controllers.get(namespace)
        if record is not None and record.pending is not None:
            state.aborted_incidents.append(
                AbortedIncident(
                    incident_id=record.pending.incident_id,
                    namespace=namespace,
                    stage_label=record.pending.stage_label,
                    aborted_at=datetime.now(timezone.utc),
                )
            )
            record.pending = None
        state.save(state_path)
        for action in (ops.stop_runtime, ops.abort_workload):
            try:
                action(target)
            except (RuntimeError, OSError, ValueError, subprocess.SubprocessError) as exc:
                errors.append(f"{namespace}: {exc}")
        try:
            leftovers = ops.abort_leftovers(target)
            errors.extend(f"{namespace}: leftover {name}" for name in leftovers)
            if not leftovers:
                # Only a quiet lane can drop its checkpoint; live work keeps its evidence.
                ops.retire_controller_state(
                    target, state_path.parent / f"{state_path.name}.abort-state" / target.control_namespace
                )
                state.controllers.pop(namespace, None)
        except (RuntimeError, OSError, ValueError, subprocess.SubprocessError) as exc:
            errors.append(f"{namespace}: could not verify quiet lane: {exc}")
        state.save(state_path)
    return errors


class AbortClusterOps(Protocol):
    def protect_incident_work(self, target: CleanupTarget, directory: Path) -> None: ...
    def stop_runtime(self, target: CleanupTarget) -> None: ...
    def abort_workload(self, target: CleanupTarget) -> None: ...
    def abort_leftovers(self, target: CleanupTarget) -> list[str]: ...
    def retire_controller_state(self, target: CleanupTarget, archive_dir: Path) -> list[str]: ...


def _publish_drained_receipts(state_path: Path) -> None:
    """Publish every drained receipt into the pipeline's results; teardown retries what fails."""

    try:
        for path in publish_deferred_receipts(state_path, state_path.parent):
            logger.info("published drained artifact %s", path)
    except OSError as exc:
        logger.warning("could not publish drained receipts yet: %s", exc)


def publish_deferred_receipts(state_path: Path, results_root: Path) -> list[Path]:
    """Copy drained receipts and controller logs into the harness's published run directories.

    SREGym publishes a stage's staging tree (``.runtime/<agent>/<opaque id>``)
    to ``results/<agent>/<problem>/run_N`` when the stage ends, before the
    drain can write the strict receipt. Each published run is matched to its
    incident by the resolution record it already contains, and the opaque id is
    replaced by the problem id, as the harness does at publication.
    """

    state = PersistentState.load(state_path)
    deferred = {receipt.incident_id: receipt for receipt in state.deferred_receipts}
    published: list[Path] = []
    for resolution_path in sorted(results_root.rglob(RESOLUTION_FILENAME)):
        run_dir = resolution_path.parent
        try:
            incident_id = json.loads(resolution_path.read_text(encoding="utf-8")).get("incident_id")
        except (OSError, json.JSONDecodeError, AttributeError):
            continue
        entry = deferred.get(incident_id) if isinstance(incident_id, str) else None
        if entry is None or entry.staging_dir.resolve() == run_dir.resolve() or not entry.staging_dir.is_dir():
            continue
        sources = [
            entry.staging_dir / STRICT_RECEIPT_FILENAME,
            entry.staging_dir / REJECTED_RECEIPT_FILENAME,
        ]
        logs_dir = entry.staging_dir / CONTROLLER_LOGS_SUBDIR
        if logs_dir.is_dir():
            sources.extend(sorted(path for path in logs_dir.iterdir() if path.is_file()))
        # Drain-time runtime evidence (usage logs, agent transcripts) is published
        # byte for byte; per-incident analysis scopes the cumulative logs itself.
        runtime_dir = entry.staging_dir / RUNTIME_ARTIFACTS_DIRNAME
        evidence = (
            sorted(path for path in runtime_dir.rglob("*") if path.is_file() and logs_dir not in path.parents)
            if runtime_dir.is_dir()
            else []
        )
        for source in [*sources, *evidence]:
            if not source.is_file():
                continue
            target = run_dir / source.relative_to(entry.staging_dir)
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = target.with_suffix(target.suffix + ".tmp")
            if source in sources:
                text = source.read_text(encoding="utf-8").replace(entry.staging_dir.name, run_dir.parent.name)
                temporary.write_text(text, encoding="utf-8")
            else:
                shutil.copyfile(source, temporary)
            temporary.replace(target)
            source.unlink()
            published.append(target)
        _remove_empty_directories(entry.staging_dir)
    return published


def _remove_empty_directories(root: Path) -> None:
    for directory in sorted((path for path in root.rglob("*") if path.is_dir()), reverse=True):
        if not any(directory.iterdir()):
            directory.rmdir()
    if root.is_dir() and not any(root.iterdir()):
        root.rmdir()


def persist_resolution(resolution: dict[str, Any], receipt_dir: Path) -> Path:
    return _write_json(resolution, receipt_dir / RESOLUTION_FILENAME)


def persist_strict_receipt(receipt: dict[str, Any], receipt_dir: Path) -> Path:
    return _write_json(receipt, receipt_dir / STRICT_RECEIPT_FILENAME)


def _write_json(document: dict[str, Any], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(document, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    temporary.replace(path)
    return path


def _timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value or value == _ZERO_TIME:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _resolution_timings(closure: dict[str, Any]) -> dict[str, Any]:
    """Resolution ends at controller-verified health; reflection is never included."""

    detected = _timestamp(closure.get("detected_at"))
    dispatched = _timestamp(closure.get("dispatched_at"))
    responder_completed = _timestamp(closure.get("responder_completed_at"))
    verified = _timestamp(closure.get("verified_at"))
    # A closure cut by the verification timeout carries no final detector states: health never cleared.
    if detected is None or verified is None or not closure.get("final_detector_states"):
        return {}
    timings: dict[str, float] = {"operational_recovery": (verified - detected).total_seconds()}
    if dispatched is not None:
        timings["detection_to_dispatch"] = (dispatched - detected).total_seconds()
    if dispatched is not None and responder_completed is not None:
        timings["responder"] = (responder_completed - dispatched).total_seconds()
    if responder_completed is not None:
        timings["verification"] = (verified - responder_completed).total_seconds()
    return {
        "incident_resolution_seconds": timings["operational_recovery"],
        "incident_resolution_scope": "detected_to_independently_verified_health",
        "resolution_phase_timings_seconds": timings,
        "detected_at": detected.isoformat(),
        "responder_started_at": dispatched.isoformat() if dispatched else None,
        "responder_finished_at": responder_completed.isoformat() if responder_completed else None,
        "verified_at": verified.isoformat(),
    }


def _observe_state(
    state: dict[str, Any],
    *,
    known: set[str] | None = None,
    incident_id: str = "",
    timeout: float = 30.0,
) -> ControllerObservation:
    try:
        observed = observe_controller_state(state, known=known, incident_id=incident_id, timeout=timeout)
    except NativeOperatorError as exc:
        raise PersistentControllerError(str(exc)) from exc
    if observed.failure.code == ControllerObservationFailure.CODE_INVALID_SNAPSHOT:
        raise PersistentControllerError(observed.failure.message)
    return observed


def _raise_observation_failure(observed: ControllerObservation, state: dict[str, Any]) -> None:
    if observed.failure.code == ControllerObservationFailure.CODE_CLOSURE_FAILED:
        raise ClosureFailedError(observed.failure.message)


def _known_incident_ids(state: dict[str, Any]) -> set[str]:
    return set(_observe_state(state).known_incident_ids)


def _wait_for_controller_pod(ops: ClusterOps, control: str, clock: Clock, timeout: float = 300.0) -> ControllerPod:
    deadline = clock.monotonic() + timeout
    while clock.monotonic() < deadline:
        pod = ops.controller_pod(control)
        if pod is not None:
            return pod
        clock.sleep(POLL_SECONDS)
    raise PersistentControllerError(f"no running controller pod in {control!r}")


def _wait_for_verified_incident(
    ops: ClusterOps,
    control: str,
    known: set[str],
    timeout: float,
    clock: Clock,
    detection_timeout: float | None = None,
) -> VerifiedIncident:
    """Return the first new incident the controller verified healthy, before reflection finishes.

    With *detection_timeout*, a stage in which no new incident has opened by
    then ends as a :class:`DetectionMissError`; once one opens, the full
    *timeout* applies.
    """

    started = clock.monotonic()
    deadline = started + timeout
    detected = False
    state: dict[str, Any] = {}
    summary = ""
    while clock.monotonic() < deadline:
        state = ops.runtime_state(control)
        observed = _observe_state(
            state,
            known=known,
            timeout=min(30.0, max(0.001, deadline - clock.monotonic())),
        )
        summary = observed.open_incident_summary
        if observed.failure.code == ControllerObservationFailure.CODE_CLOSURE_FAILED:
            _raise_observation_failure(observed, state)
        detected = detected or observed.new_incident_opened
        if detection_timeout is not None and not detected and clock.monotonic() - started >= detection_timeout:
            raise DetectionMissError(
                f"controller in {control!r}: no incident opened within {detection_timeout:.0f}s of fault injection"
            )
        if observed.verified_incident_id:
            closure = (
                json_format.MessageToDict(observed.verified_closure, preserving_proto_field_name=True)
                if observed.HasField("verified_closure")
                else None
            )
            return VerifiedIncident(
                incident_id=observed.verified_incident_id,
                closure=closure,
                queued_reflections=queued_reflection_count(state),
            )
        _raise_observation_failure(observed, state)
        clock.sleep(POLL_SECONDS)
    raise PersistentControllerError(
        f"controller in {control!r} verified no new incident within {timeout:.0f}s{summary}"
    )


def _log_records(logs: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for line in logs.splitlines():
        try:
            payload: Any = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            records.append(payload)
    return records


def inject_while_paused(
    ops: ClusterOps, control: str, generation: str, inject: Callable[[], None], *, clock: Clock | None = None
) -> None:
    """Keep partial injector writes behind maintenance until v2 validity is known."""
    clock = clock or Clock()
    paused = f"{generation}-validating-injection"
    ops.set_maintenance(control, paused=True, generation=paused, preserve_baseline=True)
    wait_for_maintenance_ack(ops, control, paused, clock)
    inject()  # failure intentionally leaves maintenance paused
    ops.set_maintenance(control, paused=False, generation=f"{generation}-injected")


def wait_for_maintenance_ack(
    ops: ClusterOps, control: str, generation: str, clock: Clock, timeout: float | None = None
) -> None:
    if timeout is None:
        timeout = MAINTENANCE_ACK_TIMEOUT_SECONDS * float(os.getenv("SDO_TIME_SCALE", "1"))
    deadline = clock.monotonic() + timeout
    while clock.monotonic() < deadline:
        records = _log_records(ops.controller_logs(control))
        if any(record.get("maintenance_generation") == generation for record in records):
            return
        clock.sleep(POLL_SECONDS)
    raise PersistentControllerError(f"controller in {control!r} did not acknowledge maintenance {generation!r}")


def controller_job_failure(job: dict[str, Any]) -> str | None:
    """A Job's terminal condition as ``reason: message``; a Failed or Complete Job never runs another pod."""

    for condition in (job.get("status") or {}).get("conditions") or []:
        terminal = isinstance(condition, dict) and condition.get("type") in {"Failed", "Complete"}
        if terminal and condition.get("status") == "True":
            reason = condition.get("reason") or condition["type"]
            return f"{reason}: {condition.get('message') or 'no message'}"
    return None


def _wait_for_reflection_drain(
    ops: ClusterOps, control: str, incident_id: str, deadline_at: datetime, clock: Clock
) -> dict[str, Any]:
    """Wait for broker acknowledgment.

    A successful closure's reflection and detector rollout precede its
    acknowledgment. A non-success closure is acknowledged on its outcome commit;
    its queued reflection and any detector rollout follow, and the next
    problem's admission waits for them through ``incident_work_open``.

    Stops early, typed, when the controller Job is terminal: no owner is left to acknowledge.
    """

    timeout = max(0.0, (deadline_at - clock.now()).total_seconds())
    deadline = clock.monotonic() + timeout
    while True:
        # Only a read that shows SDO still owning the closure makes a passed deadline a reflection deadline.
        # A failed or empty read, or a failed observation, says nothing; retry within the same budget.
        error: str | None = None
        try:
            state = ops.runtime_state(control)
        except (RuntimeError, ValueError, KeyError, OSError) as exc:
            state, error = {}, str(exc) or type(exc).__name__
        if not state:
            error = error or "empty controller state"
        else:
            try:
                observed = _observe_state(
                    state,
                    incident_id=incident_id,
                    timeout=min(30.0, max(OBSERVATION_FLOOR_SECONDS, deadline - clock.monotonic())),
                )
                _raise_observation_failure(observed, state)
            except ClosureFailedError:
                raise
            except PersistentControllerError as exc:
                error = f"observation failed: {exc}"
            else:
                if observed.acknowledged:
                    return state
            _raise_terminal_controller_job(ops, control, incident_id)
        if clock.monotonic() >= deadline:
            break
        clock.sleep(POLL_SECONDS)
    if not state or not incident_work_open(state):
        raise ControllerStateUnavailableError(
            f"incident {incident_id!r}: closure state unknown at its reflection deadline "
            f"{deadline_at.isoformat()}: {error}"
        )
    raise ReflectionDeadlineError(
        f"incident {incident_id!r} was not reflected and rolled out by its reflection deadline "
        f"{deadline_at.isoformat()}"
    )


def _raise_terminal_controller_job(ops: ClusterOps, control: str, incident_id: str) -> None:
    # An unreadable Job says nothing about the closure; the state reads keep deciding.
    try:
        failure = ops.controller_job_failure(control)
    except (PersistentControllerError, RuntimeError, ValueError, OSError) as exc:
        logger.warning("controller Job in %s unreadable while draining %s: %s", control, incident_id, exc)
        return
    if failure is not None:
        raise ControllerJobFailedError(
            f"controller Job {CONTROLLER_JOB_NAME!r} in {control!r} is terminal ({failure}) before incident "
            f"{incident_id!r} was acknowledged; no controller can acknowledge its closure"
        )


def _relaunched_after(records: list[dict[str, Any]], incident_id: str) -> bool:
    restarted = False
    relaunched = False
    for record in records:
        if record.get("controller_closure_restart") == incident_id:
            restarted = True
        elif restarted and record.get("controller_supervisor") == "relaunch":
            relaunched = True
        elif relaunched and "controller_maintenance" in record:
            # The relaunched controller is up and has read its maintenance mode.
            return True
    return False


def _main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="Persistent SDO controller lifecycle for SREGym pipelines")
    subparsers = parser.add_subparsers(dest="command", required=True)
    teardown_parser = subparsers.add_parser("teardown", help="drain pending incidents and stop every controller")
    teardown_parser.add_argument("--state", type=Path, required=True)
    teardown_parser.add_argument(
        "--publish-root",
        type=Path,
        help="pipeline directory whose published runs receive the drained receipts and controller logs",
    )
    abort_parser = subparsers.add_parser("abort", help="kill the run and stop its recorded resources without draining")
    abort_parser.add_argument("--state", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == "abort":
        errors = abort(args.state, ops=KubectlClusterOps())
        print(json.dumps({"quiet": not errors, "leftovers_or_errors": errors}, indent=2))
        return 1 if errors else 0
    state = PersistentState.load(args.state)
    kubeconfigs = {record.kubeconfig for record in state.controllers.values() if record.kubeconfig}
    if kubeconfigs:
        os.environ["KUBECONFIG"] = sorted(kubeconfigs)[0]
    errors = teardown(args.state, ops=KubectlClusterOps())
    if args.publish_root is not None:
        for path in publish_deferred_receipts(args.state, args.publish_root):
            logger.info("published deferred artifact %s", path)
    for error in errors:
        logger.error("persistent controller teardown: %s", error)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(_main())
