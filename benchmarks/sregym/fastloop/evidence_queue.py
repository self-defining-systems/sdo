"""Lane-owned durable queue that delivers sealed incident evidence off the row's critical path.

The drain seals an incident's whitelisted runtime evidence into one immutable archive on the
lane's repository PVC (the row boundary) and submits a job here. Delivery copies exactly that
archive into the earlier row's results, so it can never include the next row's mutable files.
Jobs are files under the lane's run directory: a restarted process resumes them, a retry names
the same archive, and a duplicate submission is rejected. Before anything deletes the
repository PVC, :func:`settle_lane_exports` drains the queue. A failed delivery stays recorded
in the earlier row (``runtime_artifacts_export.json``) and is never silently discarded.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from collections.abc import Callable

    from benchmarks.sregym.adapter import RuntimeConfig, RuntimeEvidenceTarget, RuntimeSnapshot
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

logger = logging.getLogger(__name__)

QUEUE_DIRNAME = "evidence-exports"
CHECKPOINT_INPUTS_SUFFIX = ".configmaps"
EXPORT_STATUS_FILENAME = "runtime_artifacts_export.json"
DEFAULT_CAPACITY = 2
DEFAULT_ATTEMPTS = 3


class ExportState(StrEnum):
    QUEUED = "queued"
    DELIVERED = "delivered"
    FAILED = "failed"


class DuplicateExportError(ValueError):
    """A job for this sealed snapshot is already owned by the lane queue."""


class ExportJob(BaseModel):
    """One sealed incident archive and its delivery state."""

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)

    schema_name: Literal["sdo.evidence-export/v1"] = Field(default="sdo.evidence-export/v1", alias="schema")
    incident_id: str = Field(min_length=1)
    snapshot_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,62}$")
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    bytes: int = Field(ge=0)
    receipt_dir: str
    control_namespace: str = Field(min_length=1)
    repository_pvc: str = Field(min_length=1)
    controller_image: str = Field(min_length=1)
    kubeconfig: str | None = None
    checkpoint_index: int | None = Field(default=None, ge=0)
    state: ExportState = ExportState.QUEUED
    attempts: int = Field(default=0, ge=0)
    error: str | None = None
    submitted_at: str
    finished_at: str | None = None

    def model_post_init(self, _context: Any) -> None:
        if not Path(self.receipt_dir).is_absolute():
            raise ValueError("export receipt directory must be absolute")

    def target(self) -> RuntimeEvidenceTarget:
        from benchmarks.sregym.adapter import RuntimeEvidenceTarget

        return RuntimeEvidenceTarget(
            self.control_namespace,
            self.repository_pvc,
            self.controller_image,
            Path(self.kubeconfig) if self.kubeconfig else None,
        )

    def snapshot(self) -> RuntimeSnapshot:
        from benchmarks.sregym.adapter import RuntimeSnapshot

        return RuntimeSnapshot(self.snapshot_id, self.sha256, self.bytes, created=False)

    def receipt_record(self) -> dict[str, Any]:
        """The receipt's ``runtime_artifacts`` record while delivery is owned by the queue."""

        return {
            "directory": str(Path(self.receipt_dir) / "sdo_runtime"),
            "error": None,
            "delivery": "queued",
            "snapshot_id": self.snapshot_id,
            "sha256": self.sha256,
            "bytes": self.bytes,
        }


def snapshot_id_for(incident_id: str) -> str:
    return "i-" + hashlib.sha256(incident_id.encode()).hexdigest()[:32]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_atomic(path: Path, document: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.partial")
    partial.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(partial, path)


class EvidenceExportQueue:
    """Bounded FIFO with one delivery worker; submissions block while ``capacity`` jobs are outstanding."""

    def __init__(
        self,
        directory: Path,
        deliver: Callable[[ExportJob], None],
        *,
        capacity: int = DEFAULT_CAPACITY,
        attempts: int = DEFAULT_ATTEMPTS,
        retry_seconds: float | None = None,
    ) -> None:
        if capacity < 1:
            raise ValueError("export queue capacity must be positive")
        if attempts < 1:
            raise ValueError("export attempts must be positive")
        if retry_seconds is None:
            from benchmarks.sregym.adapter import WaitKind, wait_for

            retry_seconds = wait_for(WaitKind.POLL)
        if retry_seconds < 0:
            raise ValueError("export retry delay must be non-negative")
        self._directory = directory
        self._deliver = deliver
        self._capacity = capacity
        self._attempts = attempts
        self._retry_seconds = retry_seconds
        self._condition = threading.Condition()
        self._pending: deque[str] = deque()
        self._outstanding: set[str] = set()
        self._closed = False
        self._thread: threading.Thread | None = None
        self.backpressure_seconds = 0.0
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        # A restarted process resumes every undelivered job from its sealed archive.
        for job in self.jobs():
            if job.state is not ExportState.DELIVERED:
                self._outstanding.add(job.snapshot_id)
                self._pending.append(job.snapshot_id)
        if self._pending:
            self._start()

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def directory(self) -> Path:
        return self._directory

    def _path(self, snapshot_id: str) -> Path:
        return self._directory / f"{snapshot_id}.json"

    def job(self, snapshot_id: str) -> ExportJob | None:
        path = self._path(snapshot_id)
        if not path.is_file():
            return None
        return ExportJob.model_validate_json(path.read_text(encoding="utf-8"))

    def jobs(self) -> list[ExportJob]:
        """Every readable job; an unreadable file is logged and left for inspection."""

        jobs: list[ExportJob] = []
        for path in sorted(self._directory.glob("*.json")):
            try:
                jobs.append(ExportJob.model_validate_json(path.read_text(encoding="utf-8")))
            except (OSError, ValueError):
                logger.exception("unreadable evidence export job %s", path)
        return jobs

    def _record(self, job: ExportJob) -> None:
        document = job.model_dump(mode="json", by_alias=True)
        _write_atomic(self._path(job.snapshot_id), document)
        # The earlier row shows whether its evidence is pending, delivered or failed.
        _write_atomic(Path(job.receipt_dir) / EXPORT_STATUS_FILENAME, document)

    def submit(self, job: ExportJob) -> None:
        """Take durable ownership of ``job``; blocks while the queue is full."""

        if job.state is not ExportState.QUEUED or job.attempts:
            raise ValueError("only a fresh job can be submitted")
        started = time.monotonic()
        with self._condition:
            if self._closed:
                raise RuntimeError("export queue is closed")
            while len(self._outstanding) >= self._capacity:
                self._condition.wait()
            self.backpressure_seconds += time.monotonic() - started
            document = job.model_dump(mode="json", by_alias=True)
            try:
                with self._path(job.snapshot_id).open("x", encoding="utf-8") as handle:
                    handle.write(json.dumps(document, indent=2, sort_keys=True) + "\n")
            except FileExistsError as exc:
                raise DuplicateExportError(f"snapshot {job.snapshot_id} is already queued") from exc
            _write_atomic(Path(job.receipt_dir) / EXPORT_STATUS_FILENAME, document)
            self._outstanding.add(job.snapshot_id)
            self._pending.append(job.snapshot_id)
            self._start()
            self._condition.notify_all()

    def _start(self) -> None:
        if self._thread is None or not self._thread.is_alive():
            self._thread = threading.Thread(target=self._work, name="sdo-evidence-export", daemon=True)
            self._thread.start()

    def _work(self) -> None:
        while True:
            with self._condition:
                while not self._pending and not self._closed:
                    self._condition.wait()
                if not self._pending:
                    return
                snapshot_id = self._pending[0]
            try:
                job = self.job(snapshot_id)
                if job is not None:
                    self._deliver_with_retries(job)
            except Exception:
                # An unreadable job must not stop the worker or hang drain();
                # its file stays for inspection and the next process retries it.
                logger.exception("evidence export job %s could not be processed", snapshot_id)
            with self._condition:
                self._pending.popleft()
                self._outstanding.discard(snapshot_id)
                self._condition.notify_all()

    def _deliver_with_retries(self, job: ExportJob) -> None:
        for attempt in range(self._attempts):
            try:
                self._deliver(job)
            except Exception as exc:  # every failure is recorded on the earlier row
                job = job.model_copy(update={"attempts": job.attempts + 1, "error": f"{type(exc).__name__}: {exc}"})
                logger.warning("evidence export %s attempt %d failed: %s", job.snapshot_id, job.attempts, exc)
                last = attempt + 1 == self._attempts
                if last:
                    job = job.model_copy(update={"state": ExportState.FAILED, "finished_at": _now()})
                self._record(job)
                if not last:
                    time.sleep(self._retry_seconds)
                continue
            job = job.model_copy(
                update={
                    "state": ExportState.DELIVERED,
                    "attempts": job.attempts + 1,
                    "error": None,
                    "finished_at": _now(),
                }
            )
            self._record(job)
            capture_delivered_checkpoint(job, self._directory)
            return

    def drain(self, timeout: float | None = None) -> list[ExportJob]:
        """Wait until every outstanding job is delivered or failed; return the failed jobs."""

        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            while self._outstanding:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise TimeoutError(f"{len(self._outstanding)} evidence exports still outstanding")
                self._condition.wait(remaining)
        return [job for job in self.jobs() if job.state is ExportState.FAILED]

    def close(self, *, drain: bool, timeout: float | None = None) -> None:
        """Stop the worker. Without ``drain``, undelivered jobs stay durable for the next process."""

        if drain:
            self.drain(timeout)
        with self._condition:
            self._closed = True
            if not drain:
                # Keep only the in-flight job; the rest resume in the next process.
                while len(self._pending) > 1:
                    self._pending.pop()
            self._condition.notify_all()
        if self._thread is not None:
            self._thread.join(timeout)


def stash_checkpoint_inputs(directory: Path, job: ExportJob, index: int, configmaps: list[dict[str, Any]]) -> ExportJob:
    """Keep the boundary's worker ConfigMaps for the replay checkpoint captured after delivery."""

    _write_atomic(directory / f"{job.snapshot_id}{CHECKPOINT_INPUTS_SUFFIX}", {"items": configmaps})
    return job.model_copy(update={"checkpoint_index": index})


def capture_delivered_checkpoint(job: ExportJob, directory: Path) -> None:
    """Capture the optional replay checkpoint from the delivered archive and the boundary ConfigMaps."""

    if job.checkpoint_index is None:
        return
    from benchmarks.sregym.fastloop.replay import CheckpointStore, capture_checkpoints

    inputs = directory / f"{job.snapshot_id}{CHECKPOINT_INPUTS_SUFFIX}"
    receipt_dir = Path(job.receipt_dir)
    store = CheckpointStore(receipt_dir.parent)
    try:
        items = json.loads(inputs.read_text(encoding="utf-8"))["items"]
        capture_checkpoints(store, job.checkpoint_index, job.incident_id, receipt_dir, items)
        store.clear_unrecorded(job.checkpoint_index)
    except Exception as exc:  # the checkpoint is optional; its absence is recorded
        logger.warning("Optional replay checkpoint %03d not recorded: %s", job.checkpoint_index, exc)
        try:
            store.mark_unrecorded(job.checkpoint_index, f"{type(exc).__name__}: {exc}")
        except OSError:
            logger.warning("Could not persist unrecorded checkpoint %03d", job.checkpoint_index, exc_info=True)
    inputs.unlink(missing_ok=True)


def lane_queue_directory(environment: FastloopEnvironment) -> Path:
    return environment.run_dir / QUEUE_DIRNAME


def deliver_job(job: ExportJob) -> None:
    from benchmarks.sregym.adapter import deliver_runtime_snapshot

    deliver_runtime_snapshot(job.target(), job.snapshot(), Path(job.receipt_dir))


_LANE_QUEUES: dict[Path, EvidenceExportQueue] = {}
_LANE_QUEUES_LOCK = threading.Lock()


def lane_queue(environment: FastloopEnvironment) -> EvidenceExportQueue:
    """The process's one queue for this lane; two workers would race for the export Pod."""

    directory = lane_queue_directory(environment).resolve()
    with _LANE_QUEUES_LOCK:
        queue = _LANE_QUEUES.get(directory)
        if queue is None or queue.closed:
            queue = EvidenceExportQueue(directory, deliver_job)
            _LANE_QUEUES[directory] = queue
        return queue


def settle_lane_exports(environment: FastloopEnvironment, *, timeout: float | None = None) -> list[ExportJob]:
    """Deliver every queued archive before the lane's repository PVC can be deleted.

    Every path that deletes the control namespace or its PVC calls this first. Returns
    the jobs that still failed; their rows keep the failure record.
    """

    if not lane_queue_directory(environment).is_dir():
        return []
    failed = lane_queue(environment).drain(timeout)
    for job in failed:
        logger.error("evidence export for incident %s failed: %s", job.incident_id, job.error)
    return failed


@dataclass(frozen=True)
class QueuedEvidenceExport:
    """Seal an incident's evidence at the drain boundary and hand delivery to the lane queue."""

    queue: EvidenceExportQueue
    snapshot: Callable[[RuntimeEvidenceTarget, str], RuntimeSnapshot]
    # Live state the post-delivery step needs (worker ConfigMaps) is read before submission.
    before_submit: Callable[[ExportJob], ExportJob] | None = None

    def export(self, config: RuntimeConfig, incident_id: str, receipt_dir: Path) -> dict[str, Any]:
        from benchmarks.sregym.adapter import RuntimeEvidenceTarget, native_install_config

        try:
            target = RuntimeEvidenceTarget.of(native_install_config(config))
            sealed = self.snapshot(target, snapshot_id_for(incident_id))
        except Exception as exc:  # recorded like a failed synchronous export
            return {"directory": None, "error": f"{type(exc).__name__}: {exc}"}
        existing = self.queue.job(sealed.snapshot_id)
        if existing is not None:
            # A drain re-run after a crash finds its own durable job; anything else is a duplicate.
            if (existing.incident_id, existing.sha256, existing.receipt_dir) != (
                incident_id,
                sealed.sha256,
                str(receipt_dir.resolve()),
            ):
                raise DuplicateExportError(f"snapshot {sealed.snapshot_id} belongs to another export")
            return existing.receipt_record()
        job = ExportJob(
            incident_id=incident_id,
            snapshot_id=sealed.snapshot_id,
            sha256=sealed.sha256,
            bytes=sealed.bytes,
            receipt_dir=str(receipt_dir.resolve()),
            control_namespace=target.control_namespace,
            repository_pvc=target.repository_pvc,
            controller_image=target.controller_image,
            kubeconfig=str(target.kubeconfig) if target.kubeconfig else None,
            submitted_at=_now(),
        )
        if self.before_submit is not None:
            job = self.before_submit(job)
        self.queue.submit(job)
        return job.receipt_record()
