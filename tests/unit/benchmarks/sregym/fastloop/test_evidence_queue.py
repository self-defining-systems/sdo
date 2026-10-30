"""Contracts of the lane-owned evidence export queue.

The native seal/deliver pair (``sdo-operator runtime-artifacts --mode snapshot|deliver``)
is exercised against real files in ``operatorcli/runtime_snapshot_test.go``. Here a
lane double keeps the same contract: sealing copies the live files once per ID and
delivery reads only the sealed copy.
"""

from __future__ import annotations

import hashlib
import io
import json
import tarfile
import threading
from pathlib import Path
from typing import Any

import pytest

from benchmarks.sregym.adapter import RuntimeConfig, RuntimeEvidenceTarget, RuntimeSnapshot
from benchmarks.sregym.fastloop import evidence_queue
from benchmarks.sregym.fastloop.evidence_queue import (
    EXPORT_STATUS_FILENAME,
    DuplicateExportError,
    EvidenceExportQueue,
    ExportJob,
    ExportState,
    QueuedEvidenceExport,
    snapshot_id_for,
)


class Lane:
    def __init__(self, root: Path) -> None:
        self.live = root / "workspace/.sdo-runtime"
        self.archives: dict[str, bytes] = {}
        self.seals = 0
        (self.live / "sessions/first").mkdir(parents=True)
        (self.live / "sessions/first/session.jsonl").write_text("first-closed\n")

    def snapshot(self, _target: RuntimeEvidenceTarget, snapshot_id: str) -> RuntimeSnapshot:
        self.seals += 1
        created = snapshot_id not in self.archives
        if created:
            buffer = io.BytesIO()
            with tarfile.open(fileobj=buffer, mode="w") as archive:
                archive.add(self.live, arcname=".")
            self.archives[snapshot_id] = buffer.getvalue()
        data = self.archives[snapshot_id]
        return RuntimeSnapshot(snapshot_id, hashlib.sha256(data).hexdigest(), len(data), created)

    def deliver(self, job: ExportJob) -> None:
        data = self.archives[job.snapshot_id]
        assert hashlib.sha256(data).hexdigest() == job.sha256
        with tarfile.open(fileobj=io.BytesIO(data)) as archive:
            archive.extractall(Path(job.receipt_dir) / "sdo_runtime", filter="data")


def _config(tmp_path: Path) -> RuntimeConfig:
    return RuntimeConfig(
        repository=tmp_path / "repository",
        namespace="toy",
        application="Toy",
        controller_image="controller:test",
        responder_image="responder:test",
        repository_pvc="sdo-application-repository",
        credentials_secret="sdo-agent-credentials",
        model="model",
        timeout_seconds=60,
        controller_namespace="toy-sdo",
    )


@pytest.fixture(autouse=True)
def _kubeconfig(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("KUBECONFIG", str(tmp_path / "kubeconfig"))
    monkeypatch.delenv("SREGYM_BASE_KUBECONFIG", raising=False)


def _row(tmp_path: Path, index: int) -> Path:
    row = tmp_path / f"results/{index:03d}_p"
    row.mkdir(parents=True)
    return row


def _status(row: Path) -> dict[str, Any]:
    return json.loads((row / EXPORT_STATUS_FILENAME).read_text())


def test_delivery_after_the_boundary_never_reads_the_next_rows_files(tmp_path: Path) -> None:
    lane = Lane(tmp_path)
    release = threading.Event()

    def slow_deliver(job: ExportJob) -> None:
        release.wait(10)
        lane.deliver(job)

    queue = EvidenceExportQueue(tmp_path / "queue", slow_deliver, retry_seconds=0)
    row = _row(tmp_path, 1)
    record = QueuedEvidenceExport(queue, lane.snapshot).export(_config(tmp_path), "incident-1", row)
    assert record["delivery"] == "queued"
    assert record["error"] is None
    assert _status(row)["state"] == "queued"
    # The next row writes while the earlier row's evidence is still in flight.
    (lane.live / "sessions/first/session.jsonl").write_text("first-closed\nnext-row\n")
    (lane.live / "sessions/second").mkdir()
    (lane.live / "sessions/second/session.jsonl").write_text("second-open\n")
    release.set()
    assert queue.drain(10) == []
    assert (row / "sdo_runtime/sessions/first/session.jsonl").read_text() == "first-closed\n"
    assert not (row / "sdo_runtime/sessions/second").exists()
    assert _status(row)["state"] == "delivered"
    queue.close(drain=False)


def test_failed_delivery_retries_the_same_archive_and_is_recorded(tmp_path: Path) -> None:
    lane = Lane(tmp_path)
    delivered: list[tuple[str, str]] = []

    def flaky(job: ExportJob) -> None:
        delivered.append((job.snapshot_id, job.sha256))
        if len(delivered) < 3:
            raise OSError("export Pod lost")
        lane.deliver(job)

    queue = EvidenceExportQueue(tmp_path / "queue", flaky, attempts=3, retry_seconds=0)
    row = _row(tmp_path, 1)
    QueuedEvidenceExport(queue, lane.snapshot).export(_config(tmp_path), "incident-1", row)
    assert queue.drain(10) == []
    assert len(set(delivered)) == 1
    assert len(delivered) == 3
    assert lane.seals == 1
    assert _status(row) | {"finished_at": None} == _status(row) | {
        "state": "delivered",
        "attempts": 3,
        "error": None,
        "finished_at": None,
    }
    queue.close(drain=False)


def test_exhausted_delivery_stays_visible_and_retries_after_restart(tmp_path: Path) -> None:
    lane = Lane(tmp_path)

    def broken(_job: ExportJob) -> None:
        raise OSError("registry unreachable")

    queue = EvidenceExportQueue(tmp_path / "queue", broken, attempts=2, retry_seconds=0)
    row = _row(tmp_path, 1)
    QueuedEvidenceExport(queue, lane.snapshot).export(_config(tmp_path), "incident-1", row)
    failed = queue.drain(10)
    assert [job.state for job in failed] == [ExportState.FAILED]
    assert _status(row)["state"] == "failed"
    assert "registry unreachable" in _status(row)["error"]
    queue.close(drain=False)

    restarted = EvidenceExportQueue(tmp_path / "queue", lane.deliver, retry_seconds=0)
    assert restarted.drain(10) == []
    assert (row / "sdo_runtime/sessions/first/session.jsonl").is_file()
    assert _status(row)["state"] == "delivered"
    assert _status(row)["attempts"] == 3
    assert lane.seals == 1
    restarted.close(drain=False)


def test_duplicate_submission_is_rejected(tmp_path: Path) -> None:
    lane = Lane(tmp_path)
    queue = EvidenceExportQueue(tmp_path / "queue", lane.deliver, retry_seconds=0)
    row = _row(tmp_path, 1)
    QueuedEvidenceExport(queue, lane.snapshot).export(_config(tmp_path), "incident-1", row)
    queue.drain(10)
    job = queue.job(snapshot_id_for("incident-1"))
    assert job is not None
    fresh = job.model_copy(update={"state": ExportState.QUEUED, "attempts": 0, "error": None})
    with pytest.raises(DuplicateExportError):
        queue.submit(fresh)
    # Another incident's export can never claim this row's sealed snapshot.
    foreign = QueuedEvidenceExport(queue, lambda target, _id: lane.snapshot(target, job.snapshot_id))
    with pytest.raises(DuplicateExportError):
        foreign.export(_config(tmp_path), "incident-2", _row(tmp_path, 2))
    queue.close(drain=False)


def test_drain_rerun_after_crash_adopts_its_own_durable_job(tmp_path: Path) -> None:
    lane = Lane(tmp_path)
    blocked = threading.Event()

    def never(_job: ExportJob) -> None:
        blocked.wait(10)
        raise OSError("process crashed")

    crashed = EvidenceExportQueue(tmp_path / "queue", never, attempts=1, retry_seconds=0)
    row = _row(tmp_path, 1)
    first = QueuedEvidenceExport(crashed, lane.snapshot).export(_config(tmp_path), "incident-1", row)
    blocked.set()
    crashed.close(drain=False, timeout=10)

    # The pending record survived the crash, so learning drains the same incident again.
    resumed = EvidenceExportQueue(tmp_path / "queue", lane.deliver, retry_seconds=0)
    again = QueuedEvidenceExport(resumed, lane.snapshot).export(_config(tmp_path), "incident-1", row)
    assert again == first
    assert resumed.drain(10) == []
    assert len(resumed.jobs()) == 1
    assert (row / "sdo_runtime").is_dir()
    resumed.close(drain=False)


def test_full_queue_applies_backpressure_instead_of_unbounded_work(tmp_path: Path) -> None:
    lane = Lane(tmp_path)
    release = threading.Event()

    def held(job: ExportJob) -> None:
        release.wait(10)
        lane.deliver(job)

    queue = EvidenceExportQueue(tmp_path / "queue", held, capacity=1, retry_seconds=0)
    exporter = QueuedEvidenceExport(queue, lane.snapshot)
    exporter.export(_config(tmp_path), "incident-1", _row(tmp_path, 1))
    second_row = _row(tmp_path, 2)
    submitted = threading.Event()

    def submit_second() -> None:
        exporter.export(_config(tmp_path), "incident-2", second_row)
        submitted.set()

    thread = threading.Thread(target=submit_second)
    thread.start()
    assert not submitted.wait(0.3)
    release.set()
    assert submitted.wait(10)
    thread.join(10)
    assert queue.drain(10) == []
    assert queue.backpressure_seconds > 0.2
    queue.close(drain=False)


def test_failed_seal_leaves_no_job_so_the_synchronous_path_runs(tmp_path: Path) -> None:
    def unavailable(_target: RuntimeEvidenceTarget, _id: str) -> RuntimeSnapshot:
        raise OSError("sync Pod not ready")

    queue = EvidenceExportQueue(tmp_path / "queue", lambda _job: None, retry_seconds=0)
    record = QueuedEvidenceExport(queue, unavailable).export(_config(tmp_path), "incident-1", _row(tmp_path, 1))
    assert record["directory"] is None
    assert "sync Pod not ready" in str(record["error"])
    assert queue.jobs() == []
    queue.close(drain=False)


def test_lane_destruction_settles_queued_exports_first(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from benchmarks.sregym.fastloop import wipe

    order: list[str] = []
    monkeypatch.setattr(evidence_queue, "settle_lane_exports", lambda _environment: order.append("settle") or [])

    def tier1(*_args: Any, **_kwargs: Any) -> Any:
        order.append("delete control namespace")
        raise RuntimeError("stop after the destructive step")

    monkeypatch.setattr(wipe, "run_tier1", tier1)

    class Environment:
        kubeconfig = tmp_path / "kubeconfig"
        namespace = "toy"
        workspace = tmp_path / "workspace"
        state_path = tmp_path / "state.json"
        parallel_root = None

    with pytest.raises(RuntimeError, match="destructive step"):
        wipe.wipe_environment(
            Environment(),  # type: ignore[arg-type]
            object(),  # type: ignore[arg-type]
            applications=("toy",),
            provision_credentials=lambda _namespace: None,
            planned_commit="0" * 40,
            timeout_seconds=1,
        )
    assert order == ["settle", "delete control namespace"]


def test_unreadable_job_does_not_stop_the_worker(tmp_path: Path) -> None:
    lane = Lane(tmp_path)
    directory = tmp_path / "queue"
    directory.mkdir()
    (directory / "i-corrupt.json").write_text("{not json")
    queue = EvidenceExportQueue(directory, lane.deliver, retry_seconds=0)
    row = _row(tmp_path, 1)
    QueuedEvidenceExport(queue, lane.snapshot).export(_config(tmp_path), "incident-1", row)
    queue.drain(10)
    assert _status(row)["state"] == "delivered"
    queue.close(drain=False)
