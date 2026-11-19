"""The fastloop lane's evidence sink: required for the SDO arm, and copied out before anything deletes the lane.

The controller's evidence sidecar mirrors every agent transcript (responder, closing turn, investigators,
lifecycle tasks) to a host directory mounted on every kind node (``--evidence-root``), so the transcripts outlive
the control namespace and the repository PVC. Three rules keep one from being lost:

* the SDO arm refuses to run on a lane without the sink (:func:`require_evidence_sink`; the one explicit waiver
  is recorded, and its cells are not gradable);
* a run copies its sink directory into its results directory when it ends (:func:`copy_out_run`), verified file
  by file, with a receipt;
* every path that deletes the control namespace or the cluster settles the lane first
  (:func:`settle_lane_evidence`) and refuses to delete when a copy is not verified.

The sink's last pass runs when the controller pod is stopped, so the copy after a namespace delete is the final one;
a copy taken while the controller runs is a consistent prefix at most one mirror interval behind.
"""

from __future__ import annotations

import json
import os
import re
import shutil
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from benchmarks.fuzzops import sink

if TYPE_CHECKING:
    from pathlib import Path

    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

RECEIPT_FILENAME = "evidence-copy.json"
WAIVER_FILENAME = "evidence-sink-waiver.json"
RECEIPT_SCHEMA = "sdo.fastloop-evidence-copy/v1"
COPY_DIRNAME = "evidence"


class EvidenceSinkError(RuntimeError):
    """The lane has no usable evidence sink, or a copy of it is not verified."""


def lane_evidence_run_id(environment: FastloopEnvironment, results_dir: Path) -> str:
    """One evidence run directory per lane and results directory, as a valid run ID."""

    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", f"{environment.cluster}-{results_dir.name}").strip("-.")
    return slug[:128] or "fastloop"


def require_evidence_sink(environment: FastloopEnvironment, results_dir: Path, *, waived: bool = False) -> None:
    """Refuse a lane whose controller mirrors no transcripts to the host.

    ``waived`` is the one explicit opt-out; it is written beside the results so the run's cells are known to be
    ungradable on transcript evidence.
    """

    if environment.evidence_root is not None:
        return
    if not waived:
        raise EvidenceSinkError(
            "the SDO arm needs the evidence sink: no agent transcript may be lost on a crash, a timeout or a lane "
            "teardown. Create the lane with `fastloop up --dev-mount ... --evidence-root DIR`, or pass "
            "--allow-no-evidence-sink to run without transcripts (its cells are then not gradable)"
        )
    results_dir.mkdir(parents=True, exist_ok=True)
    (results_dir / WAIVER_FILENAME).write_text(
        json.dumps({"schema": "sdo.fastloop-evidence-waiver/v1", "waived_at": _now()}, indent=2) + "\n"
    )


def waived(results_dir: Path) -> bool:
    return (results_dir / WAIVER_FILENAME).is_file()


@dataclass(frozen=True)
class SinkCopy:
    """One verified copy of a sink run directory."""

    run_id: str
    destination: Path
    files: int
    bytes: int
    incidents: int
    tasks: int
    #: The transcripts the copied incidents hold, as the host copy counts them.
    summary: dict[str, Any] = field(default_factory=dict)


def _fsync(path: Path) -> None:
    flags = os.O_RDONLY | (os.O_DIRECTORY if path.is_dir() else 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _verify_and_sync(source: Path, destination: Path) -> tuple[int, int]:
    """Every file of ``source`` is in ``destination`` with its size or more of it (the sink may have grown since the
    copy); fsync them and their directories. Returns (files, bytes)."""

    files = total = 0
    directories = {destination}
    for path in sorted(source.rglob("*")):
        target = destination / path.relative_to(source)
        if path.is_symlink():
            continue
        if path.is_dir():
            directories.add(target)
            continue
        try:
            size = target.stat().st_size
        except OSError as exc:
            raise EvidenceSinkError(f"{target} was not copied: {exc}") from exc
        if size > path.stat().st_size or (size < path.stat().st_size and not _grew(path, target)):
            raise EvidenceSinkError(f"{target} differs from the sink file ({size} bytes)")
        _fsync(target)
        directories.add(target.parent)
        files += 1
        total += size
    for directory in directories:
        _fsync(directory)
    return files, total


def _grew(source: Path, target: Path) -> bool:
    """The sink file grew after the copy: its mtime is newer than the copy's."""
    return source.stat().st_mtime >= target.stat().st_mtime


def _merge_run_level(source: Path, destination: Path) -> None:
    """Merge every run-level entry the interval copy does not own (telemetry, ...) into ``destination``: new and
    grown files are copied, existing destination directories are merged into, and nothing there is deleted."""

    owned = {sink.INCIDENTS, sink.TASKS, sink.LAYOUT_FILE, sink.INDEX_FILE}
    for entry in sorted(source.iterdir()):
        if entry.name in owned or entry.name.endswith(".tmp") or entry.is_symlink():
            continue
        if entry.is_dir():
            shutil.copytree(entry, destination / entry.name, symlinks=True, dirs_exist_ok=True)
        else:
            shutil.copy2(entry, destination / entry.name)


def copy_out_run(environment: FastloopEnvironment, results_dir: Path) -> SinkCopy:
    """Copy this results directory's sink run directory into ``results_dir/evidence`` and verify it.

    Raises :class:`EvidenceSinkError` when the lane has no sink, the sink run directory is missing or not an
    ``sdo.evidence/v1`` layout, or a file did not arrive. Writes the receipt ``evidence-copy.json`` on success.
    """

    if environment.evidence_root is None:
        raise EvidenceSinkError("the lane has no evidence sink (created without --evidence-root)")
    run_id = lane_evidence_run_id(environment, results_dir)
    source = environment.evidence_root / run_id
    if why := sink.layout_problem(source):
        raise EvidenceSinkError(f"no sink run directory for {run_id}: {why}")
    destination = results_dir / COPY_DIRNAME
    try:
        copied = sink.copy_interval(source, destination, None, None)
        _merge_run_level(source, destination)
        files, size = _verify_and_sync(source, destination)
    except (OSError, sink.SinkError) as exc:
        raise EvidenceSinkError(f"copying the sink run directory {run_id}: {exc}") from exc
    summary = copied.summary()
    receipt = SinkCopy(run_id, destination, files, size, len(copied.incidents), copied.tasks, summary)
    document = {
        "schema": RECEIPT_SCHEMA,
        "run_id": run_id,
        "copied_at": _now(),
        "files": files,
        "bytes": size,
        "incidents": receipt.incidents,
        "tasks": receipt.tasks,
        "transcripts": summary,
    }
    temporary = results_dir / (RECEIPT_FILENAME + ".tmp")
    temporary.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
    _fsync(temporary)
    os.replace(temporary, results_dir / RECEIPT_FILENAME)
    _fsync(results_dir)
    return receipt


def _copied_since_last_write(source: Path, results_dir: Path) -> bool:
    try:
        copied_at = datetime.fromisoformat(json.loads((results_dir / RECEIPT_FILENAME).read_text())["copied_at"])
    except (OSError, ValueError, KeyError, TypeError):
        return False
    stamps = [p.stat().st_mtime for p in source.rglob("*") if p.is_file() and not p.is_symlink()]
    return not stamps or datetime.fromtimestamp(max(stamps), UTC) <= copied_at


def settle_lane_evidence(environment: FastloopEnvironment) -> list[str]:
    """Copy every results directory's sink run directory out of the lane's sink before the lane is deleted.

    Skips a run directory unchanged since its last verified copy. Returns one message per failed copy; deleting
    the control namespace or the cluster with a non-empty answer would lose evidence. A lane without a sink has
    nothing to settle.
    """

    if environment.evidence_root is None:
        return []
    results_root = environment.run_dir / "results"
    failures: list[str] = []
    for results_dir in sorted(p for p in results_root.iterdir() if p.is_dir()) if results_root.is_dir() else []:
        source = environment.evidence_root / lane_evidence_run_id(environment, results_dir)
        if not source.is_dir():
            continue
        if _copied_since_last_write(source, results_dir):
            continue
        try:
            copy_out_run(environment, results_dir)
        except EvidenceSinkError as exc:
            failures.append(f"{results_dir.name}: {exc}")
    return failures


def _now() -> str:
    return datetime.now(UTC).isoformat()
