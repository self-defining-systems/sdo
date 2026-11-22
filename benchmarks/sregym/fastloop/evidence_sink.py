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
import subprocess
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from benchmarks.fuzzops import sink
from benchmarks.sregym.adapter import WaitKind, wait_for

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

RECEIPT_FILENAME = "evidence-copy.json"
WAIVER_FILENAME = "evidence-sink-waiver.json"
RECEIPT_SCHEMA = "sdo.fastloop-evidence-copy/v1"
COPY_DIRNAME = "evidence"


class EvidenceSinkError(RuntimeError):
    """The lane has no usable evidence sink, or a copy of it is not verified."""


class EvidenceClaimMissing(EvidenceSinkError):
    """The lane's evidence claim is gone while a run's evidence has no verified host copy."""


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
        if path.relative_to(source) == Path(sink.INDEX_FILE):
            # The copy rewrites the index (the copied units' records, serialised by Python), so its size is not the
            # sink's (the Go writer's compact lines are shorter): the records are what must agree.
            _verify_index(path, target)
        elif size > path.stat().st_size or (size < path.stat().st_size and not _grew(path, target)):
            raise EvidenceSinkError(f"{target} differs from the sink file ({size} bytes)")
        _fsync(target)
        directories.add(target.parent)
        files += 1
        total += size
    for directory in directories:
        _fsync(directory)
    return files, total


def _verify_index(source: Path, target: Path) -> None:
    """Every record of the copied ``index.jsonl`` is a record of the sink's (the copy keeps the records of the units
    it copied, in its own serialisation)."""

    def records(path: Path) -> list[dict[str, Any]]:
        return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]

    try:
        held = records(source)
        copied = records(target)
    except (OSError, ValueError) as exc:
        raise EvidenceSinkError(f"{target} is not a readable index: {exc}") from exc
    missing = [record.get("path") for record in copied if record not in held]
    if missing:
        raise EvidenceSinkError(f"{target} holds records the sink file does not: {missing[:3]}")


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


def _namespace_exists(kube: Callable[..., list[str]], namespace: str) -> bool:
    found = subprocess.run(
        kube("get", "namespace", namespace, "-o", "name"),
        capture_output=True,
        text=True,
        check=False,
        timeout=wait_for(WaitKind.MINUTE),
    )
    if found.returncode == 0:
        return True
    if "NotFound" in found.stderr:
        return False
    raise EvidenceSinkError(f"cannot read namespace {namespace}: {found.stderr.strip()[-300:]}")


def pull_cluster_sink(environment: FastloopEnvironment, run_ids: Sequence[str]) -> bool:
    """An EKS lane's sink is a claim in the control namespace: copy the ``run_ids`` run directories out of it into the
    host evidence root (other runs of the claim are not pulled).

    The puller pod is pinned to the node the claim is bound on (single-attach volumes serve one node). Returns whether
    a claim was there to pull. A claim that is gone while a run has no verified host copy is lost evidence and raises
    :class:`EvidenceClaimMissing`. Safe to repeat (the sink only appends). A kind lane's sink is already the host
    directory, so nothing happens.
    """

    if environment.platform != "eks" or environment.evidence_root is None or not run_ids:
        return False
    from benchmarks.sregym.adapter import control_namespace_for

    control = control_namespace_for(environment.namespace)
    root = environment.evidence_root

    def kube(*args: str) -> list[str]:
        return ["kubectl", "--kubeconfig", str(environment.kubeconfig), *args]

    claim = subprocess.run(
        kube("-n", control, "get", "pvc", sink.EVIDENCE_CLAIM, "-o", "json"),
        capture_output=True,
        text=True,
        check=False,
        timeout=wait_for(WaitKind.MINUTE),
    )
    if claim.returncode != 0:
        if "NotFound" in claim.stderr:
            if not _namespace_exists(kube, control):
                # SDO was never installed on this lane (the first row of a fresh lane wipes before it installs):
                # there was no claim, so there is no evidence to lose.
                return False
            unsaved = [run for run in run_ids if sink.layout_problem(root / run)]
            if unsaved:
                raise EvidenceClaimMissing(
                    f"the lane's evidence claim {sink.EVIDENCE_CLAIM} is gone and {unsaved} have no verified host copy"
                )
            return False
        raise EvidenceSinkError(f"cannot read the lane's evidence claim: {claim.stderr.strip()[-300:]}")
    try:
        node = sink.claim_node(kube, control)
    except sink.SinkError as exc:
        raise EvidenceSinkError(str(exc)) from exc
    name = f"evidence-pull-{os.getpid()}"
    try:
        created = subprocess.run(
            kube("apply", "-f", "-"),
            input=json.dumps(sink.puller_manifest(control, name, node)),
            capture_output=True,
            text=True,
            check=False,
            timeout=wait_for(WaitKind.SLOW_COMMAND),
        )
        if created.returncode != 0:
            raise EvidenceSinkError(f"evidence pull pod: {created.stderr.strip()[-300:]}")
        ready = subprocess.run(
            kube("-n", control, "wait", f"pod/{name}", "--for=condition=Ready", f"--timeout={sink.PULL_READY_TIMEOUT}"),
            capture_output=True,
            text=True,
            check=False,
            timeout=2 * wait_for(WaitKind.SLOW_COMMAND),
        )
        if ready.returncode != 0:
            raise EvidenceSinkError(
                f"evidence pull pod not ready on {node or 'any node'}: {ready.stderr.strip()[-300:]}"
            )
        root.mkdir(parents=True, exist_ok=True)
        # Only the requested run directories that the claim holds (an absent one has nothing to pull).
        listing = (
            'cd /sink && for d in "$@"; do [ -d "$d" ] && echo "$d"; done > /tmp/pull.list; tar -cf - -T /tmp/pull.list'
        )
        tar = subprocess.Popen(
            kube("-n", control, "exec", name, "-c", "pull", "--", "sh", "-c", listing, "sh", *run_ids),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        extract = subprocess.run(
            ["tar", "-C", str(root), "-xf", "-"],
            stdin=tar.stdout,
            capture_output=True,
            check=False,
            timeout=3 * wait_for(WaitKind.ROLLOUT),
        )
        assert tar.stdout is not None
        tar.stdout.close()
        error = (tar.stderr.read() if tar.stderr else b"").decode()[-300:].strip()
        if tar.wait() != 0 or extract.returncode != 0:
            raise EvidenceSinkError(f"evidence pull: {error or extract.stderr.decode()[-300:].strip()}")
    finally:
        subprocess.run(
            kube(
                "-n",
                control,
                "delete",
                "pod",
                name,
                "--ignore-not-found",
                "--wait=false",
                "--grace-period=0",
                "--force",
            ),
            capture_output=True,
            check=False,
            timeout=wait_for(WaitKind.SLOW_COMMAND),
        )
    return True


def copy_out_run(environment: FastloopEnvironment, results_dir: Path, *, pull: bool = True) -> SinkCopy:
    """Copy this results directory's sink run directory into ``results_dir/evidence`` and verify it.

    Raises :class:`EvidenceSinkError` when the lane has no sink, the sink run directory is missing or not an
    ``sdo.evidence/v1`` layout, or a file did not arrive. Writes the receipt ``evidence-copy.json`` on success.
    """

    if environment.evidence_root is None:
        raise EvidenceSinkError("the lane has no evidence sink (created without --evidence-root)")
    run_id = lane_evidence_run_id(environment, results_dir)
    if pull:
        pull_cluster_sink(environment, [run_id])
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


def _may_hold_evidence(environment: FastloopEnvironment, results_dir: Path) -> bool:
    """Whether the lane's sink can hold evidence of this row: it injected, or a host copy was started. A row that never
    injected opened no incident, so no agent wrote to the claim (which also may not exist yet on a fresh lane)."""
    from benchmarks.sregym.fastloop.injection_marker import read_injection_marker

    assert environment.evidence_root is not None
    return (
        read_injection_marker(results_dir) is not None
        or (environment.evidence_root / lane_evidence_run_id(environment, results_dir)).exists()
    )


def settle_lane_evidence(environment: FastloopEnvironment) -> list[str]:
    """Copy every results directory's sink run directory out of the lane's sink before the lane is deleted.

    Skips a run directory unchanged since its last verified copy. Returns one message per failed copy; deleting
    the control namespace or the cluster with a non-empty answer would lose evidence. A lane without a sink has
    nothing to settle.
    """

    if environment.evidence_root is None:
        return []
    results_root = environment.run_dir / "results"
    try:
        pull_cluster_sink(
            environment,
            [
                lane_evidence_run_id(environment, p)
                for p in (sorted(results_root.iterdir()) if results_root.is_dir() else [])
                if p.is_dir() and not (p / WAIVER_FILENAME).exists() and _may_hold_evidence(environment, p)
            ],
        )
    except EvidenceSinkError as exc:
        return [f"pulling the lane's evidence claim: {exc}"]
    failures: list[str] = []
    for results_dir in sorted(p for p in results_root.iterdir() if p.is_dir()) if results_root.is_dir() else []:
        source = environment.evidence_root / lane_evidence_run_id(environment, results_dir)
        if not source.is_dir():
            continue
        if _copied_since_last_write(source, results_dir):
            continue
        try:
            copy_out_run(environment, results_dir, pull=False)
        except EvidenceSinkError as exc:
            failures.append(f"{results_dir.name}: {exc}")
    return failures


def _now() -> str:
    return datetime.now(UTC).isoformat()
