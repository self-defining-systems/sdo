"""Bounded, content-addressed worker checkpoints for offline fastloop replay."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from google.protobuf.json_format import ParseError

from sdo.contracts import AgentResult, AgentTask
from sdo.contracts.proto import parse_json

STAGES = frozenset({"responder", "reflection", "drift", "broker"})
MAX_TASK_BYTES = 1 << 20
MAX_RESULT_BYTES = 1 << 20
MAX_CAPTURE_BYTES = 4 << 20
MAX_BROKER_BYTES = 1 << 20
MAX_CAPTURE_BUNDLE_BYTES = 32 << 20
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_CAPTURE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9@_.-]{0,127}\Z")


class CheckpointError(ValueError):
    """A checkpoint is incomplete, malformed, or exceeds its evidence budget."""


@dataclass(frozen=True)
class StageCheckpoint:
    request: str
    result: str | None
    capture_digests: dict[str, str] = field(default_factory=dict)
    captures: dict[str, bytes] = field(default_factory=dict)
    broker: dict[str, Any] | None = None


class CheckpointStore:
    def __init__(self, run_dir: Path) -> None:
        self.root = run_dir / "checkpoints"

    def save(
        self,
        incident: int,
        stage: str,
        *,
        request: str,
        result: str | None = None,
        captures: dict[str, bytes] | None = None,
        broker: dict[str, Any] | None = None,
    ) -> Path:
        directory = self._directory(incident, stage)
        _bounded(request.encode(), MAX_TASK_BYTES, "dispatch request")
        if result is not None:
            _bounded(result.encode(), MAX_RESULT_BYTES, "worker result checkpoint limit")
        _validate_envelopes(request, result)
        captures = captures or {}
        digests: dict[str, str] = {}
        for name, data in captures.items():
            if not _CAPTURE_NAME.fullmatch(name):
                raise CheckpointError(f"invalid corpus capture name {name!r}")
            _bounded(data, MAX_CAPTURE_BYTES, f"corpus capture {name}")
            digests[name] = hashlib.sha256(data).hexdigest()
        manifest = {"version": 1, "captures": digests, "broker": broker}
        encoded = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
        _bounded(encoded, MAX_BROKER_BYTES, "broker validation result")
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.root.chmod(0o700)
        directory.mkdir(parents=True, exist_ok=True)
        directory.chmod(0o700)
        blobs = self.root / "blobs"
        blobs.mkdir(parents=True, exist_ok=True)
        blobs.chmod(0o700)
        for name, digest in digests.items():
            _write(blobs / digest, captures[name])
        _write(directory / "request.json", request.encode())
        if result is not None:
            _write(directory / "result.json", result.encode())
        else:
            (directory / "result.json").unlink(missing_ok=True)
        _write(directory / "checkpoint.json", encoded)
        return directory

    def load(self, incident: int, stage: str) -> StageCheckpoint:
        directory = self._directory(incident, stage)
        try:
            manifest = json.loads((directory / "checkpoint.json").read_bytes())
            if manifest.get("version") != 1 or not isinstance(manifest.get("captures"), dict):
                raise CheckpointError("unsupported checkpoint manifest")
            request = (directory / "request.json").read_text(encoding="utf-8")
            result_path = directory / "result.json"
            result = result_path.read_text(encoding="utf-8") if result_path.is_file() else None
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise CheckpointError(f"read {stage} checkpoint: {exc}") from exc
        _bounded(request.encode(), MAX_TASK_BYTES, "dispatch request")
        if result is not None:
            _bounded(result.encode(), MAX_RESULT_BYTES, "worker result checkpoint limit")
        _validate_envelopes(request, result)
        captures: dict[str, bytes] = {}
        for name, digest in manifest["captures"].items():
            if (
                not isinstance(name, str)
                or not _CAPTURE_NAME.fullmatch(name)
                or not isinstance(digest, str)
                or not _DIGEST.fullmatch(digest)
            ):
                raise CheckpointError("invalid corpus capture reference")
            try:
                data = (self.root / "blobs" / digest).read_bytes()
            except OSError as exc:
                raise CheckpointError(f"read corpus capture {name}: {exc}") from exc
            _bounded(data, MAX_CAPTURE_BYTES, f"corpus capture {name}")
            if hashlib.sha256(data).hexdigest() != digest:
                raise CheckpointError(f"corpus capture {name} digest mismatch")
            captures[name] = data
        broker = manifest.get("broker")
        if broker is not None and not isinstance(broker, dict):
            raise CheckpointError("invalid broker validation result")
        return StageCheckpoint(request, result, manifest["captures"], captures, broker)

    def _directory(self, incident: int, stage: str) -> Path:
        if incident < 0 or stage not in STAGES:
            raise CheckpointError(f"invalid incident or stage: {incident}, {stage!r}")
        return self.root / f"{incident:03d}" / stage


def _bounded(data: bytes, limit: int, label: str) -> None:
    if len(data) > limit:
        raise CheckpointError(f"{label} exceeds {limit} bytes")


def _validate_envelopes(request: str, result: str | None) -> None:
    try:
        task = parse_json(request, AgentTask())
        if result is None:
            return
        response = parse_json(result, AgentResult())
    except (ValueError, ParseError) as exc:
        raise CheckpointError(f"invalid worker ProtoJSON: {exc}") from exc
    if response.task_id != task.task_id:
        raise CheckpointError("worker result task ID differs from dispatch request")
    if response.proposal_id != task.workspace.proposal_id:
        raise CheckpointError("worker result proposal ID differs from dispatch request")


def _write(path: Path, data: bytes) -> None:
    temporary = path.with_name(path.name + ".tmp")
    with os.fdopen(os.open(temporary, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600), "wb") as stream:
        stream.write(data)
    temporary.replace(path)


def capture_checkpoints(
    store: CheckpointStore,
    incident: int,
    incident_id: str,
    receipt_dir: Path,
    configmaps: list[dict[str, Any]],
) -> None:
    """Extract one incident's canonical worker payloads from exported ConfigMaps."""

    requests: dict[str, tuple[str, str, str]] = {}
    results: dict[str, str] = {}
    for item in configmaps:
        data = item.get("data")
        if not isinstance(data, dict):
            continue
        wire = data.get("agent-task.json")
        if isinstance(wire, str):
            try:
                task = parse_json(wire, AgentTask())
            except ParseError as exc:
                raise CheckpointError(f"invalid exported task ConfigMap: {exc}") from exc
            stage = _task_stage(task.task_type)
            if stage and _task_incident_id(task) == incident_id:
                metadata = item.get("metadata") or {}
                requests[task.task_id] = (stage, wire, str(metadata.get("creationTimestamp", "")))
        wire = data.get("agent-result.json")
        if isinstance(wire, str):
            try:
                result = parse_json(wire, AgentResult())
            except ParseError as exc:
                raise CheckpointError(f"invalid exported result ConfigMap: {exc}") from exc
            results[result.task_id] = wire
    if not requests:
        raise CheckpointError(f"no worker dispatch checkpoint for incident {incident_id!r}")
    ledger = _broker_ledger(receipt_dir, incident_id)
    stages: dict[str, tuple[str, str]] = {}
    for task_id, (stage, request, _) in sorted(requests.items(), key=lambda item: (item[1][2], item[0])):
        stages[stage] = (task_id, request)
    for stage, (task_id, request) in stages.items():
        result = results.get(task_id)
        captures = _referenced_captures(receipt_dir, incident_id, result)
        store.save(incident, stage, request=request, result=result, captures=captures, broker=ledger)
    broker_task = stages.get("reflection")
    if ledger is not None and broker_task is not None and results.get(broker_task[0]) is not None:
        task_id, request = broker_task
        result = results.get(task_id)
        store.save(
            incident,
            "broker",
            request=request,
            result=result,
            captures=_referenced_captures(receipt_dir, incident_id, result),
            broker=ledger,
        )


def _task_stage(task_type: str) -> str | None:
    if task_type == "incident-response":
        return "responder"
    if task_type == "outcome-reflection":
        return "reflection"
    if "drift" in task_type:
        return "drift"
    return None


def _task_incident_id(task: AgentTask) -> str | None:
    from google.protobuf.json_format import MessageToDict

    value = MessageToDict(task.request, preserving_proto_field_name=True)
    if "closure" in value:
        value = value["closure"].get("request", {})
    return value.get("incident_id") if isinstance(value, dict) else None


def _broker_ledger(receipt_dir: Path, incident_id: str) -> dict[str, Any] | None:
    for name in ("sdo_production_receipt_strict.json", "sdo_production_receipt_rejected.json"):
        path = receipt_dir / name
        if not path.is_file():
            continue
        receipt = json.loads(path.read_text(encoding="utf-8"))
        if name.endswith("rejected.json"):
            receipt = receipt.get("receipt", {})
        ledger = receipt.get("native_ledger")
        if isinstance(ledger, dict) and ledger.get("incident_id") == incident_id:
            return ledger
    return None


def _referenced_captures(receipt_dir: Path, incident_id: str, result: str | None) -> dict[str, bytes]:
    reference = json.loads(result) if result is not None else {}
    digests: set[str] = set()

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            for key, item in value.items():
                if (
                    key
                    in {
                        "open_sha256",
                        "fixed_sha256",
                        "recordings_sha256",
                        "openSha256",
                        "fixedSha256",
                        "recordingsSha256",
                    }
                    and isinstance(item, str)
                    and _DIGEST.fullmatch(item)
                ):
                    digests.add(item)
                visit(item)
        elif isinstance(value, list):
            for item in value:
                visit(item)

    visit(reference)
    session = hashlib.sha256(incident_id.encode()).hexdigest()[:32]
    root = receipt_dir / "sdo_runtime" / "sessions" / session / "corpus"
    found: dict[str, bytes] = {}
    total = 0
    for path in root.glob("*.json"):
        if path.is_symlink() or not path.is_file():
            raise CheckpointError(f"invalid corpus capture {path.name}")
        if path.stat().st_size > MAX_CAPTURE_BYTES:
            raise CheckpointError(f"corpus capture {path.name} exceeds {MAX_CAPTURE_BYTES} bytes")
        data = path.read_bytes()
        total += len(data)
        if total > MAX_CAPTURE_BUNDLE_BYTES:
            raise CheckpointError("corpus capture bundle exceeds its size limit")
        digest = hashlib.sha256(data).hexdigest()
        if (
            path.name in {"open@pre-apply.json", "open@settle.json", "open@detect.json", "fixed.json"}
            or digest in digests
        ):
            digests.discard(digest)
            found[path.name] = data
    if digests:
        raise CheckpointError(f"missing corpus captures referenced by digest: {', '.join(sorted(digests))}")
    return found


def replay_broker(store: CheckpointStore, incident: int, *, source: Path, operator_root: Path) -> dict[str, str]:
    """Run the current Go memory edge and broker against one recorded reflection result."""

    checkpoint = store.load(incident, "broker")
    if checkpoint.result is None:
        raise CheckpointError("broker replay requires a recorded worker result")
    program = Path(__file__).with_name("replay_go") / "main.go"
    with tempfile.TemporaryDirectory(prefix="sdo-fastloop-replay-") as temporary:
        directory = Path(temporary)
        _write(directory / "request.json", checkpoint.request.encode())
        _write(directory / "result.json", checkpoint.result.encode())
        capture_dir = directory / "captures"
        capture_dir.mkdir(mode=0o700)
        for name, data in checkpoint.captures.items():
            _write(capture_dir / name, data)
        command = [
            "go",
            "run",
            str(program),
            "--checkpoint",
            str(directory),
            "--source",
            str(source),
            "--operator-root",
            str(operator_root),
        ]
        completed = subprocess.run(command, cwd=operator_root, capture_output=True, text=True, timeout=300, check=False)
        if completed.returncode:
            raise CheckpointError(f"broker replay failed: {completed.stderr.strip()[-4096:]}")
        value = json.loads(completed.stdout)
        if not isinstance(value, dict) or not isinstance(value.get("status"), str):
            raise CheckpointError("broker replay returned no status")
        return value
