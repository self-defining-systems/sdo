"""Native-supported dispatches remain exact and bounded through benchmark evidence."""

from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path

import pytest

from benchmarks.sregym.fastloop import recording_worker, replay
from sdo.agents.responder.reflection import ReflectionTurn
from sdo.contracts import (
    AgentResult,
    AgentTask,
    IncidentClosure,
    ReflectionRequest,
    ReflectionResult,
    to_canonical_json,
    validate,
)
from tests.unit.sdo.agents.responder.test_reflection_outcomes import _outcome
from tests.unit.sdo.agents.responder.test_reflection_worker import _closure

ROOT = Path(__file__).resolve().parents[5]
NATIVE_REQUEST_BYTES = 512 << 10


def reflection_task(checkout: Path, base: str = "synthetic-outcome") -> AgentTask:
    closure = _closure()
    request = ReflectionRequest(
        closure=closure, outcome=_outcome(closure.request.incident_id), outcome_commit=base, session_mode="fresh"
    )
    # Logical memory, rather than invalid envelope padding, makes a real large request.
    request.memory_snapshot.accepted_commit = base
    request.memory_snapshot.architecture_markdown = "Synthetic architecture.\n" * 12000
    task = AgentTask(
        schema_version="sdo.dev/v1alpha1",
        task_id="reflection-large",
        agent_id="sdo-reflector",
        task_type="outcome-reflection-direct-v1",
        workspace={"proposal_id": "reflection-large", "checkout_path": str(checkout), "base_commit": base},
    )
    task.request.Pack(request)
    validate(task)
    return task


@pytest.fixture(scope="module")
def native_budget_binary(tmp_path_factory: pytest.TempPathFactory) -> Path:
    tmp_path = tmp_path_factory.mktemp("native-budget")
    # Compile the exported native constant: no regex assumption about its spelling/expression.
    program = tmp_path / "native_budget.go"
    program.write_text("""package main
import (
    "fmt"
    "sdo.dev/operator/worker_registry"
)
func main() { fmt.Print(worker_registry.MaxRequestBytes) }
""")
    binary = tmp_path / "native-budget"
    subprocess.run(["go", "build", "-o", str(binary), str(program)], cwd=ROOT / "sdo/operator", check=True)
    return binary


@pytest.mark.go_integration
def test_native_request_budget_parity(native_budget_binary: Path) -> None:
    native = int(subprocess.check_output([str(native_budget_binary)], timeout=60))
    assert replay.MAX_TASK_BYTES == native == NATIVE_REQUEST_BYTES
    assert replay.MAX_RESULT_BYTES == replay.MAX_BROKER_BYTES == 1 << 20


def test_large_reflection_record_ingest_load_and_replay(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    checkout = tmp_path / "accepted"
    checkout.mkdir()
    (checkout / "README.md").write_text("Synthetic accepted source.\n")
    for args in (
        ["init", "-q"],
        ["add", "."],
        ["-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "seed"],
    ):
        subprocess.run(["git", "-C", str(checkout), *args], check=True)
    base = subprocess.check_output(["git", "-C", str(checkout), "rev-parse", "HEAD"], text=True).strip()
    task = reflection_task(checkout, base)
    wire = to_canonical_json(task)
    assert 1 << 18 < len(wire.encode()) < NATIVE_REQUEST_BYTES
    receipt = tmp_path / "receipt"
    session = receipt / "sdo_runtime/sessions/synthetic"
    task_path = tmp_path / "task.json"
    task_path.write_text(wire)
    for name, value in {
        "SDO_NAMESPACE": "synthetic",
        "SDO_RESULT_CONFIGMAP": "result",
        "SDO_TASK_PATH": str(task_path),
        "SDO_SESSION_SCRATCH": str(session / "scratch"),
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("SDO_STANDBY_CLAIM_PATH", raising=False)
    monkeypatch.setattr(recording_worker, "await_standby_claim", lambda: None)
    monkeypatch.setattr(recording_worker, "prepare_codex_home", lambda **_: None)
    monkeypatch.setattr(recording_worker, "prepare_claude_home", lambda **_: None)
    published: list[str] = []

    class Publisher:
        def publish_payload(self, name: str, key: str, payload: str) -> None:
            assert (name, key) == ("result", "agent-result.json")
            published.append(payload)

    class Reflector:
        def resume(self, **kwargs: object) -> ReflectionTurn:
            assert kwargs["outcome_commit"] == base
            closure = kwargs["closure"]
            assert isinstance(closure, IncidentClosure)
            assert closure.request.memory_snapshot.architecture_markdown == ("Synthetic architecture.\n" * 12000)
            return ReflectionTurn(
                summary="Synthetic replay",
                learning_decision="no_change",
                no_change_reason="No independently verified new learning",
                playbook_ids=[],
            )

    monkeypatch.setattr(recording_worker, "KubernetesResultPublisher", lambda **_: Publisher())
    monkeypatch.setattr(recording_worker, "SessionReflector", lambda *_, **__: Reflector())
    # The actual reflection worker, validation, recording and publication all execute.
    assert recording_worker.main(["reflection"]) == 0
    recorded = session / "checkpoints" / hashlib.sha256(wire.encode()).hexdigest()
    assert (recorded / "request.json").read_bytes() == wire.encode()
    assert (recorded / "result.json").read_text() == published[0]
    assert (recorded / "request.json").stat().st_mode & 0o777 == 0o600
    store = replay.CheckpointStore(tmp_path / "run")
    replay.capture_checkpoints(store, 0, _closure().request.incident_id, receipt, [])
    checkpoint = store.load(0, "reflection")
    assert checkpoint.request == wire
    assert checkpoint.result == published[0]
    assert checkpoint.file_edits == ()
    assert replay.replay_broker_result(checkpoint, source=checkout, operator_root=ROOT / "sdo/operator") == {
        "status": "no_change"
    }
    # ConfigMap ingestion and direct save/load must use the identical envelope.
    replay.capture_checkpoints(
        store,
        1,
        _closure().request.incident_id,
        tmp_path / "absent",
        [{"data": {"agent-task.json": wire, "agent-result.json": published[0]}}],
    )
    assert store.load(1, "reflection").request == wire
    store.save(2, "reflection", request=wire, result=published[0])
    assert store.load(2, "reflection").request == wire


@pytest.mark.parametrize("entry", ["record", "save", "load", "session", "configmap", "broker"])
def test_oversized_requests_rejected(tmp_path: Path, entry: str, monkeypatch: pytest.MonkeyPatch) -> None:
    task = reflection_task(tmp_path)
    request = ReflectionRequest()
    assert task.request.Unpack(request)
    request.memory_snapshot.architecture_markdown = "x" * NATIVE_REQUEST_BYTES
    task.request.Pack(request)
    wire = to_canonical_json(task)
    assert len(wire.encode()) > NATIVE_REQUEST_BYTES
    store = replay.CheckpointStore(tmp_path / "run")
    directory = store.root / "000/reflection"
    directory.mkdir(parents=True)
    (directory / "request.json").write_text(wire)
    (directory / "checkpoint.json").write_text('{"version":1,"captures":{}}')

    def attempt() -> None:
        if entry == "record":
            monkeypatch.setenv("SDO_NAMESPACE", "synthetic")
            monkeypatch.setenv("SDO_RESULT_CONFIGMAP", "result")
            monkeypatch.setenv("SDO_TASK_PATH", str(directory / "request.json"))
            monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path / "session/scratch"))
            monkeypatch.delenv("SDO_WORKSPACE_ALIAS", raising=False)
            monkeypatch.setattr(recording_worker, "await_standby_claim", lambda: None)
            monkeypatch.setattr(recording_worker, "prepare_codex_home", lambda **_: None)
            monkeypatch.setattr(recording_worker, "prepare_claude_home", lambda **_: None)
            recording_worker.main(["reflection"])
        elif entry == "save":
            store.save(0, "reflection", request=wire)
        elif entry == "load":
            store.load(0, "reflection")
        elif entry == "broker":
            replay.replay_broker_result(
                replay.StageCheckpoint(wire, "{}"), source=tmp_path, operator_root=ROOT / "sdo/operator"
            )
        else:
            maps = [{"data": {"agent-task.json": wire}}]
            if entry == "session":
                exported = tmp_path / "sdo_runtime/sessions/synthetic/checkpoints/digest"
                exported.mkdir(parents=True)
                (exported / "request.json").write_text(wire)
                maps = []
            replay.capture_checkpoints(store, 0, _closure().request.incident_id, tmp_path, maps)

    with pytest.raises(ValueError, match=f"exceeds {NATIVE_REQUEST_BYTES} bytes"):
        attempt()
    assert not (tmp_path / "session/checkpoints").exists()


def test_result_and_broker_limits_do_not_grow_with_requests(tmp_path: Path) -> None:
    wire = to_canonical_json(reflection_task(tmp_path))
    result = AgentResult(task_id="reflection-large", proposal_id="reflection-large", status="completed")
    result.response.Pack(ReflectionResult(summary="x" * (1 << 20), learning_decision="no_change"))
    payload = to_canonical_json(result)
    with pytest.raises(ValueError, match="1048576 bytes"):
        recording_worker._record_payload(tmp_path / "result.json", payload)
    store = replay.CheckpointStore(tmp_path)
    with pytest.raises(ValueError, match="1048576 bytes"):
        store.save(0, "reflection", request=wire, result=payload)
    with pytest.raises(ValueError, match="broker validation result exceeds 1048576 bytes"):
        store.save(0, "reflection", request=wire, broker={"synthetic": "x" * (1 << 20)})


def test_exact_native_request_limit_is_accepted(tmp_path: Path) -> None:
    task = reflection_task(tmp_path)
    request = ReflectionRequest()
    assert task.request.Unpack(request)
    request.memory_snapshot.architecture_markdown = "x"
    task.request.Pack(request)
    overhead = len(to_canonical_json(task).encode()) - 1
    request.memory_snapshot.architecture_markdown = "x" * (NATIVE_REQUEST_BYTES - overhead)
    task.request.Pack(request)
    wire = to_canonical_json(task)
    assert len(wire.encode()) == NATIVE_REQUEST_BYTES
    recording_worker._record_payload(tmp_path / "request.json", wire, limit=recording_worker.MAX_TASK_BYTES)
    store = replay.CheckpointStore(tmp_path / "run")
    store.save(0, "reflection", request=wire)
    assert store.load(0, "reflection").request == wire


@pytest.mark.parametrize(
    ("tamper", "message"), [("task", "task ID"), ("proposal", "proposal ID"), ("unknown", "invalid worker ProtoJSON")]
)
def test_large_request_retains_envelope_validation(tmp_path: Path, tamper: str, message: str) -> None:
    wire = to_canonical_json(reflection_task(tmp_path))
    result = AgentResult(task_id="reflection-large", proposal_id="reflection-large", status="failed")
    if tamper == "task":
        result.task_id = "other"
    elif tamper == "proposal":
        result.proposal_id = "other"
    else:
        value = json.loads(wire)
        value["unknown_field"] = True
        wire = json.dumps(value)
    with pytest.raises(replay.CheckpointError, match=message):
        replay.CheckpointStore(tmp_path).save(0, "reflection", request=wire, result=to_canonical_json(result))


def test_loaded_broker_metadata_is_still_bounded(tmp_path: Path) -> None:
    store = replay.CheckpointStore(tmp_path)
    directory = store.save(0, "reflection", request=to_canonical_json(reflection_task(tmp_path)))
    (directory / "checkpoint.json").write_text(
        json.dumps({"version": 1, "captures": {}, "broker": {"synthetic": "x" * (1 << 20)}})
    )
    with pytest.raises(replay.CheckpointError, match="broker validation result exceeds 1048576 bytes"):
        store.load(0, "reflection")


@pytest.fixture(scope="module")
def replay_binary(tmp_path_factory: pytest.TempPathFactory) -> Path:
    binary = tmp_path_factory.mktemp("replay") / "replay"
    subprocess.run(
        ["go", "build", "-o", str(binary), str(ROOT / "benchmarks/sregym/fastloop/replay_go/main.go")],
        cwd=ROOT / "sdo/operator/runtime",
        check=True,
    )
    return binary


@pytest.mark.go_integration
def test_native_broker_entrypoint_rejects_oversized_request(tmp_path: Path, replay_binary: Path) -> None:
    # A sparse file proves the command bounds reads before decoding or opening
    # result/source evidence, without allocating another oversized ProtoJSON task.
    with (tmp_path / "request.json").open("wb") as stream:
        stream.truncate(NATIVE_REQUEST_BYTES + 1)
    completed = subprocess.run(
        [
            str(replay_binary),
            "--checkpoint",
            str(tmp_path),
            "--source",
            str(tmp_path),
            "--operator-root",
            str(ROOT / "sdo/operator"),
        ],
        cwd=ROOT / "sdo/operator/runtime",
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert completed.returncode != 0
    assert f"dispatch request exceeds {NATIVE_REQUEST_BYTES} bytes" in completed.stderr


@pytest.mark.parametrize(
    ("cause", "reason"),
    [("oversized", "32 MiB"), ("symlink", "symlink"), ("unknown-base", "returned non-zero exit status")],
)
def test_unrecordable_reflection_edits_still_publish(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cause: str, reason: str
) -> None:
    """No benchmark recording failure discards a finished production reflection."""
    checkout = tmp_path / "accepted"
    checkout.mkdir()
    (checkout / "README.md").write_text("Synthetic accepted source.\n")
    for args in (
        ["init", "-q"],
        ["add", "."],
        ["-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "seed"],
    ):
        subprocess.run(["git", "-C", str(checkout), *args], check=True)
    base = subprocess.check_output(["git", "-C", str(checkout), "rev-parse", "HEAD"], text=True).strip()
    wire = to_canonical_json(reflection_task(checkout, "0" * 40 if cause == "unknown-base" else base))
    receipt = tmp_path / "receipt"
    session = receipt / "sdo_runtime/sessions/synthetic"
    task_path = tmp_path / "task.json"
    task_path.write_text(wire)
    for name, value in {
        "SDO_NAMESPACE": "synthetic",
        "SDO_RESULT_CONFIGMAP": "result",
        "SDO_TASK_PATH": str(task_path),
        "SDO_SESSION_SCRATCH": str(session / "scratch"),
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("SDO_STANDBY_CLAIM_PATH", raising=False)
    monkeypatch.delenv("SDO_WORKSPACE_ALIAS", raising=False)
    monkeypatch.setattr(recording_worker, "await_standby_claim", lambda: None)
    monkeypatch.setattr(recording_worker, "prepare_codex_home", lambda **_: None)
    monkeypatch.setattr(recording_worker, "prepare_claude_home", lambda **_: None)
    published: list[str] = []

    class Publisher:
        def publish_payload(self, name: str, key: str, payload: str) -> None:
            published.append(payload)

    class Reflector:
        def resume(self, **_: object) -> ReflectionTurn:
            if cause == "oversized":
                # A sparse file reports a size beyond every benchmark evidence limit.
                with (checkout / "oversized.bin").open("wb") as stream:
                    stream.truncate(64 << 20)
            elif cause == "symlink":
                (checkout / "linked").symlink_to(tmp_path, target_is_directory=True)
            return ReflectionTurn(
                summary="Synthetic oversized edit set",
                learning_decision="no_change",
                no_change_reason="No independently verified new learning",
                playbook_ids=[],
            )

    monkeypatch.setattr(recording_worker, "KubernetesResultPublisher", lambda **_: Publisher())
    monkeypatch.setattr(recording_worker, "SessionReflector", lambda *_, **__: Reflector())

    assert recording_worker.main(["reflection"]) == 0
    assert len(published) == 1
    assert json.loads(published[0])["status"] == "completed"
    recorded = session / "checkpoints" / hashlib.sha256(wire.encode()).hexdigest()
    assert (recorded / "result.json").read_text() == published[0]
    assert not (recorded / "file_edits.json").exists()
    store = replay.CheckpointStore(tmp_path / "run")
    replay.capture_checkpoints(store, 0, _closure().request.incident_id, receipt, [])
    checkpoint = store.load(0, "reflection")
    assert checkpoint.result == published[0]
    assert checkpoint.file_edits is None
    assert checkpoint.unrecorded_file_edits is not None
    assert reason in checkpoint.unrecorded_file_edits
    with pytest.raises(replay.CheckpointError, match="not replayable: file edits not recorded"):
        replay.replay_broker_result(checkpoint, source=checkout, operator_root=ROOT / "sdo/operator")
