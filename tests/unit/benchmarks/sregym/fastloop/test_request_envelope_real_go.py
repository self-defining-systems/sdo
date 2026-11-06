"""Real-Go request-envelope checks, nightly tier: PR CI ignores this file.

The native budget parity test compiles the operator's exported constant and the replay test
`go run`s the broker replay program against the operator module (3 s and 6-17 s on a hosted runner,
over the PR slow-test budget). The pure-Python envelope contracts stay in test_request_envelope.py.
"""

from __future__ import annotations

import hashlib
import subprocess
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.fastloop import recording_worker, replay
from sdo.agents.responder.reflection import ReflectionTurn
from sdo.contracts import IncidentClosure, to_canonical_json
from tests.unit.benchmarks.sregym.fastloop.test_request_envelope import NATIVE_REQUEST_BYTES, ROOT, reflection_task
from tests.unit.sdo.agents.responder.test_reflection_worker import _closure

if TYPE_CHECKING:
    from pathlib import Path


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


@pytest.mark.go_integration
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
