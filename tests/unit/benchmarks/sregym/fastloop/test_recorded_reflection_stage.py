"""Recorded provider streams pass through the current reflection and Go validators."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import patch

from agentshim.testing import FakeExecutor

from benchmarks.sregym.fastloop.file_edits import record_file_edits
from benchmarks.sregym.fastloop.model_recording import RecordingExecutor
from benchmarks.sregym.fastloop.replay import CheckpointStore
from benchmarks.sregym.fastloop.replay_worker import replay_live_worker
from sdo.agents.responder import CodexSessionBackend, SessionReflector, run_reflection_task
from sdo.contracts import AgentTask, ReflectionRequest, to_canonical_json
from tests.structured_turns import reply
from tests.unit.sdo.agents.responder.test_reflection_outcomes import _outcome
from tests.unit.sdo.agents.responder.test_reflection_worker import _closure

if TYPE_CHECKING:
    import pytest

ROOT = Path(__file__).resolve().parents[5]


def reflection_recording(
    tmp_path: Path,
    outputs: list[dict],
    *,
    captures: dict[str, bytes] | None = None,
    files: dict[str, bytes] | None = None,
) -> tuple[CheckpointStore, Path]:
    source = tmp_path / "source"
    shutil.copytree(ROOT / "tests/fixtures/sdo/missing_configmap_app", source)
    memory = source / ".sdo"
    for name, body in {
        "schema-version": "3\n",
        "goal.md": "---\nschema_version: 1\nowner: human\n---\n\nKeep healthy.\n",
        "arch.md": "---\nschema_version: 1\nowner: health_judge\n---\n\nOne service.\n",
        "playbooks/README.md": "# Playbooks\n\n- [missing-configmap](missing-configmap/README.md)\n",
    }.items():
        (memory / name).write_text(body)
    existing = memory / "playbooks/missing-configmap/README.md"
    existing.write_text(
        "---\nschema_version: 1\nowner: responder\nfault_class: missing-configmap\n"
        "originating_incident: fixture\n---\n\n" + existing.read_text().split("---\n", 2)[-1]
    )
    for args in (
        ["init", "-q"],
        ["add", "."],
        ["-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "seed"],
    ):
        subprocess.run(["git", "-C", str(source), *args], check=True)
    closure = _closure()
    outcome = tmp_path / "outcome.json"
    outcome.write_text(to_canonical_json(_outcome(closure.request.incident_id)))
    recorder = tmp_path / "outcome.go"
    recorder.write_text("""package main
import (
    "context"
    "fmt"
    "os"
    "path/filepath"
    "google.golang.org/protobuf/encoding/protojson"
    "sdo.dev/operator/broker"
    contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)
func main() {
    data, err := os.ReadFile(os.Args[2]); if err != nil { panic(err) }
    outcome := new(contracts.OutcomeRecord)
    if err := protojson.Unmarshal(data, outcome); err != nil { panic(err) }
    repository, err := broker.New(os.Args[1], filepath.Join(os.Args[1], "..", "proposals"), nil)
    if err != nil { panic(err) }
    receipt, err := repository.RecordOutcome(context.Background(), outcome.GetIncidentId(), outcome)
    if err != nil { panic(err) }
    fmt.Print(receipt.Commit)
}
""")
    commit = subprocess.check_output(
        ["go", "run", str(recorder), str(source), str(outcome)], cwd=ROOT / "sdo/operator", text=True
    ).strip()
    request = ReflectionRequest(
        closure=closure, outcome=_outcome(closure.request.incident_id), outcome_commit=commit, session_mode="fresh"
    )
    request.memory_snapshot.accepted_commit = commit
    task = AgentTask(
        schema_version="sdo.dev/v1alpha1",
        task_id="reflection-1",
        agent_id="sdo-reflector",
        task_type="outcome-reflection-direct-v1",
        workspace={"proposal_id": "reflection-1", "checkout_path": str(source), "base_commit": commit},
    )
    task.request.Pack(request)
    wire = to_canonical_json(task)
    task_path = tmp_path / "request.json"
    task_path.write_text(wire)
    run_dir = tmp_path / "results/run"
    session = hashlib.sha256(closure.request.incident_id.encode()).hexdigest()[:32]
    model_dir = run_dir / "000_fault/sdo_runtime/sessions" / session / "model_io"
    runs = iter(reply("codex", output, session_id=f"recorded-{index}") for index, output in enumerate(outputs))

    def author(_request: object):
        authored = files if files is not None else readme_files()
        for name, data in authored.items():
            path = source / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        return next(runs)

    executor = RecordingExecutor(model_dir, request_json=wire, record=True, live=FakeExecutor(author))

    class Publisher:
        payload = ""

        def publish_payload(self, _name: str, _key: str, payload: str) -> None:
            self.payload = payload

    capture_dir = tmp_path / "captures"
    capture_dir.mkdir()
    for name, data in (captures or {}).items():
        (capture_dir / name).write_bytes(data)
    binary = tmp_path / "sdo-operator"
    subprocess.run(["go", "build", "-o", str(binary), "./cmd/sdo-operator"], cwd=ROOT / "sdo/operator", check=True)
    publisher = Publisher()
    with patch.dict(
        os.environ,
        {
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "SDO_OPERATOR_ROOT": str(ROOT / "sdo/operator"),
            "SDO_CAPTURED_CORPUS_DIR": str(capture_dir),
        },
    ):
        run_reflection_task(
            task_path,
            "result",
            publisher,
            reflector=SessionReflector(CodexSessionBackend(executor=executor)),
        )
    store = CheckpointStore(run_dir)
    edits = record_file_edits(source, commit)
    store.save(0, "reflection", request=wire, result=publisher.payload, captures=captures, file_edits=edits)
    subprocess.run(["git", "-C", str(source), "reset", "--hard", "--quiet", commit], check=True)
    subprocess.run(["git", "-C", str(source), "clean", "-fdq"], check=True)
    return store, source


def frontmatter_output() -> dict:
    return {
        "summary": "Learn the service port invariant",
        "learning_decision": "updated",
        "playbook_ids": ["service-targetport-mismatch"],
    }


def readme_files(*, physical: bool = False) -> dict[str, bytes]:
    body = (
        "---\nschema_version: 1\nowner: responder\nfault_class: service-targetport-mismatch\n"
        f"originating_incident: {_closure().request.incident_id}\n---\n\n"
        "# Service targetPort mismatch\n\nCheck <SERVICE>.\n"
    )
    if physical:
        body += "Read .sdo/goal.md.\n"
    return {
        ".sdo/playbooks/service-targetport-mismatch/README.md": body.encode(),
        ".sdo/playbooks/README.md": (
            b"# Playbooks\n\n- [missing-configmap](missing-configmap/README.md)\n"
            b"- [service-targetport-mismatch](service-targetport-mismatch/README.md)\n"
        ),
    }


def test_recorded_reflection_frontmatter_reaches_real_broker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("SDO_TURN_USAGE_LOG", raising=False)
    store, source = reflection_recording(tmp_path, [frontmatter_output()])
    before = subprocess.check_output(["git", "-C", str(source), "status", "--porcelain"])
    from benchmarks.sregym.fastloop.cli import main

    (tmp_path / "fastloop.json").write_text(json.dumps({"workspace": str(source)}))
    assert main(["replay", "--run-dir", str(store.root.parent), "--incident", "0", "--stage", "reflection"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "completed"
    assert result["proposals"][0]["status"] == "accepted", result["proposals"]
    assert result["proposals"][0]["artifact_ids"] == ["service-targetport-mismatch"]
    assert result["proposals"][0]["commit"]
    assert subprocess.check_output(["git", "-C", str(source), "status", "--porcelain"]) == before


def test_recorded_reflection_replays_repeated_correction_dispatches(tmp_path: Path) -> None:
    statuses = []
    for attempt in range(3):
        store, source = reflection_recording(
            tmp_path / str(attempt), [frontmatter_output()], files=readme_files(physical=attempt < 2)
        )
        result = replay_live_worker(store, 0, "reflection", source=source, live_on_cache_miss=False)
        statuses.append(result["proposals"][0]["status"])
        if attempt < 2:
            assert "text.physical-path" in result["proposals"][0]["error"]
        assert len(list(store.root.parent.glob("000_*/sdo_runtime/sessions/*/model_io/*/*.json"))) == 1
    assert statuses == ["rejected", "rejected", "accepted"]


def test_recorded_reflection_reports_all_rejections(tmp_path: Path) -> None:
    for attempt in range(3):
        store, source = reflection_recording(
            tmp_path / str(attempt), [frontmatter_output()], files=readme_files(physical=True)
        )
        result = replay_live_worker(store, 0, "reflection", source=source, live_on_cache_miss=False)
        assert result["status"] == "failed"
        assert len(result["proposals"]) == 1
        assert result["proposals"][0]["status"] == "rejected"
        assert "text.physical-path" in result["proposals"][0]["error"]


def test_recorded_program_replay_validates_capture_provenance_and_generalization(tmp_path: Path) -> None:
    fixture = ROOT / "tests/fixtures/sdo/playbook_program_app/.sdo"
    corpus = fixture / "corpus/targetport-mismatch"
    fixed = json.loads((corpus / "fixed.json").read_text())
    healthy = json.loads((corpus / "healthy.json").read_text())
    for collection in ("services", "pods"):
        fixed[collection].extend(healthy[collection])
    captures = {
        "open@detect.json": (corpus / "open.json").read_bytes(),
        "open@detect-recordings.json": (corpus / "recordings.json").read_bytes(),
        "fixed.json": json.dumps(fixed).encode(),
    }
    labels = json.loads((corpus / "labels.json").read_text())
    files = readme_files()
    # Reuse the kept hand-written program, its library, and its module definition.
    for name in ("diagnose.go", "mitigate.go"):
        path = fixture / "playbooks/service-targetport-mismatch" / name
        files[f".sdo/playbooks/service-targetport-mismatch/{name}"] = path.read_bytes()
    files[".sdo/lib/ports/ports.go"] = (fixture / "lib/ports/ports.go").read_bytes()
    files[".sdo/diagnostics/go.mod"] = (fixture / "diagnostics/go.mod").read_bytes()
    labels["provenance"] = {
        "incident_id": _closure().request.incident_id,
        "open_at": "detect",
        "open_sha256": hashlib.sha256(captures["open@detect.json"]).hexdigest(),
        "fixed_sha256": hashlib.sha256(captures["fixed.json"]).hexdigest(),
    }
    for name, data in {
        "open.json": captures["open@detect.json"],
        "fixed.json": captures["fixed.json"],
        "healthy.json": captures["fixed.json"],
        "recordings.json": captures["open@detect-recordings.json"],
        "labels.json": json.dumps(labels).encode(),
    }.items():
        files[f".sdo/corpus/replayed-targetport/{name}"] = data
    store, source = reflection_recording(tmp_path, [frontmatter_output()], captures=captures, files=files)
    result = replay_live_worker(store, 0, "reflection", source=source, live_on_cache_miss=False)
    assert result["proposals"][0]["status"] == "accepted", result
