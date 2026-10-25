"""Contracts between the scripted worker and real Go tool output."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from benchmarks.sregym.adapter import scripted_worker
from sdo.contracts import IncidentClosure

ROOT = Path(__file__).resolve().parents[5]
OPERATOR = ROOT / "sdo/operator"
APP_FIXTURE = ROOT / "tests/fixtures/sdo/playbook_program_app"


def _accepted_app(tmp_path: Path, *, verified: bool = False) -> Path:
    app = tmp_path / "app"
    shutil.copytree(APP_FIXTURE, app)
    memory = app / ".sdo"
    shutil.rmtree(memory / "playbooks/service-targetport-mismatch")
    shutil.rmtree(memory / "corpus/targetport-mismatch")
    shutil.rmtree(memory / "diagnostics/detectors/incidents/routing-relations")
    (memory / "schema-version").write_text("3\n")
    (memory / "goal.md").write_text("---\nschema_version: 1\nowner: human\n---\n\nKeep the app healthy.\n")
    (memory / "arch.md").write_text("---\nschema_version: 1\nowner: health_judge\n---\n\nOne service.\n")
    (memory / "playbooks/README.md").write_text("# Playbooks\n\n- [missing-configmap](missing-configmap/README.md)\n")
    missing = memory / "playbooks/missing-configmap/README.md"
    missing.write_text(
        '---\nschema_version: 1\nowner: "responder"\nfault_class: "missing-configmap"\n'
        'originating_incident: "fixture"\n---\n\n' + missing.read_text().split("---\n", 2)[-1]
    )
    outcomes = memory / "outcomes"
    outcomes.mkdir()
    record = {"schema_version": 1, "incident_id": "scripted-incident", "classification": "failed"}
    if verified:
        record["classification"] = "success"
        record["confirmed_root_causes"] = [
            {
                "summary": "routing fault",
                "resources": [{"api_version": "v1", "kind": "Service", "namespace": "demo", "name": "front"}],
            }
        ]
    (outcomes / f"{hashlib.sha256(b'scripted-incident').hexdigest()}.json").write_text(json.dumps(record))
    for args in (
        ("init", "-q"),
        ("add", "."),
        (
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "-qm",
            "outcome\n\nSDO-Incident: scripted-incident\nSDO-Phase: outcome",
        ),
    ):
        subprocess.run(["git", *args], cwd=app, check=True)
    return app


@pytest.mark.parametrize("drift", [False, True], ids=["ordinary-repair", "verified-drift-repair"])
def test_scripted_reflector_writes_program_that_passes_real_memory_check(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operator_binary: Path, drift: bool
) -> None:
    app = _accepted_app(tmp_path, verified=True)
    fixture = APP_FIXTURE / ".sdo/corpus/targetport-mismatch"
    capture = tmp_path / "capture"
    capture.mkdir()
    opened = json.loads((fixture / "open.json").read_text())
    fixed = json.loads((fixture / "fixed.json").read_text())
    healthy = json.loads((fixture / "healthy.json").read_text())
    fixed["services"].extend(healthy["services"])
    fixed["pods"].extend(healthy["pods"])
    (capture / "open@detect.json").write_text(json.dumps(opened))
    (capture / "open@detect-recordings.json").write_text("[]")
    (capture / "fixed.json").write_text(json.dumps(fixed))
    monkeypatch.setenv("SDO_CAPTURED_CORPUS_DIR", str(capture))
    monkeypatch.setattr(scripted_worker, "PROGRAM_SOURCE", ROOT / "benchmarks/sregym/fastloop/scripted_program")
    monkeypatch.setenv("SDO_ACTOR", "responder")
    monkeypatch.setenv("SDO_PHASE", "reflection")
    monkeypatch.setenv("SDO_BASE_COMMIT", "HEAD")
    monkeypatch.setenv("SDO_OPERATOR_ROOT", str(OPERATOR))
    monkeypatch.setenv("SDO_INCIDENT_ID", "scripted-incident")
    command = scripted_worker._command

    def real_operator(argv: list[str], *, timeout: int = 180) -> str:
        argv = [
            str(operator_binary)
            if value == "sdo-operator"
            else str(OPERATOR)
            if value == "/opt/sdo/sdo/operator"
            else value
            for value in argv
        ]
        return command(argv, timeout=timeout)

    monkeypatch.setattr(scripted_worker, "_command", real_operator)

    closure = IncidentClosure()
    closure.request.namespace = "demo"
    if drift:
        closure.request.drift_events.add(investigation_verdict="bug")
    turn = scripted_worker.ScriptedReflector().resume(
        worktree=app,
        incident_id="scripted-incident",
        closure=closure,
    )

    assert turn.learning_decision == "updated"
    assert turn.playbook_ids == [scripted_worker.PROGRAM_ID]
    program = app / ".sdo/playbooks" / scripted_worker.PROGRAM_ID
    assert (program / "diagnose.go").is_file()
    assert "originating_incident: scripted-incident" in (program / "README.md").read_text()
    labels = json.loads(next((app / ".sdo/corpus").glob("*/labels.json")).read_text())
    assert labels["provenance"]["incident_id"] == "scripted-incident"
    assert labels["provenance"]["open_at"] == "detect"
    case = next((app / ".sdo/corpus").iterdir())
    assert (case / "open.json").read_bytes() == (capture / "open@detect.json").read_bytes()
    assert (case / "recordings.json").read_bytes() == (capture / "open@detect-recordings.json").read_bytes()


def test_scripted_responder_consumes_real_go_tool_responses(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    env = dict(os.environ, SDO_SCRIPTED_TOOL_RESPONSES=str(tmp_path))
    subprocess.run(
        ["go", "test", "-run", "^TestHermeticResponderProgramReuseThroughRealToolTransport$", "-count=1", "."],
        cwd=OPERATOR / "runtime",
        env=env,
        check=True,
    )

    def command(argv: list[str], **_kwargs: object) -> str:
        assert argv[0] == "sdo"
        key = argv[1]
        if key == "plan":
            key += "." + argv[2]
        return (tmp_path / f"{key}.json").read_text() + "\nGo tool summary\n"

    monkeypatch.setattr(scripted_worker, "_command", command)
    assert scripted_worker._tool("status")["command"] == "status"
    diagnosis = scripted_worker._tool("diagnose")
    plans, programs = scripted_worker._matched_plans(diagnosis)
    assert set(programs) == {"service-targetport-mismatch", "image-pull-failure"}
    assert len(plans) == 2
    assert scripted_worker._tool("apply", *plans)["healthy"] is True
    assert scripted_worker._tool("verify")["healthy"] is True


@pytest.mark.parametrize("verified", [False, True])
def test_scripted_reflection_note_requires_executable_learning_when_verified(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operator_binary: Path, verified: bool
) -> None:
    app = _accepted_app(tmp_path, verified=verified)
    monkeypatch.setenv("SDO_SCRIPTED_MEMORY_CHANGE", "1")
    monkeypatch.setenv("SDO_CAPTURED_CORPUS_DIR", str(tmp_path / "no-captured-pair"))
    monkeypatch.setenv("SDO_ACTOR", "responder")
    monkeypatch.setenv("SDO_PHASE", "reflection")
    monkeypatch.setenv("SDO_BASE_COMMIT", "HEAD")
    monkeypatch.setenv("SDO_OPERATOR_ROOT", str(OPERATOR))
    monkeypatch.setenv("SDO_INCIDENT_ID", "scripted-incident")
    command = scripted_worker._command

    def real_operator(argv: list[str], *, timeout: int = 180) -> str:
        return command([str(operator_binary) if value == "sdo-operator" else value for value in argv], timeout=timeout)

    monkeypatch.setattr(scripted_worker, "_command", real_operator)
    closure = IncidentClosure()
    closure.request.namespace = "demo"
    if verified:
        with pytest.raises(RuntimeError, match="reflection.program-required"):
            scripted_worker.ScriptedReflector().resume(worktree=app, incident_id="scripted-incident", closure=closure)
        return
    turn = scripted_worker.ScriptedReflector().resume(worktree=app, incident_id="scripted-incident", closure=closure)
    assert turn.learning_decision == "updated"
    assert turn.playbook_ids == ["verified-repair-notes"]
    notes = app / ".sdo/playbooks/verified-repair-notes/README.md"
    assert "scripted-incident" in notes.read_text()
    assert "verified-repair-notes/README.md" in (app / ".sdo/playbooks/README.md").read_text()
    assert not list((app / ".sdo/playbooks").rglob("*.go"))


@pytest.mark.parametrize("typed", [False, True])
def test_scripted_binding_args_extract_object_bindings(typed: bool) -> None:
    objects = {"service": {"kind": "Service", "namespace": "app", "name": "front"}}
    bindings = {"objects": objects, "values": {}} if typed else objects
    assert scripted_worker._binding_args(bindings) == ["--bind", "service=Service/app/front"]


def test_scripted_binding_args_never_drop_scalar_values() -> None:
    with pytest.raises(ValueError, match="scalar value bindings"):
        scripted_worker._binding_args({"objects": {}, "values": {"count": 2}})
