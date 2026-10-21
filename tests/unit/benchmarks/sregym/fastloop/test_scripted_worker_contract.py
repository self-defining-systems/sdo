"""Contracts between the scripted worker and real Go tool output."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from benchmarks.sregym.adapter import scripted_worker

ROOT = Path(__file__).resolve().parents[5]
OPERATOR = ROOT / "sdo/operator"
APP_FIXTURE = ROOT / "tests/fixtures/sdo/playbook_program_app"


@pytest.fixture(scope="module")
def operator_binary(tmp_path_factory: pytest.TempPathFactory) -> Path:
    binary = tmp_path_factory.mktemp("sdo-operator") / "sdo-operator"
    subprocess.run(["go", "build", "-o", str(binary), "./cmd/sdo-operator"], cwd=OPERATOR, check=True)
    return binary


def _accepted_app(tmp_path: Path) -> Path:
    app = tmp_path / "app"
    shutil.copytree(APP_FIXTURE, app)
    memory = app / ".sdo"
    shutil.rmtree(memory / "playbooks/service-targetport-mismatch")
    shutil.rmtree(memory / "corpus/targetport-mismatch")
    (memory / "schema-version").write_text("1\n")
    (memory / "goal.md").write_text("---\nschema_version: 1\nowner: human\n---\n\nKeep the app healthy.\n")
    (memory / "arch.md").write_text("---\nschema_version: 1\nowner: health_judge\n---\n\nOne service.\n")
    (memory / "playbooks/README.md").write_text("# Playbooks\n\n- [missing-configmap](missing-configmap/README.md)\n")
    missing = memory / "playbooks/missing-configmap/README.md"
    missing.write_text(
        '---\nschema_version: 1\nowner: "responder"\nfault_class: "missing-configmap"\n'
        'originating_incident: "fixture"\n---\n\n' + missing.read_text()
    )
    diagnostics = memory / "diagnostics"
    incidents = diagnostics / "detectors/incidents"
    incidents.mkdir()
    (diagnostics / "detectors/missing_configmap").rename(incidents / "missing_configmap")
    manifest = diagnostics / "manifest.yaml"
    manifest.write_text(
        manifest.read_text().replace("./detectors/missing_configmap", "./detectors/incidents/missing_configmap")
    )
    for args in (
        ("init", "-q"),
        ("add", "."),
        ("-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "base"),
    ):
        subprocess.run(["git", *args], cwd=app, check=True)
    return app


def test_scripted_reflector_accepts_real_program_propose_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operator_binary: Path
) -> None:
    app = _accepted_app(tmp_path)
    fixture = APP_FIXTURE / ".sdo/corpus/targetport-mismatch"
    capture = tmp_path / "capture"
    capture.mkdir()
    opened = json.loads((fixture / "open.json").read_text())
    fixed = json.loads((fixture / "fixed.json").read_text())
    healthy = json.loads((fixture / "healthy.json").read_text())
    fixed["services"].extend(healthy["services"])
    fixed["pods"].extend(healthy["pods"])
    (capture / "open@pre-apply.json").write_text(json.dumps(opened))
    (capture / "open@pre-apply-recordings.json").write_text("[]")
    (capture / "fixed.json").write_text(json.dumps(fixed))
    monkeypatch.setenv("SDO_CAPTURED_CORPUS_DIR", str(capture))
    monkeypatch.setattr(scripted_worker, "PROGRAM_SOURCE", ROOT / "benchmarks/sregym/fastloop/scripted_program")
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

    turn = scripted_worker.ScriptedReflector().resume(
        worktree=app,
        incident_id="scripted-incident",
        closure=SimpleNamespace(request=SimpleNamespace(namespace="demo", drift_events=[])),
    )

    assert turn.learning_decision == "updated"
    assert turn.artifact_proposal is not None
    playbook = turn.artifact_proposal.playbooks[0]
    assert playbook.fault_class == scripted_worker.PROGRAM_ID
    assert playbook.originating_incident == "scripted-incident"
    assert playbook.program is not None
    assert playbook.program.corpus_cases[0].reference is not None
    assert playbook.program.corpus_cases[0].reference.capture_label == "open@pre-apply"


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
