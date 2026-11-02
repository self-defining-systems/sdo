"""Contracts between the scripted worker and real Go tool output."""

from __future__ import annotations

import hashlib
import io
import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from benchmarks.sregym.agent_script.runner import execute
from benchmarks.sregym.agent_script.wire import EventStream
from benchmarks.sregym.fastloop.reflection_script import reflection_script
from sdo.agents.responder import ReflectionTurn
from sdo.contracts import IncidentClosure
from tests.unit.benchmarks.sregym.fastloop import script_harness as scripts

pytestmark = pytest.mark.go_integration

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
    record: dict[str, object] = {"schema_version": 1, "incident_id": "scripted-incident", "classification": "failed"}
    if verified:
        resource = {"api_version": "v1", "kind": "Service", "namespace": "demo", "name": "front"}
        record["classification"] = "success"
        record["findings"] = [
            {
                "detector_id": "routing-relations",
                "rule_id": "targetport",
                "status": "active",
                "severity": "warn",
                "summary": "Service target port has no listener",
                "evidence": "selected pod exposes a different port",
                "fingerprint": "front-targetport",
                "primary_resource": resource,
            }
        ]
        record["repair_actions"] = [
            {
                "action_id": "restore-front-port",
                "kind": "kubectl",
                "target": "Service/demo/front",
                "summary": "restore accepted target port",
                "details": "target port patched and read back",
                "success": True,
                "resources": [resource],
                "started_at": "2026-01-01T00:00:01Z",
                "completed_at": "2026-01-01T00:00:02Z",
            }
        ]
        record["detector_history"] = [
            {
                "detector_id": "routing-relations",
                "status": "clear",
                "evaluated_at": "2026-01-01T00:00:03Z",
            }
        ]
        record["confirmed_root_causes"] = [
            {
                "summary": "routing fault",
                "resources": [resource],
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


def _captured_repair_receipts(app: Path) -> str:
    """Mirror memory.CaptureRepairReceipts: original receipts in captured recordings."""
    record = json.loads(next((app / ".sdo/outcomes").glob("*.json")).read_text())
    return json.dumps(
        [
            {
                "method": "RepairReceipt",
                "arguments": [],
                "result": {
                    "id": action["action_id"],
                    "incident_id": record["incident_id"],
                    "success": True,
                    "started_at": action["started_at"],
                    "completed_at": action["completed_at"],
                    "resources": action["resources"],
                },
            }
            for action in record.get("repair_actions", [])
        ]
    )


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
    (capture / "open@detect-recordings.json").write_text(_captured_repair_receipts(app))
    (capture / "fixed.json").write_text(json.dumps(fixed))
    # Freeze the hand-written known-good state as the controller's reference.
    (capture / "healthy").mkdir()
    (capture / "healthy/healthy.json").write_text(json.dumps(fixed))
    monkeypatch.setenv("SDO_CAPTURED_CORPUS_DIR", str(capture))
    monkeypatch.setenv("SDO_ACTOR", "responder")
    monkeypatch.setenv("SDO_PHASE", "reflection")
    monkeypatch.setenv("SDO_BASE_COMMIT", "HEAD")
    monkeypatch.setenv("SDO_OPERATOR_ROOT", str(OPERATOR))
    monkeypatch.setenv("SDO_INCIDENT_ID", "scripted-incident")
    tools = tmp_path / "tools"
    tools.mkdir()
    monkeypatch.setenv("SDO_TOOLS_DIR", str(tools))
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "sdo-operator").symlink_to(operator_binary)
    monkeypatch.setenv("PATH", str(bin_dir) + os.pathsep + os.environ["PATH"])

    closure = IncidentClosure()
    closure.request.namespace = "demo"
    if drift:
        closure.request.drift_events.add(investigation_verdict="bug")
    script = reflection_script(
        worktree=app,
        incident_id="scripted-incident",
        closure=closure,
        capture=capture,
    )
    output = io.StringIO()
    turn = ReflectionTurn.from_model_output(
        execute(
            script.select("reflector", "scripted-incident", 0),
            app,
            EventStream(output),
            access="workspace-write",
        )
    )
    assert "memory check" in output.getvalue()
    assert not list(tools.glob("reflection-decision-*"))

    assert turn.learning_decision == "updated"
    assert turn.playbook_ids == [scripts.PROGRAM_ID]
    program = app / ".sdo/playbooks" / scripts.PROGRAM_ID
    assert (program / "diagnose.go").is_file()
    assert "originating_incident: scripted-incident" in (program / "README.md").read_text()
    labels = json.loads(next((app / ".sdo/corpus").glob("*/labels.json")).read_text())
    assert labels["provenance"]["incident_id"] == "scripted-incident"
    assert labels["provenance"]["open_at"] == "detect"
    case = next((app / ".sdo/corpus").iterdir())
    assert (case / "open.json").read_bytes() == (capture / "open@detect.json").read_bytes()
    assert (case / "recordings.json").read_bytes() == (capture / "open@detect-recordings.json").read_bytes()


def test_respond_consumes_real_go_tool_responses(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    env = dict(os.environ, SDO_SCRIPTED_TOOL_RESPONSES=str(tmp_path))
    subprocess.run(
        [
            "go",
            "test",
            "-tags=integration",
            "-run",
            "^TestHermeticResponderProgramReuseThroughRealToolTransport$",
            "-count=1",
            ".",
        ],
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

    monkeypatch.setattr(scripts, "_command", command)
    assert scripts._tool("status")["command"] == "status"
    diagnosis = scripts._tool("diagnose")
    plans, programs = scripts._matched_plans(diagnosis)
    assert set(programs) == {"service-targetport-mismatch", "image-pull-failure"}
    assert len(plans) == 2
    assert scripts._tool("apply", *plans)["healthy"] is True
    assert scripts._tool("verify")["healthy"] is True

    from sdo.contracts import IncidentRequest
    from tests.unit.benchmarks.sregym.fastloop.test_responder_script import generated_call

    request = IncidentRequest(incident_id="go-transport", namespace="demo", repository_worktree=str(tmp_path))
    actual = generated_call(request, tmp_path, monkeypatch)
    assert actual.status == "completed"
    assert {book.playbook_id for book in actual.applied_playbooks} == {
        "service-targetport-mismatch",
        "image-pull-failure",
    }
    assert actual.verification_evidence[0].passed
    assert not actual.repair_actions
