"""Captured repairs become ordinary agent-script file and validation steps."""

import hashlib
import json
from pathlib import Path

from benchmarks.sregym.agent_script.model import AgentScript, Tool, Write
from benchmarks.sregym.fastloop.reflection_script import reflection_script
from sdo.contracts import IncidentClosure


def test_reflection_script_preserves_capture_provenance(tmp_path: Path) -> None:
    capture = tmp_path / "capture"
    capture.mkdir()
    opened = {"services": [{"metadata": {"name": "front"}, "spec": {"ports": [{"targetPort": 9999}]}}]}
    fixed = {
        "services": [
            {"metadata": {"name": "front"}, "spec": {"ports": [{"targetPort": 8080}]}},
            {"metadata": {"name": "peer"}, "spec": {"selector": {"app": "peer"}, "ports": [{"targetPort": 8080}]}},
        ],
        "pods": [
            {"metadata": {"labels": {"app": "peer"}}, "spec": {"containers": [{"ports": [{"containerPort": 8080}]}]}}
        ],
    }
    for name, value in (("open@detect.json", opened), ("fixed.json", fixed), ("open@detect-recordings.json", [])):
        (capture / name).write_text(json.dumps(value))
    closure = IncidentClosure()
    closure.request.namespace = "app"
    closure.request.drift_events.add(investigation_verdict="bug")
    script = reflection_script(worktree=tmp_path, incident_id="incident", closure=closure, capture=capture)
    AgentScript.model_validate(script.model_dump())
    turn = script.select("reflector", "incident", 0)
    writes = {step.path: step.text for step in turn.steps if isinstance(step, Write)}
    case = ".sdo/corpus/service-targetport-mismatch-" + hashlib.sha256(b"incident").hexdigest()[:8]
    assert writes[case + "/open.json"] == (capture / "open@detect.json").read_text()
    assert writes[case + "/fixed.json"] == writes[case + "/healthy.json"]
    labels = json.loads(writes[case + "/labels.json"])
    assert labels["provenance"] == {
        "incident_id": "incident",
        "open_at": "detect",
        "open_sha256": hashlib.sha256((capture / "open@detect.json").read_bytes()).hexdigest(),
        "fixed_sha256": hashlib.sha256((capture / "fixed.json").read_bytes()).hexdigest(),
    }
    assert labels["bindings"]["service"]["name"] == "front"
    assert labels["peer_bindings"]["service"]["name"] == "peer"
    assert turn.result["playbook_ids"] == ["service-targetport-mismatch"]
    assert isinstance(turn.steps[-2], Tool)
    assert turn.steps[-2].argv[:3] == ["sdo-operator", "memory", "check"]
    assert "service-targetport-mismatch/README.md" in writes[".sdo/playbooks/README.md"]
    assert not (tmp_path / ".sdo").exists(), "generation must have no checkout side effects"


def test_reflection_script_without_pair_is_explicit_no_change(tmp_path: Path) -> None:
    script = reflection_script(worktree=tmp_path, incident_id="incident", closure=IncidentClosure(), capture=tmp_path)
    turn = script.select("reflector", "incident", 0)
    assert turn.steps == []
    assert turn.result["learning_decision"] == "no_change"
    assert turn.result["playbook_ids"] == []
    assert turn.result["no_change_reason"]


def test_policy_script_keeps_same_kind_peer_and_omits_mitigator(tmp_path: Path) -> None:
    capture = tmp_path / "capture"
    capture.mkdir()
    removed = {"metadata": {"name": "deny"}, "spec": {"policyTypes": ["Ingress"]}}
    retained = {"metadata": {"name": "allow"}, "spec": {"podSelector": {"matchLabels": {"app": "peer"}}}}
    for name, value in (
        ("open@detect.json", {"networkPolicies": [removed, retained]}),
        ("fixed.json", {"networkPolicies": [retained]}),
        ("open@detect-recordings.json", []),
    ):
        (capture / name).write_text(json.dumps(value))
    # An accepted program of a different class must not suppress policy learning.
    accepted = tmp_path / ".sdo/playbooks/service-targetport-mismatch"
    accepted.mkdir(parents=True)
    (accepted / "diagnose.go").write_text("package targetport\n")
    closure = IncidentClosure()
    closure.request.namespace = "app"
    closure.request.drift_events.add(investigation_verdict="bug")
    script = reflection_script(worktree=tmp_path, incident_id="incident", closure=closure, capture=capture)
    turn = script.select("reflector", "incident", 0)
    writes = {step.path: step.text for step in turn.steps if isinstance(step, Write)}
    labels = json.loads(next(text for name, text in writes.items() if name.endswith("/labels.json")))
    assert labels["bindings"]["policy"] == {
        "api_version": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "namespace": "app",
        "name": "deny",
    }
    assert labels["peer_bindings"]["policy"]["name"] == "allow"
    assert turn.result["playbook_ids"] == ["networkpolicy-service-isolation"]
    assert not any(name.endswith("/mitigate.go") for name in writes)
    assert "--mitigate" not in turn.steps[0].argv
    assert "service-targetport-mismatch/README.md" in writes[".sdo/playbooks/README.md"]
    assert "networkpolicy-service-isolation/README.md" in writes[".sdo/playbooks/README.md"]
    assert not (tmp_path / ".sdo/playbooks/networkpolicy-service-isolation").exists()


def test_already_accepted_program_produces_no_edits(tmp_path: Path) -> None:
    accepted = tmp_path / ".sdo/playbooks/service-targetport-mismatch"
    accepted.mkdir(parents=True)
    (accepted / "diagnose.go").write_text("package targetport\n")
    script = reflection_script(worktree=tmp_path, incident_id="incident", closure=IncidentClosure(), capture=tmp_path)
    turn = script.select("reflector", "incident", 0)
    assert turn.steps == []
    assert turn.result["learning_decision"] == "no_change"
    assert turn.result["playbook_ids"] == []
    assert "already accepted" in turn.result["summary"]


def test_reflection_task_selects_incident_specific_script(tmp_path: Path, monkeypatch, capsys) -> None:
    from benchmarks.sregym.agent_script.cli import codex

    task = tmp_path / "task.json"
    task.write_text(json.dumps({"request": {"outcome": {"incident_id": "incident"}}}))
    plan = tmp_path / "script.json"
    plan.write_text(
        json.dumps(
            {
                "version": 1,
                "turns": [
                    {
                        "role": "reflector",
                        "incident": "incident",
                        "result": {"learning_decision": "no_change"},
                    }
                ],
            }
        )
    )
    schema = tmp_path / "schema.json"
    schema.write_text(json.dumps({"properties": {"learning_decision": {"type": "string"}}}))
    monkeypatch.setenv("SDO_TASK_PATH", str(task))
    monkeypatch.setenv("SDO_AGENT_SCRIPT", str(plan))
    monkeypatch.setenv("SDO_AGENT_SCRIPT_STATE", str(tmp_path / "state"))
    monkeypatch.delenv("SDO_AGENT_SCRIPT_INCIDENT", raising=False)
    monkeypatch.chdir(tmp_path)
    assert codex(["exec", "--output-schema", str(schema)], stdin="synthetic reflection prompt") == 0
    assert "no_change" in capsys.readouterr().out


def test_tool_timeout_reaches_memory_check_subprocess(tmp_path: Path, monkeypatch) -> None:
    import io
    import subprocess

    from benchmarks.sregym.agent_script import runner
    from benchmarks.sregym.agent_script.model import ScriptTurn
    from benchmarks.sregym.agent_script.wire import EventStream

    timeouts = []

    def run(argv, **kwargs):
        timeouts.append(kwargs["timeout"])
        return subprocess.CompletedProcess(argv, 0, "validated", "")

    monkeypatch.setattr(runner.subprocess, "run", run)
    turn = ScriptTurn(
        role="reflector",
        result={},
        steps=[
            Tool(kind="tool", argv=["sdo-operator", "memory", "check"], timeout=180.0),
        ],
    )
    runner.execute(turn, tmp_path, EventStream(io.StringIO()), access="workspace-write")
    assert timeouts == [180.0]
