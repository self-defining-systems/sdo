"""Task-specific scripts wrap production workers without touching accepted files."""

import sys
from pathlib import Path

import pytest

from benchmarks.sregym.agent_script.cli import load
from benchmarks.sregym.fastloop import generated_worker
from sdo.contracts import AgentTask, IncidentRequest, ReflectionRequest, to_canonical_json


@pytest.mark.parametrize("role", ["responder", "reflector"])
def test_generated_launch_uses_writable_exchange_and_real_worker(tmp_path, monkeypatch, role):
    checkout = tmp_path / "accepted"
    checkout.mkdir()
    tools = tmp_path / "tools"
    (tools / "requests").mkdir(parents=True)
    task = AgentTask(workspace={"checkout_path": str(checkout)})
    if role == "responder":
        task.request.Pack(IncidentRequest(incident_id="synthetic", namespace="app", repository_worktree=str(checkout)))
    else:
        request = ReflectionRequest()
        request.outcome.incident_id = "synthetic"
        task.request.Pack(request)
    path = tmp_path / "task.json"
    path.write_text(to_canonical_json(task))
    monkeypatch.setenv("SDO_TASK_PATH", str(path))
    monkeypatch.setenv("SDO_TOOLS_DIR", str(tools))
    monkeypatch.setenv("SDO_CAPTURED_CORPUS_DIR", str(tmp_path / "captures"))
    worker = "worker" if role == "responder" else "reflection_worker"
    command = ["--", "python3", "-m", f"sdo.agents.responder.{worker}"]
    monkeypatch.setattr(sys, "argv", ["generated_worker", role, *command])
    observed = []

    def launch(argv):
        import os

        script_path = Path(os.environ["SDO_AGENT_SCRIPT"])
        assert tools / "requests" in script_path.parents
        turn = load(script_path).select(role, "synthetic", 0)
        assert turn.role == role
        assert not list(checkout.iterdir())
        observed.append(argv)
        return 0

    monkeypatch.setattr(generated_worker, "launch", launch)
    assert generated_worker.main() == 0
    assert observed == [command]
    assert not list((tools / "requests").iterdir())


def test_abort_smoke_delays_valid_generated_turns(tmp_path, monkeypatch):
    import ast

    from benchmarks.sregym.agent_script.model import AgentScript, Wait
    from sdo.contracts import IncidentClosure

    root = Path(__file__).resolve().parents[5]
    tree = ast.parse((root / "tests/fixtures/sdo/abort_smoke/run.py").read_text())
    delay = next(
        ast.literal_eval(node.value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "delay" for target in node.targets)
    )
    original_responder = generated_worker.responder_script
    original_reflection = generated_worker.reflection_script
    monkeypatch.setattr(generated_worker, "main", lambda: 0)
    monkeypatch.setattr(generated_worker, "responder_script", original_responder)
    monkeypatch.setattr(generated_worker, "reflection_script", original_reflection)
    with pytest.raises(SystemExit) as stopped:
        exec(delay, {})
    assert stopped.value.code == 0
    monkeypatch.setenv("SDO_TOOLS_DIR", str(tmp_path))
    scripts = [
        generated_worker.responder_script(IncidentRequest(incident_id="abort")),
        generated_worker.reflection_script(
            worktree=tmp_path, incident_id="abort", closure=IncidentClosure(), capture=tmp_path
        ),
    ]
    for script in scripts:
        AgentScript.model_validate_json(script.model_dump_json())
        assert all(isinstance(step, Wait) for step in script.turns[0].steps[:10])


def test_generated_launch_waits_for_standby_claim_before_reading_the_task(tmp_path, monkeypatch):
    """A preloaded worker has no task file until the controller claims it."""
    import json
    import os
    import threading

    checkout = tmp_path / "accepted"
    checkout.mkdir()
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    task = AgentTask(workspace={"checkout_path": str(checkout)})
    task.request.Pack(IncidentRequest(incident_id="synthetic", namespace="app", repository_worktree=str(checkout)))
    ready_read, ready_write = os.pipe()
    claim_read, claim_write = os.pipe()
    monkeypatch.delenv("SDO_TASK_PATH", raising=False)
    monkeypatch.setenv("SDO_TOOLS_DIR", str(tmp_path / "tools"))
    monkeypatch.setenv("SDO_STANDBY_READY_FD", str(ready_write))
    monkeypatch.setenv("SDO_STANDBY_CLAIM_FD", str(claim_read))
    monkeypatch.setenv("SDO_STANDBY_INBOX", str(inbox))
    monkeypatch.setattr(sys, "argv", ["generated_worker", "responder", "--", "python3", "-c", "pass"])
    seen = []
    monkeypatch.setattr(generated_worker, "launch", lambda argv: seen.append(os.environ["SDO_TASK_PATH"]) or 0)

    def controller() -> None:
        with os.fdopen(ready_read, "rb", buffering=0) as ready:
            assert ready.read(1) == b"R"
        (inbox / "environment.json").write_text(json.dumps({}))
        (inbox / "agent-task.json").write_text(to_canonical_json(task))
        with os.fdopen(claim_write, "wb", buffering=0) as claim:
            claim.write(b"C")

    thread = threading.Thread(target=controller, daemon=True)
    thread.start()
    assert generated_worker.main() == 0
    thread.join(timeout=10)
    assert seen == [str(inbox / "agent-task.json")]
