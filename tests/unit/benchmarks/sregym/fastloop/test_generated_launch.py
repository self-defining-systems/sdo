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
