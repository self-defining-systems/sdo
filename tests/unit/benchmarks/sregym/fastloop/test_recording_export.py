import hashlib
import json
import subprocess
from dataclasses import replace
from pathlib import Path

import pytest

from benchmarks.sregym.fastloop.loop import AgentOutcome
from benchmarks.sregym.fastloop.replay import CheckpointError, CheckpointStore
from tests.unit.benchmarks.sregym.fastloop.test_fastloop_sdo_agent import FakeOps, _agent, _inject


@pytest.fixture
def expected_learning(tmp_path: Path) -> AgentOutcome:
    baseline = tmp_path / "baseline"
    baseline.mkdir()
    agent = _agent(baseline, FakeOps(), [])
    return agent.learn(agent.resolve(0, "p", _inject))


def test_learning_reports_incomplete_recording_export(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, expected_learning: AgentOutcome
) -> None:
    ops = FakeOps()
    agent = _agent(tmp_path, ops, [])
    resolved = agent.resolve(0, "p", _inject)
    agent._settings = replace(agent._settings, kubeconfig="captured-kubeconfig")
    monkeypatch.setattr(ops, "export_runtime_artifacts", lambda *_: {"directory": None, "error": "archive failed"})
    monkeypatch.setattr(agent, "_checkpoint", lambda *_: None)
    learned = agent.learn(resolved)
    assert learned.resolved_at == resolved.resolved_at
    assert learned.responder_tokens.model_dump() == expected_learning.responder_tokens.model_dump()
    assert learned.reflection_tokens.input_tokens > 0
    with pytest.raises(CheckpointError, match="not replayable:.*archive failed"):
        CheckpointStore(agent._settings.results_dir).load(0, "reflection")


@pytest.mark.parametrize("failure", ["size", "malformed", "transport", "disk"])
def test_checkpoint_failure_preserves_resolution_learning_and_replay_reason(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str, expected_learning: AgentOutcome
) -> None:
    agent = _agent(tmp_path, FakeOps(), [])
    agent._settings = replace(agent._settings, kubeconfig="captured-kubeconfig")
    errors = {
        "size": CheckpointError("corpus capture recordings.json exceeds 4194304 bytes"),
        "malformed": ValueError("invalid exported evidence"),
        "transport": subprocess.CalledProcessError(1, "kubectl"),
        "disk": OSError("evidence unavailable"),
    }

    def capture(*args: object, **kwargs: object) -> None:
        raise errors[failure]

    monkeypatch.setattr(
        "benchmarks.sregym.adapter.kubectl", lambda *_, **__: subprocess.CompletedProcess([], 0, '{"items":[]}')
    )
    monkeypatch.setattr("benchmarks.sregym.fastloop.sdo_agent.capture_checkpoints", capture)

    resolved = agent.resolve(0, "p", _inject)
    learned = agent.learn(resolved)

    assert learned.resolved_at == resolved.resolved_at
    assert learned.responder_tokens.input_tokens == 300000
    assert learned.reflection_tokens.input_tokens > 0
    assert learned.error == expected_learning.error
    marker = json.loads((agent._settings.results_dir / "checkpoints/000/unrecorded.json").read_text())
    assert str(errors[failure]) in marker["reason"]
    for stage in ("responder", "reflection", "broker"):
        with pytest.raises(CheckpointError, match="not replayable:"):
            CheckpointStore(agent._settings.results_dir).load(0, stage)


def test_oversized_corpus_export_keeps_live_metrics_and_finished_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, expected_learning: AgentOutcome
) -> None:
    from tests.unit.benchmarks.sregym.fastloop.test_replay import _task_and_result

    request, result = _task_and_result()
    agent = _agent(tmp_path, FakeOps(), [])
    agent._settings = replace(agent._settings, kubeconfig="captured-kubeconfig")
    session = hashlib.sha256(b"incident-1").hexdigest()[:32]
    corpus = agent._settings.results_dir / "000_p/sdo_runtime/sessions" / session / "corpus"
    corpus.mkdir(parents=True)
    (corpus / "open@detect-recordings.json").write_bytes(b" " * 5_073_969)
    maps = [{"data": {"agent-task.json": request}}, {"data": {"agent-result.json": result}}]
    monkeypatch.setattr(
        "benchmarks.sregym.adapter.kubectl",
        lambda *_, **__: subprocess.CompletedProcess([], 0, json.dumps({"items": maps})),
    )

    learned = agent.learn(agent.resolve(0, "p", _inject))

    assert learned.resolved_at == expected_learning.resolved_at
    assert learned.mitigation_applied_at == expected_learning.mitigation_applied_at
    assert learned.responder_tokens.model_dump() == expected_learning.responder_tokens.model_dump()
    assert learned.reflection_tokens.model_dump() == expected_learning.reflection_tokens.model_dump()
    assert learned.error == expected_learning.error
    checkpoint = CheckpointStore(agent._settings.results_dir).load(0, "responder")
    assert checkpoint.result == result
    assert "5073969" in checkpoint.unrecorded_captures["open@detect-recordings.json"]
