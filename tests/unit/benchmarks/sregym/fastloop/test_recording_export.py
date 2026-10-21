from dataclasses import replace
from pathlib import Path

import pytest

from tests.unit.benchmarks.sregym.fastloop.test_fastloop_sdo_agent import FakeOps, _agent, _inject


def test_learning_reports_incomplete_recording_export(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ops = FakeOps()
    agent = _agent(tmp_path, ops, [])
    resolved = agent.resolve(0, "p", _inject)
    agent._settings = replace(agent._settings, kubeconfig="captured-kubeconfig")
    monkeypatch.setattr(ops, "export_runtime_artifacts", lambda *_: {"directory": None, "error": "archive failed"})
    monkeypatch.setattr(agent, "_checkpoint", lambda *_: None)
    with pytest.raises(RuntimeError, match="archive failed"):
        agent.learn(resolved)
