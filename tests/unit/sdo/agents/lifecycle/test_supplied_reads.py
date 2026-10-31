"""Lifecycle results survive reads of worker-provided resources outside the checkout."""

from pathlib import Path

import pytest

from sdo.agents.lifecycle.agents import ClaudeLifecycleBackend, CodexLifecycleBackend
from tests.structured_turns import ScriptedAgent, fake_codex_login, reply


@pytest.mark.parametrize("provider", ["codex", "claude"])
@pytest.mark.parametrize("root", ["sdo-skills-off-turn", "arbitrary-worker-resource"])
@pytest.mark.parametrize("reader", ["cat", "python -c 'import pathlib'", "sed -n 1,80p"])
def test_lifecycle_accepts_provided_resource_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, provider: str, root: str, reader: str
) -> None:
    repository = tmp_path / "app"
    repository.mkdir()
    # Synthetic command events only: no shell command or model is executed.
    resource = tmp_path / root / "sdo-detector-authoring/references/sdk.md"
    resource.parent.mkdir(parents=True)
    resource.write_text("synthetic SDK reference\n")
    output = {"failure_patterns": ["synthetic failure"], "detector_ids": ["app-config"]}
    agent = ScriptedAgent(
        lambda request: reply(provider, output, session_id="fresh-session", commands=[f"{reader} {resource}"])
    )
    backend_type = CodexLifecycleBackend if provider == "codex" else ClaudeLifecycleBackend
    backend = backend_type(executor=agent.executor)
    with fake_codex_login(monkeypatch):
        result = backend.run_health_judge(
            repository=repository,
            application="synthetic",
            health_objective="",
            architecture="manifest inventory",
            resources=[],
            correction_feedback=None,
        )
    assert result.session_id == "fresh-session"


@pytest.mark.parametrize("provider", ["codex", "claude"])
def test_external_read_does_not_bypass_handoff_validation(tmp_path: Path, provider: str) -> None:
    from sdo.agents.lifecycle.agents import LifecycleAgentError

    agent = ScriptedAgent(
        lambda request: reply(
            provider,
            {"source_commit": "missing-required-fields"},
            session_id="fresh-session",
            commands=["cat /tmp/sdo-skills-off-turn/sdo-detector-authoring/references/sdk.md"],
        )
    )
    backend_type = CodexLifecycleBackend if provider == "codex" else ClaudeLifecycleBackend
    with pytest.raises(LifecycleAgentError, match="invalid structured"):
        backend_type(executor=agent.executor).run_health_judge(
            repository=tmp_path,
            application="synthetic",
            health_objective="",
            architecture="inventory",
            resources=[],
            correction_feedback=None,
        )
