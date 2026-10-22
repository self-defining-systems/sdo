from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from sdo.agents.provider import ResolvedAgent, resolve_agent

if TYPE_CHECKING:
    from pathlib import Path


def test_configured_provider_and_model_are_resolved_and_logged(capsys: pytest.CaptureFixture[str]) -> None:
    environ = {"SDO_AGENT_PROVIDER": " Claude ", "SDO_RESPONDER_MODEL": "claude-haiku-5-5"}
    assert resolve_agent("responder", "SDO_RESPONDER_MODEL", environ) == ResolvedAgent(
        "responder", "claude", "claude-haiku-5-5"
    )
    assert capsys.readouterr().err.strip() == "sdo-agent-resolved role=responder provider=claude model=claude-haiku-5-5"


def test_unconfigured_provider_defaults_to_codex_with_default_model(capsys: pytest.CaptureFixture[str]) -> None:
    assert resolve_agent("health_judge", "SDO_LIFECYCLE_MODEL", {}) == ResolvedAgent("health_judge", "codex", None)
    assert "provider=codex model=default" in capsys.readouterr().err


@pytest.mark.parametrize("provider", ["gemini", "claude-code", "openai"])
def test_unknown_configured_provider_fails_instead_of_defaulting(provider: str) -> None:
    with pytest.raises(ValueError, match="unsupported agent provider"):
        resolve_agent("reflection", "SDO_REFLECTION_MODEL", {"SDO_AGENT_PROVIDER": provider})


def test_resolution_is_recorded_beside_the_session_for_the_run_validity_guard(tmp_path: Path) -> None:
    environ = {"SDO_AGENT_PROVIDER": "claude", "SDO_REFLECTION_MODEL": "m", "SDO_SESSION_DIR": str(tmp_path)}
    resolve_agent("reflection", "SDO_REFLECTION_MODEL", environ)
    records = [json.loads(line) for line in (tmp_path / "agent-resolved.jsonl").read_text().splitlines()]
    assert [(r["role"], r["provider"], r["model"]) for r in records] == [("reflection", "claude", "m")]
