from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from sdo.agents.provider import ResolvedAgent, resolve_agent

if TYPE_CHECKING:
    from pathlib import Path


def test_configured_provider_and_model_are_resolved_and_logged(capsys: pytest.CaptureFixture[str]) -> None:
    environ = {"SDO_AGENT_PROVIDER": " Claude ", "SDO_RESPONDER_MODEL": "claude-haiku-5-5"}
    assert resolve_agent("responder", "responder", environ) == ResolvedAgent("responder", "claude", "claude-haiku-5-5")
    assert capsys.readouterr().err.strip() == "sdo-agent-resolved role=responder provider=claude model=claude-haiku-5-5"


def test_unconfigured_provider_defaults_to_codex_with_default_model(capsys: pytest.CaptureFixture[str]) -> None:
    assert resolve_agent("health_judge", "lifecycle", {}) == ResolvedAgent("health_judge", "codex", None)
    assert "provider=codex model=default" in capsys.readouterr().err


@pytest.mark.parametrize("provider", ["gemini", "claude-code", "openai"])
def test_unknown_configured_provider_fails_instead_of_defaulting(provider: str) -> None:
    with pytest.raises(ValueError, match="unsupported agent provider"):
        resolve_agent("reflection", "reflection", {"SDO_AGENT_PROVIDER": provider})


def test_resolution_is_recorded_beside_the_session_for_the_run_validity_guard(tmp_path: Path) -> None:
    environ = {"SDO_AGENT_PROVIDER": "claude", "SDO_REFLECTION_MODEL": "m", "SDO_SESSION_DIR": str(tmp_path)}
    resolve_agent("reflection", "reflection", environ)
    records = [json.loads(line) for line in (tmp_path / "agent-resolved.jsonl").read_text().splitlines()]
    assert [(r["role"], r["provider"], r["model"]) for r in records] == [("reflection", "claude", "m")]


def test_a_failed_session_record_does_not_stop_the_worker(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    environ = {"SDO_SESSION_DIR": str(tmp_path / "missing")}
    assert resolve_agent("responder", "responder", environ).provider == "codex"
    assert "could not record resolution" in capsys.readouterr().err


def test_workers_name_model_variables_only_through_the_shared_manifest() -> None:
    import re
    from pathlib import Path

    from sdo.agents.provider import MODEL_ENV, PROVIDER_ENV

    assert PROVIDER_ENV == "SDO_AGENT_PROVIDER"
    root = Path(__file__).resolve().parents[4] / "sdo" / "agents"
    literal = re.compile(r"SDO_[A-Z_]*MODEL\b|SDO_AGENT_PROVIDER")
    offenders = {
        str(path.relative_to(root)): sorted(set(literal.findall(path.read_text("utf-8"))))
        for path in root.rglob("*.py")
        if path.name != "provider.py" and literal.search(path.read_text("utf-8"))
    }
    assert offenders == {}, f"use MODEL_ENV/PROVIDER_ENV from sdo.agents.provider ({sorted(MODEL_ENV.values())})"


def test_every_role_variable_the_controller_sets_resolves_for_its_role() -> None:
    from sdo.agents.provider import MODEL_ENV

    environ = {"SDO_AGENT_PROVIDER": "claude", **{name: f"model-for-{role}" for role, name in MODEL_ENV.items()}}
    for role in MODEL_ENV:
        assert resolve_agent("worker", role, environ).model == f"model-for-{role}"


def test_the_configured_effort_is_resolved_recorded_and_reported(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    environ = {
        "SDO_AGENT_PROVIDER": "claude",
        "SDO_RESPONDER_MODEL": "claude-opus-5-5",
        "SDO_AGENT_EFFORT": " High ",
        "SDO_SESSION_DIR": str(tmp_path),
    }

    assert resolve_agent("responder", "responder", environ) == ResolvedAgent(
        "responder", "claude", "claude-opus-5-5", "high"
    )
    assert capsys.readouterr().err.strip().endswith("effort=high")
    (record,) = [json.loads(line) for line in (tmp_path / "agent-resolved.jsonl").read_text().splitlines()]
    assert (record["provider"], record["model"], record["effort"]) == ("claude", "claude-opus-5-5", "high")


def test_an_unconfigured_effort_is_the_one_shared_default() -> None:
    from sdo.agents.provider import DEFAULT_AGENT_EFFORT

    assert DEFAULT_AGENT_EFFORT == "medium"
    assert resolve_agent("deployment", "deployment", {}).effort == DEFAULT_AGENT_EFFORT


@pytest.mark.parametrize("effort", ["extreme", "", "medium-ish"])
def test_an_unknown_configured_effort_refuses_to_run(effort: str) -> None:
    environ = {"SDO_AGENT_EFFORT": effort}
    if not effort:
        assert resolve_agent("responder", "responder", environ).effort == "medium"
        return
    with pytest.raises(ValueError, match="unsupported agent effort"):
        resolve_agent("responder", "responder", environ)


def test_every_agent_turn_takes_its_effort_from_the_resolved_agent() -> None:
    """No SDO agent may hard-code an effort: each turn's effort comes from ``resolve_agent``."""
    import ast
    from pathlib import Path

    root = Path(__file__).resolve().parents[4] / "sdo"
    offenders: list[str] = []
    for path in root.rglob("*.py"):
        tree = ast.parse(path.read_text("utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.keyword) and node.arg == "reasoning_effort":
                if isinstance(node.value, ast.Constant):
                    offenders.append(f"{path.relative_to(root)}:{node.value.lineno} literal {node.value.value!r}")
            if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "run_structured_turn":
                if not any(keyword.arg == "reasoning_effort" for keyword in node.keywords):
                    offenders.append(f"{path.relative_to(root)}:{node.lineno} turn without reasoning_effort")
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                positional = list(zip(reversed(node.args.args), reversed(node.args.defaults), strict=False))
                for argument, default in [*positional, *zip(node.args.kwonlyargs, node.args.kw_defaults, strict=True)]:
                    if argument.arg == "reasoning_effort" and isinstance(default, ast.Constant):
                        offenders.append(f"{path.relative_to(root)}:{node.lineno} default {default.value!r}")
    assert offenders == []
