"""Large values exercise the production boundary, rather than fixture-only budgets."""

import json
from pathlib import Path

import pytest

from sdo.agents.prompt_templates import named_prompt, render_prompt
from sdo.agents.responder import tools_cli
from tests.unit.sdo.agents.responder.tool_output_cases import large_response


def test_large_prompt_sections_spill_losslessly(monkeypatch, tmp_path):
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    for name, limit in (("reflection.memory", 800), ("reflection.outcome", 3600), ("deployer.incident_data", 100)):
        original = "界 evidence\n" * 20000
        prompt = named_prompt(name, original)
        assert len(prompt) <= limit
        files = list(tmp_path.rglob("*.txt"))
        assert any(path.read_text() == original for path in files)
        assert any(path.name in prompt for path in files)


def test_unknown_prompt_section_fails_closed():
    with pytest.raises(ValueError, match="budget"):
        named_prompt("unregistered.data", "new")


def test_all_cli_commands_bound_json_output(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    for command in tools_cli.command_surface()["commands"]:
        response = large_response(command, 100)
        response["future_field"] = "evidence " * 20000
        tools_cli._print_response(response, full_json=True)
        output = capsys.readouterr().out
        assert len(output) <= 8192, command
        parsed = json.loads(output)
        path = Path(parsed.get("details_file", ""))
        assert path.is_file(), command
        document = json.loads(path.read_text())
        saved = document["actions"][0] if command in tools_cli.MUTATION_COMMANDS else document
        assert saved["future_field"] == response["future_field"]


def test_repeated_sections_share_one_aggregate_budget(monkeypatch, tmp_path):
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    prompt = named_prompt("reflection.memory", "x" * 600) + named_prompt("reflection.memory", "y" * 600)
    assert len(prompt) <= 800
