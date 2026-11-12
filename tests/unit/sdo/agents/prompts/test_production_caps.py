"""Large values exercise the production boundary, rather than fixture-only budgets."""

import json
from pathlib import Path

import pytest

from sdo.agents.prompt_templates import named_prompt
from sdo.agents.responder import tools_cli
from tests.unit.sdo.agents.responder.tool_output_cases import large_response


def test_large_prompt_sections_spill_losslessly(monkeypatch, tmp_path):
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    for name, limit in (("reflection.leftovers", 800), ("source_deployment.incident_data", 100)):
        original = "界 evidence\n" * 20000
        prompt = named_prompt(name, original, required=False)
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
    prompt = named_prompt("reflection.leftovers", "x" * 600, required=False) + named_prompt(
        "reflection.leftovers", "y" * 600, required=False
    )
    assert len(prompt) <= 800


def test_generic_summary_preserves_json_shape_and_complete_paths(monkeypatch, tmp_path):
    from sdo.agents.context_output import cap_text

    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    path = str(tmp_path / ("a" * 120 + ".json"))
    raw = json.dumps({"details_file": path, "evidence": "large " * 10000, "items": list(range(100))})
    result = cap_text(raw, 800)
    summary = json.loads(result.split("Summary (partial):\n", 1)[1])
    assert summary["details_file"] == path
    assert len(summary["items"]) + summary["items_omitted"] == 100
    assert result.context_files[0].read_text() == raw


def test_cli_entry_point_bounds_future_handlers_and_stderr(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    import sys

    def future_handler(_):
        print("new command " * 10000)
        print("new error " * 10000, file=sys.stderr)
        return 1

    monkeypatch.setattr(tools_cli, "_main", future_handler)
    assert tools_cli.main(["future-command"]) == 1
    captured = capsys.readouterr()
    assert len(captured.out + captured.err) <= 8192
    saved = list((tmp_path / "context").glob("*.txt"))
    assert len(saved) == 1
    assert "new command " * 10000 in saved[0].read_text()
    assert "new error " * 10000 in saved[0].read_text()


def test_generic_worker_session_directory_owns_overflow(monkeypatch, tmp_path):
    monkeypatch.delenv("SDO_SESSION_SCRATCH", raising=False)
    monkeypatch.setenv("SDO_SESSION_DIR", str(tmp_path))
    prompt = named_prompt("source_deployment.incident_data", "evidence " * 20000, required=False)
    assert len(prompt) <= 100
    assert "$SDO_SESSION_DIR/context/" in prompt
    assert len(prompt.context_files) == 1
    assert prompt.context_files[0].parent == tmp_path / "context"
    assert prompt.context_files[0].stat().st_mode & 0o777 == 0o600


def test_large_json_tool_reply_keeps_verdict_and_counts(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    tools_cli._print_response(large_response("status", 100), full_json=True)
    response = json.loads(capsys.readouterr().out)
    assert response["command"] == "status"
    assert response["healthy"] is False
    assert response["counts"]["findings"] == 100
    assert response["summary"]


def test_provider_boundary_rejects_uncapped_prompt_before_launch(tmp_path):
    from sdo.agents.prompt_templates import run_agent_turn

    with pytest.raises(TypeError, match="capped renderer"):
        run_agent_turn("codex", "unbounded prompt", cwd=tmp_path, access="read-only", output_schema={})
