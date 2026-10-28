"""Large values exercise the production boundary, rather than fixture-only budgets."""

import json
from pathlib import Path

import pytest

from sdo.agents.prompt_templates import named_prompt
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


@pytest.mark.asyncio
async def test_registered_mcp_tools_on_real_scale_inputs(monkeypatch, tmp_path):
    from sdo.agents.context_output import budget_for
    from sdo.agents.responder.reflection_validation import validation_server

    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path / "session"))
    monkeypatch.setenv("SDO_CAPTURED_CORPUS_DIR", str(tmp_path))
    monkeypatch.setenv("SDO_INCIDENT_ID", "synthetic-incident")
    capture = {"pods": [{"metadata": {"name": f"pod-{index}"}, "spec": {"data": "界" * 16000}} for index in range(100)]}
    (tmp_path / "open@detect.json").write_text(json.dumps(capture))
    (tmp_path / "fixed.json").write_text(json.dumps(capture))
    (tmp_path / "open@detect-recordings.json").write_text(
        json.dumps(
            [
                {"method": "Logs", "arguments": [], "result": "evidence" * 1000, "error": "diagnostic" * 1000}
                for _ in range(100)
            ]
        )
    )
    # Exercise the real async subprocess producer into the server boundary.
    validator = tmp_path / "sdo-operator"
    validator.write_text("#!/usr/bin/env python3\nprint('diagnostic ' * 40000)\n")
    validator.chmod(0o700)
    import os

    monkeypatch.setenv("PATH", str(tmp_path) + os.pathsep + os.environ["PATH"])
    fixtures = {
        "authoring_reference": {"topic": "sdk"},
        "evidence_inventory": {},
        "capture_fields": {"phase": "detect", "section": "pods"},
        "recordings_index": {},
        "create_corpus": {
            "case_id": "synthetic",
            "program_ids": ["synthetic"],
            "bindings": {},
            "observations": {"snapshot_json": json.dumps(capture)},
        },
        "check_memory": {"learning_decision": "no_change", "summary": "synthetic", "no_change_reason": "test"},
    }
    server = validation_server(tmp_path)
    names = {tool.name for tool in await server.list_tools()}
    assert names == set(fixtures), "Every registered tool needs a real-scale fixture"
    for name in sorted(names):
        result = await server.call_tool(name, fixtures[name])
        assert isinstance(result, list)
        text = "\n".join(block.text for block in result)
        assert len(text.encode()) <= budget_for("tool_reply." + name), name


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
