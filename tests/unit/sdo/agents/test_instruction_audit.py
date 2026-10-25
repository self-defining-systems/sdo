"""Contract tests for the developer-only rollout instruction audit tools."""

import importlib.util
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
SCRIPTS = ROOT / ".agents/skills/audit-agent-instructions/scripts"


def load(name: str):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / f"{name}.py")
    assert spec
    assert spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_extract_real_codex_record_shapes(tmp_path: Path) -> None:
    extract = load("extract_prompt")
    rows = [
        {"type": "session_meta", "payload": {"base_instructions": {"text": "Harness limits"}}},
        {
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "user",
                "content": [
                    {"type": "input_text", "text": 'Read the skill.\n\nIncident request:\n{"incident_id":"toy"}'}
                ],
            },
        },
        {
            "type": "response_item",
            "payload": {
                "type": "function_call",
                "name": "exec_command",
                "arguments": json.dumps({"cmd": "cat /session/skills/sdo-memory/SKILL.md\nsdo menu --help"}),
            },
        },
        {"type": "event_msg", "payload": {"type": "user_message", "message": "duplicate event"}},
    ]
    rollout = tmp_path / "mini.jsonl"
    rollout.write_text("\n".join(json.dumps(row) for row in rows))
    report = extract.extract(rollout)
    assert report["sizes"]["developer_harness"] == len("Harness limits")
    assert report["sizes"]["our_instructions"] == len("Read the skill.\n\n")
    assert report["sizes"]["request_data"] == len('Incident request:\n{"incident_id":"toy"}')
    assert report["skill_files_read"] == ["/session/skills/sdo-memory/SKILL.md"]
    assert report["help_count"] == 1
    assert len(report["tool_calls"]) == 1
    assert "toy" not in json.dumps(report)  # default report does not publish request contents


def test_name_checker_reports_command_field_and_skill_typos(tmp_path: Path) -> None:
    check = load("check_names")
    catalogs = check.catalogs(ROOT)
    text = "`sdo diagnoze` request.not_a_real_field `skills/sdo-missing/SKILL.md`"
    errors = check.check_text(text, catalogs)
    assert {error.kind for error in errors} == {"command", "field", "skill"}
    assert not check.check_text("`sdo status` request.incident_id `skills/sdo-memory/SKILL.md`", catalogs)


def test_instruction_names_match_current_cli_contracts_and_installed_skills() -> None:
    check = load("check_names")
    assert check.check_repository(ROOT) == []
