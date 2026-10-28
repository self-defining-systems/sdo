"""The scripted provider's real session writer feeds the context gate."""

import json

import pytest

from benchmarks.sregym.adapter.context_preflight import check_context_shapes
from benchmarks.sregym.assurance.scripted_codex.wire import Rollout
from sdo.agents.session_context import measure_session


def test_live_shape_preflight_measures_production_producers(tmp_path):
    measured = check_context_shapes(tmp_path, rollout_factory=lambda home: Rollout.create(home, cwd=str(tmp_path)))
    assert measured["responder"]["first_message_chars"] < 12000
    assert measured["reflection"]["first_message_chars"] < 15000
    assert measured["responder"]["tool_outputs"] > 0
    assert measured["responder"]["total_tool_output_chars"] < 131072


@pytest.mark.parametrize(("message", "output"), [("x" * 15001, ""), ("small", "x" * 131073)])
def test_gate_negative_controls_use_the_provider_session_file(tmp_path, message, output):
    rollout = Rollout.create(tmp_path, cwd=str(tmp_path))
    rollout.user_message(message)
    rollout.function_call_output("call", output, 0)
    with pytest.raises(ValueError, match="provider"):
        measure_session(rollout.path).check("reflection")


def test_custom_tool_blocks_and_continuations_are_measured_once(tmp_path):
    path = tmp_path / "rollout.jsonl"
    items = [
        {"type": "response_item", "payload": {"type": "message", "role": "user", "content": [{"text": "first"}]}},
        {
            "type": "response_item",
            "payload": {"type": "custom_tool_call_output", "output": [{"text": "abc"}, {"text": "de"}]},
        },
        {"type": "event_msg", "payload": {"type": "user_message", "message": "duplicated"}},
        {"type": "response_item", "payload": {"type": "message", "role": "user", "content": [{"text": "resumed"}]}},
    ]
    path.write_text("\n".join(json.dumps(item) for item in items))
    result = measure_session(path)
    assert result.first_message_chars == 5
    assert result.largest_message_chars == 7
    assert result.total_tool_output_chars == 5
    assert result.tool_outputs == 1


def test_missing_provider_evidence_fails_closed(tmp_path):
    path = tmp_path / "empty.jsonl"
    path.touch()
    with pytest.raises(ValueError, match="no provider"):
        measure_session(path)
