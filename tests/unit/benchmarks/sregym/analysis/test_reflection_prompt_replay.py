from __future__ import annotations

import json
from typing import TYPE_CHECKING

from google.protobuf import json_format

from benchmarks.sregym.analysis.reflection_prompt_replay import estimate_tokens, replay_directories, replay_rollout
from tests.unit.sdo.agents.responder.test_reflection_outcomes import _outcome

if TYPE_CHECKING:
    from pathlib import Path


def _event(kind: str, payload: dict[str, object], *, outer: str = "event_msg") -> str:
    return json.dumps({"type": outer, "payload": {"type": kind, **payload}})


def _user_message(text: str) -> str:
    return _event("message", {"role": "user", "content": [{"type": "input_text", "text": text}]}, outer="response_item")


def _write_rollout(path: Path, *, prior: int, reflection_requests: int = 3) -> None:
    current = _outcome("inc-current")
    history = [*(_outcome(f"inc-{index}") for index in range(prior)), current]
    current_json = json_format.MessageToJson(
        current, preserving_proto_field_name=True, indent=2, always_print_fields_with_no_presence=True
    )
    history_json = json.dumps(
        [
            json_format.MessageToDict(
                record, preserving_proto_field_name=True, always_print_fields_with_no_presence=True
            )
            for record in history
        ],
        indent=2,
    )
    request = (
        "Idempotency key: reflection:inc-current:sha\n\nstatic instructions\n"
        f"Current outcome:\n{current_json}\n\n"
        f"Outcome history:\n{history_json}\n"
    )
    lines = [
        _event("task_started", {}),
        _user_message("You are the SDO incident responder"),
        _event("token_count", {"info": {"last_token_usage": {}}}),
        _event("task_started", {}),
        _user_message(request),
        *[_event("token_count", {"info": {"last_token_usage": {}}}) for _ in range(reflection_requests)],
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_replay_reports_smaller_outcome_sections_for_a_recorded_reflection(tmp_path: Path) -> None:
    rollout = tmp_path / "rollout-a.jsonl"
    _write_rollout(rollout, prior=3)

    replay = replay_rollout(rollout)

    assert replay is not None
    assert replay.incident_id == "inc-current"
    assert replay.prior_outcomes == 3
    assert replay.requests == 3
    assert replay.new_chars < replay.old_chars / 2
    # Prompt tokens are re-read by each later request of the session.
    assert replay.saved_weighted_tokens == replay.saved_tokens * 1.2


def test_replay_skips_sessions_without_a_reflection_task(tmp_path: Path) -> None:
    rollout = tmp_path / "rollout-b.jsonl"
    rollout.write_text(_event("task_started", {}) + "\n", encoding="utf-8")

    assert replay_rollout(rollout) is None


def test_replay_counts_a_session_file_once_across_cumulative_stage_folders(tmp_path: Path) -> None:
    for stage in ("stage_0", "stage_1"):
        (tmp_path / stage).mkdir()
        _write_rollout(tmp_path / stage / "rollout-a.jsonl", prior=1)

    assert len(replay_directories([tmp_path])) == 1


def test_estimate_tokens_uses_the_measured_characters_per_token() -> None:
    assert estimate_tokens("x" * 400) == 100
