from __future__ import annotations

import hashlib
import json
import subprocess
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.skilleval.scenario import (
    REFLECTION_TYPE,
    Scenario,
    ScenarioError,
    build_scenario,
    cut_rollout,
    session_digest,
)

if TYPE_CHECKING:
    from pathlib import Path

THREAD = "thread-0001"
INCIDENT = "synthetic-app-incident-1"


def _line(record_type: str, payload: dict[str, object]) -> str:
    return json.dumps({"type": record_type, "payload": payload}) + "\n"


def _user(text: str) -> str:
    return _line(
        "response_item", {"type": "message", "role": "user", "content": [{"type": "input_text", "text": text}]}
    )


def _rollout() -> list[str]:
    return [
        _line("session_meta", {"id": THREAD}),
        _line("event_msg", {"type": "task_started"}),
        _user("Respond to the incident."),
        _line("response_item", {"type": "function_call", "arguments": "{}"}),
        _line("event_msg", {"type": "thread_settings_applied"}),
        _line("event_msg", {"type": "task_started"}),
        _line(
            "response_item",
            {"type": "message", "role": "developer", "content": [{"type": "input_text", "text": "skills"}]},
        ),
        _user(f"Idempotency key: reflection:{INCIDENT}:abc\n\nReflect."),
        _line("event_msg", {"type": "task_complete"}),
    ]


def test_cut_rollout_keeps_the_responder_turn_and_drops_the_reflection_turn() -> None:
    lines = _rollout()

    kept, thread = cut_rollout(lines)

    assert thread == THREAD
    assert kept == lines[:5]


def test_cut_rollout_rejects_a_rollout_without_a_reflection_turn() -> None:
    with pytest.raises(ScenarioError):
        cut_rollout(_rollout()[:5])


def _git(repository: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repository), *args], capture_output=True, text=True, check=True
    ).stdout.strip()


def _memory_repository(root: Path) -> tuple[Path, str]:
    repository = root / "memory"
    (repository / ".sdo" / "playbooks" / "example").mkdir(parents=True)
    (repository / ".sdo" / "playbooks" / "example" / "README.md").write_text("example\n", encoding="utf-8")
    _git(repository.parent, "init", "-q", str(repository))
    _git(repository, "add", ".")
    _git(repository, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-qm", "outcome")
    return repository, _git(repository, "rev-parse", "HEAD")


def _task(commit: str, *, attempt: int = 1, session_mode: str = "resume", responder_session: str = THREAD) -> str:
    return json.dumps(
        {
            "task_id": f"reflection-{attempt}",
            "workspace": {"checkout_path": "/workspace/proposal/checkout", "base_commit": commit},
            "request": {
                "@type": REFLECTION_TYPE,
                "outcome_commit": commit,
                "outcome": {"incident_id": INCIDENT},
                "validation_attempt": attempt,
                "session_mode": session_mode,
                "closure": {"result": {"responder_session_id": responder_session}},
            },
        }
    )


def _session(root: Path, commit: str, *, rollout: bool = True, session_mode: str = "resume") -> Path:
    session = root / "sessions" / session_digest(INCIDENT)
    codex = session / "codex"
    (codex / "sessions" / "2026" / "01" / "01").mkdir(parents=True)
    if rollout:
        (codex / "sessions" / "2026" / "01" / "01" / f"rollout-{THREAD}.jsonl").write_text(
            "".join(_rollout()), encoding="utf-8"
        )
    (codex / "auth.json").write_text("{}", encoding="utf-8")
    (codex / "state_5.sqlite").write_text("derived", encoding="utf-8")
    (session / "context").mkdir()
    (session / "context" / "note.txt").write_text("context\n", encoding="utf-8")
    (session / "corpus" / "healthy").mkdir(parents=True)
    (session / "corpus" / "open@detect.json").write_text("{}", encoding="utf-8")
    (session / "reflection-usage.jsonl").write_text(json.dumps({"model_requests": 3, "usage": {}}) + "\n")
    for attempt in (1, 2):
        wire = _task(commit, attempt=attempt, session_mode=session_mode)
        checkpoint = session / "checkpoints" / hashlib.sha256(wire.encode()).hexdigest()
        checkpoint.mkdir(parents=True)
        (checkpoint / "request.json").write_text(wire, encoding="utf-8")
        (checkpoint / "result.json").write_text(json.dumps({"status": "completed"}), encoding="utf-8")
        (checkpoint / "file_edits.json").write_text(
            json.dumps({"version": 1, "edits": [{"path": ".sdo/playbooks/example/README.md"}]}), encoding="utf-8"
        )
    responder = session / "checkpoints" / "responder"
    responder.mkdir()
    (responder / "request.json").write_text(json.dumps({"request": {"@type": "IncidentRequest"}}), encoding="utf-8")
    return session


def _row(root: Path) -> Path:
    row = root / "row"
    (row / "000_mf__synthetic__n1").mkdir(parents=True)
    return row


def test_build_scenario_rewinds_the_session_to_the_first_reflection_attempt(tmp_path: Path) -> None:
    repository, commit = _memory_repository(tmp_path)
    session = _session(tmp_path, commit)

    scenario = build_scenario(
        scenario_id="synthetic",
        row_dir=_row(tmp_path),
        incident="000",
        session_source=session,
        memory_repository=str(repository),
        image="sdo-sregym-responder:test",
        output=tmp_path / "scenario",
    )

    output = tmp_path / "scenario"
    assert Scenario.load(output) == scenario
    assert scenario.thread_id == THREAD
    assert scenario.source.problem_id == "mf__synthetic__n1"
    assert json.loads((output / "task.json").read_text())["request"]["validation_attempt"] == 1
    assert not list(output.rglob("auth.json"))
    assert not (output / "session" / "codex" / "state_5.sqlite").exists()
    assert not (output / "session" / "reflection-usage.jsonl").exists()
    assert [path.parent.name for path in (output / "session" / "checkpoints").glob("*/request.json")] == ["responder"]
    rollout = next((output / "session" / "codex").rglob("rollout-*.jsonl")).read_text().splitlines(keepends=True)
    assert rollout == _rollout()[:5]
    assert (output / "session" / "context" / "note.txt").is_file()
    assert _git(output / "checkout", "rev-parse", "HEAD") == commit
    assert [attempt["validation_attempt"] for attempt in scenario.live["attempts"]] == [1, 2]
    assert scenario.live["attempts"][0]["changed_files"] == [".sdo/playbooks/example/README.md"]
    assert (output / "live" / "attempt-2.json").is_file()


def test_build_scenario_for_a_fresh_reflection_keeps_no_rollout(tmp_path: Path) -> None:
    repository, commit = _memory_repository(tmp_path)
    session = _session(tmp_path, commit, session_mode="fresh")

    scenario = build_scenario(
        scenario_id="fresh",
        row_dir=_row(tmp_path),
        incident="000",
        session_source=session,
        memory_repository=str(repository),
        image="sdo-sregym-responder:test",
        output=tmp_path / "scenario",
    )

    assert scenario.session_mode == "fresh"
    assert scenario.thread_id == ""
    assert not list((tmp_path / "scenario" / "session").rglob("rollout-*.jsonl"))


def test_build_scenario_rejects_a_session_of_another_incident(tmp_path: Path) -> None:
    repository, commit = _memory_repository(tmp_path)
    session = _session(tmp_path, commit)
    other = session.rename(session.with_name("0" * 32))

    with pytest.raises(ScenarioError, match="not the incident session"):
        build_scenario(
            scenario_id="synthetic",
            row_dir=_row(tmp_path),
            incident="000",
            session_source=other,
            memory_repository=str(repository),
            image="sdo-sregym-responder:test",
            output=tmp_path / "scenario",
        )
