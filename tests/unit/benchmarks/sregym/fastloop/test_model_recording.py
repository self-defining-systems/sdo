from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from agentshim import CommandRequest
from agentshim.testing import FakeExecutor, FakeRun

from benchmarks.sregym.fastloop.model_recording import ModelRecorder

if TYPE_CHECKING:
    from pathlib import Path


class Sink:
    def __init__(self) -> None:
        self.stdout_lines: list[str] = []
        self.stderr_lines: list[str] = []

    def started(self, _handle: object) -> None:
        pass

    def stdout(self, line: str) -> None:
        self.stdout_lines.append(line)

    def stderr(self, line: str) -> None:
        self.stderr_lines.append(line)


def _request(prompt: str) -> CommandRequest:
    return CommandRequest(argv=["codex", "exec", "--json", "--", prompt], stdin=None, cwd=None, env={}, timeout=10)


def test_live_capture_converts_to_ordered_scripts(tmp_path: Path) -> None:
    import json

    from benchmarks.sregym.agent_script.converter import convert_recording
    from benchmarks.sregym.agent_script.runner import execute
    from benchmarks.sregym.agent_script.wire import EventStream

    runs = iter(
        FakeRun(
            stdout=[
                json.dumps(
                    {
                        "type": "item.completed",
                        "item": {"type": "agent_message", "text": json.dumps({"summary": answer})},
                    }
                )
                + "\n"
            ]
        )
        for answer in ["first", "second"]
    )
    fake = FakeExecutor(lambda _: next(runs))
<<<<<<< HEAD
    recorder = ModelRecorder(tmp_path, request_json="{}", live=fake)
    recorder.run(_request("same correction"), Sink())
    recorder.run(_request("same correction"), Sink())
    script = convert_recording(list(tmp_path.glob("*/*.json")), role="reflector")
    assert execute(script.select("reflector", None, 0), tmp_path, EventStream()) == {"summary": "first"}
    assert execute(script.select("reflector", None, 1), tmp_path, EventStream()) == {"summary": "second"}
    with pytest.raises(ValueError, match="exactly one script"):
        script.select("reflector", None, 2)
    assert len(fake.requests) == 2
=======
    recorder = RecordingExecutor(tmp_path, request_json="{}", live=fake, record=True)
    recorder.run(_request("correct"), Sink())
    recorder.run(_request("correct"), Sink())
    replay = RecordingExecutor(tmp_path, request_json="{}")
    assert replay.run(_request("correct"), Sink()).stdout == "first\n"
    assert replay.run(_request("correct"), Sink()).stdout == "second\n"
    with pytest.raises(ModelCacheMiss):
        replay.run(_request("correct"), Sink())


def test_ordered_reflection_replay_reports_changed_prompt_without_live_call(tmp_path: Path) -> None:
    fake = FakeExecutor(FakeRun(stdout=["proposal\n"]))
    RecordingExecutor(tmp_path, request_json="{}", live=fake, record=True).run(_request("old guidance"), Sink())
    replay = RecordingExecutor(tmp_path, request_json="{}", ordered=True)
    assert replay.run(_request("current guidance"), Sink()).stdout == "proposal\n"
    assert replay.prompt_changes[0]["recorded"] != replay.prompt_changes[0]["current"]
    assert len(fake.requests) == 1


def test_ordered_replay_does_not_reuse_an_earlier_turn_after_exhaustion(tmp_path: Path) -> None:
    runs = iter([FakeRun(stdout=["one\n"]), FakeRun(stdout=["two\n"])])
    recorder = RecordingExecutor(tmp_path, request_json="{}", live=FakeExecutor(lambda _: next(runs)), record=True)
    recorder.run(_request("one"), Sink())
    recorder.run(_request("two"), Sink())
    replay = RecordingExecutor(tmp_path, request_json="{}", ordered=True)
    replay.run(_request("changed one"), Sink())
    replay.run(_request("changed two"), Sink())
    with pytest.raises(ModelCacheMiss):
        replay.run(_request("one"), Sink())


def test_ordered_replay_honors_explicit_live_fallback(tmp_path: Path) -> None:
    fake = FakeExecutor(FakeRun(stdout=["recorded\n"]))
    RecordingExecutor(tmp_path, request_json="{}", live=fake, record=True).run(_request("first"), Sink())
    fallback = FakeExecutor(FakeRun(stdout=["new\n"]))
    replay = RecordingExecutor(tmp_path, request_json="{}", live=fallback, ordered=True, live_on_miss=True)
    assert replay.run(_request("first"), Sink()).stdout == "recorded\n"
    assert replay.run(_request("new prompt"), Sink()).stdout == "new\n"
    assert len(fallback.requests) == 1


def test_warm_recording_replays_with_canonical_checkout_identity(tmp_path: Path, monkeypatch) -> None:
    from benchmarks.sregym.fastloop.recording_worker import recording_executor
    from sdo.contracts import AgentTask, to_canonical_json

    wire = to_canonical_json(AgentTask(task_id="one", workspace={"checkout_path": "/workspace/incident"}))
    monkeypatch.setenv("SDO_WORKSPACE_ALIAS", "/sdo/proposal")
    fake = FakeExecutor(FakeRun(stdout=["recorded\n"]))
    recording = recording_executor(tmp_path, wire)
    recording.live = fake
    recording.run(_request("inspect /sdo/proposal"), Sink())
    replay = RecordingExecutor(
        tmp_path,
        request_json=wire,
        prompt_replacements={"/tmp/replay": "/workspace/incident"},
    )
    assert replay.run(_request("inspect /tmp/replay"), Sink()).stdout == "recorded\n"
    assert fake.requests[0].argv[-1] == "inspect /sdo/proposal"
    assert len(fake.requests) == 1


def test_reflection_turn_over_four_mib_records_each_line_once_and_replays(tmp_path: Path) -> None:
    # A resumed reflection that reads large tool results emits megabytes of JSONL in one turn.
    line = '{"type":"item.completed","item":{"type":"mcp_tool_call","result":"' + "x" * (160 << 10) + '"}}\n'
    output = [line] * 32 + ['{"type":"turn.completed"}\n']
    fake = FakeExecutor(FakeRun(stdout=output, stderr=["notice\n"]))
    streamed = Sink()

    result = RecordingExecutor(tmp_path, request_json="{}", live=fake, record=True).run(_request("reflect"), streamed)

    (recorded,) = tmp_path.glob("*/*.json")
    assert sum(map(len, output)) > 4 << 20
    assert recorded.stat().st_size < sum(map(len, output)) * 1.1
    replayed = Sink()
    assert RecordingExecutor(tmp_path, request_json="{}", live=None).run(_request("reflect"), replayed) == result
    assert replayed.stdout_lines == streamed.stdout_lines == output
    assert replayed.stderr_lines == ["notice\n"]


def test_oversized_live_turn_returns_its_result_and_replay_names_the_gap(tmp_path: Path) -> None:
    """Benchmark evidence limits never discard a finished live model turn."""
    line = "x" * (1 << 20) + "\n"
    fake = FakeExecutor(FakeRun(stdout=[line] * 65))
    recorder = RecordingExecutor(tmp_path, request_json="{}", live=fake, record=True)

    assert recorder.run(_request("large"), Sink()).stdout == line * 65
    with pytest.raises(ModelCacheMiss, match="not recorded: model I/O exceeds"):
        RecordingExecutor(tmp_path, request_json="{}", live=None).run(_request("large"), Sink())
    with pytest.raises(ModelCacheMiss, match="not recorded: model I/O exceeds"):
        RecordingExecutor(tmp_path, request_json="{}", live=None, ordered=True).run(_request("large"), Sink())
>>>>>>> origin/vic-lsh/dev/sdo-improve
