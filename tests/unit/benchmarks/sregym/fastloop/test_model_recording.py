from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from agentshim import CommandRequest
from agentshim.testing import FakeExecutor, FakeRun

from benchmarks.sregym.fastloop.model_recording import ModelCacheMiss, RecordingExecutor

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


def test_recorded_model_io_replays_by_request_and_prompt_digest(tmp_path: Path) -> None:
    fake = FakeExecutor(FakeRun(stdout=['{"type":"turn.completed"}\n'], stderr=["notice\n"]))
    first = RecordingExecutor(tmp_path, request_json='{"task_id":"one"}', live=fake, record=True)
    streamed = Sink()

    result = first.run(_request("diagnose"), streamed)
    replay = RecordingExecutor(tmp_path, request_json='{"task_id":"one"}', live=None)
    replayed = Sink()

    assert replay.run(_request("diagnose"), replayed) == result
    assert replayed.stdout_lines == streamed.stdout_lines
    assert replayed.stderr_lines == streamed.stderr_lines
    assert len(fake.requests) == 1


def test_model_cache_miss_fails_unless_live_fallback_is_selected(tmp_path: Path) -> None:
    fake = FakeExecutor(FakeRun(stdout=["ok\n"]))
    replay = RecordingExecutor(tmp_path, request_json="{}", live=fake)

    with pytest.raises(ModelCacheMiss, match="request.*prompt"):
        replay.run(_request("unseen"), Sink())
    assert not fake.requests
    assert (
        RecordingExecutor(tmp_path, request_json="{}", live=fake, live_on_miss=True)
        .run(_request("unseen"), Sink())
        .stdout
        == "ok\n"
    )


def test_model_cache_does_not_reuse_another_prompt(tmp_path: Path) -> None:
    fake = FakeExecutor(FakeRun(stdout=["ok\n"]))
    RecordingExecutor(tmp_path, request_json="{}", live=fake, record=True).run(_request("first"), Sink())

    with pytest.raises(ModelCacheMiss):
        RecordingExecutor(tmp_path, request_json="{}", live=None).run(_request("second"), Sink())


def test_model_cache_normalizes_replay_checkout_path(tmp_path: Path) -> None:
    fake = FakeExecutor(FakeRun(stdout=["ok\n"]))
    RecordingExecutor(tmp_path, request_json="{}", live=fake, record=True).run(
        _request("inspect /workspace/incident"), Sink()
    )

    replay = RecordingExecutor(
        tmp_path,
        request_json="{}",
        live=None,
        prompt_replacements={"/tmp/replay/accepted": "/workspace/incident"},
    )

    assert replay.run(_request("inspect /tmp/replay/accepted"), Sink()).stdout == "ok\n"


def test_repeated_prompt_preserves_each_correction_turn(tmp_path: Path) -> None:
    runs = iter([FakeRun(stdout=["first\n"]), FakeRun(stdout=["second\n"])])
    fake = FakeExecutor(lambda _: next(runs))
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
