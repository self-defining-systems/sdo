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
