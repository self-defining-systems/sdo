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
    recorder = ModelRecorder(tmp_path, request_json="{}", live=fake)
    recorder.run(_request("same correction"), Sink())
    recorder.run(_request("same correction"), Sink())
    script = convert_recording(list(tmp_path.glob("*/*.json")), role="reflector")
    assert execute(script.select("reflector", None, 0), tmp_path, EventStream()) == {"summary": "first"}
    assert execute(script.select("reflector", None, 1), tmp_path, EventStream()) == {"summary": "second"}
    with pytest.raises(ValueError, match="exactly one script"):
        script.select("reflector", None, 2)
    assert len(fake.requests) == 2


def test_oversized_live_turn_returns_its_result_and_names_the_gap(tmp_path: Path) -> None:
    """Benchmark evidence limits never discard a finished live model turn."""
    import json

    line = "x" * (1 << 20) + "\n"
    fake = FakeExecutor(FakeRun(stdout=[line] * 65))

    assert ModelRecorder(tmp_path, request_json="{}", live=fake).run(_request("large"), Sink()).stdout == line * 65
    (recorded,) = tmp_path.glob("*/*.json")
    assert "model I/O exceeds" in json.loads(recorded.read_text())["unrecorded"]
