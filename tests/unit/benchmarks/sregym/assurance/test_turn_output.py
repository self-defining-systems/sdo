"""Captured command documents are complete or rejected before callers decode them."""

from __future__ import annotations

import io
import json
import shlex
import sys
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.assurance.scripted_codex.turn import MAX_OUTPUT_CHARS, OutputLimitExceeded, Turn
from benchmarks.sregym.assurance.scripted_codex.wire import EventStream, Rollout

if TYPE_CHECKING:
    from pathlib import Path


def command_output(text: str, *, stderr: bool = False) -> str:
    destination = "sys.stderr" if stderr else "sys.stdout"
    return shlex.join([sys.executable, "-c", f"import sys; {destination}.write({text!r})"])


def new_turn(tmp_path: Path) -> tuple[Turn, io.StringIO]:
    events = io.StringIO()
    rollout = Rollout.create(tmp_path, cwd=str(tmp_path))
    return Turn("responder", 1, tmp_path, rollout, EventStream(events)), events


@pytest.mark.parametrize("stderr", [False, True])
def test_oversized_document_is_refused_before_decode(tmp_path: Path, stderr: bool) -> None:
    turn, events = new_turn(tmp_path)
    document = json.dumps({"evidence": "x" * MAX_OUTPUT_CHARS})
    with pytest.raises(OutputLimitExceeded, match="MAX_OUTPUT_CHARS") as failure:
        turn.run(command_output(document, stderr=stderr))
    assert type(failure.value).__name__ == "OutputLimitExceeded"
    assert failure.value.limit == MAX_OUTPUT_CHARS
    assert failure.value.actual == len(document)
    completed = json.loads(events.getvalue().splitlines()[-1])["item"]
    assert completed["exit_code"] != 0
    assert "MAX_OUTPUT_CHARS" in completed["aggregated_output"]
    assert document[:100] not in completed["aggregated_output"]


def test_document_exactly_at_bound_round_trips(tmp_path: Path) -> None:
    turn, _ = new_turn(tmp_path)
    document = json.dumps({"evidence": "é" * (MAX_OUTPUT_CHARS - 16)}, ensure_ascii=False)
    assert len(document) == MAX_OUTPUT_CHARS
    code, output = turn.run(command_output(document))
    assert code == 0
    assert output == document
    assert json.loads(output)["evidence"] == "é" * (MAX_OUTPUT_CHARS - 16)


def test_structured_capture_has_explicit_budget(tmp_path: Path) -> None:
    turn, _ = new_turn(tmp_path)
    document = json.dumps({"evidence": "x" * (MAX_OUTPUT_CHARS + 1)})
    code, output = turn.run(command_output(document), max_output_chars=len(document))
    assert code == 0
    assert json.loads(output) == json.loads(document)


def test_combined_streams_share_one_bound(tmp_path: Path) -> None:
    turn, _ = new_turn(tmp_path)
    command = shlex.join([sys.executable, "-c", "import sys; sys.stdout.write('x'*11000); sys.stderr.write('y'*11000)"])
    with pytest.raises(OutputLimitExceeded) as failure:
        turn.run(command)
    assert failure.value.actual == 22_000


def test_structured_budget_also_refuses_oversize(tmp_path: Path) -> None:
    from benchmarks.sregym.assurance.scripted_codex.turn import MAX_STRUCTURED_OUTPUT_CHARS

    turn, _ = new_turn(tmp_path)
    document = tmp_path / "large.json"
    document.write_text(json.dumps({"evidence": "x" * MAX_STRUCTURED_OUTPUT_CHARS}))
    command = shlex.join(
        [sys.executable, "-c", f"from typing import TYPE_CHECKING; print(Path({str(document)!r}).read_text())"]
    )
    with pytest.raises(OutputLimitExceeded) as failure:
        turn.run(command, max_output_chars=MAX_STRUCTURED_OUTPUT_CHARS)
    assert failure.value.limit == MAX_STRUCTURED_OUTPUT_CHARS
    assert failure.value.actual == document.stat().st_size + 1


@pytest.mark.parametrize("limit", [0, -1])
def test_invalid_capture_budget_is_rejected_before_execution(tmp_path: Path, limit: int) -> None:
    turn, events = new_turn(tmp_path)
    with pytest.raises(ValueError, match="positive"):
        turn.run("exit 0", max_output_chars=limit)
    assert not events.getvalue()
    assert not turn.commands
