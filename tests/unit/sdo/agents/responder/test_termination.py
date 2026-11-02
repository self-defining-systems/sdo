from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from sdo.agents.responder import record_worker_failure
from sdo.agents.responder.termination import failure_line, records_worker_failure

if TYPE_CHECKING:
    from pathlib import Path


def test_failure_line_is_class_and_first_message_line_only() -> None:
    line = failure_line(ValueError("bad request\nTraceback (most recent call last):\n  secret tool output"))

    assert line == "ValueError: bad request"


def test_failure_line_redacts_credential_shapes_and_is_bounded() -> None:
    key = "sk-" + "a" * 40
    line = failure_line(RuntimeError(f"auth failed with Bearer abc123 and {key} " + "x " * 400))

    assert "abc123" not in line
    assert key not in line
    assert len(line) < 260


def test_record_worker_failure_writes_one_line(tmp_path: Path) -> None:
    target = tmp_path / "termination-log"

    record_worker_failure(OSError("no such file"), path=target)

    assert target.read_text(encoding="utf-8") == "OSError: no such file\n"


def test_record_worker_failure_never_masks_the_failure(tmp_path: Path) -> None:
    record_worker_failure(OSError("x"), path=tmp_path / "missing" / "log")


def test_uncaught_exception_in_main_is_recorded_and_reraised(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    target = tmp_path / "termination-log"
    monkeypatch.setattr("sdo.agents.responder.termination.TERMINATION_LOG", target)

    @records_worker_failure
    def main() -> int:
        raise KeyError("agent-task")

    with pytest.raises(KeyError):
        main()
    assert target.read_text(encoding="utf-8") == "KeyError: 'agent-task'\n"
