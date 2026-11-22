"""Every wait of the run path says what it waits for, every minute, and a row that outlives its deadline is named."""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from benchmarks.sregym.adapter.persistent import Clock, wait_for_outcome_commit
from benchmarks.sregym.adapter.wait_progress import CURRENT_WAIT_ENV, PROGRESS_LOG_SECONDS
from benchmarks.sregym.lanes import workload
from benchmarks.sregym.lanes.workload import AdapterError, RowDeadlineError


class FakeClock:
    def __init__(self) -> None:
        self.t = 1000.0

    def monotonic(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


class NeverAcknowledges:
    def runtime_state(self, control: str) -> dict[str, object]:
        return {"last_acknowledged_incident_id": "other"}


def _clock(fake: FakeClock) -> Clock:
    base = datetime(2026, 9, 27, 12, 0, 0, tzinfo=timezone.utc)
    return Clock(monotonic=fake.monotonic, sleep=fake.advance, now=lambda: base + timedelta(seconds=fake.t - 1000.0))


def test_a_long_wait_logs_what_it_waits_for_every_progress_interval(caplog: pytest.LogCaptureFixture) -> None:
    fake = FakeClock()
    clock = _clock(fake)
    caplog.set_level(logging.INFO)
    deadline = clock.now() + timedelta(seconds=10 * PROGRESS_LOG_SECONDS)
    assert wait_for_outcome_commit(NeverAcknowledges(), "ctl", "i-1", deadline, clock) is None  # type: ignore[arg-type]
    lines = [r.getMessage() for r in caplog.records if "outcome commit" in r.getMessage()]
    # One line per interval, naming the wait and the incident, plus the line that says the deadline ended it.
    progress = [line for line in lines if "still waiting" in line]
    assert 8 <= len(progress) <= 11, lines
    assert all("i-1" in line for line in progress)
    assert any("deadline" in line and "not acknowledged" in line for line in lines), lines


def test_a_wait_names_itself_in_the_current_wait_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "current-wait.json"
    monkeypatch.setenv(CURRENT_WAIT_ENV, str(path))
    fake = FakeClock()
    clock = _clock(fake)
    wait_for_outcome_commit(NeverAcknowledges(), "ctl", "i-9", clock.now() + timedelta(seconds=30), clock)  # type: ignore[arg-type]
    named = json.loads(path.read_text())
    assert "outcome commit" in named["wait"]
    assert "i-9" in named["wait"]


def test_the_adapter_kills_a_run_at_its_row_deadline_and_names_the_wait(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A fastloop run that never ends (a wait with no bound of its own) is killed with its whole process tree."""
    wait_file = tmp_path / "current-wait.json"
    child_pid = tmp_path / "child.pid"
    wait_file.write_text(json.dumps({"wait": "outcome commit of incident i-7", "since": "2026-11-22T01:14:32Z"}))
    # A run that ignores TERM and leaves a child behind.
    script = (
        "import os, signal, subprocess, sys, time\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        f"child = subprocess.Popen(['sleep', '600'])\nopen({str(child_pid)!r}, 'w').write(str(child.pid))\n"
        "time.sleep(600)\n"
    )
    monkeypatch.setattr(workload, "KILL_GRACE_SECONDS", 1.0)
    started = time.monotonic()
    with pytest.raises(RowDeadlineError) as raised:
        workload.run_logged(
            [sys.executable, "-c", script],
            env=os.environ,
            deadline_seconds=1.5,
            current_wait=wait_file,
        )
    assert time.monotonic() - started < 20
    assert isinstance(raised.value, AdapterError)
    message = str(raised.value)
    assert "row deadline" in message
    assert "outcome commit of incident i-7" in message
    pid = int(child_pid.read_text())
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
            stat = Path(f"/proc/{pid}/stat").read_text()
        except (ProcessLookupError, FileNotFoundError):
            break
        if stat.rsplit(")", 1)[1].split()[0] == "Z":
            break
        time.sleep(0.05)
    else:
        os.kill(pid, signal.SIGKILL)
        pytest.fail("the run's child outlived the row deadline")
