"""Detached launch contracts use real process boundaries and synthetic workers."""

import json
import os
import signal
import subprocess
import sys
from pathlib import Path

import pytest

from benchmarks.sregym.adapter import detached, persistent, processes
from tests.unit.benchmarks.sregym.adapter.test_abort import AbortOps, seed_state
from tests.unit.benchmarks.sregym.adapter.test_processes import wait_for


def launch_command(tmp_path: Path, run_name: str = "run") -> list[str]:
    helpers = tmp_path / f"{run_name}-helpers.json"
    helpers.write_text(json.dumps({"bridge": [sys.executable, "-c", "import time; time.sleep(600)"]}))
    return [
        sys.executable,
        "-m",
        "benchmarks.sregym.adapter.detached",
        "launch",
        "--record",
        str(tmp_path / "lane.json"),
        "--run-dir",
        str(tmp_path / run_name),
        "--helpers",
        str(helpers),
        "--",
        sys.executable,
        "-u",
        "-c",
        "import sys, time; print('worker ready'); print('worker stderr', file=sys.stderr); time.sleep(600)",
    ]


def test_detached_survives_parent_shell_sigkill_and_record_abort_quiesces(tmp_path):
    ready = tmp_path / "shell.ready"
    # Keep the invoking shell alive until explicitly killed after launch returns.
    shell = subprocess.Popen(
        ["bash", "-c", '"$@" && touch "$READY"; kill -STOP $$', "detached-test", *launch_command(tmp_path)],
        env={**os.environ, "READY": str(ready)},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    record_path = tmp_path / "lane.json"
    try:
        wait_for(ready.exists)
        record = detached.status(record_path)
        assert record is not None
        assert record.launcher.pid == record.launcher.process_group
        assert os.getsid(record.launcher.pid) == record.launcher.pid
        assert record.launcher.process_group != shell.pid
        assert record.started_at.tzinfo is not None
        assert record.command[-2] == "-c"
        assert record.state_path == tmp_path / "run" / "sdo_persistent_controller.json"
        wait_for(lambda: len(processes.read_registry(record.state_path).processes) == 4)
        refs = processes.read_registry(record.state_path).processes
        os.kill(shell.pid, signal.SIGKILL)
        shell.wait(timeout=10)
        assert detached.status(record_path) == record
        assert all(ref.alive() for ref in refs)
        wait_for(
            lambda: "worker ready" in record.stdout_path.read_text()
            and "worker stderr" in record.stderr_path.read_text()
        )
        seed_state(record.state_path)
        ops = AbortOps()
        assert detached.abort_record(record_path, ops=ops) == []
        assert ops.stopped == ["demo-sdo"]
        assert not persistent.PersistentState.load(record.state_path).controllers
        assert all(not ref.alive() for ref in processes.read_registry(record.state_path).processes)
        assert detached.status(record_path) is None
        assert detached.abort_record(record_path, ops=ops) == []
    finally:
        if shell.poll() is None:
            shell.kill()
            shell.wait(timeout=10)
        if record_path.exists():
            detached.abort_record(record_path, ops=AbortOps())


def test_same_lane_refuses_concurrent_launch_and_can_be_adopted(tmp_path):
    first = subprocess.Popen(launch_command(tmp_path), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    second = subprocess.Popen(
        launch_command(tmp_path, "other-run"), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    try:
        results = [(p.communicate(timeout=15), p.returncode) for p in (first, second)]
        assert sorted(code for _, code in results) == [0, 1]
        assert "live run already owns lane" in "".join(err for (_, err), _ in results)
        record_path = tmp_path / "lane.json"
        record = detached.status(record_path)
        assert record is not None
        status = subprocess.run(
            [sys.executable, "-m", "benchmarks.sregym.adapter.detached", "status", "--record", str(record_path)],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert status.returncode == 0
        assert detached.LaunchRecord.model_validate_json(status.stdout) == record
    finally:
        for process in (first, second):
            if process.poll() is None:
                process.kill()
                process.wait(timeout=10)
        if (tmp_path / "lane.json").exists():
            detached.abort_record(tmp_path / "lane.json", ops=AbortOps())


def test_reused_record_pid_never_authorizes_abort(tmp_path, monkeypatch):
    subprocess.run(launch_command(tmp_path), check=True, capture_output=True, timeout=15)
    path = tmp_path / "lane.json"
    original = path.read_text()
    try:
        record = detached.LaunchRecord.model_validate_json(original)
        record.launcher.start_ticks += 1
        path.write_text(record.model_dump_json())
        assert detached.status(path) is None
        monkeypatch.setattr(os, "kill", lambda *_: pytest.fail("reused PID was signalled"))
        with pytest.raises(RuntimeError, match="identity"):
            detached.abort_record(path, ops=AbortOps())
    finally:
        monkeypatch.undo()
        path.write_text(original)
        detached.abort_record(path, ops=AbortOps())


@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT])
def test_detached_owner_keeps_deliberate_signal_abort_trap(tmp_path, signum):
    subprocess.run(launch_command(tmp_path), check=True, capture_output=True, timeout=15)
    path = tmp_path / "lane.json"
    record = detached.status(path)
    assert record is not None
    try:
        os.kill(record.launcher.pid, signum)
        wait_for(lambda: detached.status(path) is None)
        assert all(not ref.alive() for ref in processes.read_registry(record.state_path).processes)
    finally:
        detached.abort_record(path, ops=AbortOps())
