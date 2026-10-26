import json
import os
import signal
import subprocess
import sys
import time

import pytest

from benchmarks.sregym.adapter import persistent, processes
from tests.unit.benchmarks.sregym.adapter.test_abort import seed_state


def wait_for(predicate, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError("condition did not settle")


def test_reused_pid_is_never_signalled(tmp_path, monkeypatch):
    ref = processes.ProcessRef.capture(os.getpid(), "helper")
    ref.start_ticks += 1
    monkeypatch.setattr(os, "kill", lambda *_: pytest.fail("reused PID was signalled"))
    assert not ref.alive()


@pytest.mark.parametrize("cut", ["install", "incident-open", "responder", "reflection", "drain"])
def test_sigkill_launcher_leaves_no_registered_helpers(tmp_path, cut):
    state = tmp_path / "state.json"
    helpers = tmp_path / "helpers.json"
    helpers.write_text(
        json.dumps(
            {
                "credential-keeper": [sys.executable, "-c", "import time; time.sleep(600)"],
                "bridge": [sys.executable, "-c", "import time; time.sleep(600)"],
            }
        )
    )
    seed_state(state)
    if cut == "install":
        persistent.PersistentState(
            cleanup_targets={"demo": persistent.CleanupTarget(namespace="demo", control_namespace="demo-sdo")}
        ).save(state)
    phases = ["install", "incident-open", "responder", "reflection", "drain"]
    objects = {f"{phase}-job" for phase in phases[: phases.index(cut) + 1]}

    class Cluster:
        def protect_incident_work(self, target, directory):
            pass

        def stop_runtime(self, target):
            objects.clear()

        def abort_workload(self, target):
            pass

        def abort_leftovers(self, target):
            return sorted(objects)

        def retire_controller_state(self, target, archive_dir):
            return []

    child = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "benchmarks.sregym.adapter.launcher",
            "--state",
            str(state),
            "--helpers",
            str(helpers),
            "--",
            sys.executable,
            "-c",
            "import time; time.sleep(600)",
        ]
    )
    refs = []
    try:
        wait_for(lambda: len(processes.read_registry(state).processes) == 5)
        refs = processes.read_registry(state).processes
        os.kill(child.pid, signal.SIGKILL)
        child.wait(timeout=10)
        wait_for(lambda: all(not ref.alive() for ref in refs))
        assert persistent.abort(state, ops=Cluster()) == []
        assert objects == set()
        assert all(not ref.alive() for ref in processes.read_registry(state).processes)
    finally:
        if child.poll() is None:
            child.kill()
            child.wait()
        processes.terminate_run(state)


def test_bind_preserves_result_state_location_and_registry(tmp_path, monkeypatch):
    locator = tmp_path / "locator" / "state.json"
    target = tmp_path / "pipeline" / "sdo_persistent_controller.json"
    processes.register(locator, processes.ProcessRef.capture(os.getpid(), "launcher"), new_run=True)
    monkeypatch.setenv("SDO_RUN_REGISTRY_STATE", str(locator))
    processes.bind_state(target)
    seed_state(target)
    assert locator.resolve() == target
    assert processes.read_registry(target) == processes.read_registry(locator)
    from tests.unit.benchmarks.sregym.adapter.test_abort import AbortOps

    assert persistent.abort(locator, ops=AbortOps()) == []
    assert persistent.PersistentState.load(target).controllers == {}


@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT])
def test_launcher_signal_trap_aborts_without_drain(tmp_path, signum):
    state = tmp_path / "state.json"
    child = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "benchmarks.sregym.adapter.launcher",
            "--state",
            str(state),
            "--",
            sys.executable,
            "-c",
            "import time; time.sleep(600)",
        ]
    )
    try:
        wait_for(lambda: len(processes.read_registry(state).processes) == 3)
        child.send_signal(signum)
        assert child.wait(timeout=15) == 128 + signum
        assert all(not ref.alive() for ref in processes.read_registry(state).processes)
    finally:
        if child.poll() is None:
            child.kill()
            child.wait()
        processes.terminate_run(state)


def test_killing_uv_wrapper_does_not_orphan_launcher_or_helpers(tmp_path):
    state = tmp_path / "state.json"
    wrapper = subprocess.Popen(
        [
            "uv",
            "run",
            "python",
            "-m",
            "benchmarks.sregym.adapter.launcher",
            "--state",
            str(state),
            "--",
            sys.executable,
            "-c",
            "import time; time.sleep(600)",
        ]
    )
    try:
        wait_for(lambda: len(processes.read_registry(state).processes) == 3)
        wrapper.kill()
        wrapper.wait(timeout=10)
        wait_for(lambda: all(not ref.alive() for ref in processes.read_registry(state).processes), timeout=3)
    finally:
        if wrapper.poll() is None:
            wrapper.kill()
            wrapper.wait()
        processes.terminate_run(state)
