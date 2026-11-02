"""Scripted workers obey the native preload/claim transport before reading tasks."""

import json
import os
import select
import subprocess
import sys


def test_scripted_worker_waits_for_native_claim_before_reading_task(tmp_path):
    ready_read, ready_write = os.pipe()
    claim_read, claim_write = os.pipe()
    environment = {k: v for k, v in os.environ.items() if k not in {"SDO_NAMESPACE", "SDO_RESULT_CONFIGMAP"}}
    environment.update(
        SDO_STANDBY_READY_FD=str(ready_write), SDO_STANDBY_CLAIM_FD=str(claim_read), SDO_STANDBY_INBOX=str(tmp_path)
    )
    code = """from benchmarks.sregym.adapter import scripted_worker as w
w.KubernetesResultPublisher = lambda **kw: object()
def task(path, *args, **kwargs):
    assert path.read_text() == "claimed task"
    print("TASK_READ_AFTER_CLAIM", flush=True)
w.run_worker_task = task
raise SystemExit(w.main(["responder"]))
"""
    child = subprocess.Popen(
        [sys.executable, "-c", code],
        pass_fds=(ready_write, claim_read),
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    os.close(ready_write)
    os.close(claim_read)
    try:
        readable, _, _ = select.select([ready_read], [], [], 5)
        assert readable, "worker exited before native ready handshake"
        assert os.read(ready_read, 1) == b"R"
        assert child.poll() is None
        (tmp_path / "environment.json").write_text(
            json.dumps({"SDO_NAMESPACE": "app-sdo", "SDO_RESULT_CONFIGMAP": "result"})
        )
        (tmp_path / "agent-task.json").write_text("claimed task")
        os.write(claim_write, b"C")
        stdout, stderr = child.communicate(timeout=5)
        assert child.returncode == 0, stderr
        assert "TASK_READ_AFTER_CLAIM" in stdout
    finally:
        if child.poll() is None:
            child.kill()
            child.wait()
        os.close(ready_read)
        os.close(claim_write)
