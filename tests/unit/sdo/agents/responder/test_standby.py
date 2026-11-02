import json
import os
import subprocess
import sys
from pathlib import Path


def test_preloaded_worker_waits_for_daemon_and_receives_bound_environment(tmp_path: Path) -> None:
    ready_read, ready_write = os.pipe()
    claim_read, claim_write = os.pipe()
    output = tmp_path / "seen.json"
    script = """
import json,os,sys
from pathlib import Path
from sdo.agents.responder.standby import await_standby_claim
await_standby_claim()
Path(sys.argv[1]).write_text(json.dumps({
 'task':os.environ['SDO_TASK_PATH'],
 'incident':os.environ['SDO_INCIDENT_ID'],
 'standby_keys':[k for k in os.environ if k.startswith('SDO_STANDBY_')]
}))
"""
    environment = {
        **os.environ,
        "SDO_STANDBY_READY_FD": str(ready_write),
        "SDO_STANDBY_CLAIM_FD": str(claim_read),
        "SDO_STANDBY_INBOX": str(tmp_path),
    }
    process = subprocess.Popen(
        [sys.executable, "-c", script, str(output)], env=environment, pass_fds=(ready_write, claim_read)
    )
    os.close(ready_write)
    os.close(claim_read)
    try:
        assert os.read(ready_read, 1) == b"R"
        assert not output.exists()
        assert process.poll() is None
        (tmp_path / "environment.json").write_text(json.dumps({"SDO_INCIDENT_ID": "bound-incident"}))
        os.write(claim_write, b"C")
        assert process.wait(timeout=5) == 0
        assert json.loads(output.read_text()) == {
            "task": str(tmp_path / "agent-task.json"),
            "incident": "bound-incident",
            "standby_keys": [],
        }
    finally:
        os.close(ready_read)
        os.close(claim_write)
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
