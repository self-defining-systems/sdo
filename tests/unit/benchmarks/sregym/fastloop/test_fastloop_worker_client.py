from __future__ import annotations

import sys
import textwrap
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.fastloop.worker_client import SregymWorker, SregymWorkerError, worker_argv

if TYPE_CHECKING:
    from pathlib import Path

ECHO_WORKER = textwrap.dedent(
    """
    import json, sys, time
    for line in sys.stdin:
        request = json.loads(line)
        print("harness noise on stderr", file=sys.stderr)
        if request["op"] == "fail":
            reply = {"id": request["id"], "ok": False, "error": "RuntimeError: boom"}
        elif request["op"] == "hang":
            time.sleep(30)
            continue
        elif request["op"] == "die":
            sys.exit(3)
        else:
            reply = {"id": request["id"], "ok": True, "result": {"echo": request}}
        sys.stdout.write(json.dumps(reply) + "\\n")
        sys.stdout.flush()
    """
)


def _worker(tmp_path: Path) -> SregymWorker:
    script = tmp_path / "echo_worker.py"
    script.write_text(ECHO_WORKER, encoding="utf-8")
    return SregymWorker([sys.executable, str(script)], env={}, log_path=tmp_path / "worker.log")


def test_requests_are_correlated_and_harness_output_goes_to_the_log(tmp_path: Path) -> None:
    with _worker(tmp_path) as worker:
        first = worker.request("inject", problem_id="p")
        second = worker.request("oracle")

    assert first["echo"] == {"id": 1, "op": "inject", "problem_id": "p"}
    assert second["echo"]["id"] == 2
    assert "harness noise on stderr" in (tmp_path / "worker.log").read_text(encoding="utf-8")


def test_worker_errors_and_timeouts_and_crashes_raise(tmp_path: Path) -> None:
    with _worker(tmp_path) as worker:
        with pytest.raises(SregymWorkerError, match="boom"):
            worker.request("fail")
        with pytest.raises(SregymWorkerError, match="timed out"):
            worker.request("hang", timeout=0.5)
    with _worker(tmp_path) as worker, pytest.raises(SregymWorkerError, match="exited"):
        worker.request("die", timeout=10)


def test_sandboxed_worker_gets_a_private_tmp_so_fixed_fault_backup_paths_cannot_collide(tmp_path: Path) -> None:
    argv = worker_argv(tmp_path / "sregym", private_tmp=tmp_path / "tmp")

    assert argv[:6] == ["bwrap", "--dev-bind", "/", "/", "--bind", str(tmp_path / "tmp")]
    assert argv[6] == "/tmp"
    assert argv[-3:] == ["python", "-m", "benchmarks.sregym.fastloop.worker_bridge"]
    assert worker_argv(tmp_path / "sregym", private_tmp=None)[0] == "uv"


def test_unrelated_replies_do_not_renew_request_deadline(tmp_path: Path) -> None:
    import time

    script = tmp_path / "unrelated_worker.py"
    script.write_text("""import json, sys, time
for line in sys.stdin:
    request = json.loads(line)
    if request["op"] == "shutdown": break
    for _ in range(20):
        print(json.dumps({"id": -1, "ok": True}), flush=True)
        time.sleep(0.05)
    print(json.dumps({"id": request["id"], "ok": True}), flush=True)
""")
    started = time.monotonic()
    with SregymWorker([sys.executable, str(script)], env={}, log_path=tmp_path / "worker.log") as worker:
        with pytest.raises(SregymWorkerError, match="timed out"):
            worker.request("baseline_oracle", timeout=0.2)
    assert time.monotonic() - started < 0.8
