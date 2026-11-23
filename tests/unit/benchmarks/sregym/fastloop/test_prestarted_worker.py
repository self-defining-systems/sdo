"""The row continues on the warm transaction's worker: a worker holds its cluster's host-wide lock for its life."""

from __future__ import annotations

import sys
import textwrap
import threading
from pathlib import Path

import pytest

from benchmarks.sregym.fastloop.worker_client import BackgroundCall, HeldWorker, SregymWorker, SregymWorkerError

REPO = Path(__file__).resolve().parents[5]
WORKER_INFRA = REPO / "third_party" / "sregym" / "sregym" / "worker_infra.py"

# A stand-in worker process that takes the real ``cluster_lock`` (SREGym's own code, loaded by path) the way
# ``sregym.fastloop.worker`` does, then answers JSON-line requests.
STUB = textwrap.dedent(
    """
    import importlib.util, json, sys
    spec = importlib.util.spec_from_file_location("worker_infra", sys.argv[1])
    infra = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(infra)
    with infra.cluster_lock("lane-under-test"):
        for line in sys.stdin:
            request = json.loads(line)
            if request.get("op") == "shutdown":
                break
            print(json.dumps({"id": request["id"], "ok": True, "result": {"op": request["op"]}}), flush=True)
    """
)


def stub_worker(tmp_path: Path) -> SregymWorker:
    script = tmp_path / "stub_worker.py"
    script.write_text(STUB)
    return SregymWorker(
        [sys.executable, str(script), str(WORKER_INFRA)],
        env={"HOME": str(tmp_path)},  # the lock directory is under ~/.cache
        log_path=tmp_path / "worker.log",
    )


def started(worker: SregymWorker) -> SregymWorker:
    worker.start()
    # The first reply proves the child got past its lock, or reports why it exited.
    worker.request("ping", timeout=30)
    return worker


@pytest.mark.skipif(not WORKER_INFRA.is_file(), reason="SREGym harness is not checked out")
def test_a_second_worker_cannot_start_while_the_first_holds_the_cluster_lock(tmp_path: Path) -> None:
    first = started(stub_worker(tmp_path))
    try:
        second = stub_worker(tmp_path)
        second.start()
        with pytest.raises(SregymWorkerError, match="exited"):
            second.request("ping", timeout=30)
        second.close()
    finally:
        first.close()
    # Once the first closed, the lock is free: starting the row's worker after it is possible.
    again = started(stub_worker(tmp_path))
    again.close()


@pytest.mark.skipif(not WORKER_INFRA.is_file(), reason="SREGym harness is not checked out")
def test_the_row_reuses_the_transaction_worker_and_closes_it_at_the_end(tmp_path: Path) -> None:
    transaction_worker = started(stub_worker(tmp_path))
    with HeldWorker(transaction_worker) as row_worker:
        assert row_worker is transaction_worker
        assert row_worker.request("rehearse_oracles", timeout=30) == {"op": "rehearse_oracles"}
    # Closed: its lock is released for whatever runs next on the lane.
    started(stub_worker(tmp_path)).close()


@pytest.mark.skipif(not WORKER_INFRA.is_file(), reason="SREGym harness is not checked out")
def test_requests_from_two_threads_are_served_one_at_a_time(tmp_path: Path) -> None:
    worker = started(stub_worker(tmp_path))
    try:
        results: list[dict[str, str]] = []

        def ask(op: str) -> None:
            results.extend(worker.request(op, timeout=30) for _ in range(20))

        threads = [threading.Thread(target=ask, args=(op,)) for op in ("a", "b")]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        assert sorted(r["op"] for r in results) == ["a"] * 20 + ["b"] * 20
    finally:
        worker.close()


def test_a_background_call_returns_its_value_or_re_raises() -> None:
    assert BackgroundCall(lambda: 7).result() == 7

    def fail() -> None:
        raise ValueError("no")

    with pytest.raises(ValueError, match="no"):
        BackgroundCall(fail).result()
