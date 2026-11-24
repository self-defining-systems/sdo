"""A transient failure of one observability read must not fail the cell's evidence; an empty or late capture must.

The collect step's Prometheus capture is many reads through the API server proxy; one stream error (the Prometheus pod
restarting, an apiserver blip) failed the whole capture, the evidence gate held the lane for the rest of the run, and a
person had to waive it. Captures are idempotent, so each is retried a few times first. A Jaeger with no service wrote a
21-byte gzip that was recorded ok.
"""

from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.fuzzops import capture, cellevidence
from tests.unit.benchmarks.fuzzops.fake_stores import FakeJaeger, FakePrometheus

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

START, END = "2026-01-01T00:00:00Z", "2026-01-01T00:10:00Z"
OK: dict[str, Any] = {"status": "ok", "files": [], "bytes": 0}


@pytest.fixture(autouse=True)
def stores(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(capture, "service_exists", lambda *a, **k: True)
    for name in ("capture_prometheus", "capture_jaeger", "capture_logs"):
        monkeypatch.setattr(capture, name, lambda *a, **k: OK)


def run(dest: Path, **kw: Any) -> dict[str, Any]:
    return capture.capture_all(lambda *a: ["true"], "ctl", "app", dest, START, END, **kw)


def test_a_capture_that_fails_transiently_is_retried(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"prometheus": 0}

    def flaky(*_: Any) -> dict[str, Any]:
        calls["prometheus"] += 1
        if calls["prometheus"] < 3:
            raise RuntimeError("Error from server (ServiceUnavailable): error trying to reach service")
        return OK

    monkeypatch.setattr(capture, "capture_prometheus", flaky)
    receipt = run(tmp_path)
    assert receipt["prometheus"]["status"] == "ok", receipt["prometheus"]
    assert calls["prometheus"] == 3


@pytest.mark.parametrize(("error", "attempts"), [(RuntimeError, capture.ATTEMPTS), (capture.CaptureError, 1)])
def test_a_capture_that_keeps_failing_still_reports_the_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, error: type[Exception], attempts: int
) -> None:
    calls = []

    def broken(*_: Any) -> dict[str, Any]:
        calls.append(1)
        raise error("connection refused")

    monkeypatch.setattr(capture, "capture_prometheus", broken)
    receipt = run(tmp_path)
    assert receipt["prometheus"] == {"status": "error", "detail": "connection refused"}
    assert len(calls) == attempts
    assert json.loads((tmp_path / cellevidence.OBSERVABILITY).read_text()) == receipt


def test_a_killed_capture_leaves_a_receipt_that_says_what_was_not_reached(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def killed(*_: Any) -> dict[str, Any]:
        raise KeyboardInterrupt

    monkeypatch.setattr(capture, "capture_jaeger", killed)
    with pytest.raises(KeyboardInterrupt):
        run(tmp_path)
    receipt = json.loads((tmp_path / cellevidence.OBSERVABILITY).read_text())
    assert [receipt[k]["status"] for k in cellevidence.CAPTURES] == ["ok", "error", "error"]


class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def test_every_wait_is_cut_to_what_is_left_and_none_starts_after() -> None:
    clock = Clock()
    deadline = capture.Deadline(100, clock)
    assert deadline.within(60) == 60
    clock.now = 70
    assert deadline.within(60) == 30
    assert deadline.share(0.5).left() == 15
    clock.now = 100
    with pytest.raises(capture.CaptureError, match="not done within its 100 s"):
        deadline.within(1)


def test_a_proxy_read_waits_no_longer_than_its_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    clock, waits = Clock(), []

    def slow(cmd: list[str], timeout: float, **_: Any) -> subprocess.CompletedProcess[bytes]:
        waits.append(timeout)
        clock.now += timeout
        return subprocess.CompletedProcess(cmd, 0, b"{}", b"")

    monkeypatch.setattr(capture.subprocess, "run", slow)
    proxy = capture.Proxy(lambda *a: list(a), "ctl", capture.PROMETHEUS, capture.Deadline(90, clock))
    proxy.get("/x")
    proxy.get("/x")
    assert waits == [60, 30]
    with pytest.raises(capture.CaptureError):
        proxy.get("/x")


# ---- empty ---------------------------------------------------------------------------------------------------------

W = capture.window(START, END)
BEFORE, AFTER = datetime(2025, 12, 31, tzinfo=UTC), datetime(2026, 1, 1, 0, 5, tzinfo=UTC)
jaeger: Callable[..., dict[str, Any]] = capture.capture_jaeger
prometheus: Callable[..., dict[str, Any]] = capture.capture_prometheus
logs: Callable[..., dict[str, Any]] = capture.capture_logs


@pytest.mark.parametrize("traces", [{}, {"web": 0}])
def test_a_jaeger_that_holds_nothing_is_empty_and_writes_nothing(tmp_path: Path, traces: dict[str, int]) -> None:
    receipt = jaeger(FakeJaeger(traces), tmp_path, W, lambda: BEFORE)
    assert receipt["status"] == "empty"
    assert not list(tmp_path.rglob("*.gz"))


def test_a_jaeger_with_traces_is_ok(tmp_path: Path) -> None:
    receipt = jaeger(FakeJaeger({"web": 2, "idle": 0}), tmp_path, W, lambda: BEFORE)
    assert (receipt["status"], receipt["traces"]) == ("ok", 2)
    assert [line["service"] for line in capture.read_lines(tmp_path / receipt["files"][0])] == ["web"]


@pytest.mark.parametrize("since", [AFTER, None])
@pytest.mark.parametrize("traces", [{}, {"web": 2}])
def test_a_jaeger_that_restarted_lost_traces(tmp_path: Path, traces: dict[str, int], since: datetime | None) -> None:
    with pytest.raises(capture.CaptureError, match="is lost"):
        jaeger(FakeJaeger(traces), tmp_path, W, lambda: since)


def test_a_prometheus_that_holds_nothing_is_empty(tmp_path: Path) -> None:
    assert prometheus(FakePrometheus({}), tmp_path, W)["status"] == "empty"


def pods(answers: dict[str, tuple[int, bytes, bytes]]) -> Callable[..., subprocess.CompletedProcess[bytes]]:
    """``kubectl get pods`` and ``kubectl logs`` of the pods of ``answers`` (no previous container)."""

    def run_cmd(cmd: list[str], **_: Any) -> subprocess.CompletedProcess[bytes]:
        if "pods" in cmd:
            items = [{"metadata": {"name": n}} for n in answers]
            return subprocess.CompletedProcess(cmd, 0, json.dumps({"items": items}).encode(), b"")
        if "--previous" in cmd:
            return subprocess.CompletedProcess(cmd, 1, b"", b"previous terminated container not found")
        code, stdout, stderr = answers[cmd[cmd.index("logs") + 1]]
        return subprocess.CompletedProcess(cmd, code, stdout, stderr)

    return run_cmd


def read_logs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, answers: dict[str, Any]) -> dict[str, Any]:
    monkeypatch.setattr(capture.subprocess, "run", pods(answers))
    return logs(lambda *a: list(a), ["app"], tmp_path, "", capture.Deadline(60, Clock()))


def test_a_pod_that_logged_nothing_or_did_not_answer_is_named_not_filed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    answers = {"web": (0, b"hello\n", b""), "quiet": (0, b"", b""), "new": (1, b"", b"waiting to start")}
    receipt = read_logs(tmp_path, monkeypatch, answers)
    assert receipt["status"] == "ok"
    assert receipt["files"] == ["applogs/app/web.log.gz"]
    assert receipt["silent"] == ["app/quiet"]
    assert receipt["unread"] == {"app/new": "waiting to start"}


def test_logs_that_hold_nothing_are_not_ok(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    assert read_logs(tmp_path, monkeypatch, {"quiet": (0, b"", b"")})["status"] == "empty"
    assert read_logs(tmp_path, monkeypatch, {"new": (1, b"", b"Unauthorized")})["status"] == "error"


def test_a_store_that_cannot_be_looked_up_is_not_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.undo()
    answers = iter([(1, b"", b"timeout"), (0, b"", b""), (0, b"service/sdo-jaeger\n", b"")])
    monkeypatch.setattr(capture.subprocess, "run", lambda cmd, **_: subprocess.CompletedProcess(cmd, *next(answers)))
    with pytest.raises(RuntimeError, match="timeout"):
        capture.service_exists(lambda *a: list(a), "ctl", "sdo-jaeger")
    assert capture.service_exists(lambda *a: list(a), "ctl", "sdo-jaeger") is False
    assert capture.service_exists(lambda *a: list(a), "ctl", "sdo-jaeger") is True
