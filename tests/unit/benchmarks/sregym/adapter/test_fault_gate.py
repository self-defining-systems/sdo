from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pytest

from benchmarks.sregym.adapter.fault_gate import (
    FaultGateError,
    controller_active_findings_after_resume,
    controller_baseline_clear,
    inject_fault_after_controller_baseline,
    inject_fault_after_resumed_baseline,
)


def _evaluation(*statuses: str) -> str:
    return json.dumps(
        {
            "controller_iteration": 0,
            "returncode": 0,
            "findings": [{"rule_id": f"r{index}", "status": status} for index, status in enumerate(statuses)],
        }
    )


def test_baseline_requires_a_controller_evaluation_without_active_findings() -> None:
    assert not controller_baseline_clear("")
    assert not controller_baseline_clear("building detectors\n")
    assert not controller_baseline_clear(_evaluation("active", "resolved"))
    assert controller_baseline_clear("noise\n" + _evaluation("resolved"))
    assert not controller_baseline_clear(_evaluation("resolved") + "\n" + _evaluation("active"))
    assert controller_baseline_clear(json.dumps({"controller_iteration": 3, "returncode": 0, "findings": None}))


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


NOT_BEFORE = datetime(2026, 9, 27, 12, 0, 0, tzinfo=timezone.utc)


def _logs(*outputs: tuple[int, str], created: str = "2026-09-27T12:00:05Z"):
    remaining = list(outputs)
    calls: list[list[str]] = []

    def kubectl(args: list[str], *, namespace: str, check: bool) -> subprocess.CompletedProcess[str]:
        assert namespace == "hotel"
        assert check is False
        calls.append(args)
        if args[0] == "get":
            job = {"metadata": {"creationTimestamp": created}}
            return subprocess.CompletedProcess(args, 0, stdout=json.dumps(job), stderr="")
        returncode, stdout = remaining.pop(0) if len(remaining) > 1 else remaining[0]
        return subprocess.CompletedProcess(args, returncode, stdout=stdout, stderr="not yet")

    return kubectl, calls


def test_gate_injects_once_after_controller_reports_all_clear() -> None:
    clock = _Clock()
    kubectl, calls = _logs((1, ""), (0, _evaluation("active")), (0, _evaluation("resolved")))
    injected: list[str] = []

    timings = inject_fault_after_controller_baseline(
        "hotel",
        inject=lambda: injected.append("fault"),
        kubectl_runner=kubectl,
        not_before=NOT_BEFORE,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        timeout_seconds=60,
    )

    assert injected == ["fault"]
    assert ["logs", "job/sdo-controller-run", "--tail=200"] in calls
    assert timings["controller_baseline_wait"] == pytest.approx(2.0)


def test_gate_fails_without_injecting_when_controller_never_settles() -> None:
    clock = _Clock()
    kubectl, _ = _logs((0, _evaluation("active")))
    injected: list[str] = []

    with pytest.raises(FaultGateError, match=r"all-clear.*active findings \['r0'\]"):
        inject_fault_after_controller_baseline(
            "hotel",
            inject=lambda: injected.append("fault"),
            kubectl_runner=kubectl,
            not_before=NOT_BEFORE,
            monotonic=clock.monotonic,
            sleep=clock.sleep,
            timeout_seconds=10,
        )

    assert injected == []


def test_gate_ignores_a_previous_rounds_controller_job() -> None:
    clock = _Clock()
    kubectl, calls = _logs((0, _evaluation("resolved")), created="2026-09-27T11:59:00Z")
    injected: list[str] = []

    with pytest.raises(FaultGateError):
        inject_fault_after_controller_baseline(
            "hotel",
            inject=lambda: injected.append("fault"),
            kubectl_runner=kubectl,
            not_before=NOT_BEFORE,
            monotonic=clock.monotonic,
            sleep=clock.sleep,
            timeout_seconds=5,
        )

    assert injected == []
    assert all(call[0] == "get" for call in calls)


def test_sdo_registry_entry_defers_fault_injection() -> None:
    import yaml

    registry = Path(__file__).resolve().parents[5] / "benchmarks" / "sregym" / "registry.yaml"
    agents = {agent["name"]: agent for agent in yaml.safe_load(registry.read_text(encoding="utf-8"))["agents"]}

    assert agents["sdo_codex"]["defer_fault_injection"] is True
    assert "defer_fault_injection" not in agents["codex"]


def _maintenance(mode: str, generation: str) -> str:
    return json.dumps({"controller_maintenance": mode, "maintenance_generation": generation})


def test_resumed_baseline_counts_only_evaluations_after_this_stages_resume() -> None:
    before = _evaluation("resolved")
    assert controller_active_findings_after_resume(before, "stage-1") is None
    assert controller_active_findings_after_resume(before + "\n" + _maintenance("active", "stage-0"), "stage-1") is None
    resumed = "\n".join([before, _maintenance("paused", "stage-0-end"), _maintenance("active", "stage-1")])
    assert controller_active_findings_after_resume(resumed, "stage-1") is None
    assert controller_active_findings_after_resume(resumed + "\n" + _evaluation("active"), "stage-1") == ["r0"]
    assert controller_active_findings_after_resume(resumed + "\n" + _evaluation("resolved"), "stage-1") == []
    repaused = resumed + "\n" + _evaluation("resolved") + "\n" + _maintenance("paused", "stage-1-end")
    assert controller_active_findings_after_resume(repaused, "stage-1") is None


def test_persistent_gate_waits_for_the_resumed_controllers_all_clear_before_injecting() -> None:
    clock = _Clock()
    outputs = [
        _evaluation("resolved"),  # previous stage's evaluation: must not count
        _evaluation("resolved") + "\n" + _maintenance("active", "stage-1"),
        _evaluation("resolved") + "\n" + _maintenance("active", "stage-1") + "\n" + _evaluation("resolved"),
    ]
    calls: list[tuple[list[str], str]] = []

    def kubectl(args: list[str], *, namespace: str, check: bool) -> subprocess.CompletedProcess[str]:
        calls.append((args, namespace))
        stdout = outputs.pop(0) if len(outputs) > 1 else outputs[0]
        return subprocess.CompletedProcess(args, 0, stdout=stdout, stderr="")

    injected: list[str] = []
    timings = inject_fault_after_resumed_baseline(
        "hotel-sdo",
        "stage-1",
        inject=lambda: injected.append("fault"),
        kubectl_runner=kubectl,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        timeout_seconds=60,
    )

    assert injected == ["fault"]
    assert timings["controller_baseline_wait"] == pytest.approx(2.0)
    assert all(namespace == "hotel-sdo" for _, namespace in calls)
    assert calls[0][0][:2] == ["logs", "job/sdo-controller-run"]


def test_persistent_gate_poll_can_follow_fastloop_time_scale() -> None:
    clock = _Clock()
    outputs = [_maintenance("active", "stage-1"), _maintenance("active", "stage-1") + "\n" + _evaluation()]

    def kubectl(args: list[str], *, namespace: str, check: bool) -> subprocess.CompletedProcess[str]:
        stdout = outputs.pop(0) if len(outputs) > 1 else outputs[0]
        return subprocess.CompletedProcess(args, 0, stdout=stdout, stderr="")

    timings = inject_fault_after_resumed_baseline(
        "hotel-sdo",
        "stage-1",
        inject=lambda: None,
        kubectl_runner=kubectl,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        poll_seconds=0.1,
    )

    assert timings["controller_baseline_wait"] == pytest.approx(0.1)


@pytest.mark.parametrize("logs", ["", _maintenance("active", "previous-stage") + "\n" + _evaluation()])
def test_persistent_gate_has_a_bounded_startup_wait(logs: str, caplog: pytest.LogCaptureFixture) -> None:
    clock = _Clock()

    def kubectl(args: list[str], *, namespace: str, check: bool) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(args, 0, stdout=logs, stderr="")

    with caplog.at_level("INFO"), pytest.raises(FaultGateError, match="hotel-sdo.*stage-1.*startup resume.*1800s"):
        inject_fault_after_resumed_baseline(
            "hotel-sdo",
            "stage-1",
            inject=lambda: pytest.fail("fault injected before readiness"),
            kubectl_runner=kubectl,
            monotonic=clock.monotonic,
            sleep=clock.sleep,
        )

    assert clock.now == 1800
    assert "waiting for controller baseline" in caplog.text


def test_persistent_gate_reports_forbidden_observation_as_readiness_failure() -> None:
    clock = _Clock()
    denied = (
        'services is forbidden: User "system:serviceaccount:hotel-sdo:sdo-controller" '
        'cannot list resource "services" in the namespace "hotel"'
    )

    def kubectl(args: list[str], *, namespace: str, check: bool) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(args, 0, stdout=denied, stderr="")

    with pytest.raises(FaultGateError, match="namespace-scoped.*RoleBinding.*services is forbidden"):
        inject_fault_after_resumed_baseline(
            "hotel-sdo",
            "stage-1",
            inject=lambda: pytest.fail("fault injected without namespace access"),
            kubectl_runner=kubectl,
            monotonic=clock.monotonic,
            sleep=clock.sleep,
            timeout_seconds=3,
            startup_timeout_seconds=3,
        )

    assert clock.now == 3


def test_resumed_gate_readiness_budget_starts_after_slow_startup() -> None:
    clock = _Clock()
    injected = []

    def kubectl(args: list[str], *, namespace: str, check: bool) -> subprocess.CompletedProcess[str]:
        if clock.now < 400:
            logs = "health refresh still running"
        else:
            logs = _maintenance("active", "stage-1")
            logs += "\n" + (_evaluation("active") if clock.now < 699 else _evaluation())
        return subprocess.CompletedProcess(args, 0, stdout=logs, stderr="")

    timings = inject_fault_after_resumed_baseline(
        "hotel-sdo",
        "stage-1",
        inject=lambda: injected.append(clock.now),
        kubectl_runner=kubectl,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    assert injected == [699]
    assert timings["controller_startup_wait"] == 400
    assert timings["controller_resumed_baseline_wait"] == 299
    assert timings["controller_baseline_wait"] == 699


def test_resumed_gate_still_expires_after_full_readiness_budget() -> None:
    clock = _Clock()

    def kubectl(args: list[str], *, namespace: str, check: bool) -> subprocess.CompletedProcess[str]:
        logs = (
            "starting health refresh"
            if clock.now < 10
            else _maintenance("active", "stage-1") + "\n" + _evaluation("active")
        )
        return subprocess.CompletedProcess(args, 0, stdout=logs, stderr="")

    with pytest.raises(FaultGateError, match="300s.*active findings"):
        inject_fault_after_resumed_baseline(
            "hotel-sdo",
            "stage-1",
            inject=lambda: pytest.fail("unhealthy baseline injected"),
            kubectl_runner=kubectl,
            monotonic=clock.monotonic,
            sleep=clock.sleep,
        )
    assert clock.now == 310
