"""The in-place swap of a resident SDO install to a fresh install's state: its order is the contract."""

from __future__ import annotations

import pytest

from benchmarks.sregym.fastloop.reseed import PHASES, ReseedError, ReseedSteps, reseed_lane

HEAD = "b" * 40


def steps(events: list[str], fail: str = "") -> ReseedSteps:
    def step(name: str, result=None):
        def run(*args):
            events.append(name if not args else f"{name}({args[0]})")
            if name == fail:
                raise RuntimeError("secret-bearing command text")
            return result

        return run

    return ReseedSteps(
        settle_evidence=step("settle_evidence"),
        read_head=step("read_head", HEAD),
        stop_controller=step("stop_controller"),
        retire_control_state=step("retire_control_state"),
        fresh_observability=step("fresh_observability"),
        install_controller=step("install_controller"),
        wait_started=step("wait_started"),
        record_controller=step("record_controller"),
        await_observability=step("await_observability"),
    )


def test_the_swap_runs_in_order_and_times_every_phase():
    events: list[str] = []
    ticks = iter(range(100))
    seconds = reseed_lane(steps(events), monotonic=lambda: float(next(ticks)))
    assert events == [
        "settle_evidence",
        "read_head",
        f"stop_controller({HEAD})",
        "retire_control_state",
        "fresh_observability",
        "install_controller",
        "wait_started",
        "record_controller",
        "await_observability",
    ]
    assert tuple(seconds) == PHASES
    assert all(value == 1.0 for value in seconds.values())


def test_evidence_is_delivered_and_the_controller_stopped_before_any_state_changes():
    events: list[str] = []
    reseed_lane(steps(events))
    first_change = events.index("retire_control_state")
    assert events.index("settle_evidence") < events.index(f"stop_controller({HEAD})") < first_change
    assert first_change < events.index("fresh_observability") < events.index("install_controller")
    assert events.index("install_controller") < events.index("wait_started") < events.index("record_controller")


@pytest.mark.parametrize("phase", PHASES)
def test_a_failed_phase_names_itself_never_the_errors_text_and_stops_the_swap(phase):
    events: list[str] = []
    with pytest.raises(ReseedError) as raised:
        reseed_lane(steps(events, fail=phase))
    assert raised.value.phase == phase
    assert raised.value.reason == f"{phase}:RuntimeError"
    assert "secret-bearing" not in str(raised.value)
    assert events[-1].startswith(phase)
