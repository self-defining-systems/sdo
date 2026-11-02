"""Recreation is the producer of clean lane history, including after a crash."""

from pathlib import Path

import pytest

from benchmarks.sregym.fastloop.wipe import LaneInjectionState, Tier1Receipt, Tier2Trigger, run_tier2
from tests.unit.benchmarks.sregym.fastloop.test_wipe import faulted_lane


@pytest.mark.parametrize("crash", [False, True])
def test_recreate_establishes_clean_history_only_after_healthy_deployment(tmp_path: Path, crash: bool):
    state = LaneInjectionState(tmp_path / "kubeconfig", "hotel")
    # A missing journal is unknown. The real recreation producer must establish
    # the fact consumed by the next admission; no handwritten clean document.
    reason = state.reason()
    assert reason is not None

    def deploy():
        assert state.reason().trigger is Tier2Trigger.LANE_INJECTION_UNKNOWN
        if crash:
            raise RuntimeError("deploy interrupted")

    def reset():
        return run_tier2(
            reason,
            Tier1Receipt(0, (), False, ()),
            cluster=faulted_lane(),
            namespaces=("hotel-reservation",),
            deploy=deploy,
            injection_state=state,
            timeout_seconds=30,
            poll_seconds=0,
            sleep=lambda _: None,
        )

    if crash:
        with pytest.raises(RuntimeError, match="interrupted"):
            reset()
        assert state.reason().trigger is Tier2Trigger.LANE_INJECTION_UNKNOWN
    else:
        reset()
        assert state.reason() is None
