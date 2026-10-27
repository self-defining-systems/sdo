"""Complete baseline captures retry without retaining partial probe state."""

import pytest

from benchmarks.sregym.fastloop.runtime_observation import ObservationUnavailable, baseline_observation


def test_baseline_restarts_complete_capture_then_proceeds():
    attempts = []
    clock = [0.0]

    def capture():
        attempts.append(len(attempts))
        if len(attempts) < 3:
            raise ObservationUnavailable("temporary")
        return "ready"

    assert (
        baseline_observation(capture, monotonic=lambda: clock[0], sleep=lambda t: clock.__setitem__(0, clock[0] + t))
        == "ready"
    )
    assert len(attempts) == 3


def test_baseline_exhaustion_is_bounded():
    clock = [0.0]

    def capture():
        raise ObservationUnavailable("temporary")

    with pytest.raises(ObservationUnavailable, match="baseline observation unavailable"):
        baseline_observation(capture, monotonic=lambda: clock[0], sleep=lambda t: clock.__setitem__(0, clock[0] + t))
    assert clock[0] == 90
