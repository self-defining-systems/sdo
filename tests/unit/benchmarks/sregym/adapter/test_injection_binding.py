"""A row is bound to the incident its injection caused, never to one the admission proof opened."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from benchmarks.sregym.adapter import run_persistent_stage
from benchmarks.sregym.protocol.injection import IncidentOpenBeforeInjectionError
from tests.unit.benchmarks.sregym.adapter.test_persistent import FakeOps, _clock, _inputs


@dataclass
class _ProofClosesAnIncidentOps(FakeOps):
    """The 700-1000 s admission proof saw incident ``proof-incident`` open and close; the injection's opens later."""

    reads_before_incident: int = 3
    reads: int = 0
    armed: dict[str, Any] | None = None

    def inject_after_resume(self, control_namespace, generation, inject):
        self.states[control_namespace]["last_acknowledged_incident_id"] = "proof-incident"
        timings = super().inject_after_resume(control_namespace, generation, inject)
        # The base fake opens the injected incident at once; hold it back like a detector that fires later.
        self.armed = self.states[control_namespace].pop("pending_closure")
        return timings

    def runtime_state(self, control_namespace):
        if self.armed is not None:
            self.reads += 1
            if self.reads >= self.reads_before_incident:
                self.states[control_namespace]["pending_closure"] = self.armed
                self.armed = None
        return super().runtime_state(control_namespace)


def test_the_row_binds_to_the_incident_the_injection_opened(tmp_path: Path) -> None:
    ops = _ProofClosesAnIncidentOps()
    resolution = run_persistent_stage(
        _inputs(tmp_path, "s0"),
        ops=ops,
        run_lifecycle=lambda: True,
        inject=lambda: ops.events.append(("fault", "s0")),
        clock=_clock(ops),
    )
    # Before the fix, `known` was read before the proof and the proof's incident was returned at once.
    assert resolution["incident_id"] == "incident-1"


@dataclass
class _ProofLeavesIncidentOpenOps(FakeOps):
    def inject_after_resume(self, control_namespace, generation, inject):
        self.states[control_namespace].update(incident_open=True, incident_request={"incident_id": "proof-incident"})
        return super().inject_after_resume(control_namespace, generation, inject)


def test_incident_work_open_at_the_injection_point_is_a_failed_proof_not_a_dispatched_responder(
    tmp_path: Path,
) -> None:
    ops = _ProofLeavesIncidentOpenOps()
    injected: list[str] = []
    with pytest.raises(IncidentOpenBeforeInjectionError) as raised:
        run_persistent_stage(
            _inputs(tmp_path, "s0"),
            ops=ops,
            run_lifecycle=lambda: True,
            inject=lambda: injected.append("fault"),
            clock=_clock(ops),
        )
    assert injected == [], "the fault was applied to a lane that was not admitted"
    assert raised.value.infra_error == "incident_open_before_injection"
    assert ops.incidents == 0
