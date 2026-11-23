"""One lane, three arms in sequence (p1, cold, p2): each starts from the state a fresh install has.

A simulated lane holds what an SDO install keeps between rows: the accepted repository on its volume (with the memory
each commit holds), the controller's persisted state, its ConfigMaps, the controller process and its standby, the
observability stores. The simulation encodes the controller's documented rules (state is persisted in
``sdo-controller-state``; a start with no state is a new install whose startup period begins now and whose onboarding is
computed on the repository it finds, unless the install's start-up record (``sdo-install-startup``) says the install
was already established: a restart from memory continues that period; a start with state restores it, startup period
included). The real transaction
(``prepare_lane``), the real reseed order (``reseed_lane``), the real session-ConfigMap predicate and the real
fresh-install comparison (``resident_start_violations``) run on it, so the test fails when any carry-over class the
comparison names survives a row: incident and closure state, session and reflection ConfigMaps, startup period,
onboarding, memory of another arm.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.sregym.fastloop.reseed import ReseedSteps, reseed_lane
from benchmarks.sregym.fastloop.resident_start import (
    INSTALL_CONFIGMAPS,
    ResidentStart,
    ResidentStartMismatch,
    resident_start_violations,
)
from benchmarks.sregym.fastloop.warm_environment import _session_state
from benchmarks.sregym.fastloop.warm_lane import LaneIdentity, prepare_lane

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

BASE = "a" * 40
P1_MEMORY = "b" * 40  # what p1's closing turn left: the base plus a learned playbook
COLD_MEMORY = "c" * 40  # what the cold arm's closing turn left (a different commit, its own playbook)
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
OBJECTS = ["apps/v1/Deployment/app/web", "v1/Service/app/web"]
IMAGES = "controller:1"


def iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def managed(name: str) -> dict[str, Any]:
    return {"metadata": {"name": name, "labels": {"app.kubernetes.io/managed-by": "sdo"}}, "data": {}}


@dataclass
class SimLane:
    """The install: everything below survives a reseed except what the reseed retires."""

    memory: dict[str, set[str]] = field(
        default_factory=lambda: {
            BASE: {"architecture"},
            P1_MEMORY: {"architecture", "playbook/readiness"},
            COLD_MEMORY: {"architecture", "playbook/readiness-cold"},
        }
    )
    clock: datetime = START
    volume_head: str = BASE
    host_head: str = BASE
    configmaps: dict[str, dict[str, Any]] = field(default_factory=dict)
    state: dict[str, Any] | None = None
    pod: int = 0
    controller_running: bool = False
    observability_generation: int = 0
    skip_reseed: bool = False
    app: str = "baseline"
    fallbacks: int = 0
    reference: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        self.configmaps = {
            name: managed(name)
            for name in INSTALL_CONFIGMAPS
            if name not in ("kube-root-ca.crt", "sdo-install-startup")
        }

    def tick(self, seconds: float = 1.0) -> datetime:
        self.clock += timedelta(seconds=seconds)
        return self.clock

    # ---- the controller process ----
    def start_controller(self) -> None:
        self.pod += 1
        self.controller_running = True
        persisted = self.configmaps.get("sdo-controller-state")
        if persisted is not None:
            # A restart restores the install's state: incident work, startup period and onboarding included.
            self.state = persisted["state"]
        else:
            record = self.configmaps.get("sdo-install-startup")
            if record is not None:
                # A restart of an established install from retired state continues the install's period.
                began, established = record["began"], record["established"]
            else:
                began = self.tick(5)
                established = self.tick(150)
                self.configmaps["sdo-install-startup"] = {
                    **managed("sdo-install-startup"),
                    "began": began,
                    "established": established,
                }
            self.state = {
                "setup_evaluated_revision": self.volume_head,
                "dispatch_state": "DISPATCH_STATE_IDLE",
                "history": [{"detector_id": "health", "evaluated_at": iso(self.tick(1)), "status": "clear"}],
                "detector_clear_since": {"health": iso(self.tick(1))},
                "startup": {"began_at": iso(began), "established_at": iso(established)},
                "namespace_onboarding": {"accepted_commit": self.volume_head, "objects": list(OBJECTS)},
            }
            self.configmaps["sdo-controller-state"] = {**managed("sdo-controller-state"), "state": self.state}

    def fresh_install(self, commit: str) -> None:
        """What the row-start wipe and install-existing leave: the reference a fresh install's onboarding records."""
        self.host_head = self.volume_head = commit
        self.configmaps.pop("sdo-controller-state", None)
        self.configmaps.pop("sdo-install-startup", None)
        self.start_controller()
        self.reference = {"accepted_commit": commit, "objects": list(OBJECTS)}

    # ---- a row's work (what an agent turn, a closure and a closing turn leave behind) ----
    def run_row(self, *, learned_memory: str | None) -> None:
        assert self.state is not None
        self.tick(600)
        self.state.update(
            dispatch_state="DISPATCH_STATE_IDLE",
            closure_state="CLOSURE_STATE_ACKNOWLEDGED",
            last_acknowledged_incident_id="app-incident-1",
            incident_dispatched_at=iso(self.clock),
        )
        self.state["history"].append({"detector_id": "health", "evaluated_at": iso(self.clock), "status": "clear"})
        self.configmaps["sdo-app-incident-1-request"] = managed("sdo-app-incident-1-request")
        self.configmaps["sdo-reflection-app-incident-1-result"] = managed("sdo-reflection-app-incident-1-result")
        self.app = "mitigated"
        if learned_memory is not None:
            self.volume_head = learned_memory  # the broker's outcome commit: the memory the closing turn left

    # ---- the warm transaction's lane ----
    def identity(self) -> LaneIdentity:
        return LaneIdentity(f"controller-{self.pod}", f"standby-{self.pod}", self.volume_head)


class Lane:
    """``warm_lane.Lane`` over the simulation, with the transaction's real phases."""

    def __init__(self, sim: SimLane, planned: str) -> None:
        self.sim, self.planned = sim, planned

    def inspect(self) -> LaneIdentity:
        assert self.sim.controller_running
        return self.sim.identity()

    def reidentify(self) -> LaneIdentity:
        assert self.sim.controller_running
        return self.sim.identity()

    def pause(self) -> None:
        self.sim.host_head = self.planned  # the host copy at the row's planned memory, before the app is restored

    def reset_changed(self, restored: Callable[[], None] | None = None) -> None:
        self.sim.app = "baseline"
        if restored is not None:
            restored()

    def reseed(self, application_restored: Callable[[], None] | None = None) -> None:
        if self.sim.skip_reseed:
            return
        sim = self.sim

        def retire() -> None:
            # Go reset-control-state --keep-install-startup: the controller state goes, the install's record stays.
            sim.configmaps.pop("sdo-controller-state", None)
            for name in [n for n, item in sim.configmaps.items() if _session_state(item)]:
                del sim.configmaps[name]

        def stop(_head: str) -> None:
            sim.controller_running = False

        def install() -> None:
            # The new controller onboards and judges the application it finds: the row's restored one.
            assert sim.app == "baseline", "the controller was installed before the application was restored"
            sim.volume_head = sim.host_head  # Go install-existing seeds the volume from the host copy
            sim.start_controller()

        noop = lambda *args: None  # noqa: E731
        reseed_lane(
            ReseedSteps(
                settle_evidence=noop,
                read_head=lambda: sim.volume_head,
                stop_controller=stop,
                retire_control_state=retire,
                fresh_observability=lambda: setattr(sim, "observability_generation", sim.observability_generation + 1),
                await_application=application_restored or noop,
                install_controller=install,
                wait_started=noop,
                record_controller=noop,
                await_observability=noop,
            )
        )

    def adopt(self) -> str:
        return self.sim.volume_head

    def prove(self, not_before: datetime, revision: str) -> dict[str, Any]:
        sim = self.sim
        assert sim.state is not None
        assert sim.reference is not None
        violations = resident_start_violations(
            ResidentStart(
                state=sim.state,
                planned_commit=self.planned,
                accepted_head=revision,
                boundary=iso(not_before),
                control_configmaps=tuple(sorted(sim.configmaps)),
                config_digest=IMAGES,
                expected_config_digest=IMAGES,
                onboarding_reference=sim.reference,
            )
        )
        if violations:
            raise ResidentStartMismatch(violations)
        return {"passed": True}

    def full_reset(self) -> None:
        self.sim.fallbacks += 1


def start_row(sim: SimLane, planned: str, tmp_path: Path) -> Any:
    boundary = sim.tick(1)
    return prepare_lane(Lane(sim, planned), tmp_path / f"warm-{planned[:4]}.json", now=lambda: boundary)


def test_p1_then_cold_then_p2_on_one_lane_each_start_from_a_fresh_installs_state(tmp_path):
    sim = SimLane()
    sim.fresh_install(BASE)  # the lane's first row installs fresh and records the onboarding reference
    first_pod = sim.pod
    sim.run_row(learned_memory=P1_MEMORY)  # p1: incident, closing turn, outcome commit

    before_cold = sim.clock
    cold = start_row(sim, BASE, tmp_path)  # cold: the base memory, not what p1 learned
    assert cold.reused, cold.fallback_reason
    assert sim.clock - before_cold < timedelta(seconds=150)  # a restart from memory starts no new start-up period
    assert sim.volume_head == BASE
    assert "playbook/readiness" not in sim.memory[sim.volume_head]
    assert sim.state is not None
    assert "last_acknowledged_incident_id" not in sim.state
    assert not any("incident" in name or "reflection" in name for name in sim.configmaps)
    assert sim.observability_generation == 1
    assert sim.pod > first_pod  # the controller process was replaced; the install around it was not
    sim.run_row(learned_memory=COLD_MEMORY)

    p2 = start_row(sim, P1_MEMORY, tmp_path)  # p2: exactly the memory p1 left, wherever it ran
    assert p2.reused, p2.fallback_reason
    assert sim.volume_head == P1_MEMORY
    assert "playbook/readiness" in sim.memory[sim.volume_head]  # the learned playbook is there to be applied
    assert "playbook/readiness-cold" not in sim.memory[sim.volume_head]  # and nothing the cold arm learned
    assert sim.state is not None
    assert sim.state["setup_evaluated_revision"] == P1_MEMORY
    assert not any("incident" in name or "reflection" in name for name in sim.configmaps)
    assert sim.observability_generation == 2
    assert sim.fallbacks == 0


def test_a_start_that_keeps_the_earlier_rows_state_is_refused_for_each_carry_over_class(tmp_path):
    """The comparison has teeth: with no reseed the lane keeps p1's closure, sessions, startup period and memory."""
    sim = SimLane()
    sim.fresh_install(BASE)
    sim.run_row(learned_memory=P1_MEMORY)
    sim.skip_reseed = True
    receipt = start_row(sim, BASE, tmp_path)
    assert not receipt.reused
    assert sim.fallbacks == 1
    assert receipt.fallback_reason == "prove:ResidentStartMismatch"
    named = " | ".join(receipt.violations)
    for carried in (
        "planned memory",  # the volume still holds p1's memory
        "closure_state carries over",
        "last_acknowledged_incident_id carries over",
        "session or reflection state remains",
    ):
        assert carried in named, named


@pytest.mark.parametrize("order", [("cold", "p2", "p1"), ("p2", "p1", "cold")])
def test_the_arms_may_come_in_any_order_each_with_its_own_memory(tmp_path, order):
    planned = {"p1": BASE, "cold": BASE, "p2": P1_MEMORY}
    sim = SimLane()
    sim.fresh_install(BASE)
    sim.run_row(learned_memory=COLD_MEMORY)
    for arm in order:
        receipt = start_row(sim, planned[arm], tmp_path)
        assert receipt.reused, (arm, receipt.fallback_reason, receipt.violations)
        assert sim.volume_head == planned[arm]
        sim.run_row(learned_memory={"p1": P1_MEMORY, "cold": COLD_MEMORY, "p2": None}[arm])
    assert sim.fallbacks == 0
