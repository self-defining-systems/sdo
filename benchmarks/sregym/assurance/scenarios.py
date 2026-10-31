"""Named scripted scenarios: incident sequences with their scripts, chaos and expected outcomes."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Literal

from benchmarks.sregym.assurance.scripts import assurance_script

if TYPE_CHECKING:
    from benchmarks.sregym.agent_script.model import AgentScript

NP = "network_policy_block"
MCM_GEO = "missing_configmap_hotel_reservation"
MCM_RATE = "missing_configmap_mongodb_rate_hotel_reservation"

NP_DETECTOR = "deny-all-networkpolicy-isolation"
MCM_DETECTOR = "required-configmap-missing"
CLAIMED_DETECTOR = "wedged-workload-restart"

#: What the harness expects SDO to do with one incident.
Resolution = Literal["mitigated", "not-mitigated", "loud-failure"]


@dataclass(frozen=True)
class Expectation:
    resolution: Resolution = "mitigated"
    #: True: the warm path must fire; False: it must not; None: either.
    warm: bool | None = None
    #: Incident detectors that must be registered in memory after the incident.
    learned: tuple[str, ...] = ()
    #: Incident detectors that must NOT be registered after the incident.
    not_learned: tuple[str, ...] = ()
    #: The broker skipped the reflection model turn deterministically.
    reflection_skipped: bool | None = None
    #: Minimum reflection attempts recorded by the broker.
    min_reflection_attempts: int | None = None
    #: The outcome classification SDO must record, when it matters (for example ``cancelled``).
    classification: str | None = None


@dataclass(frozen=True)
class IncidentSpec:
    problem_id: str
    scenario: str
    fault: str
    target: str
    script: AgentScript
    warm_script: AgentScript
    usage_seed: int = 0
    expect: Expectation = Expectation()
    #: Chaos action to run during the incident (see ``benchmarks.sregym.assurance.chaos``).
    chaos: str | None = None

    def __post_init__(self) -> None:
        if not self.problem_id:
            raise ValueError("problem_id must not be empty")


_TARGETS = {NP: ("network_policy_block", "recommendation"), MCM_GEO: ("missing_configmap", "mongodb-geo")}
_TARGETS[MCM_RATE] = ("missing_configmap", "mongodb-rate")


def incident(
    problem_id: str,
    scenario: str,
    seed: int,
    *,
    expect: Expectation | None = None,
    chaos: str | None = None,
    **options: object,
) -> IncidentSpec:
    fault, target = _TARGETS[problem_id]
    return IncidentSpec(
        problem_id=problem_id,
        scenario=scenario,
        fault=fault,
        target=target,
        usage_seed=seed,
        script=assurance_script(fault=fault, target=target, **options),  # type: ignore[arg-type]
        warm_script=assurance_script(fault=fault, target=target, warm=True, **options),  # type: ignore[arg-type]
        expect=expect or Expectation(),
        chaos=chaos,
    )


def _learned(detector: str) -> Expectation:
    return Expectation(warm=False, learned=(detector,), reflection_skipped=False)


def _warm(detector: str) -> Expectation:
    return Expectation(warm=True, learned=(detector,), reflection_skipped=True)


SCENARIOS: dict[str, tuple[IncidentSpec, ...]] = {
    # First encounter learns a detector and playbook; the exact repeat takes the warm path.
    "first-repeat": (
        incident(NP, "first-repeat/first", 1, expect=_learned(NP_DETECTOR)),
        incident(NP, "first-repeat/repeat", 2, expect=_warm(NP_DETECTOR)),
    ),
    "wrong-then-correct": (
        incident(MCM_GEO, "wrong-then-correct", 3, mitigation="wrong_then_correct", expect=_learned(MCM_DETECTOR)),
    ),
    # A wrong repair that the responder admits, then one it believes fixed the incident.
    "wrong-only": (
        incident(
            NP,
            "wrong-only/honest",
            4,
            mitigation="wrong_only_honest",
            expect=Expectation(resolution="not-mitigated", not_learned=(CLAIMED_DETECTOR, NP_DETECTOR)),
        ),
        incident(
            NP,
            "wrong-only/claimed",
            5,
            mitigation="wrong_only_claimed",
            expect=Expectation(resolution="not-mitigated", not_learned=(CLAIMED_DETECTOR, NP_DETECTOR)),
        ),
    ),
    # The fault heals by itself while the responder waits; the responder claims ``completed``
    # with no repair action. SDO must record it as cleared without action, learn nothing, and dispatch the next one.
    "healed-stray": (
        incident(
            NP,
            "healed-stray/noop",
            31,
            mitigation="healed_noop",
            chaos="heal-fault",
            expect=Expectation(
                resolution="not-mitigated", classification="cleared_without_action", not_learned=(NP_DETECTOR,)
            ),
        ),
        incident(MCM_GEO, "healed-stray/next", 32, expect=_learned(MCM_DETECTOR)),
    ),
    "sequence": (
        incident(NP, "sequence/np", 11, expect=_learned(NP_DETECTOR)),
        incident(MCM_GEO, "sequence/geo", 12, expect=_learned(MCM_DETECTOR)),
        incident(NP, "sequence/np-repeat", 13, expect=_warm(NP_DETECTOR)),
        incident(MCM_RATE, "sequence/rate-variant", 14, expect=Expectation(learned=(MCM_DETECTOR,))),
        incident(MCM_GEO, "sequence/geo-repeat", 15, expect=_warm(MCM_DETECTOR)),
    ),
}


_RECOVERS_NP = Expectation(learned=(NP_DETECTOR,))
_RECOVERS_MCM = Expectation(learned=(MCM_DETECTOR,))

#: One incident per chaos action; each must end in durable recovery or a loud, actionable failure.
CHAOS_SCENARIOS: dict[str, tuple[IncidentSpec, ...]] = {
    "chaos-kill-controller": (
        incident(
            NP,
            "chaos/kill-controller",
            21,
            chaos="kill-controller",
            pause_before_repair_seconds=45,
            expect=_RECOVERS_NP,
        ),
    ),
    "chaos-kill-responder": (
        incident(
            MCM_GEO,
            "chaos/kill-responder",
            22,
            chaos="kill-responder",
            pause_before_repair_seconds=60,
            # Exactly-once dispatch forbids a second responder: the incident must fail loudly,
            # be recorded as failed once health recovers, and teach nothing.
            expect=Expectation(resolution="not-mitigated", not_learned=(MCM_DETECTOR,)),
        ),
    ),
    "chaos-kill-prober": (
        incident(NP, "chaos/kill-prober", 23, chaos="kill-prober", pause_before_repair_seconds=20, expect=_RECOVERS_NP),
    ),
    "chaos-broker-reject": (
        incident(
            MCM_GEO,
            "chaos/broker-reject",
            24,
            reflection="invalid_then_learn",
            expect=Expectation(learned=(MCM_DETECTOR,), min_reflection_attempts=2),
        ),
    ),
    "chaos-reflection-crash": (
        incident(NP, "chaos/reflection-crash-once", 25, reflection="crash_once", expect=_RECOVERS_NP),
    ),
    "chaos-reflection-crash-always": (
        incident(
            MCM_GEO,
            "chaos/reflection-crash-always",
            26,
            reflection="crash_always",
            # The broker bounds failing reflection turns, records no_change and completes the
            # closure, so the controller moves on without learning.
            expect=Expectation(not_learned=(MCM_DETECTOR,), min_reflection_attempts=2),
        ),
    ),
    "chaos-pause-apiserver": (
        incident(
            NP,
            "chaos/pause-apiserver",
            27,
            chaos="pause-apiserver",
            pause_before_repair_seconds=45,
            expect=_RECOVERS_NP,
        ),
    ),
    "chaos-concurrent-commit": (
        incident(
            MCM_GEO,
            "chaos/concurrent-commit",
            28,
            chaos="concurrent-commit",
            pause_before_repair_seconds=30,
            expect=_RECOVERS_MCM,
        ),
    ),
}
SCENARIOS.update(CHAOS_SCENARIOS)


def soak(count: int) -> tuple[IncidentSpec, ...]:
    """``count`` mixed incidents on one controller: every fault class, first encounters, repeats and variants."""

    cycle = [
        (NP, {}),
        (MCM_GEO, {}),
        (NP, {}),
        (MCM_RATE, {}),
        (MCM_GEO, {"mitigation": "wrong_then_correct"}),
        (NP, {"reflection": "no_change"}),
        (MCM_RATE, {}),
    ]
    specs = []
    for index in range(count):
        problem, extra = cycle[index % len(cycle)]
        specs.append(incident(problem, f"soak/{index:02d}", 100 + index, **extra))
    return tuple(specs)


SCENARIOS["soak"] = soak(21)


def with_expectation(spec: IncidentSpec, **changes: object) -> IncidentSpec:
    return replace(spec, expect=replace(spec.expect, **changes))  # type: ignore[arg-type]
