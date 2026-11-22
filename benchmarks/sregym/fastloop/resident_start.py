"""Validity guard for a resident SDO install: a row may start only from state a fresh install would have at injection.

A resident install skips the per-row reinstall (Tier-1 wipe, controller cold start, onboarding and start-up hold,
about 6 of a row's 7 pre-injection minutes). It is valid only while each row's starting conditions equal a fresh
install's. This module reads the controller's own state (the harness never writes it) and names every item that
differs; any violation sends the warm transaction to its full reset, so an unprovable item keeps the reinstall.

What a fresh install holds at injection, and the rule each item follows here:

- planned memory: the accepted head and the evaluated setup revision equal the row's planned commit (pass-1 base or
  the pass-2 snapshot) exactly;
- incident work: no dispatch, pending batch, finding, drift event, closure, failed closure or acknowledged incident;
- evaluation state: every history row and detector clear time postdates this row's boundary (the resume), so no
  detector judgement carries over;
- start-up period: began and was established after the boundary, as a fresh install's admission waits for it;
- onboarding: equal to the reference a fresh install recorded for the same planned commit; no reference, no reuse;
- sessions and caches: no incident or reflection ConfigMap remains in the control namespace;
- agent model and configuration: the installed configuration digest equals the row's requested one.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

IDLE = "DISPATCH_STATE_IDLE"
#: Incident and closure state a fresh install never holds before injection.
NO_CARRY_OVER = (
    "pending_batch",
    "finding_states",
    "drift_events",
    "failed_closures",
    "pending_closure",
    "closure_state",
    "closure_receipt",
    "last_acknowledged_incident_id",
    "incident_detected_at",
    "incident_dispatched_at",
    "incident_finding_keys",
    "queued_reflection_incident_ids",
)
#: Control-namespace ConfigMaps every install has; any other ``sdo-`` ConfigMap belongs to an incident or reflection.
INSTALL_CONFIGMAPS = frozenset(
    {
        "kube-root-ca.crt",
        "sdo-config",
        "sdo-controller-state",
        "sdo-controller-maintenance",
        "sdo-controller-maintenance-ack",
        "sdo-namespace-onboarding",
        "sdo-namespace-onboarding-attempt",
        "sdo-namespace-onboarding-pending",
        "sdo-namespace-onboarding-window",
        "sdo-prometheus",
        "sdo-telemetry-environment",
        "sdo-operability",
    }
)


class ResidentStartMismatch(ValueError):
    """The resident install differs from a fresh install; ``violations`` names every item (our text, safe to record)."""

    def __init__(self, violations: list[str]) -> None:
        super().__init__("eval: resident install differs from a fresh install: " + "; ".join(violations))
        self.violations = tuple(violations)


@dataclass(frozen=True)
class ResidentStart:
    """What the warm transaction read after reseeding and proving the resident install."""

    state: dict[str, Any]
    planned_commit: str
    accepted_head: str
    boundary: str
    control_configmaps: tuple[str, ...]
    config_digest: str
    expected_config_digest: str
    onboarding_reference: dict[str, Any] | None = field(default=None)

    def __post_init__(self) -> None:
        if not self.planned_commit or not self.boundary or not self.expected_config_digest:
            raise ValueError("eval: a resident start needs the planned commit, boundary and requested configuration")


def _time(value: Any) -> datetime | None:
    """An RFC 3339 time with up to nanoseconds (Go) as an aware datetime; anything else is unknown."""
    if not isinstance(value, str):
        return None
    match = re.fullmatch(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d+))?(Z|[+-]\d\d:\d\d)", value)
    if match is None:
        return None
    stamp, fraction, zone = match.groups()
    text = stamp + (f".{(fraction or '')[:6]:0<6}") + ("+00:00" if zone == "Z" else zone)
    return datetime.fromisoformat(text)


def resident_start_violations(start: ResidentStart) -> list[str]:
    """Every way the resident state differs from a fresh install at injection; empty means the row may start."""
    state, boundary = start.state, _time(start.boundary)
    if boundary is None:
        raise ValueError("eval: resident start boundary is not a timestamp")
    violations = []
    if start.accepted_head != start.planned_commit:
        violations.append(
            f"accepted head {start.accepted_head[:12]} is not the planned memory {start.planned_commit[:12]}"
        )
    if state.get("setup_evaluated_revision") != start.planned_commit:
        violations.append("controller has not evaluated the planned memory")
    if state.get("dispatch_state", IDLE) != IDLE:
        violations.append(f"dispatch state {state.get('dispatch_state')} is not idle")
    violations += [f"{name} carries over from an earlier row" for name in NO_CARRY_OVER if state.get(name)]

    def stale(value: Any) -> bool:
        when = _time(value)
        return when is None or when <= boundary

    if any(stale(row.get("evaluated_at")) for row in state.get("history") or []):
        violations.append("history has evaluations from before this row")
    if any(stale(value) for value in (state.get("detector_clear_since") or {}).values()):
        violations.append("detector_clear_since has clear times from before this row")
    startup = state.get("startup") or {}
    if stale(startup.get("began_at")) or stale(startup.get("established_at")):
        violations.append("startup period was not begun and established in this row")
    if start.onboarding_reference is None or state.get("namespace_onboarding") != start.onboarding_reference:
        violations.append("onboarding differs from a fresh install's on the planned memory (or has no reference)")
    sessions = sorted(name for name in start.control_configmaps if name not in INSTALL_CONFIGMAPS)
    if sessions:
        violations.append(f"session or reflection state remains: {', '.join(sessions)}")
    if start.config_digest != start.expected_config_digest:
        violations.append("installed agent model or configuration differs from the requested one")
    return violations
