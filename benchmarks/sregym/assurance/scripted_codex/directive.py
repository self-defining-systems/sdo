"""What the scripted Codex CLI does for one incident.

The assurance harness writes one :class:`Directive` into the controller
namespace before it injects a fault. The first responder turn of an incident
binds the directive to the incident ID, so a restarted responder Job and the
later reflection turns replay the same plan even after the harness has moved
on to the next incident.

This module is stdlib-only: it runs inside the controller and responder images.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field, fields
from typing import Any

#: ConfigMap (in the controller namespace) holding the directive for the next incident.
DIRECTIVE_CONFIGMAP = "sdo-scripted-directive"
DIRECTIVE_KEY = "directive.json"
#: Label on every ConfigMap the scripted CLI writes, so the harness can list and delete them.
SCRIPTED_LABEL = "sdo.dev/scripted-codex"
BINDING_KEY = "binding.json"
TURN_KEY = "turn.json"

MITIGATION_MODES = ("correct", "wrong_then_correct", "wrong_only_honest", "wrong_only_claimed", "healed_noop")
REFLECTION_MODES = ("learn", "no_change", "crash_once", "crash_always", "invalid_then_learn")
FAULTS = ("network_policy_block", "missing_configmap", "service_target_port")


class DirectiveError(ValueError):
    """Raised when a directive is malformed; the scripted CLI then fails the turn loudly."""


@dataclass(frozen=True)
class Directive:
    """One incident's scripted plan.

    Attributes:
        scenario: Harness label, echoed into every turn record.
        fault: The fault class the plan diagnoses and repairs (see ``FAULTS``).
        target: The faulted workload (for example ``recommendation`` or ``mongodb-geo``).
        mitigation: Which repairs the responder applies, in order.
        reflection: How the reflection turn behaves.
        usage_seed: Varies the synthetic token counts so incidents are distinguishable.
        pause_before_repair_seconds: Sleep after diagnosis, before any repair, to widen chaos windows.
        pause_before_result_seconds: Sleep after verification, before the result is returned.
        status_attempts: How many times the agent status command is polled for health after a repair.
    """

    scenario: str
    fault: str
    target: str
    mitigation: str = "correct"
    reflection: str = "learn"
    usage_seed: int = 0
    pause_before_repair_seconds: float = 0.0
    pause_before_result_seconds: float = 0.0
    status_attempts: int = 24
    extra: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.scenario:
            raise DirectiveError("directive scenario must not be empty")
        if self.fault not in FAULTS:
            raise DirectiveError(f"unknown fault {self.fault!r}; expected one of {FAULTS}")
        if not self.target:
            raise DirectiveError("directive target must not be empty")
        if self.mitigation not in MITIGATION_MODES:
            raise DirectiveError(f"unknown mitigation mode {self.mitigation!r}; expected one of {MITIGATION_MODES}")
        if self.reflection not in REFLECTION_MODES:
            raise DirectiveError(f"unknown reflection mode {self.reflection!r}; expected one of {REFLECTION_MODES}")
        if self.usage_seed < 0:
            raise DirectiveError("usage_seed must not be negative")
        if self.pause_before_repair_seconds < 0 or self.pause_before_result_seconds < 0:
            raise DirectiveError("pauses must not be negative")
        if self.status_attempts < 1:
            raise DirectiveError("status_attempts must be positive")

    def to_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True)

    @classmethod
    def from_json(cls, text: str) -> Directive:
        try:
            payload: Any = json.loads(text)
        except json.JSONDecodeError as exc:
            raise DirectiveError(f"directive is not JSON: {exc}") from exc
        if not isinstance(payload, dict):
            raise DirectiveError("directive must be a JSON object")
        known = {item.name for item in fields(cls)}
        unknown = sorted(set(payload) - known)
        if unknown:
            raise DirectiveError(f"directive has unknown fields {unknown}")
        try:
            return cls(**payload)
        except TypeError as exc:
            raise DirectiveError(f"directive is incomplete: {exc}") from exc


@dataclass(frozen=True)
class Binding:
    """A directive bound to the incident whose first responder turn read it."""

    incident_id: str
    directive: Directive
    bound_at: str

    def to_json(self) -> str:
        return json.dumps(
            {"incident_id": self.incident_id, "directive": asdict(self.directive), "bound_at": self.bound_at},
            sort_keys=True,
        )

    @classmethod
    def from_json(cls, text: str) -> Binding:
        payload = json.loads(text)
        return cls(
            incident_id=str(payload["incident_id"]),
            directive=Directive.from_json(json.dumps(payload["directive"])),
            bound_at=str(payload["bound_at"]),
        )
