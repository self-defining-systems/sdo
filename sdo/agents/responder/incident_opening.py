"""What opened an incident and when it began, in the words of the responder's first message."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sdo.contracts import Finding, IncidentRequest

#: Rule prefix and detector prefix of a failing user-test finding (``sdo/operator/sdk/traffic`` and
#: ``memory.TrafficDetectorPrefix``); Go decides which of them open an incident, and records the keys.
_TEST_RULE_PREFIX = "scenario-slo."
_TEST_DETECTOR_PREFIX = "traffic-"
_MAX_CHARS = 160


def _clip(text: str) -> str:
    text = " ".join(text.split())
    return text if len(text) <= _MAX_CHARS else text[: _MAX_CHARS - 1] + "…"


def _key(finding: Finding) -> str:
    return finding.detector_id + "\x00" + finding.fingerprint


def _opening(request: IncidentRequest) -> list[Finding]:
    keys = set(request.opening_keys)
    opening = [finding for finding in request.findings if _key(finding) in keys]
    return opening or list(request.findings)


def opened_on(request: IncidentRequest) -> str:
    """Failing user tests, the failing object and condition, or the external alert."""
    if request.external_message:
        return f"external alert: {_clip(request.external_message)}; return when it is fixed"
    opening = _opening(request)
    tests = [
        finding.rule_id.removeprefix(_TEST_RULE_PREFIX)
        for finding in opening
        if finding.detector_id.startswith(_TEST_DETECTOR_PREFIX) and finding.rule_id.startswith(_TEST_RULE_PREFIX)
    ]
    if tests:
        return _clip("failing user tests: " + ", ".join(sorted(set(tests))))
    if not opening:
        return "an unknown alert"
    resource = opening[0].primary_resource
    name = "/".join(part for part in (resource.kind, resource.name) if part)
    return _clip(f"{name} {opening[0].rule_id}".strip())


def onset(request: IncidentRequest) -> str:
    """The time the incident's alerts were first detected, UTC; ``sdo detect`` names the failing test's first sample."""
    if not request.HasField("detected_at"):
        return "unknown"
    return request.detected_at.ToDatetime().strftime("%H:%M:%S")
