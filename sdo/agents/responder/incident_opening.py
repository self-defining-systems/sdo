"""What opened an incident and when it began, in the words of the responder's first message."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from google.protobuf import json_format

if TYPE_CHECKING:
    from sdo.contracts import Finding, IncidentRequest

#: Rule prefix and detector prefix of a failing user-test finding (``sdo/operator/sdk/traffic`` and
#: ``memory.TrafficDetectorPrefix``); Go decides which of them open an incident, and records the keys.
_TEST_RULE_PREFIX = "scenario-slo."
_TEST_DETECTOR_PREFIX = "traffic-"
_MAX_CHARS = 160
_MAX_EXCERPT = 90
_MAX_GROUPS = 4
_MAX_TESTS = 6
_MAX_PASSING = 10
_MAX_SYMPTOM = 1100


def _clip(text: str) -> str:
    text = " ".join(text.split())
    return text if len(text) <= _MAX_CHARS else text[: _MAX_CHARS - 1] + "…"


def _short(text: str) -> str:
    text = " ".join(text.split())
    return text if len(text) <= _MAX_EXCERPT else text[: _MAX_EXCERPT - 1] + "…"


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


def _failing_tests(request: IncidentRequest) -> list[tuple[str, dict[str, Any]]]:
    return [
        (finding.rule_id.removeprefix(_TEST_RULE_PREFIX), json_format.MessageToDict(finding.metadata))
        for finding in _opening(request)
        if finding.detector_id.startswith(_TEST_DETECTOR_PREFIX) and finding.rule_id.startswith(_TEST_RULE_PREFIX)
    ]


def _status(status: str) -> str:
    return f"HTTP {status}" if status.isdigit() else status


def _hms(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime("%H:%M:%S")


def _names(names: list[str], shown: int) -> str:
    text = ", ".join(names[:shown])
    return f"{text} (+{len(names) - shown})" if len(names) > shown else text


def symptom(request: IncidentRequest) -> str:
    """What the incident opened on: for failing user tests the status, the server's error, the failed count of
    the window, the passing tests and the first failing sample; otherwise the one-line condition and open time."""
    tests = [] if request.external_message else _failing_tests(request)
    if not tests:
        opened = request.detected_at.ToDatetime(tzinfo=UTC) if request.HasField("detected_at") else None
        return f"Opened on {opened_on(request)}" + (f" at {_hms(opened)} UTC." if opened else ".")
    firsts = [datetime.fromisoformat(meta["first_failure_at"]) for _, meta in tests if meta.get("first_failure_at")]
    since = f" since {_hms(min(firsts))} UTC (first failing sample)" if firsts else ""
    groups: dict[tuple[str, str], list[str]] = {}
    for name, meta in sorted(tests):
        failed = int(meta.get("errors", 0)) + int(meta.get("timeouts", 0))
        key = (_status(str(meta.get("failure_status", "an unrecorded error"))), _short(str(meta.get("failure_observed", ""))))
        total = int(meta.get("samples", 0))
        groups.setdefault(key, []).append(f"{name} {failed}/{total}" if total else name)
    lines = [f"Failing user tests{since}; counts are failed/total samples in the window:"]
    for (status, observed), members in sorted(groups.items(), key=lambda item: -len(item[1]))[:_MAX_GROUPS]:
        lines.append(f"- {_names(members, _MAX_TESTS)} failed with {status}" + (f": {observed}" if observed else ""))
    if len(groups) > _MAX_GROUPS:
        lines.append(f"- (+{len(groups) - _MAX_GROUPS} more error groups)")
    passing = _names(list(request.passing_tests), _MAX_PASSING) or "none"
    lines.append(f"Passing: {passing}. Re-check with `sdo verify`.")
    return "\n".join(lines)[:_MAX_SYMPTOM]
