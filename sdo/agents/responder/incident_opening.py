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
#: Every failing user test is named, so the agent sees which operations fail and what they share. The cap is a
#: hard bound, not a summary: the largest probe set measured on a real multi-service application is about two
#: dozen scenarios, and 48 leaves twofold headroom; beyond it the rest are counted and left to `sdo detect`.
_MAX_FAILING = 48
_MAX_GROUPS = 8
_MAX_PASSING = 24
_MAX_NAME = 40
#: Worst case: 48 failing names (<= 48 chars + counts), 8 groups, 24 passing names; the block is bounded
#: structurally and this is the final guard, mirrored by the symptom placeholder budget.
_MAX_SYMPTOM = 5200


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
        return "external alert"
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


def _name(name: str) -> str:
    return name if len(name) <= _MAX_NAME else name[: _MAX_NAME - 1] + "…"


def symptom(request: IncidentRequest) -> str:
    """What the incident opened on: for failing user tests every failing test by name with failed/total, grouped
    by the status and server error they share (largest group first), then the passing tests; otherwise the
    one-line condition and open time."""
    opened = request.detected_at.ToDatetime(tzinfo=UTC) if request.HasField("detected_at") else None
    if request.external_message:
        # The alert is the page a person or harness wrote; it reaches the agent whole, line breaks kept. The
        # contract bounds it (max_len 4096), so it always fits inside the symptom cap.
        at = f" at {_hms(opened)} UTC" if opened else ""
        return f"Opened on external alert{at}; return when it is fixed:\n{request.external_message.strip()}"[
            :_MAX_SYMPTOM
        ]
    tests = _failing_tests(request)
    if not tests:
        return f"Opened on {opened_on(request)}" + (f" at {_hms(opened)} UTC." if opened else ".")
    firsts = [datetime.fromisoformat(meta["first_failure_at"]) for _, meta in tests if meta.get("first_failure_at")]
    since = f" since {_hms(min(firsts))} UTC (first failing sample)" if firsts else ""
    ordered = sorted(tests)
    groups: dict[tuple[str, str], list[str]] = {}
    for name, meta in ordered[:_MAX_FAILING]:
        failed = int(meta.get("errors", 0)) + int(meta.get("timeouts", 0))
        key = (
            _status(str(meta.get("failure_status", "an unrecorded error"))),
            _short(str(meta.get("failure_observed", ""))),
        )
        total = int(meta.get("samples", 0))
        groups.setdefault(key, []).append(f"{_name(name)} {failed}/{total}" if total else _name(name))
    lines = [
        f"{len(tests)} failing user tests{since}; each name is followed by failed/total samples in the window."
        " Shared symptom per group:"
    ]
    ranked = sorted(groups.items(), key=lambda item: -len(item[1]))
    for (status, observed), members in ranked[:_MAX_GROUPS]:
        lines.append(f"- {status}" + (f" {observed}" if observed else "") + f" ({len(members)}): " + ", ".join(members))
    if len(ranked) > _MAX_GROUPS:
        lines.append(f"- (+{len(ranked) - _MAX_GROUPS} more error groups: sdo detect tests)")
    if len(tests) > _MAX_FAILING:
        lines.append(f"- (+{len(tests) - _MAX_FAILING} more failing tests not listed: sdo detect tests)")
    passing = _names([_name(name) for name in request.passing_tests], _MAX_PASSING) or "none"
    lines.append(f"Passing: {passing}. Re-check one with `sdo detect TEST`.")
    return "\n".join(lines)[:_MAX_SYMPTOM]
