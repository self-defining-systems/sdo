"""The responder's first message carries the symptom: what failed, how, since when, and what still works."""

from __future__ import annotations

from datetime import UTC, datetime

from google.protobuf import json_format

from sdo.agents.responder.incident_opening import symptom
from sdo.contracts import Finding, IncidentRequest

OPENED = datetime(2026, 1, 1, 12, 0, 40, tzinfo=UTC)


def _test_finding(
    name: str,
    *,
    status: str = "429",
    observed: str = 'text "Retry later"',
    first: str = "2026-01-01T12:00:10Z",
    errors: int = 3,
    timeouts: int = 0,
    samples: int = 6,
) -> Finding:
    finding = Finding(
        detector_id="traffic-probe-call",
        rule_id=f"scenario-slo.{name}",
        fingerprint=f"fp-{name}",
        summary="synthetic scenario violates its SLO",
        evidence="e",
    )
    json_format.ParseDict(
        {
            "first_failure_at": first,
            "failure_status": status,
            "failure_observed": observed,
            "errors": errors,
            "timeouts": timeouts,
            "samples": samples,
        },
        finding.metadata,
    )
    return finding


def _request(*findings: Finding, passing: tuple[str, ...] = ()) -> IncidentRequest:
    request = IncidentRequest(incident_id="inc-1", namespace="app", passing_tests=list(passing))
    request.detected_at.FromDatetime(OPENED)
    request.findings.extend(findings)
    request.opening_keys.extend(f"{f.detector_id}\x00{f.fingerprint}" for f in findings)
    return request


def test_failing_tests_sharing_a_status_and_error_are_grouped_with_their_counts() -> None:
    request = _request(
        _test_finding("commit-call"),
        _test_finding("user-api", errors=4),
        _test_finding("git-over-ssh", status="timeout", observed="i/o timeout", timeouts=3, errors=0, samples=5),
        passing=("fixture-read", "pipeline-call"),
    )
    assert symptom(request) == (
        "Failing user tests since 12:00:10 UTC (first failing sample):\n"
        '- commit-call 3/6, user-api 4/6 failed with HTTP 429: text "Retry later"\n'
        "- git-over-ssh 3/5 failed with timeout: i/o timeout\n"
        "Passing: fixture-read, pipeline-call. Re-check with `sdo verify`."
    )


def test_the_onset_is_the_earliest_first_failing_sample_not_the_open_time() -> None:
    request = _request(
        _test_finding("late", first="2026-01-01T12:00:30Z"),
        _test_finding("early", first="2026-01-01T12:00:05.250Z"),
    )
    assert "since 12:00:05 UTC" in symptom(request)
    assert "12:00:40" not in symptom(request)


def test_the_error_excerpt_is_short_and_the_block_stays_bounded() -> None:
    findings = [_test_finding(f"test-{i}", status=str(500 + i), observed="e" * 500) for i in range(30)]
    block = symptom(_request(*findings, passing=tuple(f"ok-{i}" for i in range(80))))
    assert len(block) <= 1100
    assert "e" * 120 not in block
    assert "sdo verify" in block


def test_no_passing_tests_says_so() -> None:
    assert "Passing: none." in symptom(_request(_test_finding("a")))


def test_other_openers_keep_their_one_line_description_with_the_open_time() -> None:
    finding = Finding(detector_id="workload-health", rule_id="unavailable", fingerprint="f", summary="s", evidence="e")
    finding.primary_resource.kind = "Deployment"
    finding.primary_resource.name = "web"
    request = _request(finding)
    assert symptom(request) == "Opened on Deployment/web unavailable at 12:00:40 UTC."


def test_an_external_alert_is_stated_as_is() -> None:
    request = IncidentRequest(incident_id="x", namespace="app", external_message="disk full")
    request.detected_at.FromDatetime(OPENED)
    assert symptom(request) == "Opened on external alert: disk full; return when it is fixed at 12:00:40 UTC."
