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
        "3 failing user tests since 12:00:10 UTC (first failing sample); each name is followed by failed/total"
        " samples in the window. Shared symptom per group:\n"
        '- HTTP 429 text "Retry later" (2): commit-call 3/6, user-api 4/6\n'
        "- timeout i/o timeout (1): git-over-ssh 3/5\n"
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
    assert len(block) <= 5200
    assert "e" * 120 not in block
    assert "sdo verify" in block
    assert "(+22 more error groups" in block


def test_twenty_five_failing_tests_in_three_groups_are_all_named_under_their_shared_symptom() -> None:
    shapes = [
        ("429", 'text "Retry later"', 12),
        ("timeout", "i/o timeout", 8),
        ("502", 'text "Bad Gateway"', 5),
    ]
    findings = [
        _test_finding(f"{status}-op-{i:02d}", status=status, observed=observed, errors=i + 1, samples=20)
        for status, observed, count in shapes
        for i in range(count)
    ]
    block = symptom(_request(*findings, passing=("fixture-read",)))
    lines = block.splitlines()
    assert lines[0].startswith("25 failing user tests since")
    assert lines[1].startswith('- HTTP 429 text "Retry later" (12): ')
    assert lines[2].startswith("- timeout i/o timeout (8): ")
    assert lines[3].startswith('- HTTP 502 text "Bad Gateway" (5): ')
    for finding in findings:
        assert finding.rule_id.removeprefix("scenario-slo.") + " " in block
    assert "429-op-11 12/20" in block
    assert "more" not in block
    assert lines[-1] == "Passing: fixture-read. Re-check with `sdo verify`."


def test_failing_tests_beyond_the_hard_cap_are_counted_not_dropped_silently() -> None:
    findings = [_test_finding(f"op-{i:03d}") for i in range(60)]
    block = symptom(_request(*findings))
    assert "op-047" in block
    assert "op-048" not in block
    assert "(+12 more failing tests not listed: sdo detect --finding tests)" in block
    assert len(block) <= 5200


def test_a_worst_case_block_fits_the_symptom_budget_without_truncation() -> None:
    findings = [
        _test_finding("n" * 80 + str(i), status=str(500 + i % 9), observed="e" * 500, errors=100, samples=100)
        for i in range(60)
    ]
    block = symptom(_request(*findings, passing=tuple("p" * 80 + str(i) for i in range(80))))
    assert block.endswith("Re-check with `sdo verify`.")


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
    assert symptom(request) == "Opened on external alert at 12:00:40 UTC; return when it is fixed:\ndisk full"


def test_an_external_alert_reaches_the_agent_whole_with_its_line_breaks() -> None:
    lines = [
        "Page for the application in Kubernetes namespace app.",
        "Users are seeing failures:",
        "  - op-a: 500 (3 failed of 57 requests in the last 10 s)",
        "These still work: " + ", ".join(f"op-{i}" for i in range(400)) + ".",
        "To check an operation yourself: curl -s 'http://entry/api/a'",
    ]
    message = "\n".join(lines)
    assert len(message) <= 4096  # the contract's max_len for external_message
    request = IncidentRequest(incident_id="x", namespace="app", external_message=message)
    request.detected_at.FromDatetime(OPENED)
    block = symptom(request)
    assert block.endswith("\n" + message)
    assert "…" not in block
