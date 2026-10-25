from __future__ import annotations

import json

from sdo.agents.responder.mutation_output import command_report_schema, format_mutation_response


def finding(name: str) -> dict:
    return {
        "detector_id": "service-reachability",
        "fingerprint": name,
        "primary_resource": {"kind": "Service", "namespace": "app", "name": name},
        "summary": name + " is unreachable",
        "evidence": "dial history and hints " * 120,
    }


def test_seven_finding_response_preserves_full_report_and_bounded_verdict(tmp_path, monkeypatch):
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    before = [finding(str(i)) for i in range(7)]
    after = before[1:]
    action = {
        "command": "mitigate",
        "summary": "completed",
        "targeted_healthy": True,
        "healthy": False,
        "findings": after,
        "program_runs": [{"program_id": "route", "evidence": "full diagnosis" * 100}],
        "report": {"rounds": [{"request_id": "request", "observed_values": "complete" * 100}]},
    }
    text, path = format_mutation_response(before, after, [action])
    assert text.startswith("This action: target finding cleared.")
    assert "Incident: NOT verified, still broken: 6" in text
    assert len(text.encode()) <= 2048
    assert path.is_relative_to(tmp_path / "sdo-reports")
    report = json.loads(path.read_text())
    assert report["actions"] == [action]
    assert report["findings"] == after
    assert set(report) == set(command_report_schema())
    assert all(key + "[]:" in text for key in report)
    assert "dial history" not in text
    for item in after:
        assert "Service/app/" + item["fingerprint"] in text


def test_partial_progress_and_possible_regression(tmp_path, monkeypatch):
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    a, b, c = finding("A"), finding("B"), finding("C")
    text, path = format_mutation_response([a, b], [b, c], [{"command": "act", "success": True}])
    assert "fixed by/since this action: 1" in text
    assert "new since this action: 1 (possible regression)" in text
    assert "still broken, unchanged: 1" in text
    assert text.index("Service/app/C") < text.index("Service/app/B")
    report = json.loads(path.read_text())
    assert report["fixed"] == [a]
    assert report["new"] == [c]
    assert report["unchanged"] == [b]


def test_schema_and_each_action_survive_combined_response(tmp_path, monkeypatch):
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    actions = [
        {"command": "mitigate", "summary": "first", "receipt": {"action_id": "one"}},
        {"command": "mitigate", "summary": "second", "receipt": {"action_id": "two"}},
    ]
    text, path = format_mutation_response([finding("A")], [], actions)
    assert "Actions: 2 completed" in text
    assert json.loads(path.read_text())["actions"] == actions


def test_changed_evidence_is_visible_as_possible_regression(tmp_path, monkeypatch):
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    before = finding("A")
    after = {**before, "evidence": "different observed configuration"}
    _, path = format_mutation_response([before], [after], [{"command": "apply"}])
    report = json.loads(path.read_text())
    assert report["new"] == [after]
    assert not report["unchanged"]
    assert not report["fixed"]
