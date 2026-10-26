from __future__ import annotations

import json
from pathlib import Path

import pytest

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


@pytest.mark.parametrize("command", ["act", "apply", "mitigate"])
@pytest.mark.parametrize("full_json", [False, True])
@pytest.mark.parametrize("count", [7, 100])
def test_emitted_mutation_stdout_utf8_including_json_framing_and_newline(
    tmp_path,
    monkeypatch,
    capsys,
    command,
    full_json,
    count,
):
    from sdo.agents.responder import tools_cli

    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    before = [finding(str(i)) for i in range(count)]
    after = [
        {**item, "summary": '"界"\\' * 50, "evidence": item["evidence"] + " new observation"} for item in before[1:]
    ]
    action = {
        "command": command,
        "targeted_healthy": True,
        "healthy": False,
        "before_findings": before,
        "controller_findings": after,
        "receipt": {"success": True},
    }
    tools_cli._print_response(action, full_json=full_json)
    stdout = capsys.readouterr().out
    assert len(stdout.encode("utf-8")) <= 2048
    if full_json:
        envelope = json.loads(stdout)
        text, path = envelope["summary"], Path(envelope["details_file"])
    else:
        text = stdout
        path = Path(text.split("Full JSON: ", 1)[1].splitlines()[0])
    assert text.startswith("This action: target finding cleared")
    assert f"still broken: {count - 1}" in text
    assert f"new since this action: {count - 1}" in text
    assert "still broken, unchanged: 0" in text
    report = json.loads(path.read_text())
    assert report["actions"] == [action]
    assert report["findings"] == after
    assert set(report) == set(command_report_schema())


@pytest.mark.parametrize("command", ["act", "apply", "mitigate", "rollback"])
@pytest.mark.parametrize("full_json", [False, True])
@pytest.mark.parametrize("error", ["💥" * 500, '"\\\n\t界' * 500], ids=["emoji", "escaped"])
def test_rec_mandatory_envelope_byte_cap(tmp_path, monkeypatch, capsys, command, full_json, error):
    from sdo.agents.responder import tools_cli

    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    action = {"command": command, "error": error, "failure_reason": "execution_failed", "summary": "repair failed"}
    tools_cli._print_response(action, full_json=full_json)
    stdout = capsys.readouterr().out
    assert len(stdout.encode("utf-8")) <= 2048
    path = (
        Path(json.loads(stdout)["details_file"]) if full_json else Path(stdout.split("Full JSON: ")[1].splitlines()[0])
    )
    assert json.loads(path.read_text())["actions"][0]["error"] == error


@pytest.mark.parametrize("full_json", [False, True])
def test_rec_long_escaped_report_path_remains_readable(tmp_path, monkeypatch, capsys, full_json):
    from sdo.agents.responder import tools_cli

    directory = tmp_path
    for _ in range(20):
        directory /= '"\\界' * 20
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(directory))
    action = {"command": "mitigate", "error": "💥" * 500, "summary": "repair failed"}
    tools_cli._print_response(action, full_json=full_json)
    stdout = capsys.readouterr().out
    assert len(stdout.encode("utf-8")) <= 2048
    path = (
        Path(json.loads(stdout)["details_file"]) if full_json else Path(stdout.split("Full JSON: ")[1].splitlines()[0])
    )
    report = json.loads(path.read_text())
    assert report["actions"] == [action]
    originals = list((directory / "sdo-reports").glob("*/*-mitigate.json"))
    assert len(originals) == 1
    assert json.loads(originals[0].read_text()) == report
    assert path != originals[0]


@pytest.mark.parametrize("snapshots", [{}, {"before_findings": [], "controller_findings": []}])
def test_rec_controller_progress_is_authoritative(tmp_path, monkeypatch, capsys, snapshots):
    from sdo.agents.responder import tools_cli

    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    b = finding("B")
    action = {
        "command": "mitigate",
        "findings": [b],
        "progress": {"fixed": [], "new": [b], "unchanged": []},
        **snapshots,
    }
    tools_cli._print_response(action, full_json=True)
    envelope = json.loads(capsys.readouterr().out)
    report = json.loads(Path(envelope["details_file"]).read_text())
    assert report["new"] == [b]
    assert report["unchanged"] == []
    assert "new since this action: 1" in envelope["summary"]


@pytest.mark.parametrize(("before", "after"), [(None, None), ([], []), ([], [finding("B")]), ([finding("A")], [])])
def test_rec_snapshots_never_use_action_local_findings(tmp_path, monkeypatch, capsys, before, after):
    from sdo.agents.responder import tools_cli

    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    action = {
        "command": "apply",
        "findings": [finding("live")],
        "before_findings": before,
        "controller_findings": after,
    }
    tools_cli._print_response(action, full_json=True)
    envelope = json.loads(capsys.readouterr().out)
    report = json.loads(Path(envelope["details_file"]).read_text())
    assert report["findings"] == (after or [])
    assert not any(item["fingerprint"] == "live" for key in ("fixed", "new", "unchanged") for item in report[key])


@pytest.mark.parametrize("command", ["act", "apply", "mitigate", "rollback"])
@pytest.mark.parametrize("full_json", [False, True])
def test_rec_maximum_variable_fields_and_path(tmp_path, monkeypatch, capsys, command, full_json):
    from sdo.agents.responder import tools_cli

    directory = tmp_path / "line\nbreak"
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(directory))
    monkeypatch.setenv("SDO_INCIDENT_ID", "i" * 199)
    error = '\x00"\\💥' * 500
    action = {
        "command": command,
        "error": error,
        "summary": error,
        "failure_reason": error,
        "before_findings": [],
        "controller_findings": [],
        "incident_verified": {"message": error, "verified_at": error},
    }
    tools_cli._print_response(action, full_json=full_json)
    stdout = capsys.readouterr().out
    assert len(stdout.encode("utf-8")) <= 2048
    envelope = json.loads(stdout) if full_json else None
    text = envelope["summary"] if envelope else stdout
    assert "failed:" in text
    path = Path(envelope["details_file"]) if envelope else Path(text.split("Full JSON: ")[1].splitlines()[0])
    assert json.loads(path.read_text())["actions"] == [action]
    originals = list((directory / "sdo-reports").glob("*/*-" + command + ".json"))
    assert len(originals) == 1
    assert json.loads(originals[0].read_text())["actions"] == [action]


@pytest.mark.parametrize(("before", "after"), [(None, [finding("B")]), ([finding("A")], None)])
def test_rec_missing_snapshot_never_invents_progress(tmp_path, monkeypatch, capsys, before, after):
    from sdo.agents.responder import tools_cli

    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    tools_cli._print_response(
        {"command": "apply", "before_findings": before, "controller_findings": after}, full_json=True
    )
    envelope = json.loads(capsys.readouterr().out)
    assert "progress unknown" in envelope["summary"]
    report = json.loads(Path(envelope["details_file"]).read_text())
    assert report["fixed"] == report["new"] == report["unchanged"] == []


@pytest.mark.parametrize("full_json", [False, True])
def test_rec_rollback_honors_existing_smaller_policy(tmp_path, monkeypatch, capsys, full_json):
    from sdo.agents.responder import tools_cli

    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    after = [finding(str(index)) for index in range(100)]
    action = {
        "command": "rollback",
        "summary": "rollback completed",
        "before_findings": [],
        "controller_findings": after,
        "progress": {"fixed": [], "new": after, "unchanged": []},
    }
    tools_cli._print_response(action, full_json=full_json)
    stdout = capsys.readouterr().out
    assert len(stdout.encode("utf-8")) <= 1500
    envelope = json.loads(stdout) if full_json else None
    text = envelope["summary"] if envelope else stdout
    assert "still broken: 100" in text
    assert "new since this action: 100" in text
    for key, description in command_report_schema().items():
        assert key + "[]: " + description.removeprefix("array of ").split(": ", 1)[0] in text
    path = Path(envelope["details_file"]) if envelope else Path(text.split("Full JSON: ")[1].splitlines()[0])
    report = json.loads(path.read_text())
    assert report["actions"] == [action]
    assert report["findings"] == report["new"] == after


@pytest.mark.parametrize("command", ["act", "apply", "mitigate", "rollback"])
@pytest.mark.parametrize("full_json", [False, True])
def test_rec_schema_growth_cannot_bypass_hard_output_limit(tmp_path, monkeypatch, capsys, command, full_json):
    from sdo.agents.responder import mutation_output, tools_cli

    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    schema = command_report_schema()
    schema["actions"] = "array of {" + ",".join(f"future_field_{index}" for index in range(300)) + "}: full output"
    monkeypatch.setattr(mutation_output, "command_report_schema", lambda: schema)
    action = {"command": command, "summary": "completed", "before_findings": [], "controller_findings": []}
    with pytest.raises(ValueError, match="2048-byte hard limit"):
        tools_cli._print_response(action, full_json=full_json)
    assert capsys.readouterr().out == ""
    originals = list((tmp_path / "sdo-reports").glob("*/*-" + command + ".json"))
    assert len(originals) == 1
    assert json.loads(originals[0].read_text())["actions"] == [action]


def test_failed_act_line_says_why_it_failed(tmp_path, monkeypatch, capsys):
    # BASE1 C: a failed act printed only its summary, so the agent opened the report to learn why.
    from sdo.agents.responder import tools_cli

    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    action = {
        "command": "act",
        "summary": "scale checkout",
        "success": False,
        "failure_reason": "execution_failed",
        "exit_code": 1,
        "output": "\nerror: unknown flag: --json\nSee 'kubectl scale --help' for usage.\n",
    }
    tools_cli._print_response(action, full_json=False)
    first = capsys.readouterr().out.splitlines()[0]
    assert "execution_failed" in first
    assert "exit 1" in first
    assert "error: unknown flag: --json" in first
    assert "kubectl scale --help" not in first
    assert len(first) <= 200
