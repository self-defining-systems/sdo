"""Bounded verification uses the incident transport, without network discovery."""

from sdo.agents.responder import status_cli, tools_cli


def test_status_cli_forwards_to_controller(monkeypatch):
    calls = []
    monkeypatch.setattr(status_cli, "tools_main", lambda args: calls.append(args) or 0)
    assert status_cli.main(["--workload", "verify"]) == 0
    assert calls == [["verify", "--workload", "verify"]]


def test_verify_selection_is_an_incident_tool_request():
    args = tools_cli._parser().parse_args(["verify", "--workload", "check", "--scenario", "read"])
    assert tools_cli._request(args) == {"command": "verify", "workload": "check", "scenarios": ["read"]}
