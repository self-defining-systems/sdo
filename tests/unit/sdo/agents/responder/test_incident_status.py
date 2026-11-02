"""Bounded verification uses the incident transport, without network discovery."""

from sdo.agents.responder import status_cli, tools_cli


def test_status_cli_forwards_the_selection_to_the_controller(monkeypatch):
    calls = []
    monkeypatch.setattr(
        tools_cli, "_exchange", lambda request, _directory: calls.append(request) or {"healthy": True, "summary": "ok"}
    )
    assert status_cli.main(["--workload", "verify", "--scenario", "read"]) == 0
    assert calls == [{"command": "verify", "workload": "verify", "scenarios": ["read"]}]


def test_verify_selection_is_an_incident_tool_request():
    args = tools_cli._parser().parse_args(["verify", "--workload", "check", "--scenario", "read"])
    assert tools_cli._request(args) == {"command": "verify", "workload": "check", "scenarios": ["read"]}
