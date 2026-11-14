"""Bounded verification uses the incident transport, without network discovery."""

from sdo.agents.responder import status_cli, tools_cli


def test_status_cli_forwards_the_selection_to_the_controller(monkeypatch):
    calls = []
    monkeypatch.setattr(
        tools_cli, "_exchange", lambda request, _directory: calls.append(request) or {"healthy": True, "summary": "ok"}
    )
    assert status_cli.main(["--workload", "verify", "--scenario", "read"]) == 0
    assert calls == [{"command": "verify", "workload": "verify", "scenarios": ["read"]}]


def test_verification_is_not_an_agent_command():
    """The adapters' status check reads the controller's verification; the agent's sdo command has no verify."""
    assert "verify" not in tools_cli.command_surface()["commands"]
