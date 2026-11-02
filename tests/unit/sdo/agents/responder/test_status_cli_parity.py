import json

import pytest

from sdo.agents.responder import status_cli, tools_cli


@pytest.mark.parametrize(
    ("healthy", "error", "expected"), [(True, "", 0), (False, "", 1), (False, "prober unavailable", 3)]
)
def test_status_cli_uses_controller_verify(monkeypatch, capsys, healthy, error, expected):
    calls = []
    response = {"command": "verify", "healthy": healthy, "error": error, "summary": "closure check"}
    monkeypatch.setattr(tools_cli, "_exchange", lambda request, _: calls.append(request) or response)
    assert status_cli.main(["--json"]) == expected
    assert calls == [{"command": "verify"}]
    payload = json.loads(capsys.readouterr().out)
    assert payload["healthy"] == healthy
