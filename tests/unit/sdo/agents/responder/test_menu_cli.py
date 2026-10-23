"""The CLI preserves one semantic request across retries and fallback tools."""

from __future__ import annotations

import json

import pytest

from sdo.agents.responder import tools_cli


def selection_payload() -> dict[str, object]:
    return {
        "version": 1,
        "incident_id": "synthetic-b3",
        "session_id": "provider-session",
        "dispatch_epoch": "7",
        "request_id": "retry-stable-request",
        "menu_id": "menu-revision",
        "option_id": "routing-then-restart",
        "program_revision": "program-digest",
        "dependency_revision": "dependencies-digest",
        "source_commit": "accepted-source",
        "scope_digest": "named-scope",
        "policy_digest": "menu-policy",
        "round_limit": 2,
    }


def test_menu_cli_sends_named_bindings(monkeypatch) -> None:
    seen = []
    monkeypatch.setattr(tools_cli, "_exchange", lambda request, _: seen.append(request) or {"summary": "menu"})
    assert tools_cli.main(["menu", "composite", "--bind", "service=Service/app/backend"]) == 0
    assert seen == [{"command": "menu", "program_id": "composite", "bindings": {"service": "Service/app/backend"}}]


def test_mitigate_cli_retries_the_same_semantic_selection(tmp_path, monkeypatch) -> None:
    selection = tmp_path / "selection.json"
    selection.write_text(json.dumps(selection_payload()))
    seen = []
    monkeypatch.setattr(
        tools_cli,
        "_exchange",
        lambda request, _: seen.append(request) or {"summary": "report", "report": {"status": "STATUS_UNRESOLVED"}},
    )
    for _ in range(2):
        assert tools_cli.main(["mitigate", "--selection", str(selection)]) == 0
    assert seen == [{"command": "mitigate", "selection": selection_payload()}] * 2


@pytest.mark.parametrize(
    "change",
    [
        {"round_limit": 101},
        {"session_id": ""},
        {"dispatch_epoch": 0},
        {"actions": [{"delete": "Secret/app/token"}]},
        {"request_id": ""},
    ],
)
def test_mitigate_cli_refuses_invalid_selection_before_exchange(tmp_path, monkeypatch, capsys, change) -> None:
    selection = tmp_path / "selection.json"
    selection.write_text(json.dumps(selection_payload() | change))
    monkeypatch.setattr(tools_cli, "_exchange", lambda *_: pytest.fail("invalid selection reached controller"))
    assert tools_cli.main(["mitigate", "--selection", str(selection)]) == 1
    assert "invalid mitigation selection" in capsys.readouterr().err


def test_report_then_act_uses_existing_session_and_capture(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("SDO_TOOLS_DIR", str(tmp_path))
    calls = []

    def exchange(request, directory):
        assert directory == tmp_path
        calls.append(request)
        return {"summary": "settled", "report": {"status": "STATUS_UNRESOLVED"}}

    monkeypatch.setattr(tools_cli, "_exchange", exchange)
    monkeypatch.setattr(tools_cli.recorded_actions, "run_and_record", lambda *_a, **_k: 0)
    assert (
        tools_cli.main(
            ["report", "attempt-1", "--incident-id", "synthetic-b3", "--session-id", "provider-session", "--epoch", "7"]
        )
        == 0
    )
    assert tools_cli.main(["act", "--resource", "Deployment/app/frontend", "--", "true"]) == 0
    assert calls == [
        {
            "command": "report",
            "report_query": {
                "version": 1,
                "incident_id": "synthetic-b3",
                "session_id": "provider-session",
                "dispatch_epoch": "7",
                "attempt_id": "attempt-1",
            },
        },
        {"command": "pre-apply"},
    ]
