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
        {
            "command": "pre-apply",
            "refs": [{"api_version": "apps/v1", "kind": "Deployment", "namespace": "app", "name": "frontend"}],
        },
    ]


@pytest.mark.parametrize("named", [False, True])
def test_logical_program_id_with_parameters_resolves_binding_ambiguity(monkeypatch, named) -> None:
    seen = []
    options = [{"option_id": f"candidate-{i}", "program_id": "adopted-high-cpu"} for i in range(2)]
    parameters = {"config": "ConfigMap/astronomy-shop/flags", "workload": "Deployment/astronomy-shop/ad"}

    def exchange(request, _):
        seen.append(request)
        if request["command"] == "menu":
            if request.get("parameters"):
                assert request == {"command": "menu", "program_id": "adopted-high-cpu", "parameters": parameters}
                return {
                    "menu": {
                        "remaining_batches": 5,
                        "options": [{"option_id": "bound-option", "program_id": "adopted-high-cpu"}],
                    }
                }
            return {"menu": {"remaining_batches": 5, "options": options}}
        assert request["selection"]["option_id"] == "bound-option"
        assert request["parameters"] == parameters
        return {"summary": "applied"}

    monkeypatch.setattr(tools_cli, "_exchange", exchange)
    args = ["mitigate", "adopted-high-cpu"]
    for name, value in parameters.items():
        args += [f"--{name}", value] if named else ["--param", f"{name}={value}"]
    assert tools_cli.main(args) == 0
    assert len(seen) == 3


def test_logical_program_id_remains_ambiguous_without_parameters(monkeypatch, capsys) -> None:
    def exchange(request, _):
        assert request == {"command": "menu"}
        return {
            "menu": {
                "remaining_batches": 5,
                "options": [{"option_id": f"candidate-{i}", "program_id": "same"} for i in range(2)],
            }
        }

    monkeypatch.setattr(tools_cli, "_exchange", exchange)
    assert tools_cli.main(["mitigate", "same"]) == 1
    assert "unavailable or ambiguous" in capsys.readouterr().err


def test_logical_program_id_refuses_failed_parameter_binding(monkeypatch, capsys) -> None:
    seen = []

    def exchange(request, _):
        seen.append(request)
        assert request["command"] == "menu"
        if request.get("parameters"):
            return {"error": "required workload binding missing"}
        return {
            "menu": {
                "remaining_batches": 5,
                "options": [{"option_id": f"candidate-{i}", "program_id": "same"} for i in range(2)],
            }
        }

    monkeypatch.setattr(tools_cli, "_exchange", exchange)
    assert tools_cli.main(["mitigate", "same", "--param", "config=ConfigMap/app/flags"]) == 1
    assert "bound mitigation menu unavailable" in capsys.readouterr().err
    assert len(seen) == 2
