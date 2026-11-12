"""The CLI preserves one semantic request across retries and fallback tools."""

from __future__ import annotations

import pytest

from sdo.agents.responder import tools_cli


def test_menu_cli_sends_named_bindings(monkeypatch) -> None:
    seen = []
    monkeypatch.setattr(tools_cli, "_exchange", lambda request, _: seen.append(request) or {"summary": "menu"})
    assert tools_cli.main(["menu", "composite", "--bind", "service=Service/app/backend"]) == 0
    assert seen == [{"command": "menu", "program_id": "composite", "bindings": {"service": "Service/app/backend"}}]


def _capture(monkeypatch, reply=None):
    seen = []
    monkeypatch.setattr(tools_cli, "_exchange", lambda request, _: seen.append(request) or (reply or {"summary": "ok"}))
    return seen


def test_mitigate_takes_the_name_and_bind_that_diagnose_takes(monkeypatch) -> None:
    seen = _capture(monkeypatch)
    binding = "deployment=Deployment/app/api"
    assert tools_cli.main(["diagnose", "redis-acl", "--bind", binding]) == 0
    assert tools_cli.main(["mitigate", "redis-acl", "--bind", binding]) == 0
    assert seen == [
        {"command": "diagnose", "program_id": "redis-acl", "parameters": {"deployment": "Deployment/app/api"}},
        {
            "command": "mitigate",
            "targets": [{"name": "redis-acl", "parameters": {"deployment": "Deployment/app/api"}}],
        },
    ]


def test_mitigate_bind_carries_plain_values_and_built_in_actions_use_the_same_form(monkeypatch) -> None:
    seen = _capture(monkeypatch)
    assert tools_cli.main(["mitigate", "DeletePod", "--bind", "ref=Pod/app/api-0", "--finding", "fp1"]) == 0
    assert tools_cli.main(["mitigate", "scale", "--bind", "replicas=3", "--bind", 'extra={"a": 1}']) == 0
    assert seen[0]["targets"] == [
        {"name": "DeletePod", "parameters": {"ref": "Pod/app/api-0"}, "finding_fingerprints": ["fp1"]}
    ]
    assert seen[1]["targets"] == [{"name": "scale", "parameters": {"replicas": "3", "extra": '{"a": 1}'}}]


def test_mitigate_without_bind_names_only_the_playbook(monkeypatch) -> None:
    seen = _capture(monkeypatch)
    assert tools_cli.main(["mitigate", "redis-acl"]) == 0
    assert seen == [{"command": "mitigate", "targets": [{"name": "redis-acl"}]}]


def test_independent_fixes_are_separated_by_double_dash(monkeypatch) -> None:
    seen = _capture(monkeypatch)
    argv = ["mitigate", "a", "--bind", "x=Deployment/ns/a", "--", "b", "--bind", "y=Deployment/ns/b"]
    assert tools_cli.main(argv) == 0
    assert seen[0]["targets"] == [
        {"name": "a", "parameters": {"x": "Deployment/ns/a"}},
        {"name": "b", "parameters": {"y": "Deployment/ns/b"}},
    ]


def test_rounds_belongs_to_one_fix(monkeypatch, capsys) -> None:
    seen = _capture(monkeypatch)
    assert tools_cli.main(["mitigate", "a", "--rounds", "3"]) == 0
    assert seen[0]["rounds"] == 3
    with pytest.raises(SystemExit):
        tools_cli.main(["mitigate", "a", "--", "b", "--rounds", "2"])


@pytest.mark.parametrize(
    "argv",
    [
        ["--option", "a"],
        ["a", "--param", "x=y"],
        ["--selection", "f.json"],
        ["--selection-set", "f.json"],
        ["--match", "a"],
    ],
)
def test_removed_mitigate_forms_are_rejected_before_exchange(monkeypatch, argv) -> None:
    monkeypatch.setattr(tools_cli, "_exchange", lambda *_: pytest.fail("removed form reached the controller"))
    with pytest.raises(SystemExit):
        tools_cli.main(["mitigate", *argv])


@pytest.mark.parametrize("command", ["plan", "apply"])
def test_plan_and_apply_are_not_responder_commands(command) -> None:
    with pytest.raises(SystemExit):
        tools_cli.main([command, "x"])


def test_repeated_mitigate_bind_carries_list_elements_in_order(monkeypatch) -> None:
    seen = _capture(monkeypatch)
    argv = ["mitigate", "Exec", "--bind", "pod=Pod/app/db-0", "--bind", "command=sh", "--bind", "command=-c"]
    assert tools_cli.main([*argv, "--bind", "command=psql -c 'select 1'"]) == 0
    assert seen[0]["targets"] == [
        {"name": "Exec", "parameters": {"pod": "Pod/app/db-0", "command": ["sh", "-c", "psql -c 'select 1'"]}}
    ]


@pytest.mark.parametrize("command", ["diagnose", "menu"])
def test_duplicate_playbook_bind_names_are_refused(monkeypatch, capsys, command) -> None:
    monkeypatch.setattr(tools_cli, "_exchange", lambda *_: pytest.fail("duplicate reached the controller"))
    assert tools_cli.main([command, "a", "--bind", "x=1", "--bind", "x=2"]) == 1
    assert "duplicate binding name" in capsys.readouterr().err


def test_controller_error_for_a_wrong_role_reaches_the_agent(monkeypatch, capsys) -> None:
    message = "redis-acl has no parameter 'redis'; it takes deployment=Deployment/NAMESPACE/NAME"
    _capture(monkeypatch, {"error": message, "summary": message})
    for command in ("diagnose", "mitigate"):
        assert tools_cli.main([command, "redis-acl", "--bind", "redis=Deployment/app/redis"]) == 1
        assert message in capsys.readouterr().out


def test_diagnose_match_prints_the_command_that_fixes_it(monkeypatch, capsys) -> None:
    command = "sdo mitigate redis-acl --bind deployment=Deployment/app/redis"
    run = {"program_id": "redis-acl", "verdict": {"result": "Match"}, "can_mitigate": True, "mitigate_command": command}
    _capture(monkeypatch, {"command": "diagnose", "summary": "1 program diagnoses", "program_runs": [run]})
    assert tools_cli.main(["diagnose", "redis-acl", "--bind", "deployment=Deployment/app/redis"]) == 0
    output = capsys.readouterr().out
    assert f"To fix: {command}" in output
    assert "Next commands" not in output


def test_menu_lists_commands_not_hash_ids(monkeypatch, capsys) -> None:
    options = [
        {
            "option_id": "program-" + "ab" * 32,
            "program_id": "redis-acl",
            "command": "sdo mitigate redis-acl --bind d=Deployment/app/r",
        },
        {
            "option_id": "DeletePod",
            "program_id": "action:DeletePod",
            "command": "sdo mitigate DeletePod --bind ref=...",
        },
    ]
    _capture(monkeypatch, {"command": "menu", "summary": "2 options", "menu": {"options": options}})
    assert tools_cli.main(["menu"]) == 0
    output = capsys.readouterr().out
    assert "sdo mitigate redis-acl --bind d=Deployment/app/r" in output
    assert "program-" + "ab" * 32 not in output


def test_report_uses_existing_session(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("SDO_TOOLS_DIR", str(tmp_path))
    calls = []

    def exchange(request, directory):
        assert directory == tmp_path
        calls.append(request)
        return {"summary": "settled", "report": {"status": "STATUS_UNRESOLVED"}}

    monkeypatch.setattr(tools_cli, "_exchange", exchange)
    assert (
        tools_cli.main(
            ["report", "attempt-1", "--incident-id", "synthetic-b3", "--session-id", "provider-session", "--epoch", "7"]
        )
        == 0
    )
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
    ]


def test_applied_fix_waiting_for_recovery_is_not_reported_as_unresolved(monkeypatch, capsys) -> None:
    summary = "fix applied; waiting for traffic to recover; next: sdo verify --wait 60"
    _capture(
        monkeypatch,
        {
            "command": "mitigate",
            "summary": summary,
            "report": {"status": "STATUS_UNRESOLVED", "reason_code": "applied_awaiting_recovery"},
        },
    )
    assert tools_cli.main(["mitigate", "redis-acl"]) == 0
    output = capsys.readouterr().out
    assert summary in output
    assert "unresolved" not in output.lower().split("full json")[0]


def test_batch_row_names_the_fix_and_says_waiting(monkeypatch, capsys) -> None:
    reply = {
        "command": "mitigate",
        "summary": "2 mitigation options",
        "option_reports": [
            {
                "option_id": "program-" + "cd" * 32,
                "application_id": "a1",
                "status": "STATUS_UNRESOLVED",
                "reason_code": "applied_awaiting_recovery",
            },
            {"option_id": "DeletePod", "application_id": "a2", "status": "STATUS_SUCCESS"},
        ],
        "report": {
            "attempt_id": "batch",
            "execution": [
                {"program_id": "redis-acl", "pending": False},
                {"program_id": "action:DeletePod", "pending": False},
            ],
        },
    }
    _capture(monkeypatch, reply)
    assert tools_cli.main(["mitigate", "redis-acl", "--", "DeletePod", "--bind", "ref=Pod/app/x"]) == 0
    output = capsys.readouterr().out
    assert "fix: redis-acl applied, waiting for recovery" in output
    assert "program-" + "cd" * 32 not in output
