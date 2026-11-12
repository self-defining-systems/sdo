import json
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

from sdo.agents.responder import tools_cli
from tests.context_helpers import expand_context


def test_sdo_cli_exchanges_typed_request_with_fake_controller(tmp_path, monkeypatch, capsys):
    requests = tmp_path / "requests"
    responses = tmp_path / "responses"
    requests.mkdir()
    responses.mkdir()
    monkeypatch.setenv("SDO_TOOLS_DIR", str(tmp_path))
    seen = []

    def controller(_seconds):
        # _exchange has published its request before its first response poll.
        files = list(requests.glob("*.json"))
        assert len(files) == 1
        request = json.loads(files[0].read_text())
        seen.append(request)
        (responses / files[0].name).write_text(
            json.dumps({"command": "diagnose", "summary": "one match", "program_runs": []})
        )

    monkeypatch.setattr(tools_cli.time, "sleep", controller)
    assert tools_cli.main(["--json", "diagnose", "targetport", "--bind", "service=Service/app/api"]) == 0
    assert seen == [
        {
            "command": "diagnose",
            "program_id": "targetport",
            "parameters": {"service": "Service/app/api"},
        }
    ]
    output = expand_context(capsys.readouterr().out)
    assert '"command": "diagnose"' in output
    assert "one match" in output


def test_sdo_source_cli_sends_a_namespaced_resource(monkeypatch, capsys):
    seen = []

    def exchange(request, _directory):
        seen.append(request)
        return {"command": "source", "summary": "rendered source", "source": {"kind": "Deployment"}}

    monkeypatch.setattr(tools_cli, "_exchange", exchange)

    assert tools_cli.main(["--json", "source", "Deployment/app/rate"]) == 0
    assert seen == [
        {
            "command": "source",
            "ref": "Deployment/app/rate",
        }
    ]
    assert '"source": {"kind": "Deployment"}' in expand_context(capsys.readouterr().out)


@pytest.mark.parametrize("command", [[], ["source"], ["mitigate"]])
def test_cli_help_shows_concrete_source_and_named_binding_examples(command, capsys):
    with pytest.raises(SystemExit) as captured:
        tools_cli.main(["--json", *command, "--help"])
    assert captured.value.code == 0
    output = expand_context(capsys.readouterr().out)
    if command != ["mitigate"]:
        assert "sdo source Deployment/app/api" in output
    if command != ["source"]:
        assert "sdo mitigate targetport --bind service=Service/app/api" in output


def test_mitigate_help_is_short_and_names_one_form(capsys):
    with pytest.raises(SystemExit):
        tools_cli.main(["mitigate", "--help"])
    output = capsys.readouterr().out
    assert len(output) < 2500
    for removed in ("--option", "--param", "--selection", "--match", "sdo plan", "sdo apply"):
        assert removed not in output


def test_status_help_points_at_stage_results(capsys):
    with pytest.raises(SystemExit):
        tools_cli.main(["status", "--help"])
    assert "prior_stages" in capsys.readouterr().out


def test_documented_cli_examples_send_valid_requests(monkeypatch):
    seen = []
    monkeypatch.setattr(tools_cli, "_exchange", lambda request, _: seen.append(request) or {"summary": "ok"})

    for example in tools_cli.CLI_EXAMPLES:
        assert tools_cli.main(["--json", *shlex.split(example)[1:]]) == 0

    assert seen[0] == {
        "command": "source",
        "ref": "Deployment/app/api",
    }
    assert seen[-1] == {
        "command": "mitigate",
        "targets": [{"name": "targetport", "parameters": {"service": "Service/app/api"}}],
    }


@pytest.mark.parametrize("can_mitigate", [True, False])
def test_sdo_cli_preserves_program_capability_metadata(monkeypatch, capsys, can_mitigate):
    response = {
        "command": "diagnose",
        "summary": "one match",
        "program_runs": [
            {
                "program_id": "targetport",
                "can_mitigate": can_mitigate,
                "verdict": {"result": "Match"},
            }
        ],
    }
    monkeypatch.setattr(tools_cli, "_exchange", lambda _request, _directory: response)

    assert tools_cli.main(["--json", "diagnose", "targetport", "--bind", "service=Service/app/api"]) == 0

    output = json.loads(expand_context(capsys.readouterr().out).splitlines()[0])
    assert output["program_runs"][0]["can_mitigate"] is can_mitigate


@pytest.mark.parametrize("raw", ["deploy/api", "StatefulSet/app/db", "Service/a; Service/b", "not a ref"])
def test_cli_passes_agent_ref_text_to_go_without_parsing(raw, monkeypatch):
    seen = []
    monkeypatch.setattr(tools_cli, "_exchange", lambda request, _: seen.append(request) or {"summary": "ok"})
    assert tools_cli.main(["--json", "source", raw]) == 0
    assert seen == [{"command": "source", "ref": raw}]
    assert tools_cli.main(["--json", "mitigate", "toy", "--bind", "resource=" + raw]) == 0
    assert seen[-1]["targets"] == [{"name": "toy", "parameters": {"resource": raw}}]


def test_sdo_cli_start_does_not_import_the_responder_stack():
    code = (
        "import sys\n"
        "from sdo.agents.responder import tools_cli\n"
        "tools_cli._parser()\n"
        "heavy = ('sdo.agents.responder.codex', 'sdo.agents.responder.reflection', 'sdo.agents.responder.job',\n"
        "         'sdo.agents.responder.incident_status', 'pydantic', 'kubernetes', 'google.protobuf')\n"
        "print(','.join(name for name in heavy if name in sys.modules))\n"
    )
    result = subprocess.run([sys.executable, "-I", "-c", code], capture_output=True, text=True, check=True)
    assert result.stdout.strip() == ""


def test_responder_package_exports_resolve_lazily():
    from sdo.agents import responder

    assert responder.run_worker_task.__module__ == "sdo.agents.responder.worker"
    assert responder.IncidentStatusReport.__module__ == "sdo.agents.responder.incident_status"
    assert "run_worker_task" in dir(responder)
    assert not hasattr(responder, "not_an_export")


def test_local_help_is_a_concise_capability_index() -> None:
    help_text = tools_cli._parser().format_help()
    assert "accepted source" in help_text
    assert "record" in help_text
    assert "verify" in help_text
    assert len(help_text) < 2000


def test_incident_command_index_keeps_details_in_help(monkeypatch: pytest.MonkeyPatch) -> None:
    import argparse

    parser = tools_cli._parser()
    commands = next(action for action in parser._actions if isinstance(action, argparse._SubParsersAction))
    purposes = {action.dest: action.help for action in commands._choices_actions}
    monkeypatch.setattr(tools_cli, "_parser", lambda: parser)
    surface = tools_cli.command_surface()
    assert surface["commands"] == {
        name: f"sdo {name}: {purpose}; read sdo {name} --help for arguments." for name, purpose in purposes.items()
    }
    assert surface["reference"] == "sdo-operating/SKILL.md"
    assert all("\n" not in text for text in surface["commands"].values())

    for name, command in commands.choices.items():
        command.description = f"Detailed {name} behavior " * 100
        command.epilog = f"Detailed {name} output " * 100
    assert tools_cli.command_surface() == surface


@pytest.mark.parametrize("response", [{"summary": "captured"}, {"error": "capture failed"}])
def test_act_waits_for_controller_capture_before_running(tmp_path, monkeypatch, response):
    seen = []

    def exchange(request, _directory):
        assert request == {
            "command": "pre-apply",
            "refs": [{"api_version": "v1", "kind": "Service", "namespace": "app", "name": "api"}],
        }
        seen.append("capture")
        return response

    def run(*_args, **_kwargs):
        assert seen == ["capture"]
        seen.append("mutation")
        return 0

    monkeypatch.setattr(tools_cli, "_exchange", exchange)
    monkeypatch.setattr(tools_cli.recorded_actions, "run_and_record", run)
    result = tools_cli.main(["--json", "act", "--resource", "Service/app/api", "--", "true"])
    assert result == 0
    assert seen == ["capture", "mutation"]


def test_menu_does_not_gate_direct_act(monkeypatch):
    monkeypatch.setenv("SDO_MITIGATION_SESSION", "1")
    seen = []
    monkeypatch.setattr(tools_cli, "_exchange", lambda request, _directory: seen.append(request["command"]) or {})
    monkeypatch.setattr(tools_cli.recorded_actions, "run_and_record", lambda *_args, **_kwargs: 0)
    assert tools_cli.main(["--json", "act", "--resource", "Deployment/app/api", "--", "true"]) == 0
    assert seen == ["pre-apply"]


def test_detect_repeated_detector_selection(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[dict[str, object]] = []
    monkeypatch.setattr(tools_cli, "_exchange", lambda request, _: seen.append(request) or {"summary": "fresh"})
    assert tools_cli.main(["--json", "detect"]) == 0
    assert tools_cli.main(["--json", "detect", "--detector", "a", "b", "--detector", "c"]) == 0
    assert seen == [{"command": "detect"}, {"command": "detect", "detector_ids": ["a", "b", "c"]}]


def test_detect_prints_the_compact_summary_and_expands_one_item(monkeypatch, capsys, tmp_path):
    seen: list[dict[str, object]] = []
    summary = "incident inc-1\nOPENED ON\ncreate  HTTP 429\n  more: sdo detect --finding create"
    monkeypatch.setenv("SDO_API_HISTORY_DIR", str(tmp_path))
    monkeypatch.setattr(
        tools_cli,
        "_exchange",
        lambda request, _: seen.append(request)
        or {"command": "detect", "text": summary, "findings": [{"summary": "x"}]},
    )
    assert tools_cli.main(["detect"]) == 0
    assert capsys.readouterr().out.strip() == summary
    assert tools_cli.main(["detect", "--finding", "create"]) == 0
    assert seen == [{"command": "detect"}, {"command": "detect", "finding": "create"}]
    assert tools_cli.main(["--json", "detect"]) == 0
    assert json.loads(capsys.readouterr().out.splitlines()[-1])["findings"] == [{"summary": "x"}]


def test_mitigate_cli_preserves_explicit_target_finding_fingerprints(monkeypatch):
    seen = []
    monkeypatch.setattr(tools_cli, "_exchange", lambda request, _: seen.append(request) or {"summary": "ok"})
    argv = ["--json", "mitigate", "repair", "--finding", "health/api/config", "--finding", "health/api/ready"]
    assert tools_cli.main(argv) == 0
    assert seen[-1]["targets"][0]["finding_fingerprints"] == ["health/api/config", "health/api/ready"]


@pytest.mark.parametrize("detection_error", [False, True])
def test_check_retains_fresh_detection_and_independent_closure(monkeypatch, capsys, detection_error):
    seen = []
    detection = {"command": "detect", "findings": [{"fingerprint": "peer"}], "summary": "fresh peer"}
    if detection_error:
        detection["error"] = "one detector failed"
    closure = {"command": "verify", "healthy": False, "summary": "peer remains", "findings": [{"fingerprint": "peer"}]}

    def exchange(request, _directory):
        seen.append(request)
        return detection if request["command"] == "detect" else closure

    monkeypatch.setattr(tools_cli, "_exchange", exchange)
    assert tools_cli.main(["--json", "check", "--detector", "routing", "--detector", "availability"]) == int(
        detection_error
    )
    assert seen == [{"command": "detect", "detector_ids": ["routing", "availability"]}, {"command": "verify"}]
    output = json.loads(expand_context(capsys.readouterr().out).splitlines()[0])
    assert output["command"] == "check"
    assert output["detection"] == detection
    assert all(output[key] == value for key, value in closure.items() if key != "command")
    assert output["healthy"] is False
    assert output["findings"] == closure["findings"]


def test_check_does_not_report_health_when_detection_failed(monkeypatch, capsys):
    responses = iter(
        [
            {"command": "detect", "error": "read unavailable", "summary": "read unavailable"},
            {"command": "verify", "healthy": True, "summary": "closure clear"},
        ]
    )
    monkeypatch.setattr(tools_cli, "_exchange", lambda *_: next(responses))
    assert tools_cli.main(["--json", "check"]) == 1
    output = json.loads(expand_context(capsys.readouterr().out).splitlines()[0])
    assert output["healthy"] is False
    assert output["error"] == "read unavailable"
    assert output["detection"]["error"] == "read unavailable"


@pytest.mark.parametrize("command", ["menu", "detect", "mitigate", "source"])
def test_cli_bounds_stdout_and_preserves_full_response_in_private_json(monkeypatch, capsys, tmp_path, command):
    response = {
        "command": command,
        "summary": "peer findings remain",
        "healthy": False,
        "targeted_healthy": True,
        "application_id": "application",
        "findings": [{"fingerprint": f"finding-{i}", "evidence": "large live observation " * 500} for i in range(12)],
        "report": {"status": "STATUS_UNRESOLVED", "details": "complete repair evidence " * 1000},
        "menu": {"round_limit": 3, "options": [{"option_id": "repair", "program_id": "repair"}]},
        "source": {"spec": {"arbitrary": "accepted intent " * 1000}},
    }
    if command == "mitigate":
        response.update(before_findings=[], controller_findings=response["findings"])
    monkeypatch.setattr(tools_cli, "_exchange", lambda *_: response)
    args = {
        "menu": ["menu"],
        "detect": ["detect"],
        "mitigate": ["mitigate", "repair"],
        "source": ["source", "Service/app/api"],
    }[command]

    assert tools_cli.main(args) == 0
    stdout = expand_context(capsys.readouterr().out)
    assert len(stdout) <= 3200
    if command == "mitigate":
        assert "This action: target finding cleared" in stdout
        assert "Incident: NOT verified, still broken: 12" in stdout
        assert len(stdout.encode()) <= 2049
    else:
        assert "healthy=false" in stdout.split()
        assert "targeted_healthy=true" in stdout.split()
        assert "application_id=application" in stdout.split()
        assert "findings=12" in stdout.split()
    assert "large live observation" not in stdout
    details = Path(stdout.split("Full JSON: ", 1)[1].splitlines()[0])
    try:
        assert details.stat().st_mode & 0o777 == 0o600
        full = json.loads(details.read_text())
        if command == "mitigate":
            assert full["findings"] == response["findings"]
            full = full["actions"][0]
        assert full == response
    finally:
        details.unlink()


def test_cli_persists_small_responses_too(monkeypatch, capsys):
    response = {"command": "verify", "healthy": True, "summary": "clear"}
    monkeypatch.setattr(tools_cli, "_exchange", lambda *_: response)
    assert tools_cli.main(["verify"]) == 0
    output = expand_context(capsys.readouterr().out)
    details = Path(output.split("Full JSON: ", 1)[1].splitlines()[0])
    try:
        assert "sdo verify: clear" in output
        assert "healthy=true" in output.split()
        assert json.loads(details.read_text()) == response
    finally:
        details.unlink()


@pytest.mark.parametrize("arguments", [["--json", "verify"], ["verify", "--json"]])
def test_json_flag_works_before_or_after_the_subcommand(monkeypatch, capsys, arguments):
    response = {"command": "verify", "healthy": False, "findings": [{"evidence": "full live fact"}]}
    monkeypatch.setattr(tools_cli, "_exchange", lambda *_: response)
    assert tools_cli.main(arguments) == 0
    assert json.loads(expand_context(capsys.readouterr().out)) == response


def test_act_full_json_preserves_existing_output_redaction(monkeypatch, capsys, tmp_path):
    from sdo.agents.responder import tool_output

    monkeypatch.setattr(tool_output, "OUTPUT_ROOT", tmp_path / "outputs")
    monkeypatch.setenv("SDO_INCIDENT_ID", "toy")
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path / "scratch"))
    monkeypatch.setattr(tools_cli, "_exchange", lambda *_: {"summary": "captured"})
    assert (
        tools_cli.main(
            [
                "act",
                "--resource",
                "Deployment/toy/api",
                "--summary",
                "Update secret-dependent settings",
                "--",
                sys.executable,
                "-c",
                "print('SYNTHETIC_PRIVATE_CANARY')",
            ]
        )
        == 0
    )
    text = expand_context(capsys.readouterr().out)
    details = Path(text.split("Full JSON: ", 1)[1].splitlines()[0])
    full = details.read_text()
    assert "SYNTHETIC_PRIVATE_CANARY" not in text + full
    assert json.loads(full)["actions"][0]["output"] == "[withheld]"
    assert json.loads(full)["actions"][0]["receipt"]["success"] is True


def test_client_side_failures_also_produce_compact_output_and_complete_json(monkeypatch, capsys):
    monkeypatch.setattr(tools_cli, "_exchange", lambda *_: {"error": "menu read failed"})
    assert tools_cli.main(["mitigate", "repair"]) == 1
    text = expand_context(capsys.readouterr().out)
    details = Path(text.split("Full JSON: ", 1)[1].splitlines()[0])
    try:
        assert json.loads(details.read_text())["actions"][0]["error"] == "menu read failed"
        assert "menu read failed" in text
    finally:
        details.unlink()


def test_act_does_not_bypass_pending_mutation_refusal(monkeypatch):
    called = []
    monkeypatch.setattr(
        tools_cli,
        "_exchange",
        lambda *_: {"error": "pending target", "report": {"reason_code": "pending_write_conflict"}},
    )
    monkeypatch.setattr(tools_cli.recorded_actions, "run_and_record", lambda *_args, **_kw: called.append(True) or 0)
    assert tools_cli.main(["act", "--resource", "Deployment/app/api", "--", "true"]) == 2
    assert not called


def test_verify_wait_uses_cached_controller_protocol(monkeypatch, capsys):
    seen = []
    monkeypatch.setattr(tools_cli, "_exchange", lambda request, _: seen.append(request) or {"summary": "cached"})
    assert tools_cli.main(["verify", "--wait", "0.25"]) == 0
    assert seen == [{"command": "verify", "wait_seconds": 0.25}]
    assert "cached" in expand_context(capsys.readouterr().out)


@pytest.mark.parametrize("value", ["-1", "nan", "inf", "86401"])
def test_verify_wait_rejects_invalid_timeouts(value, monkeypatch):
    monkeypatch.setattr(tools_cli, "_exchange", lambda *_: pytest.fail("invalid wait reached controller"))
    assert tools_cli.main(["verify", "--wait", value]) == 1


def test_mitigate_passes_structured_values_as_json_text(monkeypatch):
    seen = []
    monkeypatch.setattr(tools_cli, "_exchange", lambda request, _: seen.append(request) or {"summary": "ok"})
    roles = '[{"database": "app", "name": "readWrite"}]'
    assert tools_cli.main(["--json", "mitigate", "EnsureDatabaseUser", "--bind", "roles=" + roles]) == 0
    assert json.loads(seen[0]["targets"][0]["parameters"]["roles"]) == [{"database": "app", "name": "readWrite"}]
