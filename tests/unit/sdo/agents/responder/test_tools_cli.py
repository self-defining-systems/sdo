import json
import shlex
import subprocess
import sys
import threading
import time

import pytest

from sdo.agents.responder import tools_cli


def test_sdo_cli_exchanges_typed_request_with_fake_controller(tmp_path, monkeypatch, capsys):
    requests = tmp_path / "requests"
    responses = tmp_path / "responses"
    requests.mkdir()
    responses.mkdir()
    monkeypatch.setenv("SDO_TOOLS_DIR", str(tmp_path))
    seen = []

    def controller():
        for _ in range(100):
            files = [path for path in requests.glob("*.json") if not path.name.startswith(".")]
            if files:
                request = json.loads(files[0].read_text())
                seen.append(request)
                (responses / files[0].name).write_text(
                    json.dumps({"command": "diagnose", "summary": "one match", "program_runs": []})
                )
                return
            time.sleep(0.01)

    worker = threading.Thread(target=controller)
    worker.start()
    try:
        assert tools_cli.main(["--json", "diagnose", "targetport", "--bind", "service=Service/app/api"]) == 0
    finally:
        worker.join(timeout=2)
    assert seen == [
        {
            "command": "diagnose",
            "program_id": "targetport",
            "bindings": {"service": "Service/app/api"},
        }
    ]
    output = capsys.readouterr().out
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
    assert '"source": {"kind": "Deployment"}' in capsys.readouterr().out


@pytest.mark.parametrize("command", [[], ["source"], ["plan"]])
def test_cli_help_shows_concrete_source_and_named_binding_examples(command, capsys):
    with pytest.raises(SystemExit) as captured:
        tools_cli.main(["--json", *command, "--help"])
    assert captured.value.code == 0
    output = capsys.readouterr().out
    if command != ["plan"]:
        assert "sdo source Deployment/app/api" in output
    if command != ["source"]:
        assert "sdo plan targetport --bind service=Service/app/api" in output


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
        "command": "plan",
        "program_id": "targetport",
        "bindings": {"service": "Service/app/api"},
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

    output = json.loads(capsys.readouterr().out.splitlines()[0])
    assert output["program_runs"][0]["can_mitigate"] is can_mitigate


@pytest.mark.parametrize("raw", ["deploy/api", "StatefulSet/app/db", "Service/a; Service/b", "not a ref"])
def test_cli_passes_agent_ref_text_to_go_without_parsing(raw, monkeypatch):
    seen = []
    monkeypatch.setattr(tools_cli, "_exchange", lambda request, _: seen.append(request) or {"summary": "ok"})
    assert tools_cli.main(["--json", "source", raw]) == 0
    assert seen == [{"command": "source", "ref": raw}]
    assert tools_cli.main(["--json", "plan", "toy", "--bind", "resource=" + raw]) == 0
    assert seen[-1]["bindings"] == {"resource": raw}


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


def test_mitigate_option_uses_fresh_menu_and_incident_round_allowance(monkeypatch):
    seen = []
    menu = {
        "version": 1,
        "incident_id": "incident",
        "session_id": "session",
        "dispatch_epoch": "1",
        "menu_id": "fresh-menu",
        "source_commit": "source",
        "scope_digest": "scope",
        "policy_digest": "policy",
        "round_limit": 4,
        "remaining_batches": 4,
        "bindings": [],
        "options": [
            {
                "option_id": "route",
                "program_id": "routing",
                "program_revision": "revision",
                "dependency_revision": "dependencies",
                "bindings": [],
            }
        ],
    }

    def exchange(request, _directory):
        seen.append(request)
        return {"menu": menu, "summary": "menu"} if request["command"] == "menu" else {"summary": "report"}

    monkeypatch.setattr(tools_cli, "_exchange", exchange)
    assert tools_cli.main(["--json", "mitigate", "route", "--rounds", "3"]) == 0
    assert [request["command"] for request in seen] == ["menu", "mitigate"]
    assert seen[1]["selection"]["round_limit"] == 3
    assert seen[1]["selection"]["option_id"] == "route"
    assert seen[1]["selection"]["request_id"]


def test_mitigate_passes_named_parameters_for_controller_schema_parsing(monkeypatch):
    seen = []

    def exchange(request, _directory):
        seen.append(request)
        if request["command"] == "menu":
            return {"menu": {"round_limit": 5, "options": [{"option_id": "ScaleWorkload"}]}}
        return {"summary": "report"}

    monkeypatch.setattr(tools_cli, "_exchange", exchange)
    assert (
        tools_cli.main(["--json", "mitigate", "ScaleWorkload", "--ref", "Deployment/app/api", "--replicas", "3"]) == 0
    )
    assert seen[-1]["parameters"] == {"ref": "Deployment/app/api", "replicas": "3"}
    assert seen[-1]["selection"]["round_limit"] == 1


@pytest.mark.parametrize("error", [None, "unknown mitigation option"])
def test_mitigate_resolves_previously_advertised_id_in_controller_catalog(monkeypatch, error):
    seen = []
    advertised = "program-previous-explicit-bindings"

    def exchange(request, _directory):
        seen.append(request)
        if request["command"] == "menu":
            # The current presentation omits the previously advertised binding.
            return {"menu": {"remaining_batches": 3, "options": [{"option_id": "ScaleWorkload"}]}}
        assert request["selection"]["option_id"] == advertised
        assert request["selection"]["round_limit"] == 2
        return {"error": error} if error else {"summary": "applied retained option"}

    monkeypatch.setattr(tools_cli, "_exchange", exchange)
    assert tools_cli.main(["--json", "mitigate", advertised, "--rounds", "2"]) == int(error is not None)
    assert [request["command"] for request in seen] == ["menu", "mitigate"]


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


@pytest.mark.parametrize("command", ["plan", "mitigate"])
def test_repair_cli_preserves_explicit_target_finding_fingerprints(monkeypatch, command):
    seen = []

    def exchange(request, _directory):
        seen.append(request)
        return {
            "menu": {
                "round_limit": 3,
                "remaining_batches": 3,
                "options": [{"option_id": "repair", "program_id": "repair"}],
            },
            "summary": "ok",
        }

    monkeypatch.setattr(tools_cli, "_exchange", exchange)
    assert (
        tools_cli.main(["--json", command, "repair", "--finding", "health/api/config", "--finding", "health/api/ready"])
        == 0
    )
    assert seen[-1]["finding_fingerprints"] == ["health/api/config", "health/api/ready"]


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
    output = json.loads(capsys.readouterr().out.splitlines()[0])
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
    output = json.loads(capsys.readouterr().out.splitlines()[0])
    assert output["healthy"] is False
    assert output["error"] == "read unavailable"
    assert output["detection"]["error"] == "read unavailable"


@pytest.mark.parametrize("command", ["menu", "detect", "mitigate", "source"])
def test_cli_bounds_stdout_and_preserves_full_response_in_private_json(monkeypatch, capsys, tmp_path, command):
    from pathlib import Path

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
    stdout = capsys.readouterr().out
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
    from pathlib import Path

    response = {"command": "verify", "healthy": True, "summary": "clear"}
    monkeypatch.setattr(tools_cli, "_exchange", lambda *_: response)
    assert tools_cli.main(["verify"]) == 0
    output = capsys.readouterr().out
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
    assert json.loads(capsys.readouterr().out) == response


def test_act_full_json_preserves_existing_output_redaction(monkeypatch, capsys, tmp_path):
    from pathlib import Path

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
    text = capsys.readouterr().out
    details = Path(text.split("Full JSON: ", 1)[1].splitlines()[0])
    full = details.read_text()
    assert "SYNTHETIC_PRIVATE_CANARY" not in text + full
    assert json.loads(full)["actions"][0]["output"] == "[withheld]"
    assert json.loads(full)["actions"][0]["receipt"]["success"] is True


def test_client_side_failures_also_produce_compact_output_and_complete_json(monkeypatch, capsys):
    from pathlib import Path

    monkeypatch.setattr(tools_cli, "_exchange", lambda *_: {"error": "menu read failed"})
    assert tools_cli.main(["mitigate", "repair"]) == 1
    text = capsys.readouterr().out
    details = Path(text.split("Full JSON: ", 1)[1].splitlines()[0])
    try:
        assert json.loads(details.read_text())["actions"][0]["error"] == "menu read failed"
        assert "menu read failed" in text
    finally:
        details.unlink()


def test_mitigate_selection_set_has_independent_parameters(tmp_path, monkeypatch):
    selection = tmp_path / "selection.json"
    selection.write_text(
        json.dumps(
            {
                "version": 1,
                "options": [
                    {"option_id": "ScaleWorkload", "parameters": {"ref": "Deployment/app/a", "replicas": "2"}},
                    {"option_id": "ScaleWorkload", "parameters": {"ref": "Deployment/app/b", "replicas": "3"}},
                ],
            }
        )
    )
    seen = []
    monkeypatch.setattr(tools_cli, "_exchange", lambda request, _: seen.append(request) or {"summary": "ok"})
    assert tools_cli.main(["--json", "mitigate", "--selection-set", str(selection)]) == 0
    assert seen[0]["selection_set"]["options"][1]["parameters"]["replicas"] == "3"


def test_mitigate_repeated_options_group_parameters(monkeypatch):
    seen = []
    monkeypatch.setattr(tools_cli, "_exchange", lambda request, _: seen.append(request) or {"summary": "ok"})
    assert (
        tools_cli.main(
            [
                "--json",
                "mitigate",
                "--option",
                "ScaleWorkload",
                "--param",
                "ref=Deployment/app/a",
                "--param",
                "replicas=2",
                "--option",
                "ScaleWorkload",
                "--param",
                "ref=Deployment/app/b",
                "--param",
                "replicas=3",
            ]
        )
        == 0
    )
    assert seen[0]["selection_set"]["options"] == [
        {"option_id": "ScaleWorkload", "parameters": {"ref": "Deployment/app/a", "replicas": "2"}},
        {"option_id": "ScaleWorkload", "parameters": {"ref": "Deployment/app/b", "replicas": "3"}},
    ]


def _coverage_menu(*options):
    return {"menu": {"round_limit": 5, "remaining_batches": 5, "options": list(options)}}


def _drift_option(option_id, name, **extra):
    return {
        "option_id": option_id,
        "program_id": "deployment-source-drift",
        "bindings": [{"name": "workload", "object": {"kind": "Deployment", "namespace": "app", "name": name}}],
        **extra,
    }


def test_mitigate_match_applies_every_ready_option_of_a_playbook_in_one_call(monkeypatch):
    seen = []

    def exchange(request, _directory):
        seen.append(request)
        if request["command"] == "menu":
            return _coverage_menu(
                _drift_option("program-a", "api"),
                _drift_option("program-b", "web"),
                {"option_id": "program-other", "program_id": "other"},
            )
        return {"summary": "applied"}

    monkeypatch.setattr(tools_cli, "_exchange", exchange)
    assert tools_cli.main(["--json", "mitigate", "--match", "deployment-source-drift"]) == 0
    assert [request["command"] for request in seen] == ["menu", "mitigate"]
    assert seen[0]["program_id"] == "deployment-source-drift"
    assert seen[0]["match"] is True
    assert seen[1]["selection_set"] == {
        "version": 1,
        "options": [{"option_id": "program-a"}, {"option_id": "program-b"}],
    }


def test_mitigate_match_narrows_by_binding_and_skips_disabled_options(monkeypatch):
    seen = []

    def exchange(request, _directory):
        seen.append(request)
        if request["command"] == "menu":
            return _coverage_menu(
                _drift_option("program-b", "web"),
                _drift_option("program-c", "web", disabled_reason="blocked"),
            )
        return {"summary": "applied"}

    monkeypatch.setattr(tools_cli, "_exchange", exchange)
    argv = ["--json", "mitigate", "--match", "deployment-source-drift", "--bind", "workload=Deployment/app/web"]
    assert tools_cli.main(argv) == 0
    assert seen[0]["bindings"] == {"workload": "Deployment/app/web"}
    assert seen[1]["selection_set"]["options"] == [{"option_id": "program-b"}]


@pytest.mark.parametrize(
    ("options", "message"),
    [
        ([], "no ready option"),
        ([_drift_option("program-a", "api", disabled_reason="blocked")], "blocked"),
    ],
)
def test_mitigate_match_without_a_ready_option_fails_before_any_write(monkeypatch, capsys, options, message):
    seen = []

    def exchange(request, _directory):
        seen.append(request["command"])
        return _coverage_menu(*options)

    monkeypatch.setattr(tools_cli, "_exchange", exchange)
    assert tools_cli.main(["--json", "mitigate", "--match", "deployment-source-drift"]) == 1
    assert seen == ["menu"]
    assert message in capsys.readouterr().err


@pytest.mark.parametrize(
    "extra",
    [
        ["route"],
        ["--selection", "file.json"],
        ["--selection-set", "file.json"],
        ["--option", "route"],
        ["--param", "a=b"],
        ["--rounds", "2"],
    ],
)
def test_mitigate_match_rejects_hand_built_selection_inputs(monkeypatch, capsys, extra):
    seen = []
    monkeypatch.setattr(tools_cli, "_exchange", lambda request, _: seen.append(request) or {"summary": "ok"})
    assert tools_cli.main(["--json", "mitigate", "--match", "deployment-source-drift", *extra]) == 1
    assert seen == []
    assert "--match" in capsys.readouterr().err


def test_mitigate_match_is_discoverable_in_help_and_generated_surface(capsys):
    with pytest.raises(SystemExit):
        tools_cli.main(["mitigate", "--help"])
    assert "--match ID" in capsys.readouterr().out
    assert "--match" in tools_cli.command_surface()["commands"]["mitigate"]


def test_multi_option_help_and_generated_surface_are_discoverable(capsys):
    with pytest.raises(SystemExit):
        tools_cli.main(["mitigate", "--help"])
    help_text = capsys.readouterr().out
    assert "independent options" in help_text
    assert "overlapping" in help_text.lower()
    assert "independent options" in tools_cli.command_surface()["mitigation"]


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


def test_repeated_options_keep_finding_scope_per_option(monkeypatch):
    seen = []
    monkeypatch.setattr(tools_cli, "_exchange", lambda request, _: seen.append(request) or {"summary": "ok"})
    assert (
        tools_cli.main(
            [
                "--json",
                "mitigate",
                "--option",
                "one",
                "--finding",
                "fault-one",
                "--option",
                "two",
                "--finding",
                "fault-two",
            ]
        )
        == 0
    )
    assert [option["finding_fingerprints"] for option in seen[0]["selection_set"]["options"]] == [
        ["fault-one"],
        ["fault-two"],
    ]


def test_verify_wait_uses_cached_controller_protocol(monkeypatch, capsys):
    seen = []
    monkeypatch.setattr(tools_cli, "_exchange", lambda request, _: seen.append(request) or {"summary": "cached"})
    assert tools_cli.main(["verify", "--wait", "0.25"]) == 0
    assert seen == [{"command": "verify", "wait_seconds": 0.25}]
    assert "cached" in capsys.readouterr().out


@pytest.mark.parametrize("value", ["-1", "nan", "inf", "86401"])
def test_verify_wait_rejects_invalid_timeouts(value, monkeypatch):
    monkeypatch.setattr(tools_cli, "_exchange", lambda *_: pytest.fail("invalid wait reached controller"))
    assert tools_cli.main(["verify", "--wait", value]) == 1


def test_mitigation_help_preserves_database_reference_details(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as stopped:
        tools_cli.main(["mitigate", "--help"])
    assert stopped.value.code == 0
    help_text = " ".join(capsys.readouterr().out.split())
    assert "EnsureDatabaseUser" in help_text
    assert "config_map" in help_text
    assert "assignment" in help_text
    assert "complete findings/evidence" in help_text


@pytest.mark.parametrize("command", ["diagnose", "plan", "mitigate"])
def test_program_help_explains_current_checkout_and_revision(command, capsys):
    with pytest.raises(SystemExit) as stopped:
        tools_cli.main([command, "--help"])
    assert stopped.value.code == 0
    output = " ".join(capsys.readouterr().out.split())
    assert "current checkout" in output
    assert "compile" in output
    assert "working-tree:" in output


def test_mitigate_preserves_actionable_program_compile_error(monkeypatch, capsys):
    message = "playbook broken compile failed: undefined: Missing; fix checkout source and retry"
    monkeypatch.setattr(
        tools_cli,
        "_exchange",
        lambda *_: {
            "menu": {
                "round_limit": 5,
                "options": [{"option_id": "program-broken", "program_id": "broken", "disabled_reason": message}],
            }
        },
    )
    assert tools_cli.main(["--json", "mitigate", "broken"]) == 1
    assert message in capsys.readouterr().err


def test_selection_set_accepts_structured_parameters(tmp_path):
    path = tmp_path / "selection.json"
    parameters = {
        "target": {
            "engine": "mongodb",
            "pod": {"api_version": "v1", "kind": "Pod", "namespace": "app", "name": "db"},
            "container": "mongo",
            "database": "app",
        },
        "roles": [{"database": "app", "name": "readWrite"}],
        "password": {"config_map": {"name": "bootstrap", "key": "init.sh", "assignment": "PASSWORD"}},
        "allow_empty": False,
        "port": 80,
    }
    path.write_text(
        json.dumps({"version": 1, "options": [{"option_id": "EnsureDatabaseUser", "parameters": parameters}]})
    )
    result = tools_cli._selection_set(path)["options"][0]["parameters"]
    for name, value in parameters.items():
        assert json.loads(result[name]) == value


def test_selection_set_reports_field_shape_without_echoing_values(tmp_path):
    path = tmp_path / "selection.json"
    path.write_text(
        json.dumps({"version": 1, "options": [{"option_id": "EnsureDatabaseUser", "parameters": ["PRIVATE_CANARY"]}]})
    )
    with pytest.raises(ValueError, match="expected object") as error:
        tools_cli._selection_set(path)
    assert "options[0].parameters" in str(error.value)
    assert "object" in str(error.value)
    assert "PRIVATE_CANARY" not in str(error.value)
