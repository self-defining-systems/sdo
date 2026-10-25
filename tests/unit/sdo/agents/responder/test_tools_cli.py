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
        assert tools_cli.main(["diagnose", "targetport", "--bind", "service=Service/app/api"]) == 0
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

    assert tools_cli.main(["source", "Deployment/app/rate"]) == 0
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
        tools_cli.main([*command, "--help"])
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
        assert tools_cli.main(shlex.split(example)[1:]) == 0

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

    assert tools_cli.main(["diagnose", "targetport", "--bind", "service=Service/app/api"]) == 0

    output = json.loads(capsys.readouterr().out.splitlines()[0])
    assert output["program_runs"][0]["can_mitigate"] is can_mitigate


@pytest.mark.parametrize("raw", ["deploy/api", "StatefulSet/app/db", "Service/a; Service/b", "not a ref"])
def test_cli_passes_agent_ref_text_to_go_without_parsing(raw, monkeypatch):
    seen = []
    monkeypatch.setattr(tools_cli, "_exchange", lambda request, _: seen.append(request) or {"summary": "ok"})
    assert tools_cli.main(["source", raw]) == 0
    assert seen == [{"command": "source", "ref": raw}]
    assert tools_cli.main(["plan", "toy", "--bind", "resource=" + raw]) == 0
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
    result = tools_cli.main(["act", "--resource", "Service/app/api", "--", "true"])
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
    assert tools_cli.main(["mitigate", "route", "--rounds", "3"]) == 0
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
    assert tools_cli.main(["mitigate", "ScaleWorkload", "--ref", "Deployment/app/api", "--replicas", "3"]) == 0
    assert seen[-1]["parameters"] == {"ref": "Deployment/app/api", "replicas": "3"}
    assert seen[-1]["selection"]["round_limit"] == 1


def test_menu_does_not_gate_direct_act(monkeypatch):
    monkeypatch.setenv("SDO_MITIGATION_SESSION", "1")
    seen = []
    monkeypatch.setattr(tools_cli, "_exchange", lambda request, _directory: seen.append(request["command"]) or {})
    monkeypatch.setattr(tools_cli.recorded_actions, "run_and_record", lambda *_args, **_kwargs: 0)
    assert tools_cli.main(["act", "--resource", "Deployment/app/api", "--", "true"]) == 0
    assert seen == ["pre-apply"]


def test_detect_repeated_detector_selection(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[dict[str, object]] = []
    monkeypatch.setattr(tools_cli, "_exchange", lambda request, _: seen.append(request) or {"summary": "fresh"})
    assert tools_cli.main(["detect"]) == 0
    assert tools_cli.main(["detect", "--detector", "a", "b", "--detector", "c"]) == 0
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
    assert tools_cli.main([command, "repair", "--finding", "health/api/config", "--finding", "health/api/ready"]) == 0
    assert seen[-1]["finding_fingerprints"] == ["health/api/config", "health/api/ready"]
