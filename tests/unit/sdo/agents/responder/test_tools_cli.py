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
        assert request == {"command": "pre-apply"}
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
