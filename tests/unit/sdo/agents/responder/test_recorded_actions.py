from __future__ import annotations

import json
from pathlib import Path

import pytest
from agentshim.testing import FakeRun
from google.protobuf import json_format

from sdo.agents.responder import tools_cli
from sdo.agents.responder.codex import _responder_prompt, execute_incident
from sdo.contracts import IncidentRequest, IncidentResult
from tests.proto_messages import evolve
from tests.structured_turns import ScriptedAgent

_FIXTURES = Path(__file__).resolve().parents[4] / "fixtures" / "sdo" / "contracts"


def _request() -> IncidentRequest:
    document = json.loads((_FIXTURES / "incident_request.json").read_text(encoding="utf-8"))
    document["memory_snapshot"] = {
        "accepted_commit": document["repository_base_commit"],
        "goal_markdown": "Stay healthy.",
        "architecture_markdown": "# Architecture\nSource-grounded system.",
    }
    document["max_response_loops"] = 3
    document["repair_policy"] = "recorded-actions"
    return json_format.Parse(json.dumps(document), IncidentRequest())


def _recorded(scratch: Path) -> list[dict[str, object]]:
    path = scratch / "repair-actions.jsonl"
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


@pytest.fixture
def scratch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    directory = tmp_path / "scratch"
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(directory))
    return directory


def test_act_runs_one_mutation_and_records_its_receipt(scratch: Path, capsys: pytest.CaptureFixture[str]) -> None:
    code = tools_cli.main(
        [
            "act",
            "--resource",
            "Deployment/app/rate",
            "--resource",
            "ConfigMap/app/rate-config",
            "--summary",
            "Restored the accepted rate image",
            "--",
            "echo",
            "deployment.apps/rate image updated",
        ]
    )

    assert code == 0
    assert "deployment.apps/rate image updated" in capsys.readouterr().out
    (receipt,) = _recorded(scratch)
    assert receipt["action_id"] == "action-1"
    assert receipt["kind"] == "command-echo"
    assert receipt["target"] == "Deployment/app/rate"
    assert receipt["resources"] == [
        {"api_version": "apps/v1", "kind": "Deployment", "namespace": "app", "name": "rate"},
        {"api_version": "v1", "kind": "ConfigMap", "namespace": "app", "name": "rate-config"},
    ]
    assert receipt["summary"] == "Restored the accepted rate image"
    assert receipt["success"] is True
    assert receipt["reversible"] is True
    assert "$ echo 'deployment.apps/rate image updated'" in str(receipt["details"])
    assert receipt["started_at"] <= receipt["completed_at"]


def test_act_names_kubectl_verbs_and_numbers_actions(scratch: Path, tmp_path: Path) -> None:
    kubectl = tmp_path / "bin" / "kubectl"
    kubectl.parent.mkdir()
    kubectl.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    kubectl.chmod(0o755)

    for argv in (["-n", "app", "set", "image", "deployment/rate", "rate=img"], ["-n", "app", "apply", "-f", "x.yaml"]):
        assert tools_cli.main(["act", "--resource", "Deployment/app/rate", "--", str(kubectl), *argv]) == 0

    first, second = _recorded(scratch)
    assert (first["action_id"], first["kind"]) == ("action-1", "kubectl-set-image")
    assert (second["action_id"], second["kind"]) == ("action-2", "kubectl-apply")


def test_act_records_a_failed_mutation_and_propagates_its_exit_code(scratch: Path) -> None:
    code = tools_cli.main(["act", "--resource", "Service/app/api", "--", "sh", "-c", "echo boom >&2; exit 3"])

    assert code == 3
    (receipt,) = _recorded(scratch)
    assert receipt["success"] is False
    assert "boom" in str(receipt["details"])


def test_act_rejects_an_unknown_resource_kind_before_running_anything(scratch: Path) -> None:
    marker = scratch / "ran"
    with pytest.raises(SystemExit) as exit_info:
        tools_cli.main(["act", "--resource", "Widget/app/x", "--", "touch", str(marker)])

    assert exit_info.value.code == 2
    assert not marker.exists()
    assert not (scratch / "repair-actions.jsonl").exists()


def test_act_withholds_the_command_line_when_a_secret_is_involved(scratch: Path) -> None:
    assert tools_cli.main(["act", "--resource", "Secret/app/db", "--", "echo", "hunter2"]) == 0

    (receipt,) = _recorded(scratch)
    assert "hunter2" not in str(receipt["details"])
    assert receipt["details"]


def test_act_help_documents_the_mutation_form(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        tools_cli.main(["act", "--help"])

    assert "sdo act --resource" in capsys.readouterr().out


def _scripted_reply(result: IncidentResult, *, during_turn: list[str]) -> ScriptedAgent:
    message = {
        "type": "agent_message",
        "id": "msg",
        "text": json.dumps(json_format.MessageToDict(result, preserving_proto_field_name=True)),
    }

    def run(_request: object) -> FakeRun:
        for line in during_turn:
            assert tools_cli.main(["act", "--resource", line, "--", "true"]) == 0
        return FakeRun(
            stdout=[
                '{"type":"thread.started","thread_id":"s"}\n',
                json.dumps({"type": "item.completed", "item": message}) + "\n",
                '{"type":"turn.completed","usage":{"input_tokens":1,"cached_input_tokens":0,'
                '"cache_write_input_tokens":0,"output_tokens":1,"reasoning_output_tokens":0}}\n',
            ]
        )

    return ScriptedAgent(run)


def _bare_result() -> IncidentResult:
    document = json.loads((_FIXTURES / "incident_result.json").read_text(encoding="utf-8"))
    document["repair_actions"] = []
    document.pop("responder_session_id", None)
    return json_format.ParseDict(document, IncidentResult())


def test_recorded_actions_become_the_result_repair_actions(scratch: Path) -> None:
    request = _request()
    result = evolve(_bare_result(), incident_id=request.incident_id)
    agent = _scripted_reply(result, during_turn=["Service/app/geo", "Deployment/app/rate"])

    completed = execute_incident(request, model="m", executor=agent.executor)

    assert [action.target for action in completed.repair_actions] == ["Service/app/geo", "Deployment/app/rate"]
    assert [action.success for action in completed.repair_actions] == [True, True]
    assert completed.repair_actions[0].resources[0].api_version == "v1"


def test_stale_actions_from_an_earlier_session_are_dropped(scratch: Path) -> None:
    request = _request()
    scratch.mkdir(parents=True)
    (scratch / "repair-actions.jsonl").write_text('{"action_id": "stale"}\n', encoding="utf-8")
    result = evolve(_bare_result(), incident_id=request.incident_id)

    completed = execute_incident(request, model="m", executor=_scripted_reply(result, during_turn=[]).executor)

    assert list(completed.repair_actions) == []


def test_model_receipt_with_a_recorded_id_is_not_duplicated(scratch: Path) -> None:
    request = _request()
    fixture = json.loads((_FIXTURES / "incident_result.json").read_text(encoding="utf-8"))
    fixture["repair_actions"][0]["action_id"] = "action-1"
    fixture.pop("responder_session_id", None)
    fixture["incident_id"] = request.incident_id
    result = json_format.ParseDict(fixture, IncidentResult())
    agent = _scripted_reply(result, during_turn=["Service/app/geo"])

    completed = execute_incident(request, model="m", executor=agent.executor)

    assert [action.action_id for action in completed.repair_actions] == ["action-1"]


def test_prompt_routes_mutations_through_act_and_forbids_helper_scripts() -> None:
    prompt = _responder_prompt(_request())

    assert "sdo act" in prompt
    assert "repair-actions" not in prompt
    assert "parameterized script" not in prompt
    assert "Do not write helper scripts" in prompt
    assert "run one `sdo verify` and" in prompt


def test_i01_shaped_repair_and_verify_take_two_shell_calls_and_one_receipt(
    scratch: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Offline measure: the r6 i01 responder used 4 shell calls for this phase (restore script write and run,
    verify script write and run). With ``sdo act`` and a single ``sdo verify`` it is two."""

    log = tmp_path / "kubectl.log"
    kubectl = tmp_path / "bin" / "kubectl"
    kubectl.parent.mkdir()
    kubectl.write_text(f'#!/bin/sh\necho "$@" >> {log}\nexit 0\n', encoding="utf-8")
    kubectl.chmod(0o755)
    exchanges: list[str] = []
    monkeypatch.setattr(
        tools_cli,
        "_exchange",
        lambda request, _directory: exchanges.append(str(request["command"])) or {"healthy": True, "summary": "ok"},
    )
    shell_calls = 0

    def shell(argv: list[str]) -> int:
        nonlocal shell_calls
        shell_calls += 1
        return tools_cli.main(argv)

    assert (
        shell(
            [
                "act",
                "--resource",
                "Service/hotel-reservation/reservation",
                "--",
                str(kubectl),
                "-n",
                "hotel-reservation",
                "patch",
                "service",
                "reservation",
                "-p",
                '{"spec":{"ports":[{"port":8087,"targetPort":8087}]}}',
            ]
        )
        == 0
    )
    assert shell(["verify"]) == 0  # status_cli runs in the same shell call as `sdo verify`

    assert shell_calls == 2
    assert exchanges == ["verify"]
    assert "patch service reservation" in log.read_text(encoding="utf-8")
    (receipt,) = _recorded(scratch)
    assert receipt["kind"] == "kubectl-patch"
    assert receipt["resources"] == [
        {"api_version": "v1", "kind": "Service", "namespace": "hotel-reservation", "name": "reservation"}
    ]
    assert "targetPort" in str(receipt["details"])
    capsys.readouterr()
