from __future__ import annotations

import contextlib
import json
import logging
import os
import re
import shlex
import signal
import subprocess
import sys
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from google.protobuf import json_format
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from benchmarks.sregym.protocol import validate_production_receipt
from sdo.agents.responder import recorded_actions, tools_cli
from sdo.agents.responder.codex import ResponderExecutionError, execute_incident
from sdo.agents.responder.credentials import SKILLS_ROOT
from sdo.agents.responder.reflection_brief import responder_shell_commands
from sdo.contracts import IncidentRequest, IncidentResult
from tests.context_helpers import expand_context
from tests.proto_messages import evolve
from tests.structured_turns import ScriptedAgent, reply
from tests.unit.sdo.agents.prompts.cases import responder_prompt as _responder_prompt

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    from agentshim.testing import FakeRun

_FIXTURES = Path(__file__).resolve().parents[4] / "fixtures" / "sdo" / "contracts"
_OBJECT_REF_GO = Path(__file__).resolve().parents[5] / "sdo" / "operator" / "sdk" / "object_ref.go"


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


def _records(scratch: Path) -> list[dict[str, object]]:
    path = scratch / "repair-actions.jsonl"
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def _recorded(scratch: Path) -> list[dict[str, object]]:
    """The final receipt of each recorded action, in order."""

    last: dict[str, dict[str, object]] = {}
    for record in _records(scratch):
        last[record["receipt"]["action_id"]] = record["receipt"]  # type: ignore[index]
    return list(last.values())


@pytest.fixture
def scratch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    directory = tmp_path / "scratch"
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(directory))
    monkeypatch.setattr(tools_cli, "_exchange", lambda _request, _directory: {"summary": "captured"})
    return directory


def test_act_runs_one_mutation_and_records_its_receipt(scratch: Path, capsys: pytest.CaptureFixture[str]) -> None:
    code = tools_cli.main(
        [
            "--json",
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
    printed = json.loads(expand_context(capsys.readouterr().out))
    full = json.loads(Path(printed["details_file"]).read_text())
    assert "deployment.apps/rate image updated" in full["actions"][0]["output"]
    (receipt,) = _recorded(scratch)
    assert str(receipt["action_id"]).startswith("sdo-act-")
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


def test_act_names_kubectl_verbs_and_gives_each_action_its_own_id(scratch: Path, tmp_path: Path) -> None:
    kubectl = tmp_path / "bin" / "kubectl"
    kubectl.parent.mkdir()
    kubectl.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    kubectl.chmod(0o755)

    for argv in (["-n", "app", "set", "image", "deployment/rate", "rate=img"], ["-n", "app", "apply", "-f", "x.yaml"]):
        assert tools_cli.main(["act", "--resource", "Deployment/app/rate", "--", str(kubectl), *argv]) == 0

    first, second = _recorded(scratch)
    assert (first["kind"], second["kind"]) == ("kubectl-set-image", "kubectl-apply")
    assert first["action_id"] != second["action_id"]


def test_act_records_a_failed_mutation_and_propagates_its_exit_code(scratch: Path) -> None:
    code = tools_cli.main(["act", "--resource", "Service/app/api", "--", "sh", "-c", "echo boom >&2; exit 3"])

    assert code == 3
    (receipt,) = _recorded(scratch)
    assert receipt["success"] is False
    assert "boom" in str(receipt["details"])


@pytest.mark.parametrize(
    "failure", [OSError("tools unavailable"), TimeoutError("no controller"), ValueError("invalid response")]
)
@pytest.mark.parametrize("exit_code", [0, 7])
def test_act_capture_transport_failure_keeps_command_exit_code_and_receipt(
    scratch: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    failure: Exception,
    exit_code: int,
) -> None:
    def exchange(_request: object, _directory: Path) -> dict[str, object]:
        raise failure

    monkeypatch.setattr(tools_cli, "_exchange", exchange)
    assert (
        tools_cli.main(
            ["act", "--resource", "Service/app/api", "--", sys.executable, "-c", f"raise SystemExit({exit_code})"]
        )
        == exit_code
    )
    (receipt,) = _recorded(scratch)
    assert receipt["success"] is (exit_code == 0)
    assert "pre-apply evidence gap" in capsys.readouterr().err


def test_act_rejects_a_malformed_resource_before_running_anything(scratch: Path) -> None:
    marker = scratch / "ran"
    with pytest.raises(SystemExit) as exit_info:
        tools_cli.main(["act", "--resource", "not-a-ref", "--", "touch", str(marker)])

    assert exit_info.value.code == 2
    assert not marker.exists()
    assert not (scratch / "repair-actions.jsonl").exists()


def test_act_accepts_a_cluster_scoped_kind_the_go_table_does_not_know(scratch: Path) -> None:
    assert tools_cli.main(["act", "--resource", "PersistentVolume/data", "--", "true"]) == 0

    (receipt,) = _recorded(scratch)
    assert receipt["resources"] == [{"api_version": "", "kind": "PersistentVolume", "namespace": "", "name": "data"}]
    assert receipt["target"] == "PersistentVolume/data"


@pytest.mark.parametrize("verb", ["patch", "delete"])
@pytest.mark.parametrize("combined", [False, True])
@pytest.mark.parametrize(("kind", "namespace"), [("PersistentVolume", ""), ("Widget", "app")])
def test_act_attributes_unknown_kinds_independent_of_kubectl_case(
    scratch: Path, tmp_path: Path, verb: str, combined: bool, kind: str, namespace: str
) -> None:
    kubectl = tmp_path / "kubectl"
    kubectl.write_text("#!/bin/sh\nexit 0\n")
    kubectl.chmod(0o755)
    reference = "/".join(part for part in (kind, namespace, "data") if part)
    target = [kind.lower() + "/data"] if combined else [kind.lower(), "data"]
    scope = ["-n", namespace] if namespace else []

    peer = "/".join(part for part in (kind, namespace, "peer") if part)
    assert (
        tools_cli.main(["act", "--resource", reference, "--resource", peer, "--", str(kubectl), *scope, verb, *target])
        == 0
    )

    (receipt,) = _recorded(scratch)
    assert receipt["resources"] == [recorded_actions.resource_ref(reference)]
    assert receipt["target"] == reference
    assert receipt["success"] is True


def test_act_warns_about_a_kubectl_object_no_resource_covers(
    scratch: Path, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    kubectl = tmp_path / "bin" / "kubectl"
    kubectl.parent.mkdir()
    kubectl.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    kubectl.chmod(0o755)

    code = tools_cli.main(
        ["act", "--resource", "Deployment/app/api", "--", str(kubectl), "-n", "app", "delete", "svc/geo", "deploy/api"]
    )

    assert code == 0  # a warning, not a refusal
    stderr = capsys.readouterr().err
    assert "svc/geo" in stderr
    assert "deploy/api" not in stderr
    assert len(_recorded(scratch)) == 1


def test_undecodable_redacted_command_is_rejected_before_execution(scratch: Path, tmp_path: Path) -> None:
    marker = tmp_path / "executed"
    command = tmp_path / "secret-command"
    command.write_text(f"#!/bin/sh\ntouch {shlex.quote(str(marker))}\n", encoding="utf-8")
    command.chmod(0o755)
    with pytest.raises(ValueError, match="cannot record an invalid repair receipt"):
        recorded_actions.run_and_record([str(command), "\udc80"], [recorded_actions.resource_ref("Deployment/app/api")])
    assert not marker.exists()
    assert not recorded_actions.actions_path().exists()


def test_act_withholds_the_command_line_when_a_secret_is_involved(scratch: Path) -> None:
    assert tools_cli.main(["act", "--resource", "Secret/app/db", "--", "echo", "hunter2"]) == 0

    (receipt,) = _recorded(scratch)
    assert "hunter2" not in str(receipt["details"])
    assert receipt["details"]


@pytest.mark.parametrize(
    ("resource", "command", "summary"),
    [
        ("Deployment/app/api", ["echo", "create", "secret", "generic", "db", "--from-literal=pw=hunter2"], None),
        ("Deployment/app/api", ["echo", "get", "secret/db", "hunter2"], None),
        ("Deployment/app/api", ["echo", "create", "configmap", "c", "--from-file=pw=hunter2"], None),
        ("Deployment/app/api", ["echo", "-n", "app", "set", "env", "deployment/api", "DB_PASSWORD=hunter2"], None),
        (
            "Deployment/app/api",
            [
                "echo",
                "patch",
                "deployment",
                "api",
                "-p",
                '{"spec":{"template":{"spec":{"containers":[{"env":[{"name":"X","value":"hunter2"}]}]}}}}',
            ],
            None,
        ),
        ("Deployment/app/api", ["echo", "ok"], "set DB_PASSWORD=hunter2 on api"),
        (
            "Deployment/app/api",
            [
                "echo",
                "patch",
                "deployment",
                "api",
                "--type=json",
                "-p",
                '[{"op":"add","path":"/spec/template/spec/containers/0/env/-","value":{"name":"X","value":"hunter2"}}]',
            ],
            None,
        ),  # fmt: skip
        ("ConfigMap/app/c", ["echo", "patch", "configmap", "c", "-p", '{"data":{"DB_PASSWORD":"hunter2"}}'], None),
        ("ConfigMap/app/c", ["echo", "patch", "configmap", "c", "-p", '{"data":{"api_key": "hunter2"}}'], None),
        ("ServiceAccount/app/sa", ["echo", "-n", "app", "create", "token", "sa", "hunter2"], None),
        ("Pod/app/api", ["echo", "exec", "api", "--", "printenv", "hunter2"], None),
        ("Pod/app/api", ["echo", "exec", "api", "--", "env", "hunter2"], None),
    ],
)
def test_act_withholds_command_and_output_that_may_carry_secret_values(
    scratch: Path, resource: str, command: list[str], summary: str | None
) -> None:
    arguments = ["act", "--resource", resource, *(["--summary", summary] if summary else []), "--", *command]

    assert tools_cli.main(arguments) == 0

    (receipt,) = _recorded(scratch)
    assert "hunter2" not in json.dumps(receipt)
    assert "withheld" in str(receipt["details"])


def test_act_keeps_the_command_line_of_an_ordinary_mutation(scratch: Path) -> None:
    assert tools_cli.main(["act", "--resource", "Deployment/app/api", "--", "echo", "scale", "--replicas=2"]) == 0

    (receipt,) = _recorded(scratch)
    assert "--replicas=2" in str(receipt["details"])


def test_reflection_shell_commands_withhold_sensitive_sdo_act_lines(tmp_path: Path) -> None:
    log = tmp_path / "usage.jsonl"
    lines = [
        "python tools_cli.py act --resource Pod/app/api -- kubectl exec api -- printenv X=hunter2",
        "uv run python sdo/agents/responder/tools_cli.py act --resource Pod/app/api -- kubectl exec api -- env hunter2",
        "sdo act --resource Deployment/app/api -- kubectl -n app set env deployment/api DB_PASSWORD=hunter2",
        "sdo act --resource Deployment/app/api -- kubectl -n app scale deployment/api --replicas=2",
        "kubectl get secret db -o yaml",
    ]
    log.write_text(json.dumps({"session_id": "s", "shell_command_lines": lines}) + "\n", encoding="utf-8")

    commands = responder_shell_commands(log, "s")

    assert commands is not None
    assert "hunter2" not in json.dumps(commands)
    assert commands[0].startswith("sdo act [command withheld")
    assert commands[1].startswith("sdo act [command withheld")
    assert commands[3] == lines[3]
    assert commands[4] == lines[4]  # only recorded `sdo act` lines are scrubbed


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("Deployment/app/api", {"api_version": "apps/v1", "kind": "Deployment", "namespace": "app", "name": "api"}),
        ("deploy/app/api", {"api_version": "apps/v1", "kind": "Deployment", "namespace": "app", "name": "api"}),
        (
            "Role/app/reader",
            {"api_version": "rbac.authorization.k8s.io/v1", "kind": "Role", "namespace": "app", "name": "reader"},
        ),
        ("PersistentVolume/data", {"api_version": "", "kind": "PersistentVolume", "namespace": "", "name": "data"}),
        ("Node/worker-1", {"api_version": "", "kind": "Node", "namespace": "", "name": "worker-1"}),
        (
            "deployments.apps/app/api",
            {"api_version": "apps/v1", "kind": "Deployment", "namespace": "app", "name": "api"},
        ),
        ("app/Deployment/api", {"api_version": "apps/v1", "kind": "Deployment", "namespace": "app", "name": "api"}),
    ],
)
def test_resource_refs_follow_the_go_object_ref_grammar(value: str, expected: dict[str, str]) -> None:
    assert recorded_actions.resource_ref(value) == expected


@pytest.mark.parametrize(
    "value", ["", "Deployment", "a/b/c/d", "Deployment/app/", "Deployment/a b/api", "Deployment/app/x;y"]
)
def test_malformed_resource_refs_are_rejected(value: str) -> None:
    with pytest.raises(ValueError, match="Kind/name"):
        recorded_actions.resource_ref(value)


def _go_tables() -> tuple[dict[str, str], dict[str, str]]:
    source = _OBJECT_REF_GO.read_text(encoding="utf-8")
    kinds_block = source[source.index("var objectKinds") : source.index("// CanonicalObjectKind")]
    kinds = dict(re.findall(r'"([a-z]+)": "([A-Za-z]+)"', kinds_block))
    # Every `"alias": "Kind"` pair must match the strict pattern, or an odd alias would vanish from the comparison.
    assert len(kinds) == len(re.findall(r'"[^"]*"\s*:\s*"[^"]*"', kinds_block))
    versions_block = source[source.index("func objectAPIVersion") : source.index("func refComponent")]
    versions: dict[str, str] = {}
    for names, version in re.findall(r'case ((?:"[A-Za-z]+",? ?)+):\s*return "([^"]+)"', versions_block):
        versions.update(dict.fromkeys(re.findall(r'"([A-Za-z]+)"', names), version))
    return kinds, versions


def test_the_python_ref_tables_equal_the_go_object_ref_tables() -> None:
    kinds, versions = _go_tables()

    assert kinds
    assert kinds == recorded_actions.OBJECT_KINDS
    assert versions == recorded_actions.API_VERSIONS


def test_act_help_documents_the_mutation_form(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        tools_cli.main(["act", "--help"])

    assert "sdo act --resource" in expand_context(capsys.readouterr().out)


def _scripted_reply(result: IncidentResult, *, during_turn: list[str], raw_lines: Sequence[str] = ()) -> ScriptedAgent:
    output = json_format.MessageToDict(result, preserving_proto_field_name=True)

    def run(_request: object) -> FakeRun:
        for line in during_turn:
            assert tools_cli.main(["act", "--resource", line, "--", "true"]) == 0
        if raw_lines:
            recorded_actions.actions_path().parent.mkdir(parents=True, exist_ok=True)
            with recorded_actions.actions_path().open("a", encoding="utf-8") as stream:
                stream.writelines(f"{raw}\n" for raw in raw_lines)
        return reply("codex", output, session_id="s", branch="incident_result")

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


def test_a_receipt_the_model_returns_never_replaces_a_recorded_one(scratch: Path) -> None:
    request = _request()
    fixture = json.loads((_FIXTURES / "incident_result.json").read_text(encoding="utf-8"))
    fixture.pop("responder_session_id", None)
    fixture["incident_id"] = request.incident_id
    result = json_format.ParseDict(fixture, IncidentResult())
    model_ids = [action.action_id for action in result.repair_actions]
    agent = _scripted_reply(result, during_turn=["Service/app/geo"])

    completed = execute_incident(request, model="m", executor=agent.executor)

    recorded = [action for action in completed.repair_actions if action.action_id not in model_ids]
    assert [action.target for action in recorded] == ["Service/app/geo"]
    assert [action.action_id for action in completed.repair_actions][: len(model_ids)] == model_ids


def test_prompt_routes_mutations_through_the_recorder_and_leaves_receipts_to_it() -> None:
    prompt = _responder_prompt(_request())
    assert "sdo-incident-response/SKILL.md" in prompt
    workflow = (SKILLS_ROOT / "sdo-incident-response/SKILL.md").read_text(encoding="utf-8")
    assert "Use recorded actions for live mutations" in workflow
    assert "declare every changed resource" in workflow
    assert "never hand-write receipts" in workflow


def test_the_prompt_example_is_a_command_the_cli_accepts() -> None:
    parsed = tools_cli._parser().parse_args(shlex.split(tools_cli.ACT_EXAMPLE)[1:])

    assert parsed.command == "act"
    assert parsed.resource == [recorded_actions.resource_ref("Deployment/app/api")]
    assert parsed.act_command[0] == "--"


def _stub(directory: Path, body: str) -> Path:
    directory.mkdir(exist_ok=True)
    stub = directory / "kubectl"
    stub.write_text(f"#!/bin/sh\n{body}\n", encoding="utf-8")
    stub.chmod(0o755)
    return stub


@pytest.fixture
def controller_tools(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    tools = tmp_path / "tools"
    requests, responses = tools / "requests", tools / "responses"
    requests.mkdir(parents=True)
    responses.mkdir()
    monkeypatch.setenv("SDO_TOOLS_DIR", str(tools))
    stop = threading.Event()

    def serve() -> None:
        while not stop.wait(0.005):
            for path in requests.glob("*.json"):
                if path.name.startswith("."):
                    continue
                assert json.loads(path.read_text())["command"] == "pre-apply"
                (responses / path.name).write_text(json.dumps({"summary": "captured"}))
                path.unlink()

    thread = threading.Thread(target=serve)
    thread.start()
    try:
        yield tools
    finally:
        stop.set()
        thread.join(timeout=2)


def test_three_parallel_acts_leave_three_receipts_that_all_reach_the_result(
    scratch: Path, tmp_path: Path, controller_tools: Path
) -> None:
    kubectl = _stub(tmp_path / "bin", "sleep 0.3")
    command = [sys.executable, "-I", "-m", "sdo.agents.responder.tools_cli"]
    env = {**os.environ, "SDO_SESSION_SCRATCH": str(scratch), "PYTHONPATH": str(Path(__file__).resolve().parents[5])}

    processes = [
        subprocess.Popen(
            [*command, "act", "--resource", f"Service/app/s{index}", "--", str(kubectl), "-n", "app", "get", "svc"],
            env=env,
        )
        for index in range(3)
    ]
    assert [process.wait(timeout=30) for process in processes] == [0, 0, 0]

    result = evolve(_bare_result())
    recorded_actions.merge_into(result)
    assert sorted(action.target for action in result.repair_actions) == [
        "Service/app/s0",
        "Service/app/s1",
        "Service/app/s2",
    ]
    assert len({action.action_id for action in result.repair_actions}) == 3


def _wait_for(predicate: object, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():  # type: ignore[operator]
            return
        time.sleep(0.05)
    raise AssertionError("timed out waiting for the condition")


def _is_dead(pid: int) -> bool:
    try:
        return Path(f"/proc/{pid}/stat").read_text(encoding="utf-8").rsplit(")", 1)[1].split()[0] == "Z"
    except (FileNotFoundError, ProcessLookupError):
        return True


def test_a_sigterm_mid_command_leaves_a_finished_receipt_and_no_orphans(
    scratch: Path, tmp_path: Path, controller_tools: Path
) -> None:
    pid_file = tmp_path / "grandchild.pid"
    kubectl = _stub(tmp_path / "bin", f"sleep 30 &\necho $! > {pid_file}\nwait")
    env = {**os.environ, "SDO_SESSION_SCRATCH": str(scratch), "PYTHONPATH": str(Path(__file__).resolve().parents[5])}
    process = subprocess.Popen(
        [
            sys.executable, "-I", "-m", "sdo.agents.responder.tools_cli",
            "act", "--resource", "Deployment/app/api", "--",
            str(kubectl), "-n", "app", "rollout", "restart", "deploy/api",
        ],
        env=env,
        start_new_session=True,
    )  # fmt: skip
    path = scratch / "repair-actions.jsonl"
    _wait_for(lambda: pid_file.exists() and pid_file.read_text(encoding="utf-8").strip())
    grandchild = int(pid_file.read_text(encoding="utf-8"))
    try:
        process.send_signal(signal.SIGTERM)
        process.wait(timeout=20)

        _wait_for(lambda: _is_dead(grandchild), timeout=5.0)  # the command's own children died too
        records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        assert records[-1]["state"] == "finished"
        result = evolve(_bare_result())
        recorded_actions.merge_into(result)
        (action,) = result.repair_actions
        assert action.success is False
        assert "interrupted" in action.details
        assert action.kind == "kubectl-rollout-restart"
    finally:
        with contextlib.suppress(ProcessLookupError):
            os.kill(grandchild, signal.SIGKILL)
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)


def test_a_started_only_entry_is_a_recorded_unfinished_mutation(scratch: Path) -> None:
    started = {
        "action_id": "sdo-act-x",
        "kind": "kubectl-patch",
        "target": "Service/app/api",
        "resources": [{"api_version": "v1", "kind": "Service", "namespace": "app", "name": "api"}],
        "summary": "patched",
        "details": "$ kubectl patch\ninterrupted before the command finished",
        "started_at": "2026-10-22T10:00:00.000Z",
        "completed_at": "2026-10-22T10:00:00.000Z",
        "success": False,
        "reversible": True,
    }
    scratch.mkdir(parents=True)
    (scratch / "repair-actions.jsonl").write_text(
        json.dumps({"state": "started", "receipt": started}) + "\n", encoding="utf-8"
    )
    result = evolve(_bare_result())

    recorded_actions.merge_into(result)

    (action,) = result.repair_actions
    assert (action.action_id, action.success) == ("sdo-act-x", False)
    assert "interrupted" in action.details


def test_a_torn_line_is_skipped_with_a_warning(scratch: Path, caplog: pytest.LogCaptureFixture) -> None:
    assert tools_cli.main(["act", "--resource", "Service/app/api", "--", "true"]) == 0
    with (scratch / "repair-actions.jsonl").open("a", encoding="utf-8") as stream:
        stream.write('{"state": "started", "receipt": {"action_id": "sdo-act-to')
    result = evolve(_bare_result())

    with caplog.at_level(logging.WARNING):
        recorded_actions.merge_into(result)

    assert [action.target for action in result.repair_actions] == ["Service/app/api"]
    assert "torn line" in caplog.text


def test_an_invalid_recorded_receipt_fails_the_incident_with_a_typed_error(scratch: Path) -> None:
    request = _request()
    bad = json.dumps({"state": "finished", "receipt": {"action_id": "sdo-act-bad", "success": "yes"}})
    agent = _scripted_reply(evolve(_bare_result(), incident_id=request.incident_id), during_turn=[], raw_lines=[bad])

    with pytest.raises(ResponderExecutionError, match="sdo-act-bad"):
        execute_incident(request, model="m", executor=agent.executor)


def test_an_sdo_act_receipt_passes_the_strict_production_receipt_validation(scratch: Path, tmp_path: Path) -> None:
    kubectl = _stub(tmp_path / "bin", "exit 0")
    assert (
        tools_cli.main(["act", "--resource", "Service/app/api", "--", str(kubectl), "-n", "app", "patch", "svc", "api"])
        == 0
    )
    result = evolve(_bare_result())
    recorded_actions.merge_into(result)
    actions = [json_format.MessageToDict(action, preserving_proto_field_name=True) for action in result.repair_actions]
    fixture = json.loads(
        (_FIXTURES.parents[1] / "sregym" / "phase1_rejected_receipts" / "b2.json").read_text(encoding="utf-8")
    )["receipt"]
    receipt = {
        **fixture,
        "recovery_attribution": "responder",
        "repair_actions": actions,
        "same_session_reflection": True,
        "reflection_commit": "reflection",
        "validator_evidence_commit": "reflection",
    }

    validate_production_receipt(receipt)


def test_a_repair_and_verify_leave_one_receipt_and_run_the_stubbed_tools(
    scratch: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    log = tmp_path / "kubectl.log"
    kubectl = _stub(tmp_path / "bin", f'printf "%s\\n" "$*" >> {log}')
    exchanges: list[str] = []
    monkeypatch.setattr(
        tools_cli,
        "_exchange",
        lambda request, _directory: exchanges.append(str(request["command"])) or {"healthy": True, "summary": "ok"},
    )
    patch = '{"spec":{"ports":[{"port":8087,"targetPort":8087}]}}'

    assert (
        tools_cli.main(
            ["act", "--resource", "Service/hotel/reservation", "--", str(kubectl), "-n", "hotel", "patch", "service",
             "reservation", "-p", patch]
        )
        == 0
    )  # fmt: skip
    assert tools_cli.main(["verify"]) == 0

    assert exchanges == ["pre-apply", "verify"]
    assert log.read_text(encoding="utf-8").splitlines() == [f"-n hotel patch service reservation -p {patch}"]
    (receipt,) = _recorded(scratch)
    assert receipt["kind"] == "kubectl-patch"
    assert receipt["resources"] == [
        {"api_version": "v1", "kind": "Service", "namespace": "hotel", "name": "reservation"}
    ]
    assert "targetPort" in str(receipt["details"])


_OBJECT_REF_CORPUS = Path(__file__).resolve().parents[4] / "fixtures" / "sdo" / "contracts" / "object_ref_grammar.json"


@pytest.mark.parametrize(
    "case", json.loads(_OBJECT_REF_CORPUS.read_text(encoding="utf-8")), ids=lambda c: repr(c["input"])
)
def test_resource_ref_matches_the_shared_object_ref_corpus(case: dict[str, Any]) -> None:
    # The Go test TestObjectRefGrammarCorpus checks the same file against ParseObjectRef.
    if case["python"] is None:
        with pytest.raises(ValueError, match="Kind/name"):
            recorded_actions.resource_ref(case["input"])
    else:
        assert recorded_actions.resource_ref(case["input"]) == case["python"]


def test_command_output_that_is_not_utf8_is_still_recorded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    resources = [recorded_actions.resource_ref("Deployment/app/api")]
    code = recorded_actions.run_and_record(["printf", r"\377ok"], resources)
    assert code == 0
    [receipt] = recorded_actions._read_receipts(recorded_actions.actions_path())
    assert receipt.success
    assert "ok" in receipt.details


# argv from the OS can carry undecodable bytes, which Python represents as lone surrogates.
_ARGV_TEXT = st.text(max_size=40) | st.text(
    alphabet=st.characters(min_codepoint=0xDC80, max_codepoint=0xDCFF), max_size=5
)


@settings(max_examples=300, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(
    command=st.lists(_ARGV_TEXT, min_size=1, max_size=8),
    output=st.text(max_size=2000),
    summary=st.none() | st.text(max_size=80),
    code=st.none() | st.integers(-300, 300),
)
def test_arbitrary_command_text_round_trips_through_the_recorded_receipts(
    tmp_path: Path, command: list[str], output: str, summary: str | None, code: int | None
) -> None:
    # `sdo act` records whatever argv and output the responder produced; the record must load back as one receipt.
    path = tmp_path / "repair-actions.jsonl"
    path.unlink(missing_ok=True)
    resources = [recorded_actions.resource_ref("Deployment/app/api")]
    now = datetime.now(UTC)

    def build() -> dict[str, Any]:
        return recorded_actions._receipt(
            "sdo-act-1",
            command,
            resources,
            summary=summary,
            reversible=True,
            started=now,
            finished=now,
            code=code,
            output=output,
        )

    if any(re.search("[\udc80-\udcff]", token) for token in command):
        # Undecodable argv is refused before the command runs.
        with pytest.raises(ValueError, match="cannot record an invalid repair receipt"):
            build()
        return
    receipt = build()
    recorded_actions._append(path, "finished", receipt)
    [loaded] = recorded_actions._read_receipts(path)
    assert loaded.action_id == "sdo-act-1"
    assert loaded.success == (code == 0)


def test_delete_receipt_does_not_claim_extra_existing_resources(scratch: Path, tmp_path: Path) -> None:
    kubectl = tmp_path / "kubectl"
    kubectl.write_text(
        '#!/bin/sh\ncase " $* " in\n'
        "*' get '*) echo serviceaccount/cleanup;;\n"
        "*) echo deployment.apps/cleanup deleted;; esac\n"
    )
    kubectl.chmod(0o755)
    tools_cli.main(
        [
            "act",
            "--resource",
            "Deployment/app/cleanup",
            "--resource",
            "ServiceAccount/app/cleanup",
            "--",
            str(kubectl),
            "delete",
            "deployment/cleanup",
            "-n",
            "app",
        ]
    )
    (receipt,) = _recorded(scratch)
    assert receipt["success"] is False
    assert receipt["failure_reason"] == "delete_not_confirmed"


def test_patch_receipt_attributes_only_the_executed_target(scratch: Path, tmp_path: Path) -> None:
    kubectl = tmp_path / "kubectl"
    kubectl.write_text("#!/bin/sh\necho configmap/token patched\n")
    kubectl.chmod(0o755)
    assert (
        tools_cli.main(
            [
                "act",
                "--resource",
                "Deployment/app/cleanup",
                "--resource",
                "ServiceAccount/app/cleanup",
                "--resource",
                "ConfigMap/app/token",
                "--",
                str(kubectl),
                "-n",
                "app",
                "patch",
                "configmap",
                "token",
                "--type=merge",
                "-p",
                '{"metadata":{"finalizers":[]}}',
            ]
        )
        == 0
    )
    (receipt,) = _recorded(scratch)
    assert receipt["success"] is True
    assert receipt["resources"] == [recorded_actions.resource_ref("ConfigMap/app/token")]
    assert receipt["target"] == "ConfigMap/app/token"


def test_zero_executed_delete_targets_are_not_confirmed(scratch: Path, tmp_path: Path) -> None:
    kubectl = tmp_path / "kubectl"
    kubectl.write_text("#!/bin/sh\nexit 0\n")
    kubectl.chmod(0o755)
    tools_cli.main(
        [
            "act",
            "--resource",
            "Service/app/declared",
            "--",
            str(kubectl),
            "-n",
            "app",
            "delete",
            "services",
            "--all",
        ]
    )
    (receipt,) = _recorded(scratch)
    assert receipt["resources"] == []
    assert receipt["success"] is False
    assert receipt["failure_reason"] == "delete_not_confirmed"


def test_verified_incident_act_returns_rejection_receipt_without_running(tmp_path, monkeypatch, capsys):
    history = tmp_path / "history"
    history.mkdir()
    (history / "mutation.lock").touch(mode=0o600)
    warning = {
        "verified_at": "2026-10-25T00:00:00Z",
        "message": "SDO verified this incident; you may finish your report",
    }
    (history / "incident-verified.json").write_text(json.dumps(warning))
    monkeypatch.setenv("SDO_API_HISTORY_DIR", str(history))
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path / "scratch"))
    monkeypatch.setattr(tools_cli, "_exchange", lambda *_: {"summary": "readable", "incident_verified": warning})
    marker = tmp_path / "must-not-run"
    assert tools_cli.main(["act", "--resource", "Service/app/api", "--", "touch", str(marker)]) == 1
    assert not marker.exists()
    text = expand_context(capsys.readouterr().out)
    details = Path(text.split("Full JSON: ", 1)[1].splitlines()[0])
    action = json.loads(details.read_text())["actions"][0]
    assert action["receipt"]["failure_reason"] == "incident_verified"
    assert action["receipt"]["success"] is False
    assert action["incident_verified"] == warning


def test_cluster_reference_cli_keeps_kind_version_and_scope() -> None:
    for kind in ("ClusterRole", "ClusterRoleBinding"):
        assert recorded_actions.resource_ref(f"{kind}/app-cleanup") == {
            "api_version": "rbac.authorization.k8s.io/v1",
            "kind": kind,
            "namespace": "",
            "name": "app-cleanup",
        }


@pytest.mark.parametrize(
    "reference",
    [
        "Deployment/app/worker",
        '{"kind":"Deployment","namespace":"app","name":"worker"}',
        '{"api_version":"apps/v1","kind":"deploy","namespace":"app","name":"worker"}',
    ],
)
def test_act_accepts_printed_object_reference_forms(scratch: Path, reference: str) -> None:
    assert tools_cli.main(["act", "--resource", reference, "--", "true"]) == 0
    (receipt,) = _recorded(scratch)
    assert receipt["resources"] == [
        {"api_version": "apps/v1", "kind": "Deployment", "namespace": "app", "name": "worker"}
    ]


def test_act_on_a_controlled_object_runs_only_the_write(scratch: Path, tmp_path: Path) -> None:
    # REV12 F1: owner chains come from SDO's own capture, so `sdo act` reads nothing before
    # the write and its receipt carries no agent-side ownership claim.
    calls = tmp_path / "calls"
    kubectl = _stub(tmp_path / "bin", f'printf "%s\\n" "$*" >> {calls}')
    rs = ["act", "--resource", "ReplicaSet/hotel-reservation/geo-67f6d98b75", "--", str(kubectl)]
    assert tools_cli.main([*rs, "-n", "hotel-reservation", "scale", "rs", "geo-67f6d98b75", "--replicas=0"]) == 0

    assert calls.read_text().splitlines() == ["-n hotel-reservation scale rs geo-67f6d98b75 --replicas=0"]
    (scaled,) = _recorded(scratch)
    assert "owners" not in scaled


def _argv_recorder(directory: Path, log: Path) -> Path:
    """A kubectl that records its argv and rejects flags kubectl does not define, as cobra does."""
    return _stub(
        directory,
        f'printf "%s\\n" "$@" > {log}\n'
        'for a; do [ "$a" = -- ] && break\n'
        '  case "$a" in --json|--summary|--summary=*|--irreversible) '
        'echo "error: unknown flag: $a" >&2; echo "See \'kubectl --help\' for usage." >&2; exit 1;; esac\n'
        "done",
    )


@pytest.mark.parametrize("separator", [["--"], []])
def test_sdo_options_trailing_a_kubectl_command_belong_to_sdo(
    scratch: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str], separator: list[str]
) -> None:
    # LE P4 (11 failed acts): the responder appended SDO's --json after the kubectl command.
    log = tmp_path / "argv"
    kubectl = _argv_recorder(tmp_path / "bin", log)
    code = tools_cli.main(
        [
            "act", "--resource", "Deployment/app/rate", *separator,
            str(kubectl), "-n", "app", "set", "image", "deployment/rate", "rate=img",
            "--json", "--summary", "restore rate image", "--irreversible",
        ]
    )  # fmt: skip

    assert code == 0
    assert log.read_text().split() == ["-n", "app", "set", "image", "deployment/rate", "rate=img"]
    assert json.loads(expand_context(capsys.readouterr().out))["command"] == "act"
    (receipt,) = _recorded(scratch)
    assert receipt["summary"] == "restore rate image"
    assert receipt["reversible"] is False


def test_kubectl_keeps_every_flag_it_defines_and_its_own_wrapped_command(scratch: Path, tmp_path: Path) -> None:
    log = tmp_path / "argv"
    kubectl = _argv_recorder(tmp_path / "bin", log)
    argv = ["-n", "app", "exec", "db-0", "--", "mongosh", "--json", "--summary=x"]
    assert tools_cli.main(["act", "--resource", "Pod/app/db-0", "--", str(kubectl), *argv]) == 0
    assert log.read_text().split() == argv

    argv = ["-n", "app", "create", "role", "reader", "--verb=get", "--resource=pods"]
    assert tools_cli.main(["act", "--resource", "Role/app/reader", "--", str(kubectl), *argv]) == 0
    assert log.read_text().split() == argv


def test_other_wrapped_programs_receive_their_arguments_verbatim(
    scratch: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert tools_cli.main(["act", "--resource", "Deployment/app/api", "--", "echo", "patched", "--json"]) == 0
    (receipt,) = _recorded(scratch)
    assert "$ echo patched --json" in str(receipt["details"])


@pytest.mark.parametrize("full_json", [False, True])
def test_a_wrapped_usage_error_says_how_to_call_act_in_one_line(
    scratch: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str], full_json: bool
) -> None:
    kubectl = _stub(
        tmp_path / "bin",
        "echo 'error: unknown flag: --wait' >&2; echo \"See 'kubectl patch --help' for usage.\" >&2; exit 1",
    )
    argv = ["act", "--resource", "Service/app/api", "--", str(kubectl), "patch", "svc", "api", "--wait=true"]
    assert tools_cli.main(["--json", *argv] if full_json else argv) == 1

    out = expand_context(capsys.readouterr().out)
    hint = json.loads(out)["hint"] if full_json else next(line for line in out.splitlines() if "sdo act" in line)
    assert "\n" not in hint
    assert "kubectl" in hint
    assert "sdo act --resource" in hint
    assert " -- " in hint
