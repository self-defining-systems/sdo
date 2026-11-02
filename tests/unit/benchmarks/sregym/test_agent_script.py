"""Hand-authored scripts replace the CLI, retaining the real provider parser."""

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from benchmarks.sregym.agent_script.converter import convert_recording
from benchmarks.sregym.agent_script.model import AgentScript


def script(**turn):
    return {"version": 1, "turns": [{"role": "responder", "result": {}, **turn}]}


@pytest.mark.parametrize(
    "payload",
    [
        {"version": 2, "turns": []},
        script(steps=[{"kind": "write", "path": "../escape", "text": "x"}]),
        script(steps=[{"kind": "command", "command": "true", "attempts": 0}]),
        script(role="unknown"),
        script(steps=[{"kind": "wait", "seconds": -1}]),
        script(unrecognized=True),
        script(steps=[{"kind": "exit", "code": 0}]),
        script(steps=[{"kind": "exit", "code": 256}]),
    ],
)
def test_invalid_scripts_fail_before_execution(payload):
    with pytest.raises(ValidationError):
        AgentScript.model_validate(payload)


def test_selector_refuses_missing_or_ambiguous_turn():
    plan = AgentScript.model_validate(script(incident="incident-a", index=1))
    assert plan.select("responder", "incident-a", 1).index == 1
    with pytest.raises(ValueError, match="exactly one"):
        plan.select("responder", "incident-b", 1)
    with pytest.raises(ValidationError):
        AgentScript.model_validate({"version": 1, "turns": script()["turns"] * 2})


def test_recording_conversion_preserves_command_order_and_answer(tmp_path: Path):
    def event(item):
        return ["stdout", json.dumps({"type": "item.completed", "item": item})]

    recording = {
        "returncode": 0,
        "turn_index": 2,
        "events": [
            event({"type": "command_execution", "command": "sdo verify", "exit_code": 0}),
            event({"type": "agent_message", "text": '{"status":"completed"}'}),
        ],
    }
    path = tmp_path / "recording.json"
    path.write_text(json.dumps(recording))
    converted = convert_recording([path], role="responder")
    assert converted.turns[0].index == 2
    assert converted.turns[0].result == {"status": "completed"}
    assert converted.turns[0].steps[0].command == "sdo verify"


@pytest.fixture
def scripted_cli(tmp_path, monkeypatch):
    import os
    import sys

    binary = tmp_path / "bin"
    binary.mkdir()
    codex = binary / "codex"
    codex.write_text(f'#!/bin/sh\nexec {sys.executable} -m benchmarks.sregym.agent_script codex "$@"\n')
    codex.chmod(0o755)
    monkeypatch.setenv("PATH", f"{binary}:{os.environ['PATH']}")
    monkeypatch.setenv("PYTHONPATH", str(Path(__file__).resolve().parents[4]))
    monkeypatch.setenv("SDO_AGENT_SCRIPT_STATE", str(tmp_path / "state"))
    plan_path = tmp_path / "script.json"
    monkeypatch.setenv("SDO_AGENT_SCRIPT", str(plan_path))
    root = tmp_path / "checkout"
    root.mkdir()
    return plan_path, root


@pytest.mark.parametrize(
    ("role", "key"),
    [
        ("source-deployment", "proposed"),
        ("health-judge", "detector_ids"),
        ("responder", "confirmed_root_causes"),
        ("reflector", "learning_decision"),
    ],
)
def test_all_roles_use_real_provider_parser_and_command_audit(scripted_cli, role, key):
    from libs.agent_cli.structured import run_structured_turn

    path, root = scripted_cli
    answer = {key: "synthetic"}
    path.write_text(
        json.dumps(
            script(
                role=role,
                steps=[
                    {"kind": "write", "path": "authored.txt", "text": "authored"},
                    {"kind": "command", "command": "cat authored.txt", "contains": "authored"},
                ],
                result=answer,
            )
        )
    )
    turn = run_structured_turn(
        provider="codex",
        model="unused",
        cwd=root,
        prompt="synthetic test",
        output_schema={
            "type": "object",
            "properties": {key: {"type": "string"}},
            "required": [key],
            "additionalProperties": False,
        },
        access="danger-full-access",
    )
    assert json.loads(turn.output_json) == answer
    assert (root / "authored.txt").read_text() == "authored"
    assert "cat authored.txt" in turn.shell_commands


def test_commands_cannot_write_outside_checkout(scripted_cli):
    from libs.agent_cli.structured import StructuredTurnError, run_structured_turn

    path, root = scripted_cli
    outside = root.parent / "escaped.txt"
    path.write_text(json.dumps(script(steps=[{"kind": "command", "command": f"echo escape > {outside}"}])))
    with pytest.raises(StructuredTurnError, match="script assertion failed"):
        run_structured_turn(
            provider="codex",
            model="unused",
            cwd=root,
            prompt="test",
            output_schema={
                "type": "object",
                "properties": {"confirmed_root_causes": {"type": "array", "items": {"type": "string"}}},
                "required": ["confirmed_root_causes"],
                "additionalProperties": False,
            },
            access="danger-full-access",
        )
    assert not outside.exists()


def test_symlink_file_step_fails(scripted_cli):
    import io

    from benchmarks.sregym.agent_script.runner import execute
    from benchmarks.sregym.agent_script.wire import EventStream

    path, root = scripted_cli
    (root / "escape").symlink_to(root.parent, target_is_directory=True)
    turn = AgentScript.model_validate(script(steps=[{"kind": "write", "path": "escape/outside", "text": "bad"}])).turns[
        0
    ]
    with pytest.raises(ValueError, match="symlink"):
        execute(turn, root, EventStream(io.StringIO()))


def test_responder_turns_and_reflection_resume_share_session(scripted_cli):
    from libs.agent_cli.structured import run_structured_turn

    path, root = scripted_cli
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "turns": [
                    {"role": "responder", "index": 0, "result": {"confirmed_root_causes": "first"}},
                    {"role": "responder", "index": 1, "result": {"confirmed_root_causes": "second"}},
                    {"role": "reflector", "result": {"learning_decision": "no_change"}},
                ],
            }
        )
    )

    def run(key, resume=None):
        return run_structured_turn(
            provider="codex",
            model="unused",
            cwd=root,
            prompt="test",
            output_schema={
                "type": "object",
                "properties": {key: {"type": "string"}},
                "required": [key],
                "additionalProperties": False,
            },
            access="danger-full-access",
            resume_session_id=resume,
        )

    first = run("confirmed_root_causes")
    second = run("confirmed_root_causes", first.session_id)
    reflection = run("learning_decision", first.session_id)
    assert json.loads(second.output_json)["confirmed_root_causes"] == "second"
    assert reflection.session_id == first.session_id


def test_agent_script_module_cli_accepts_script(scripted_cli):
    import subprocess
    import sys

    path, root = scripted_cli
    path.write_text(json.dumps(script(result={"confirmed_root_causes": []})))
    schema = root.parent / "schema.json"
    schema.write_text(json.dumps({"properties": {"confirmed_root_causes": {"type": "array"}}}))
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "benchmarks.sregym.agent_script",
            "codex",
            "exec",
            "--output-schema",
            str(schema),
            "test",
        ],
        cwd=root,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert '"turn.completed"' in result.stdout


def test_responder_repair_tool_cli_and_reflection_edit_against_fake_api(scripted_cli, monkeypatch):
    """Real responder/reflection sessions and tool wire; synthetic controller endpoint."""
    import os
    import threading
    import urllib.request

    from benchmarks.sregym.fastloop.fake_kube import FakeKubernetesAPIServer
    from sdo.agents.responder.codex import execute_incident
    from sdo.agents.responder.reflection import CodexSessionBackend
    from sdo.contracts import IncidentRequest, parse_json

    path, root = scripted_cli
    repo = Path(__file__).resolve().parents[4]
    request_payload = json.loads((repo / "tests/fixtures/sdo/contracts/incident_request.json").read_text())
    request_payload["repository_worktree"] = str(root)
    request_payload["memory_snapshot"] = {"accepted_commit": request_payload["repository_base_commit"]}
    incident = request_payload["incident_id"]
    result = json.loads((repo / "tests/fixtures/sdo/contracts/incident_result.json").read_text())
    result["incident_id"] = "{{incident_id}}"
    result["confirmed_root_causes"][0]["resources"] = ["{{request.findings.0.primary_resource}}"]
    result["confirmed_root_causes"][0]["explained_detectors"] = ["{{request.findings.0.detector_id}}"]
    result["timing"] = {"started_at": "{{turn.started_at}}", "completed_at": "{{now}}"}
    tools = root.parent / "tools"
    for name in ("requests", "responses"):
        (tools / name).mkdir(parents=True)
    unpublished = tools / "requests/.unpublished.json"
    unpublished.write_text(json.dumps({"command": "apply", "plan_ids": ["synthetic-repair"]}))
    monkeypatch.setenv("SDO_TOOLS_DIR", str(tools))
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tools / "scratch"))
    monkeypatch.setenv("SDO_AGENT_SCRIPT_INCIDENT", incident)
    bin_dir = Path(os.environ["PATH"].split(":")[0])
    import sys

    binary = bin_dir / "sdo"
    binary.write_text(f'#!/bin/sh\nexec {sys.executable} -m sdo.agents.responder.tools_cli "$@"\n')
    binary.chmod(0o755)
    # The reflection worker runs the shared memory check itself before it returns.
    memory = bin_dir / "sdo-operator"
    memory.write_text("#!/bin/sh\nexit 0\n")
    memory.chmod(0o755)
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "turns": [
                    {
                        "role": "responder",
                        "incident": incident,
                        "steps": [
                            {
                                "kind": "tool",
                                "argv": ["sdo", "--json", "apply", "synthetic-repair"],
                                "contains": "details_file",
                            }
                        ],
                        "result": result,
                    },
                    {
                        "role": "reflector",
                        "incident": incident,
                        "steps": [
                            {
                                "kind": "write",
                                "path": ".sdo/diagnostics/detectors/incidents/synthetic/detector.go",
                                "text": "package synthetic\n",
                            }
                        ],
                        "result": {
                            "summary": "Added synthetic incident detector",
                            "learning_decision": "updated",
                            "proposed_artifact_ids": ["synthetic"],
                            "no_change_reason": None,
                            "sdk_gaps": [],
                        },
                    },
                ],
            }
        )
    )
    stop = threading.Event()
    failures = []
    snapshot = {
        "namespace": "demo",
        "services": [{"metadata": {"name": "api"}, "spec": {"ports": [{"port": 80, "targetPort": 9999}]}}],
    }
    with FakeKubernetesAPIServer(snapshot) as api:
        config = json.loads(api.write_kubeconfig(root.parent / "kubeconfig").read_text())
        url = config["clusters"][0]["cluster"]["server"] + "/api/v1/namespaces/demo/services/api"

        def controller():
            try:
                while not stop.is_set():
                    for request in (tools / "requests").glob("*.json"):
                        if request.name.startswith("."):
                            continue
                        assert json.loads(request.read_text()) == {"command": "apply", "plan_ids": ["synthetic-repair"]}
                        patch = urllib.request.Request(
                            url,
                            data=json.dumps({"spec": {"ports": [{"port": 80, "targetPort": 80}]}}).encode(),
                            method="PATCH",
                            headers={"Content-Type": "application/merge-patch+json"},
                        )
                        with urllib.request.urlopen(patch, timeout=5) as response:
                            assert response.status == 200
                        (tools / "responses" / request.name).write_text('{"applied": true}')
                        request.unlink()
                    # Back off between scans; the stop event ends the loop at once.
                    stop.wait(0.01)
            except Exception as exc:
                failures.append(exc)

        thread = threading.Thread(target=controller)
        thread.start()
        try:
            incident_request = parse_json(json.dumps(request_payload), IncidentRequest())
            # The worker's mounted task carries the request; the prompt only summarizes it.
            from sdo.contracts import AgentTask, to_canonical_json

            task = AgentTask(
                schema_version="sdo.dev/v1alpha1", task_id="t", agent_id="a", task_type="incident-response"
            )
            task.request.Pack(incident_request)
            task_path = root.parent / "agent-task.json"
            task_path.write_text(to_canonical_json(task), encoding="utf-8")
            monkeypatch.setenv("SDO_TASK_PATH", str(task_path))
            completed = execute_incident(incident_request, provider="codex", model="unused")
            assert completed.status == "completed"
            assert (
                completed.confirmed_root_causes[0].resources[0].name
                == request_payload["findings"][0]["primary_resource"]["name"]
            )
            assert completed.confirmed_root_causes[0].explained_detectors == [
                request_payload["findings"][0]["detector_id"]
            ]
            assert unpublished.exists(), "the synthetic controller must ignore atomic request staging files"
            assert api.objects[("services", "api")]["spec"]["ports"][0]["targetPort"] == 80
            reflected = CodexSessionBackend(model="unused").resume(
                session_id=completed.responder_session_id,
                worktree=root,
                prompt="Reflect on independently verified closure",
                idempotency_key=f"reflection:{incident}:{'a' * 40}",
            )
            assert reflected.learning_decision == "updated"
            assert (root / ".sdo/diagnostics/detectors/incidents/synthetic/detector.go").exists()
            assert not failures
        finally:
            stop.set()
            thread.join(timeout=5)


def test_converter_requires_bytes_for_recorded_file_changes(tmp_path):
    import base64

    from benchmarks.sregym.fastloop.file_edits import FileEdit, encode_file_edits

    record = tmp_path / "recording.json"
    record.write_text(
        json.dumps(
            {
                "returncode": 0,
                "turn_index": 0,
                "events": [
                    ["stdout", json.dumps({"type": "item.completed", "item": {"type": "file_change"}})],
                    [
                        "stdout",
                        json.dumps(
                            {
                                "type": "item.completed",
                                "item": {"type": "agent_message", "text": '{"learning_decision":"updated"}'},
                            }
                        ),
                    ],
                ],
            }
        )
    )
    with pytest.raises(ValueError, match="file_change"):
        convert_recording([record], role="reflector")
    edits = tmp_path / "file_edits.json"
    edits.write_bytes(encode_file_edits([FileEdit("authored.txt", base64.b64encode(b"editable").decode())]))
    plan = convert_recording([record], role="reflector", file_edits=edits)
    assert plan.turns[0].steps[0].text == "editable"


def test_read_only_turn_refuses_checkout_writes(scripted_cli):
    import io

    from benchmarks.sregym.agent_script.runner import execute
    from benchmarks.sregym.agent_script.wire import EventStream

    _, root = scripted_cli
    turn = AgentScript.model_validate(script(steps=[{"kind": "write", "path": "forbidden", "text": "bad"}])).turns[0]
    with pytest.raises(ValueError, match="script assertion"):
        execute(turn, root, EventStream(io.StringIO()), access="read-only")
    assert not (root / "forbidden").exists()


def test_literal_placeholders_fail_closed_and_are_revalidated():
    from benchmarks.sregym.agent_script.template import expand

    payload = script(steps=[{"kind": "write", "path": "{{checkout}}", "text": "incident {{incident_id}}"}])
    with pytest.raises(ValueError, match="unavailable script placeholder"):
        expand(payload, {})
    with pytest.raises(ValidationError):
        AgentScript.model_validate(expand(payload, {"checkout": "../escape", "incident_id": "synthetic"}))
    assert expand({"incident_id": "{{incident_id}}"}, {"incident_id": "synthetic"}) == {"incident_id": "synthetic"}


def test_scripted_crash_consumes_turn_without_emitting_result(scripted_cli, monkeypatch, capsys):
    from benchmarks.sregym.agent_script.cli import codex

    path, root = scripted_cli
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "turns": [
                    {"role": "reflector", "index": 0, "steps": [{"kind": "exit", "code": 17}], "result": {}},
                    {"role": "reflector", "index": 1, "result": {"learning_decision": "no_change"}},
                ],
            }
        )
    )
    schema = root / "schema.json"
    schema.write_text(json.dumps({"properties": {"learning_decision": {"type": "string"}}}))
    monkeypatch.chdir(root)
    args = ["exec", "--output-schema", str(schema), "--json", "-"]
    assert codex(args, stdin="reflect") == 17
    events = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert not any(event["type"] in {"item.completed", "turn.completed"} for event in events)
    assert codex(args, stdin="reflect") == 0
    assert "no_change" in capsys.readouterr().out


def test_agent_script_has_no_dependency_on_legacy_menus():
    root = Path(__file__).resolve().parents[4] / "benchmarks/sregym/agent_script"
    assert not any("assurance.scripted_codex" in path.read_text() for path in root.glob("*.py"))


def test_planned_crash_is_a_real_provider_failure(scripted_cli):
    from libs.agent_cli.structured import StructuredTurnError, run_structured_turn

    path, root = scripted_cli
    path.write_text(
        json.dumps(
            script(
                steps=[
                    {"kind": "exit", "code": 19},
                    {"kind": "write", "path": "after-crash", "text": "must not run"},
                ]
            )
        )
    )
    with pytest.raises(StructuredTurnError):
        run_structured_turn(
            provider="codex",
            model="unused",
            cwd=root,
            prompt="test",
            output_schema={"type": "object", "properties": {"confirmed_root_causes": {"type": "array"}}},
            access="danger-full-access",
        )
    assert not (root / "after-crash").exists()


@pytest.mark.parametrize(
    "reference",
    [
        "now.bad",
        "request.no_such_field",
        "request.findings.0.no_such_field",
        "request.findings.-1.primary_resource",
        "request.findings.0.primary_resource.nope",
    ],
)
def test_unknown_template_references_rejected_at_load(reference):
    with pytest.raises(ValidationError, match="reference"):
        AgentScript.model_validate(script(result={"value": "{{" + reference + "}}"}))


def test_task_templates_preserve_types_and_clock_is_per_step(tmp_path):
    import io

    from benchmarks.sregym.agent_script.runner import execute
    from benchmarks.sregym.agent_script.template import TemplateContext
    from benchmarks.sregym.agent_script.wire import EventStream

    resource = {"api_version": "v1", "kind": "Service", "namespace": "demo", "name": "api"}
    request = {
        "incidentId": "live-incident",
        "findings": [{"primaryResource": resource, "parameterBindings": {"target": resource}}],
        "surfacedPlaybooks": [{"playbookId": "repair"}],
    }
    ticks = iter(["start", "step0", "end0", "step1", "end1", "result"])
    context = TemplateContext(request, tmp_path, clock=lambda: next(ticks))
    turn = AgentScript.model_validate(
        script(
            steps=[
                {"kind": "write", "path": "one", "text": "{{now}}"},
                {"kind": "write", "path": "two", "text": "{{now}}"},
            ],
            result={
                "resource": "{{request.findings.0.primary_resource}}",
                "bindings": "{{request.findings.0.parameter_bindings}}",
                "playbook": "{{request.surfaced_playbooks.0.playbook_id}}",
                "start": "{{steps.0.started_at}}",
                "end": "{{steps.0.completed_at}}",
                "now": "{{now}}",
            },
        )
    ).turns[0]
    answer = execute(turn, tmp_path, EventStream(io.StringIO()), context=context)
    assert (tmp_path / "one").read_text() == "step0"
    assert (tmp_path / "two").read_text() == "step1"
    assert answer == {
        "resource": resource,
        "bindings": {"target": resource},
        "playbook": "repair",
        "start": "step0",
        "end": "end0",
        "now": "result",
    }


@pytest.mark.parametrize(
    ("steps", "result"),
    [
        ([], {"time": "{{steps.0.started_at}}"}),
        ([{"kind": "write", "path": "x", "text": "{{steps.0.completed_at}}"}], {}),
        ([{"kind": "write", "path": "x", "text": "{{steps.1.started_at}}"}], {}),
    ],
)
def test_capture_references_require_an_existing_completed_step(steps, result):
    with pytest.raises(ValidationError, match="step reference"):
        AgentScript.model_validate(script(steps=steps, result=result))


def test_missing_binding_fails_before_writing(tmp_path):
    import io

    from benchmarks.sregym.agent_script.runner import execute
    from benchmarks.sregym.agent_script.template import TemplateContext
    from benchmarks.sregym.agent_script.wire import EventStream

    turn = AgentScript.model_validate(
        script(
            steps=[{"kind": "write", "path": "should-not-exist", "text": "hello"}],
            result={"resource": "{{request.findings.0.parameter_bindings.missing}}"},
        )
    ).turns[0]
    with pytest.raises(ValueError, match="unavailable"):
        execute(turn, tmp_path, EventStream(io.StringIO()), context=TemplateContext({}, tmp_path))
    assert not (tmp_path / "should-not-exist").exists()


def test_go_composite_literals_are_not_template_references():
    from benchmarks.sregym.agent_script.template import expand

    source = '[]sdk.ObjectRef{{APIVersion: "v1", Kind: "Pod"}}'
    plan = AgentScript.model_validate(script(steps=[{"kind": "write", "path": "detector.go", "text": source}]))
    assert expand(plan.turns[0].steps[0].text, {}) == source


def test_scripted_session_persists_a_rollout_before_the_worker_checkpoints_it(scripted_cli, tmp_path, monkeypatch):
    """The production responder's session recovery needs the transcript the moment the session starts."""
    from libs.agent_cli.structured import run_structured_turn

    path, root = scripted_cli
    home = tmp_path / "codex-home"
    monkeypatch.setenv("CODEX_HOME", str(home))
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "turns": [
                    {"role": "responder", "index": index, "result": {"confirmed_root_causes": "turn"}}
                    for index in range(2)
                ],
            }
        )
    )
    started: list[list[Path]] = []

    def run(resume=None):
        return run_structured_turn(
            provider="codex",
            model="unused",
            cwd=root,
            prompt="test",
            output_schema={
                "type": "object",
                "properties": {"confirmed_root_causes": {"type": "string"}},
                "required": ["confirmed_root_causes"],
                "additionalProperties": False,
            },
            access="danger-full-access",
            resume_session_id=resume,
            on_session_started=lambda session: started.append(list(home.glob(f"sessions/**/rollout-*{session}.jsonl"))),
        )

    first = run()
    run(first.session_id)
    assert [len(found) for found in started] == [1, 1]
    assert started[0][0].stat().st_size > 0
