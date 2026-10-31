"""Generated responder scripts through subprocess tools and the provider parser."""

import io
import json
import os
import sys
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
from google.protobuf import json_format

from benchmarks.sregym.agent_script.runner import execute
from benchmarks.sregym.agent_script.wire import EventStream
from benchmarks.sregym.fastloop.responder_script import responder_script
from sdo.contracts import DriftEvent, IncidentRequest, IncidentResult
from tests.unit.benchmarks.sregym.fastloop import script_harness as scripts


def test_generator_is_pure_and_uses_live_outputs(tmp_path):
    request = IncidentRequest(incident_id="example", namespace="app", repository_worktree=str(tmp_path))
    script = responder_script(request)
    assert list(tmp_path.iterdir()) == []
    assert len(script.turns[0].steps) >= 4
    assert script.turns[0].result["status"].startswith("{{steps.")


def test_command_json_output_is_typed(tmp_path):
    from benchmarks.sregym.agent_script.model import AgentScript

    script = AgentScript.model_validate(
        {
            "version": 1,
            "turns": [
                {
                    "role": "responder",
                    "steps": [
                        {
                            "kind": "command",
                            "command": """printf '%s' '{"healthy":true,"items":[1]}' """,
                            "capture_json": True,
                        }
                    ],
                    "result": {"healthy": "{{steps.0.output.healthy}}", "items": "{{steps.0.output.items}}"},
                }
            ],
        }
    )
    result = execute(script.turns[0], tmp_path, EventStream(io.StringIO()))
    assert result == {"healthy": True, "items": [1]}


# Synthetic scenarios use a real local HTTP boundary for subprocess commands.


@contextmanager
def tool_transport(tmp_path, monkeypatch):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_POST(self):
            argv = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            try:
                if argv[0] == "sdo":
                    value = json.dumps(scripts._tool(*argv[1:])) + "\nTool summary\n"
                elif argv[0] == "probe":
                    with scripts.socket.create_connection(tuple(argv[1:]), timeout=2):
                        pass
                    value = ""
                else:
                    value = scripts._command(argv)
                data = {"value": value}
            except Exception as exc:
                data = {"error": type(exc).__name__, "message": str(exc)}
            self.send_response(200)
            self.end_headers()
            self.wfile.write(json.dumps(data).encode())

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    tools = tmp_path / "exchange"
    tools.mkdir(exist_ok=True)
    monkeypatch.setenv("SDO_TOOLS_DIR", str(tools))
    # This shim supplies the hermetic tool endpoints; all generated command code
    # still runs in subprocesses, through bubblewrap and the normal wire parser.
    shim = f"""
import urllib.request
from contextlib import nullcontext

_real_connect = socket.create_connection
def rpc(argv):
    probe_connect = socket.create_connection
    socket.create_connection = _real_connect
    payload = json.dumps(argv).encode()
    try:
        response = json.load(urllib.request.urlopen('http://127.0.0.1:{server.server_port}', data=payload))
    finally:
        socket.create_connection = probe_connect
    if 'error' in response:
        errors = {{'ConnectionRefusedError': ConnectionRefusedError, 'TimeoutError': TimeoutError, 'OSError': OSError}}
        cls = errors.get(response['error'], RuntimeError)
        raise cls(response['message'])
    return response['value']
socket.create_connection = lambda address, **kw: (rpc(['probe', *address]), nullcontext())[1]
"""
    binary = tools / "bin"
    binary.mkdir(exist_ok=True)
    for name in ("sdo", "kubectl"):
        path = binary / name
        path.write_text(
            f"#!{sys.executable}\nimport sys, json, socket\n"
            + shim.split("socket.create_connection = lambda")[0]
            + f"\nprint(rpc([{name!r}, *sys.argv[1:]]), end='')\n"
        )
        path.chmod(0o755)
    monkeypatch.setenv("PATH", str(binary) + os.pathsep + os.environ["PATH"])
    try:
        yield shim
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def generated_call(request, tmp_path, monkeypatch, *, drift=None):
    import inspect

    from benchmarks.sregym.fastloop import responder_steps

    with tool_transport(tmp_path, monkeypatch) as shim:
        if drift is None:
            script = responder_script(request)
        else:
            from benchmarks.sregym.agent_script.model import AgentScript

            code = inspect.getsource(responder_steps) + "\nfrom google.protobuf import json_format\n"
            code += f"request = json_format.Parse({json_format.MessageToJson(request)!r}, IncidentRequest())\n"
            code += f"event = json_format.Parse({json_format.MessageToJson(drift)!r}, DriftEvent())\n"
            code += shim + "\nprint(json_format.MessageToJson(_assess_drift(request, event)))"
            import shlex

            script = AgentScript.model_validate(
                {
                    "version": 1,
                    "turns": [
                        {
                            "role": "responder",
                            "steps": [
                                {
                                    "kind": "command",
                                    "command": shlex.join([sys.executable, "-c", code]),
                                    "capture_json": True,
                                }
                            ],
                            "result": {"label": "{{steps.0.output}}"},
                        }
                    ],
                }
            )
        if drift is None:
            # Inject endpoints immediately before each phase, after helper definitions.
            for step in script.turns[0].steps:
                import shlex

                argv = shlex.split(step.command)
                marker = "request = json_format.ParseDict("
                argv[-1] = argv[-1].replace(marker, shim + "\n" + marker, 1)
                step.command = shlex.join(argv)
        root = Path(request.repository_worktree or tmp_path)
        from benchmarks.sregym.agent_script.template import TemplateContext

        result = execute(
            script.turns[0],
            root,
            EventStream(io.StringIO()),
            context=TemplateContext(json_format.MessageToDict(request, preserving_proto_field_name=True), root),
        )
        return json_format.ParseDict(
            result["label"] if drift is not None else result, DriftEvent() if drift is not None else IncidentResult()
        )


@pytest.mark.parametrize(
    "scenario",
    [
        "listener",
        "no_pods",
        "refused",
        "timeout",
        "accepts",
        "source_refused",
        "policy",
        "allow_rule",
        "accepted_policy",
        "no_pod",
        "changed_service",
        "unknown_selector",
    ],
)
def test_existing_drift_scenarios_through_script_transport(tmp_path, monkeypatch, scenario):
    from tests.unit.benchmarks.sregym.adapter import test_scripted_drift as cases

    monkeypatch.setattr(
        scripts,
        "_assess_drift",
        lambda request, event: generated_call(request, tmp_path, monkeypatch, drift=event),
    )
    monkeypatch.setattr(scripts, "respond", lambda request: generated_call(request, tmp_path, monkeypatch))
    if scenario == "listener":
        cases.test_scripted_drift_requires_source_mismatch_and_observed_listening_ports(monkeypatch, tmp_path)
    elif scenario == "no_pods":
        cases.test_scripted_drift_is_unsure_without_selected_pods(monkeypatch, tmp_path)
    elif scenario in {"refused", "timeout", "accepts", "source_refused"}:
        cases.test_scripted_drift_assesses_scratch_pods_without_exec_utilities(monkeypatch, tmp_path, scenario)
    else:
        cases.test_scripted_policy_drift_requires_source_absence_and_total_ingress_isolation(
            monkeypatch, "" if scenario == "policy" else scenario
        )


@pytest.mark.parametrize(
    "scenario",
    [
        "service",
        "mutable-image",
        "immutable-image",
        "probes",
        "matched",
        "healthy",
        "stale",
        "unsure",
        "exhausted",
        "missing-plan",
        "mixed",
    ],
)
def test_full_generated_result_and_calls(tmp_path, monkeypatch, scenario):
    from copy import deepcopy

    from google.protobuf.any_pb2 import Any as ProtoAny

    from libs.agent_cli.structured import run_structured_turn
    from sdo.agents.responder import run_worker_task
    from sdo.agents.responder.codex import _incident_result_schema
    from sdo.contracts import AgentTask, to_canonical_json

    root = tmp_path / "app"
    root.mkdir()
    kind = "Deployment" if scenario in {"mutable-image", "immutable-image", "probes", "mixed"} else "Service"
    ref = {"api_version": "apps/v1" if kind == "Deployment" else "v1", "kind": kind, "namespace": "app", "name": "api"}
    request = IncidentRequest(
        incident_id="equivalence",
        namespace="app",
        repository_worktree=str(root),
        findings=[
            {"detector_id": "health.api", "summary": "broken", "evidence": "live check", "primary_resource": ref}
        ],
    )
    request.memory_snapshot.accepted_commit = "a" * 40
    request.repository_base_commit = "a" * 40
    if kind == "Deployment":
        request.state_changes.changes.add(
            kind=kind,
            name="api",
            change="modified",
            fields=[{"field": "container[api].image", "before": "healthy:baseline"}],
        )
    if scenario in {"stale", "unsure"}:
        event = request.drift_events.add(ref=ref, investigation_verdict="bug")
        if scenario == "unsure":
            request.findings[0].detector_id = "drift.unsupported"
        else:
            event.ref.name = "old"
    if scenario == "mixed":
        request.state_changes.changes.insert(
            0, type(request.state_changes.changes[0])(kind="Service", name="front", change="modified")
        )
    source = (
        {"spec": {"ports": [{"port": 80, "targetPort": 8080}]}}
        if kind == "Service"
        else {
            "spec": {
                "template": {
                    "spec": {
                        "containers": [
                            {
                                "name": "api",
                                "image": "accepted@sha256:" + "b" * 64
                                if scenario == "immutable-image"
                                else "accepted:tag",
                                "readinessProbe": {"httpGet": {"path": "/", "port": 8080}},
                                "livenessProbe": {"tcpSocket": {"port": 8080}},
                                "startupProbe": {"tcpSocket": {"port": 8080}},
                            }
                        ]
                    }
                }
            }
        }
    )
    if scenario == "exhausted":
        del request.findings[:]
        monkeypatch.setattr(scripts.time, "sleep", lambda _: None)
    calls = []
    repaired = False

    def command(argv, **_kwargs):
        nonlocal repaired
        calls.append(argv)
        if "patch" in argv:
            repaired = True
        return ""

    def tool(*args):
        nonlocal repaired
        calls.append(["sdo", *args])
        if args[0] == "diagnose":
            run = {"program_id": "repair", "verdict": {"result": "Match"}, "bindings": {"z": ref, "a": ref}}
            return (
                {
                    "program_runs": [
                        run,
                        deepcopy(run),
                        {"verdict": {"result": "NoMatch"}},
                        {"program_id": "", "verdict": {"result": "Match"}},
                    ]
                }
                if scenario in {"matched", "missing-plan"}
                else {}
            )
        if args[0] == "plan":
            return {} if scenario == "missing-plan" else {"plan_id": "plan-1"}
        if args[0] == "apply":
            repaired = True
            return {}
        if args[0] == "source":
            if args[1].startswith("Service/"):
                return {"source": {"spec": {"ports": [{"port": 80, "targetPort": 8080}]}}}
            return {"source": source}
        return {"healthy": repaired or scenario in {"healthy", "stale"}, "summary": "live verification"}

    monkeypatch.setattr(scripts, "_tool", tool)
    monkeypatch.setattr(scripts, "_command", command)
    with tool_transport(tmp_path, monkeypatch):
        script = responder_script(request)
        if scenario == "exhausted":
            for step in script.turns[0].steps:
                step.command = step.command.replace("time.sleep(5)", "time.sleep(0)")
        path = tmp_path / "script.json"
        # Select by task context using the same generic script CLI used by workers.
        script.turns[0].incident = None
        path.write_text(script.model_dump_json())
        binary = tmp_path / "codex-bin"
        binary.mkdir()
        (binary / "codex").write_text(
            f'#!/bin/sh\nexec {sys.executable} -m benchmarks.sregym.agent_script codex "$@"\n'
        )
        (binary / "codex").chmod(0o755)
        monkeypatch.setenv("PATH", str(binary) + os.pathsep + os.environ["PATH"])
        monkeypatch.setenv("PYTHONPATH", str(Path(__file__).resolve().parents[5]))
        monkeypatch.setenv("SDO_AGENT_SCRIPT", str(path))
        monkeypatch.setenv("SDO_AGENT_SCRIPT_ROLE", "responder")
        monkeypatch.setenv("SDO_AGENT_SCRIPT_STATE", str(tmp_path / "script-state"))
        packed = ProtoAny()
        packed.Pack(request)
        task = AgentTask(
            schema_version="sdo.dev/v1alpha1",
            task_id="test",
            agent_id="responder",
            task_type="incident-response",
            request=packed,
            workspace={"proposal_id": "test", "checkout_path": str(root)},
        )
        task_path = tmp_path / "task.json"
        task_path.write_text(to_canonical_json(task))
        monkeypatch.setenv("SDO_TASK_PATH", str(task_path))

        class Publisher:
            def publish_payload(self, *_args):
                pass

        def executor(typed):
            completed = run_structured_turn(
                provider="codex",
                model="unused",
                cwd=root,
                prompt="hermetic responder",
                access="danger-full-access",
                output_schema=_incident_result_schema(),
            )
            return json_format.Parse(completed.output_json, IncidentResult())

        result = run_worker_task(task_path, "result", Publisher(), executor=executor)
        actual = IncidentResult()
        assert result.response.Unpack(actual)
    assert calls[0] == ["sdo", "diagnose"]
    assert calls[-1] == ["sdo", "verify"]

    assert actual.status == ("failed" if scenario in {"unsure", "exhausted"} else "completed")
    assert actual.incident_id == request.incident_id
    assert bool(actual.repair_actions) == (scenario not in {"matched", "healthy", "stale", "unsure", "exhausted"})
    assert len(actual.verification_evidence) >= 1
    if scenario == "matched":
        assert [p.playbook_id for p in actual.applied_playbooks] == ["repair"]
    if scenario == "immutable-image":
        assert any("accepted@sha256:" in str(call) for call in calls)
    if scenario == "mutable-image":
        assert any("healthy:baseline" in str(call) for call in calls)
    assert not list((tmp_path / "exchange").glob("responder-*.json"))


@pytest.mark.parametrize("value", ["not json", "{}\ntrailing"])
def test_json_capture_rejects_invalid_command_output(tmp_path, value):
    import shlex

    from benchmarks.sregym.agent_script.model import AgentScript

    script = AgentScript.model_validate(
        {
            "version": 1,
            "turns": [
                {
                    "role": "responder",
                    "steps": [{"kind": "command", "command": "printf %s " + shlex.quote(value), "capture_json": True}],
                    "result": {},
                }
            ],
        }
    )
    with pytest.raises(json.JSONDecodeError):
        execute(script.turns[0], tmp_path, EventStream(io.StringIO()))


def test_output_forward_reference_is_rejected():
    from benchmarks.sregym.agent_script.model import AgentScript

    with pytest.raises(ValueError, match="unavailable step reference"):
        AgentScript.model_validate(
            {
                "version": 1,
                "turns": [
                    {"role": "responder", "steps": [{"kind": "command", "command": "{{steps.0.output}}"}], "result": {}}
                ],
            }
        )


@pytest.mark.parametrize("preserve_case", [True, False], ids=["snake-case", "proto-json"])
def test_generator_cli_reads_typed_task_without_side_effects(tmp_path, monkeypatch, preserve_case):
    from google.protobuf.any_pb2 import Any as ProtoAny

    from benchmarks.sregym.fastloop.responder_script import main
    from sdo.contracts import AgentTask

    packed = ProtoAny()
    packed.Pack(IncidentRequest(incident_id="cli", namespace="app", repository_worktree=str(tmp_path)))
    task = AgentTask(request=packed)
    task_path, output = tmp_path / "task.json", tmp_path / "script.json"
    task_path.write_text(json_format.MessageToJson(task, preserving_proto_field_name=preserve_case))
    monkeypatch.setattr(sys, "argv", ["responder_script", "--task", str(task_path), "--out", str(output)])
    main()
    data = json.loads(output.read_text())
    assert data["turns"][0]["incident"] == "cli"
    assert all(step["input_json"] == "{{request}}" for step in data["turns"][0]["steps"])
    assert {path.name for path in tmp_path.iterdir()} == {"task.json", "script.json"}
