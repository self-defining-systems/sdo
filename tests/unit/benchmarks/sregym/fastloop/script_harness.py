"""In-process script step harness for focused synthetic tool scenarios.

Transport contracts separately exercise the generated commands in subprocesses.
"""

import io
import json
import os
import shlex
import socket as socket
import time as time
import types
from pathlib import Path
from unittest.mock import patch

from google.protobuf import json_format

from benchmarks.sregym.fastloop import reflection_script as reflection
from benchmarks.sregym.fastloop import responder_steps
from benchmarks.sregym.fastloop.reflection_script import (
    NETWORKPOLICY_PROGRAM_ID as NETWORKPOLICY_PROGRAM_ID,
)
from benchmarks.sregym.fastloop.reflection_script import (
    NETWORKPOLICY_PROGRAM_SOURCE,
    PROGRAM_SOURCE,
)
from benchmarks.sregym.fastloop.reflection_script import (
    PROGRAM_ID as PROGRAM_ID,
)
from benchmarks.sregym.fastloop.reflection_script import (
    _captured_pair as _captured_pair,
)
from benchmarks.sregym.fastloop.reflection_script import (
    _captured_policy_pair as _captured_policy_pair,
)
from benchmarks.sregym.fastloop.responder_script import responder_script
from benchmarks.sregym.fastloop.responder_steps import (
    _command,
)
from sdo.agents.responder import ReflectionTurn, run_reflection_task, run_worker_task
from sdo.agents.responder.worker import KubernetesResultPublisher
from sdo.contracts import IncidentResult

for _name, _value in vars(responder_steps).items():
    if not _name.startswith("__"):
        globals()[_name] = (
            types.FunctionType(_value.__code__, globals(), _name, _value.__defaults__, _value.__closure__)
            if isinstance(_value, types.FunctionType)
            else _value
        )
        if isinstance(_value, types.FunctionType):
            globals()[_name].__kwdefaults__ = _value.__kwdefaults__


def respond(request):
    """Execute the generated phases with injected tool boundaries."""
    import tempfile

    from benchmarks.sregym.agent_script.template import TemplateContext

    with tempfile.TemporaryDirectory() as temporary:
        with patch.dict(os.environ, {"SDO_TOOLS_DIR": temporary}):
            script = responder_script(request)
            context = TemplateContext(
                json_format.MessageToDict(request, preserving_proto_field_name=True),
                Path(request.repository_worktree or temporary),
            )
            for index, step in enumerate(script.turns[0].steps):
                code = shlex.split(step.command)[-1]
                marker = "started = _now()" if index == 0 else "state = json.loads(state_path.read_text())"
                head, tail = code.split(marker, 1)
                scope = {}
                output = io.StringIO()
                with patch("sys.stdin", io.StringIO(json_format.MessageToJson(request))), patch("sys.stdout", output):
                    exec(head, scope)
                    scope.update(
                        {
                            name: globals()[name]
                            for name in ("_tool", "_command", "_patch_from_source", "_assess_drift", "_matched_plans")
                        }
                    )
                    exec(marker + tail, scope)
                if not step.capture_json:
                    continue
                value = json.loads(output.getvalue())

                def flatten(prefix, item):
                    context.values[prefix] = item
                    if isinstance(item, dict):
                        for key, child in item.items():
                            flatten(prefix + "." + key, child)

                flatten(f"steps.{index}.output", value)
            return json_format.ParseDict(context.expand(script.turns[0].result, now=context.clock()), IncidentResult())


class Reflector:
    def resume(self, *, worktree, incident_id, closure, **kwargs):
        from benchmarks.sregym.agent_script.model import Delete, ScriptTurn, Tool, Write
        from benchmarks.sregym.agent_script.runner import execute
        from benchmarks.sregym.agent_script.wire import EventStream

        tools = worktree.parent / "reflection-tools"
        tools.mkdir(exist_ok=True)
        with patch.dict(os.environ, {"SDO_TOOLS_DIR": str(tools)}):
            script = reflection.reflection_script(
                worktree=worktree,
                incident_id=incident_id,
                closure=closure,
                capture=Path(os.environ["SDO_CAPTURED_CORPUS_DIR"]),
                program_source=PROGRAM_SOURCE,
                policy_program_source=NETWORKPOLICY_PROGRAM_SOURCE,
            )
            for step in script.turns[0].steps:
                if isinstance(step, Write):
                    path = worktree / step.path
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(step.text)
                elif isinstance(step, Delete):
                    (worktree / step.path).unlink(missing_ok=True)
                elif isinstance(step, Tool):
                    _command(step.argv)
                else:
                    # Decision commands use the same writable exchange as the generator.
                    execute(ScriptTurn(role="reflector", steps=[step], result={}), worktree, EventStream(io.StringIO()))
        result = dict(script.turns[0].result)
        result["playbook_ids"] = result.pop("proposed_artifact_ids", [])
        return ReflectionTurn(**result)


def main(argv):
    publisher = KubernetesResultPublisher(namespace=os.environ["SDO_NAMESPACE"])
    args = (Path(os.environ["SDO_TASK_PATH"]), os.environ["SDO_RESULT_CONFIGMAP"], publisher)
    if argv == ["responder"]:
        run_worker_task(*args, executor=respond)
    else:
        run_reflection_task(*args, reflector=Reflector())
    return 0
