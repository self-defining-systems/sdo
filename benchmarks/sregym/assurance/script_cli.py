"""Assurance telemetry around the general agent-script executor.

A bundle is bound once to an incident, so a later case cannot change reflection
or retries. This wrapper has no mitigation, reflection or fault menus.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import tempfile
from pathlib import Path
from typing import Any

from benchmarks.sregym.agent_script import cli
from benchmarks.sregym.agent_script.invocation import Invocation
from benchmarks.sregym.agent_script.template import utc_now
from benchmarks.sregym.agent_script.wire import TOKEN_FIELDS, EventStream, Rollout
from benchmarks.sregym.assurance.script_store import default_store
from benchmarks.sregym.assurance.usage import request_usage


def run(argv: list[str], stdin: str) -> int:
    if not argv or argv[0] != "exec":
        print("codex-cli 0.0.0-agent-script (no model)")
        return 0
    invocation = Invocation(argv)
    prompt = invocation.prompt_argument or stdin
    store = default_store()
    path = Path(os.environ.get("SDO_TASK_PATH", os.environ.get("SDO_REQUEST_PATH", "/sdo/request/agent-task.json")))
    request = json.loads(path.read_text()).get("request", {}) if path.exists() else {}
    if not request and "Incident request:\n" in prompt:
        request = json.loads(prompt.split("Incident request:\n", 1)[1])
    reflection = re.search(r"Idempotency key: reflection:(.+):([0-9a-f]{7,40})\s*$", prompt, re.MULTILINE)
    incident = reflection[1] if reflection else request.get("incident_id", request.get("incidentId"))
    if not incident:
        print("no scripted plan: missing incident", file=sys.stderr)
        return 3
    bundle = store.binding(incident) or store.bind(incident, store.script())
    warm = "Warm path: validated incident memory matches this incident." in prompt
    kind = (
        ("reflection-resume" if invocation.resume else "reflection-fresh")
        if reflection
        else ("responder-warm" if warm else "responder")
    )
    if "You are correcting an operational-memory proposal" in prompt:
        kind = "reflection-retry"
    home = Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex")))
    rollout = (
        Rollout.resume(home, invocation.resume) if invocation.resume else Rollout.create(home, cwd=str(Path.cwd()))
    )
    usage = dict.fromkeys(TOKEN_FIELDS, 0)
    commands: list[dict[str, Any]] = []
    requests = 0
    answer: dict[str, Any] = {}

    class RecordedStream(EventStream):
        def response(self) -> None:
            nonlocal requests
            item = request_usage(bundle.seed, kind, requests)
            requests += 1
            rollout.token_count(item)
            for field in TOKEN_FIELDS:
                usage[field] += getattr(item, field)

        def thread_started(self, session_id: str) -> None:
            super().thread_started(rollout.session_id)
            rollout.task_started()
            rollout.user_message(prompt)

        def command_started(self, command: str) -> str:
            self.response()
            item = super().command_started(command)
            rollout.function_call(item, command)
            return item

        def command_completed(self, item_id: str, command: str, output: str, exit_code: int) -> None:
            commands.append({"command": command, "exit_code": exit_code})
            rollout.function_call_output(item_id, output, exit_code)
            super().command_completed(item_id, command, output, exit_code)

        def agent_message(self, text: str) -> None:
            nonlocal answer
            answer = json.loads(text)
            if set(answer) == {"incident_result", "reflection"}:
                # Turns answer in their own branch of the session wrapper.
                answer = answer["incident_result"] or answer["reflection"]
            self.response()
            rollout.assistant_message(text)
            super().agent_message(text)

        def turn_completed(self, session_totals: dict[str, int]) -> None:
            super().turn_completed(rollout.totals)

    original = cli.EventStream
    cli.EventStream = RecordedStream
    started = cli.TemplateContext({}, Path.cwd()).values["turn.started_at"]
    code = 1
    try:
        with tempfile.TemporaryDirectory() as directory:
            plan = Path(directory) / "script.json"
            plan.write_text(bundle.warm if warm else bundle.cold)
            task = Path(directory) / "task.json"
            if reflection:
                request = {"incident_id": incident, "outcome_commit": reflection[2]}
            task.write_text(json.dumps({"request": request}))
            # Stable script path and state identity across CLI retries in this checkout.
            stable = Path(os.environ.get("SDO_AGENT_SCRIPT_STATE", "/sdo/session/agent-script-state"))
            stable.mkdir(parents=True, exist_ok=True)
            plan = stable / (("warm" if warm else "cold") + ".json")
            plan.write_text(bundle.warm if warm else bundle.cold)
            os.environ["SDO_AGENT_SCRIPT"] = str(plan)
            os.environ["SDO_TASK_PATH"] = str(task)
            role = "reflector" if reflection else "responder"
            index = 0
            while not store.claim(f"{incident}:{role}:{index}"):
                index += 1
            key = hashlib.sha256(
                json.dumps([str(plan.resolve()), str(Path.cwd()), role, incident]).encode()
            ).hexdigest()
            (stable / f"{key}.json").write_text(json.dumps(index))
            try:
                code = cli.codex(argv, stdin=prompt)
            except ValueError as exc:
                if reflection or not str(exc).startswith("script assertion failed:"):
                    raise
                # An asserted live precondition or health check failed. Report
                # failed investigation, with no credited cause or repair.
                stream = RecordedStream()
                failed = {
                    "incident_id": incident,
                    "status": "failed",
                    "confirmed_root_causes": [],
                    "applied_playbooks": [],
                    "repair_actions": [],
                    "verification_evidence": [],
                    "repair_changes": [str(exc)],
                    "timing": {"started_at": started, "completed_at": utc_now()},
                }
                # The responder answers in its own branch of the session wrapper.
                stream.agent_message(json.dumps({"incident_result": failed, "reflection": None}))
                stream.turn_completed(rollout.totals)
                code = 0
            return code
    finally:
        cli.EventStream = original
        store.record_turn(
            f"{rollout.session_id}:{started}",
            {
                "incident_id": incident,
                "kind": kind,
                "session_id": rollout.session_id,
                "started_at": started,
                "resumed": bool(invocation.resume),
                "requests": requests,
                "usage": usage,
                "session_totals": rollout.totals,
                "commands": commands,
                "outcome": (answer.get("status") or answer.get("learning_decision")) if code == 0 else "crashed",
            },
        )


def main() -> int:
    try:
        return run(sys.argv[1:], sys.stdin.read() if sys.argv[1:2] == ["exec"] else "")
    except (ValueError, KeyError, OSError) as exc:
        print(f"agent script: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
