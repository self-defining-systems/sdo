"""``codex`` command-line entry point of the scripted CLI.

Accepts what agentshim's Codex provider passes (``codex --help``, ``codex exec
[resume <thread> -] ... --json ...`` with the prompt on stdin) and refuses any
turn that is not an SDO incident responder or reflection turn, so nothing can
silently stand in for a model call the assurance run did not plan.

Stdlib-only: this runs inside the controller and responder images.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .directive import Binding
from .reflection import ReflectionCrash, idempotency, reflect
from .responder import respond, warm_playbooks
from .store import Store, StoreError, default_store
from .turn import Turn
from .wire import CLI_VERSION, EventStream, ResumeError, Rollout

RESPONDER_MARKER = "You are the SDO incident responder for incident "
REFLECTION_MARKER = "Idempotency key: reflection:"
REQUEST_PATH = Path("/sdo/request/agent-task.json")
EXIT_UNPLANNED = 3

HELP = """Scripted Codex CLI for SDO assurance runs (no model).

Usage: codex exec [resume <THREAD_ID> -] [OPTIONS] [PROMPT]
"""


class Invocation:
    def __init__(self, argv: list[str]) -> None:
        if not argv or argv[0] != "exec":
            raise ValueError(f"unsupported codex invocation: {argv[:3]}")
        rest = argv[1:]
        self.resume: str | None = None
        if rest[:1] == ["resume"]:
            if len(rest) < 2:
                raise ValueError("codex exec resume needs a thread id")
            self.resume = rest[1]
            rest = rest[3:] if rest[2:3] == ["-"] else rest[2:]
        self.model: str | None = None
        positional: list[str] = []
        takes_value = {"--model", "-m", "--config", "-c", "--output-schema", "-C", "--cd", "--sandbox", "-s"}
        index = 0
        while index < len(rest):
            token = rest[index]
            if token in takes_value:
                if token in {"--model", "-m"} and index + 1 < len(rest):
                    self.model = rest[index + 1]
                index += 2
                continue
            if not token.startswith("-"):
                positional.append(token)
            index += 1
        self.prompt_argument = positional[-1] if positional and positional[-1] != "-" else None


def _unpack_task(task: object) -> dict[str, Any]:
    """The IncidentRequest packed in the mounted AgentTask envelope (protojson, stdlib only)."""

    packed = task.get("request") if isinstance(task, dict) else None
    if not isinstance(packed, dict):
        raise ValueError("agent task has no packed incident request")
    return {_snake(key): value for key, value in packed.items() if key != "@type"}


def _snake(name: str) -> str:
    return "".join(f"_{char.lower()}" if char.isupper() else char for char in name)


def _request(prompt: str) -> dict[str, Any]:
    path = Path(os.environ.get("SDO_REQUEST_PATH", str(REQUEST_PATH)))
    if path.is_file():
        payload = _unpack_task(json.loads(path.read_text(encoding="utf-8")))
    else:
        marker = "Incident request:\n"
        if marker not in prompt:
            raise ValueError("no incident request file and no request in the prompt")
        payload = json.loads(prompt.split(marker, 1)[1])
    if not isinstance(payload, dict):
        raise ValueError("incident request must be a JSON object")
    return payload


def _binding(store: Store, incident_id: str) -> Binding:
    existing = store.binding(incident_id)
    if existing is not None:
        return existing
    directive = store.directive()
    return store.bind(
        Binding(incident_id=incident_id, directive=directive, bound_at=datetime.now(timezone.utc).isoformat())
    )


def run(argv: list[str], *, stdin: str, store: Store | None = None) -> int:
    if not argv or argv[0] in {"--help", "-h", "help"}:
        print(HELP)
        return 0
    if argv[0] in {"--version", "-V"}:
        print(f"codex-cli {CLI_VERSION}")
        return 0
    try:
        invocation = Invocation(argv)
    except ValueError as exc:
        print(f"scripted codex: {exc}", file=sys.stderr)
        return 2
    prompt = invocation.prompt_argument if invocation.prompt_argument is not None else stdin
    if RESPONDER_MARKER in prompt[:200]:
        kind = "responder"
    elif REFLECTION_MARKER in prompt:
        kind = "reflection-resume" if invocation.resume else "reflection-fresh"
        if "You are correcting an operational-memory proposal" in prompt:
            kind = "reflection-retry"
    else:
        print(
            "scripted codex: no scripted plan for this turn (not an SDO incident responder or reflection turn); "
            "refusing so no unplanned model call is faked",
            file=sys.stderr,
        )
        return EXIT_UNPLANNED
    store = store or default_store()
    cwd = Path.cwd()
    home = Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex")))
    try:
        if kind == "responder":
            request = _request(prompt)
            incident_id = str(request["incident_id"])
            if warm_playbooks(prompt):
                kind = "responder-warm"
        else:
            incident_id, _commit = idempotency(prompt)
            request = {}
        binding = _binding(store, incident_id)
    except (StoreError, ValueError, KeyError, OSError) as exc:
        print(f"scripted codex: cannot plan the turn: {exc}", file=sys.stderr)
        return 1
    try:
        rollout = Rollout.resume(home, invocation.resume) if invocation.resume else Rollout.create(home, cwd=str(cwd))
    except ResumeError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    stream = EventStream()
    stream.thread_started(rollout.session_id)
    stream.turn_started()
    rollout.task_started()
    rollout.user_message(prompt)
    turn = Turn(kind=kind, seed=binding.directive.usage_seed, cwd=cwd, rollout=rollout, stream=stream)
    record_extra: dict[str, object] = {
        "incident_id": incident_id,
        "scenario": binding.directive.scenario,
        "resumed": invocation.resume is not None,
        "model": invocation.model,
    }
    try:
        if kind.startswith("responder"):
            answer = respond(turn, binding.directive, request, prompt, cwd)
        else:
            answer = reflect(turn, binding, prompt, cwd, store)
    except ReflectionCrash as exc:
        store.record_turn(f"{rollout.session_id}:{turn.started_at}", turn.record(outcome="crashed", **record_extra))
        print(f"scripted codex: {exc}", file=sys.stderr)
        return 1
    stopped = [note for note in answer.get("repair_changes") or [] if str(note).startswith("scripted plan stopped")]
    if stopped:
        record_extra["plan_error"] = stopped[0]
        print(f"scripted codex: {stopped[0]}", file=sys.stderr)
    text = json.dumps(answer)
    turn.finish(text)
    store.record_turn(
        f"{rollout.session_id}:{turn.started_at}",
        turn.record(outcome=str(answer.get("status") or answer.get("learning_decision")), **record_extra),
    )
    return 0


def main() -> int:
    argv = sys.argv[1:]
    stdin = "" if not argv or argv[0] != "exec" or sys.stdin.isatty() else sys.stdin.read()
    return run(argv, stdin=stdin)
