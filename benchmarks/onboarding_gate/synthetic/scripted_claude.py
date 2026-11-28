#!/usr/bin/env python3
"""A stand-in for ``claude -p`` in stream-json mode that runs a fixed onboarding script, no model.

It reads the first user message, runs each step of ``/kit/scripted_agent.sh`` (steps are separated
by ``# step NAME`` lines) as one Bash tool call, prints the calls and results as stream-json, and
ends the turn with a successful result. Any later user message (an export rejection) ends the
session with an error result, so a rejected scripted export fails the run instead of looping.
Standard library only: it runs inside the session sandbox.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

SCRIPT = Path("/kit/scripted_agent.sh")


def emit(event: dict[str, object]) -> None:
    print(json.dumps(event), flush=True)


def steps(script: str) -> list[tuple[str, str]]:
    found: list[tuple[str, list[str]]] = []
    for line in script.splitlines():
        if line.startswith("# step "):
            found.append((line.removeprefix("# step ").strip(), []))
        elif found:
            found[-1][1].append(line)
    return [(name, "\n".join(body)) for name, body in found]


def main() -> int:
    session = "scripted-session"
    if not sys.stdin.readline():
        return 1
    emit({"type": "system", "subtype": "init", "session_id": session})
    failed = False
    for index, (name, command) in enumerate(steps(SCRIPT.read_text())):
        tool_id = f"scripted-{index}"
        emit(
            {
                "type": "assistant",
                "session_id": session,
                "message": {
                    "id": f"msg-{index}",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": tool_id,
                            "name": "Bash",
                            "input": {"command": command, "description": name},
                        }
                    ],
                },
            }
        )
        done = subprocess.run(["bash", "-c", command], capture_output=True, text=True, check=False, cwd="/work")
        output = (done.stdout + done.stderr)[-20000:]
        emit(
            {
                "type": "user",
                "session_id": session,
                "message": {
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": tool_id,
                            "content": [{"type": "text", "text": output}],
                            "is_error": done.returncode != 0,
                        }
                    ]
                },
            }
        )
        if done.returncode != 0 and not name.endswith("(may fail)"):
            print(f"scripted step {name} failed ({done.returncode}):\n{output}", file=sys.stderr)
            failed = True
            break
    emit(
        {
            "type": "result",
            "subtype": "error_during_execution" if failed else "success",
            "is_error": failed,
            "session_id": session,
            "result": "scripted onboarding finished",
            "total_cost_usd": 0,
            "modelUsage": {},
        }
    )
    for line in sys.stdin:
        print(f"scripted session got a follow-up it cannot answer:\n{line[:4000]}", file=sys.stderr)
        emit({"type": "result", "subtype": "error_during_execution", "is_error": True, "session_id": session})
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
