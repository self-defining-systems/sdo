"""Convert Codex model_io records to editable actions, never replay stale output."""

import base64
import json
from pathlib import Path

from benchmarks.sregym.fastloop.file_edits import decode_file_edits

from .model import AgentScript, Command, Delete, Role, ScriptTurn, Step, Write


def convert_recording(paths: list[Path], *, role: Role, file_edits: Path | None = None) -> AgentScript:
    turns = []
    for path in paths:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload["returncode"] != 0:
            raise ValueError(f"cannot convert failed turn: {path}")
        steps: list[Step] = []
        answer = None
        file_changes = False
        for stream, line in payload["events"]:
            if stream != "stdout":
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if event.get("type") != "item.completed":
                continue
            item = event["item"]
            if item["type"] == "command_execution":
                steps.append(Command(kind="command", command=item["command"], exit_code=item.get("exit_code", 0)))
            elif item["type"] == "file_change":
                file_changes = True
            elif item["type"] == "agent_message":
                try:
                    answer = json.loads(item["text"])
                except json.JSONDecodeError:
                    pass
        if file_changes and file_edits is None:
            raise ValueError(
                f"recording {path} has file_change events without replayable bytes; express these as write/delete steps"
            )
        if not isinstance(answer, dict):
            raise ValueError(f"no structured Codex answer in {path}")
        if set(answer) == {"incident_result", "reflection"}:
            # Responder and reflection turns share one wrapper; the script holds the role's own branch.
            branch = "incident_result" if role == "responder" else "reflection"
            answer = (
                answer[branch]
                if isinstance(answer[branch], dict)
                else next((value for value in answer.values() if isinstance(value, dict)), answer)
            )
        turns.append(ScriptTurn(role=role, index=payload["turn_index"], steps=steps, result=answer))
    turns.sort(key=lambda turn: turn.index)
    if file_edits is not None:
        for edit in decode_file_edits(file_edits.read_bytes()):
            if edit.content_base64 is None:
                turns[-1].steps.append(Delete(kind="delete", path=edit.path))
            else:
                text = base64.b64decode(edit.content_base64).decode("utf-8")
                turns[-1].steps.append(Write(kind="write", path=edit.path, text=text, executable=edit.mode == 0o755))
    return AgentScript(version=1, turns=turns)
