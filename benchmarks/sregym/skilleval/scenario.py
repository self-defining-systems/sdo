"""Build a reflection scenario from a recorded run row.

The scenario is the state at reflection start:

- ``task.json``: the recorded reflection ``AgentTask`` (``checkpoints/<n>/reflection/request.json``).
- ``session/``: the incident session directory (``$SDO_SESSION_DIR``: context files, corpus,
  scratch, tools, API history and the Codex home) with credentials, reflection usage, the
  reflection's own worker checkpoint and Codex's derived SQLite state removed, and the
  responder rollout cut before the reflection turn.
- ``checkout/``: a standalone clone of the memory repository at the outcome commit.
- ``live/``: what the live reflection did (usage lines and its worker result), for comparison.
- ``scenario.json``: identity, source and expectation.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

REFLECTION_MARKER = "Idempotency key: reflection:"
SCENARIO_VERSION = 1
# Codex rebuilds these from the rollout; a copy taken after reflection would leak the reflection turn.
_CODEX_DERIVED = ("state_", "thread_history_", "goals_", "queue_", "logs_", "memories_")
_SECRET_NAMES = frozenset({"auth.json", ".credentials.json"})


class ScenarioError(ValueError):
    """A recorded row cannot be turned into a faithful reflection scenario."""


@dataclass(frozen=True)
class ScenarioSource:
    host: str
    row_dir: str
    incident: str
    problem_id: str
    encounter: str

    def __post_init__(self) -> None:
        if not self.row_dir or not self.incident:
            raise ValueError("scenario source requires a row directory and incident index")


@dataclass(frozen=True)
class Scenario:
    scenario_id: str
    description: str
    expectation: str
    image: str
    incident_id: str
    session_digest: str
    thread_id: str
    checkout_path: str
    outcome_commit: str
    source: ScenarioSource
    session_mode: str = "resume"
    live: dict[str, Any] = field(default_factory=dict)
    version: int = SCENARIO_VERSION

    def __post_init__(self) -> None:
        if not self.scenario_id or "/" in self.scenario_id:
            raise ValueError(f"invalid scenario id {self.scenario_id!r}")
        if not self.image:
            raise ValueError("scenario requires the recorded responder image")
        if not self.checkout_path.startswith("/"):
            raise ValueError("scenario checkout path must be the absolute in-pod path")

    @classmethod
    def load(cls, directory: Path) -> Scenario:
        data = json.loads((directory / "scenario.json").read_text(encoding="utf-8"))
        data["source"] = ScenarioSource(**data["source"])
        return cls(**data)

    def write(self, directory: Path) -> None:
        (directory / "scenario.json").write_text(json.dumps(asdict(self), indent=2) + "\n", encoding="utf-8")


def session_digest(incident_id: str) -> str:
    """The session directory name the controller derives from an incident ID."""

    return hashlib.sha256(incident_id.encode()).hexdigest()[:32]


def cut_rollout(lines: list[str]) -> tuple[list[str], str]:
    """Return the rollout prefix that ends with the responder turn, and the thread ID.

    Live responder turns end when the worker submits its result, so a responder turn
    may have no ``task_complete`` event. The cut is therefore the turn boundary: the
    ``task_started`` event of the first turn whose user input carries the reflection
    idempotency key. Everything before it is the responder session.
    """

    thread_id = ""
    turn_start: int | None = None
    for index, line in enumerate(lines):
        record = json.loads(line)
        payload = record.get("payload")
        if not isinstance(payload, dict):
            continue
        if record.get("type") == "session_meta":
            thread_id = str(payload.get("id") or payload.get("session_id") or "")
        if record.get("type") == "event_msg" and payload.get("type") == "task_started":
            turn_start = index
        if (
            record.get("type") == "response_item"
            and payload.get("type") == "message"
            and payload.get("role") == "user"
            and any(REFLECTION_MARKER in str(part.get("text", "")) for part in payload.get("content", []))
        ):
            if turn_start is None or turn_start == 0:
                raise ScenarioError("reflection turn has no preceding responder turn")
            if not thread_id:
                raise ScenarioError("rollout has no session_meta thread ID")
            return lines[:turn_start], thread_id
    raise ScenarioError("rollout has no reflection turn to cut before")


def _copy_session(source: Path, destination: Path) -> None:
    def ignore(directory: str, names: list[str]) -> set[str]:
        return {name for name in names if name in _SECRET_NAMES}

    shutil.copytree(source, destination, ignore=ignore, symlinks=True)


REFLECTION_TYPE = "type.googleapis.com/sdodev.contracts.v1alpha1.ReflectionRequest"


def first_reflection_attempt(session: Path) -> str | None:
    """The first reflection attempt's task wire among the session's worker checkpoints."""

    attempts: list[tuple[int, str]] = []
    for request in (session / "checkpoints").glob("*/request.json"):
        wire = request.read_text(encoding="utf-8")
        payload = json.loads(wire).get("request") or {}
        if payload.get("@type") == REFLECTION_TYPE:
            attempts.append((int(payload.get("validation_attempt") or 1), wire))
    if not attempts:
        return None
    attempts.sort(key=lambda item: item[0])
    return attempts[0][1]


def prepare_session(session: Path, *, fresh: bool = False) -> tuple[str, dict[str, Any]]:
    """Rewind a copied session directory to reflection start; return the thread ID and live evidence.

    A fresh reflection (no responder session to resume) starts with no rollout at all.
    """

    live: dict[str, Any] = {"attempts": []}
    usage = session / "reflection-usage.jsonl"
    if usage.is_file():
        live["usage"] = [json.loads(line) for line in usage.read_text(encoding="utf-8").splitlines() if line.strip()]
        usage.unlink()
    for request in sorted((session / "checkpoints").glob("*/request.json")):
        payload = json.loads(request.read_text(encoding="utf-8")).get("request") or {}
        if payload.get("@type") != REFLECTION_TYPE:
            continue
        attempt: dict[str, Any] = {"validation_attempt": int(payload.get("validation_attempt") or 1)}
        result = request.parent / "result.json"
        if result.is_file():
            attempt["result"] = json.loads(result.read_text(encoding="utf-8"))
        edits = request.parent / "file_edits.json"
        if edits.is_file():
            attempt["file_edits"] = json.loads(edits.read_text(encoding="utf-8"))
        live["attempts"].append(attempt)
        shutil.rmtree(request.parent)
    live["attempts"].sort(key=lambda item: item["validation_attempt"])
    codex = session / "codex"
    for child in list(codex.iterdir()) if codex.is_dir() else []:
        if child.is_file() and child.name.startswith(_CODEX_DERIVED):
            child.unlink()
    rollouts = sorted(codex.glob("sessions/**/rollout-*.jsonl"))
    thread_id = ""
    if fresh:
        for rollout in rollouts:
            rollout.unlink()
        return thread_id, live
    for rollout in rollouts:
        lines = rollout.read_text(encoding="utf-8").splitlines(keepends=True)
        try:
            kept, thread = cut_rollout(lines)
        except ScenarioError:
            # A rollout without the reflection turn is a later fresh or retry session, not the responder's.
            rollout.unlink()
            continue
        if thread_id:
            raise ScenarioError(f"more than one rollout carries the reflection turn: {rollout}")
        thread_id = thread
        rollout.write_text("".join(kept), encoding="utf-8")
    if not thread_id:
        raise ScenarioError(f"no responder rollout with a reflection turn under {codex}")
    return thread_id, live


def clone_checkout(memory_repository: str, destination: Path, commit: str) -> None:
    for command in (
        ["git", "clone", "--quiet", "--no-hardlinks", "--no-checkout", "--", memory_repository, str(destination)],
        ["git", "-C", str(destination), "reset", "--quiet", "--hard", commit],
    ):
        completed = subprocess.run(command, capture_output=True, text=True, check=False)
        if completed.returncode:
            raise ScenarioError(f"prepare checkout: {' '.join(command[:3])}: {completed.stderr.strip()[-2000:]}")


def build_scenario(
    *,
    scenario_id: str,
    row_dir: Path,
    incident: str,
    session_source: Path,
    memory_repository: str,
    image: str,
    output: Path,
    description: str = "",
    expectation: str = "",
    host: str = "",
    encounter: str = "",
) -> Scenario:
    """Write one scenario directory; ``session_source`` must be the full live session directory."""

    digest_dir = session_source.name
    if output.exists():
        raise ScenarioError(f"scenario output already exists: {output}")
    wire = first_reflection_attempt(session_source)
    if wire is None:
        request_path = row_dir / "checkpoints" / incident / "reflection" / "request.json"
        if not request_path.is_file():
            raise ScenarioError(f"row has no reflection request: {request_path}")
        wire = request_path.read_text(encoding="utf-8")
    task = json.loads(wire)
    request = task["request"]
    if int(request.get("validation_attempt") or 1) != 1:
        raise ScenarioError("the recorded reflection request is a retry; the first attempt is not recorded")
    incident_id = request["outcome"]["incident_id"]
    digest = session_digest(incident_id)
    if digest_dir != digest:
        raise ScenarioError(f"session {digest_dir} is not the incident session {digest}")
    output.mkdir(parents=True)
    (output / "task.json").write_text(wire, encoding="utf-8")
    _copy_session(session_source, output / "session")
    session_mode = request.get("session_mode") or "fresh"
    if session_mode not in {"fresh", "resume"}:
        raise ScenarioError(f"unknown reflection session mode {session_mode!r}")
    thread_id, live = prepare_session(output / "session", fresh=session_mode == "fresh")
    closure_session = request.get("closure", {}).get("result", {}).get("responder_session_id", "")
    if session_mode == "resume" and closure_session != thread_id:
        raise ScenarioError(f"responder session {closure_session} is not the cut rollout thread {thread_id}")
    live_dir = output / "live"
    live_dir.mkdir()
    for attempt in live["attempts"]:
        # The live reflection's own edits and result, for side-by-side comparison with a run.
        name = f"attempt-{attempt['validation_attempt']}"
        (live_dir / f"{name}.json").write_text(json.dumps(attempt, indent=1) + "\n", encoding="utf-8")
    commit = request["outcome_commit"]
    clone_checkout(memory_repository, output / "checkout", commit)
    problems = sorted(path.name for path in row_dir.glob(f"{incident}_*") if path.is_dir())
    scenario = Scenario(
        scenario_id=scenario_id,
        description=description,
        expectation=expectation,
        image=image,
        incident_id=incident_id,
        session_digest=digest,
        thread_id=thread_id,
        checkout_path=task["workspace"]["checkout_path"],
        outcome_commit=commit,
        source=ScenarioSource(
            host=host,
            row_dir=str(row_dir),
            incident=incident,
            problem_id=problems[0][len(incident) + 1 :] if problems else "",
            encounter=encounter,
        ),
        live=_live_summary(live),
        session_mode=session_mode,
    )
    scenario.write(output)
    return scenario


def _live_summary(live: dict[str, Any]) -> dict[str, Any]:
    """What the live reflection cost and decided, without its file contents."""

    summary: dict[str, Any] = {"usage": live.get("usage", [])}
    summary["attempts"] = [
        {
            "validation_attempt": attempt["validation_attempt"],
            "status": (attempt.get("result") or {}).get("status"),
            "failure_reason": (attempt.get("result") or {}).get("failure_reason"),
            "changed_files": sorted(
                edit.get("path", "") for edit in (attempt.get("file_edits") or {}).get("edits") or []
            ),
        }
        for attempt in live.get("attempts", [])
    ]
    for record in summary["usage"]:
        record.pop("shell_command_lines", None)
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--id", required=True)
    parser.add_argument("--row-dir", type=Path, required=True)
    parser.add_argument("--incident", default="000")
    parser.add_argument("--session", type=Path, required=True, help="full live session directory (readable copy)")
    parser.add_argument("--memory-repository", required=True, help="git URL or path holding the outcome commit")
    parser.add_argument("--image", required=True, help="the responder image the live reflection ran")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--description", default="")
    parser.add_argument("--expectation", default="")
    parser.add_argument("--host", default="")
    parser.add_argument("--encounter", default="")
    args = parser.parse_args(argv)
    scenario = build_scenario(
        scenario_id=args.id,
        row_dir=args.row_dir,
        incident=args.incident,
        session_source=args.session,
        memory_repository=args.memory_repository,
        image=args.image,
        output=args.output,
        description=args.description,
        expectation=args.expectation,
        host=args.host,
        encounter=args.encounter,
    )
    print(json.dumps(asdict(scenario)["source"] | {"id": scenario.scenario_id, "thread": scenario.thread_id}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
