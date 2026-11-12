"""The closing turn: the responder's own session gets one short message after Recovered.

SDO sends it once the opening alerts cleared and stayed clear. It either asks the agent to turn the fix into a playbook
(``closing_prompt``) or, when the incident earned no learning (an escalated close, a restore only), to keep
or remove the app files it left uncaptured (``cleanup_prompt``). Go decides which and renders the recorded changes;
the agent edits ``.sdo/playbooks/`` and replies with one line. Go derives the decision, the corpus case, the match key
and credit.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
import time
from typing import TYPE_CHECKING, ClassVar, Protocol

from agentshim import SessionResumeError
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

from libs.agent_cli.structured import (
    AgentProvider,
    StructuredTurn,
    StructuredTurnError,
    turn_usage,
)
from sdo.agents.context_output import save_context
from sdo.agents.prompt_templates import RenderedPrompt, named_prompt, render_prompt
from sdo.agents.prompt_templates import run_agent_turn as run_structured_turn
from sdo.agents.provider import memory_check_timeout_seconds
from sdo.agents.replay import selected_skills_root
from sdo.agents.responder.credentials import SKILLS_ROOT
from sdo.agents.responder.memory_check import (
    VALIDATION_TIMEOUT_FILE_ENV,
    MemoryCheckTimeout,
    ValidationResult,
    check_reflection_result,
)
from sdo.agents.responder.session_turn import (
    WrongBranchError,
    branch_correction,
    branch_instruction,
    session_turn_options,
    unwrap_branch,
)
from sdo.contracts import ReflectionResult, validate

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from pathlib import Path

    from agentshim import CommandExecutor

    from sdo.contracts import IncidentClosure

#: The memory check runs once after the turn and, when it fails, once more after one correction turn.
MAX_CORRECTION_TURNS = 1
_PLAYBOOK_FILE = re.compile(r"^\.sdo/playbooks/([a-z][a-z0-9_-]*)/")


class ReflectionTurn(BaseModel):
    """What the closing turn returned: one line. Go derives everything else from the checkout's diff."""

    model_config = ConfigDict(extra="forbid")

    note: str = Field(min_length=1)
    # Provider accounting for the turn; kept out of the model-facing schema.
    _usage: dict[str, int | float] = PrivateAttr(default_factory=dict)

    @property
    def usage(self) -> dict[str, int | float]:
        return dict(self._usage)

    @classmethod
    def from_model_output(cls, payload: object) -> ReflectionTurn:
        if not isinstance(payload, dict) or not isinstance(payload.get("note"), str) or not payload["note"].strip():
            raise ValueError("closing turn requires a one-line note")
        return cls(note=payload["note"].strip()[:1000])

    def with_usage(self, usage: dict[str, int | float]) -> ReflectionTurn:
        self._usage = dict(usage)
        return self

    def to_result(self, worktree: Path | None = None) -> ReflectionResult:
        """The typed result: the decision the checkout's diff already says, with the agent's line as its reason."""
        changed = changed_playbooks(worktree) if worktree is not None else []
        if changed:
            result = ReflectionResult(summary=self.note, learning_decision="updated", proposed_artifact_ids=changed)
        else:
            result = ReflectionResult(summary=self.note, learning_decision="no_change", no_change_reason=self.note)
        validate(result)
        return result


def changed_memory(worktree: Path) -> bool:
    """Whether the checkout differs from its base anywhere under .sdo/."""
    listed = subprocess.run(
        ["git", "-C", str(worktree), "status", "--porcelain", "--untracked-files=all", "--", ".sdo"],
        capture_output=True,
        text=True,
        check=False,
    )
    return bool(listed.stdout.strip())


def changed_playbooks(worktree: Path) -> list[str]:
    """IDs of the playbooks whose files the closing turn changed in the checkout."""
    listed = subprocess.run(
        ["git", "-C", str(worktree), "status", "--porcelain", "--untracked-files=all", "--", ".sdo/playbooks"],
        capture_output=True,
        text=True,
        check=False,
    )
    ids = {
        match.group(1)
        for line in listed.stdout.splitlines()
        if (match := _PLAYBOOK_FILE.match(line[3:].strip().split(" -> ")[-1]))
    }
    return sorted(ids)


def reflection_output_schema() -> dict[str, object]:
    """The model-facing schema of the closing turn's reply: one line."""
    return {
        "type": "object",
        "properties": {"note": {"type": "string", "description": "One line."}},
        "required": ["note"],
        "additionalProperties": False,
    }


def closing_prompt(*, incident_id: str, changes: Sequence[str]) -> RenderedPrompt:
    """The learning turn: about 600 bytes plus the change list Go rendered from the recording."""
    return render_prompt(
        "responder",
        "scriptify",
        part="reflection.scriptify",
        incident_id=incident_id,
        skills_root=selected_skills_root(SKILLS_ROOT),
        changes=named_prompt("reflection.changes", "\n".join(changes) or "(none recorded)"),
        branch_instruction=branch_instruction("reflection"),
    )


def cleanup_prompt(*, leftovers: Sequence[str], turn: int) -> RenderedPrompt:
    """The cleanup turn: keep or remove the app files left uncaptured."""
    shown = list(leftovers)[:12]
    listed = "\n".join(shown) + (f"\n(and {len(leftovers) - len(shown)} more)" if len(leftovers) > len(shown) else "")
    return render_prompt(
        "responder",
        "cleanup",
        part="reflection.cleanup",
        leftovers=named_prompt("reflection.leftovers", listed),
        turn=str(turn),
        branch_instruction=branch_instruction("reflection"),
    )


class StatefulResponderBackend(Protocol):
    def resume(
        self,
        *,
        session_id: str,
        worktree: Path,
        prompt: RenderedPrompt,
        idempotency_key: str,
        check: bool = True,
    ) -> ReflectionTurn: ...


class ResponderSessionUnavailable(RuntimeError):
    """The provider cannot continue the responder's incident session."""


#: Absolute Unix time of the controller's frozen reflection task deadline. The
#: worker binds it from the task; the Job and any claimed standby end no earlier.
REFLECTION_DEADLINE_ENV = "SDO_REFLECTION_DEADLINE"
#: The agent stops this long before that deadline so its typed result is published first.
REFLECTION_RESULT_MARGIN_SECONDS = 120.0


class ReflectionDeadlineExceeded(RuntimeError):
    """The closing turn used its whole budget before the agent returned a valid result."""


def reflection_budget_seconds() -> float:
    """Seconds the closing turn may use, ending before the frozen task deadline."""
    raw = os.environ.get(REFLECTION_DEADLINE_ENV)
    if not raw:
        raise ReflectionDeadlineExceeded(
            f"{REFLECTION_DEADLINE_ENV} is not set; the controller binds every reflection task to a deadline"
        )
    budget = float(raw) - time.time() - REFLECTION_RESULT_MARGIN_SECONDS
    if not budget > 0:
        raise ReflectionDeadlineExceeded("reflection task deadline leaves no agent budget")
    return budget


class SessionReflector:
    """Send the closing turn into the responder's own session."""

    def __init__(self, backend: StatefulResponderBackend) -> None:
        self.backend = backend

    def learn(
        self,
        *,
        session_id: str,
        incident_id: str,
        outcome_commit: str,
        worktree: Path,
        changes: Sequence[str],
        closure: IncidentClosure | None = None,
    ) -> ReflectionTurn:
        return self.backend.resume(
            session_id=session_id,
            worktree=worktree,
            prompt=closing_prompt(incident_id=incident_id, changes=changes),
            idempotency_key=f"closing:{incident_id}:{outcome_commit}",
        )

    def cleanup(
        self, *, session_id: str, incident_id: str, worktree: Path, leftovers: Sequence[str], turn: int
    ) -> ReflectionTurn:
        return self.backend.resume(
            session_id=session_id,
            worktree=worktree,
            prompt=cleanup_prompt(leftovers=leftovers, turn=turn),
            idempotency_key=f"closing-cleanup:{incident_id}:{turn}",
            check=False,
        )


def memory_check_correction(check: ValidationResult, idempotency_key: str) -> RenderedPrompt:
    details = check.stdout + "\n" + check.stderr
    feedback = details
    try:
        report = json.loads(check.stdout)
        diagnostics = report.get("diagnostics", [])
        if diagnostics:
            feedback = "\n".join(
                f"[{item.get('rule', '')}] {item.get('path', '')}: {item.get('message', '')}" for item in diagnostics
            )
            _, path = save_context(details)
            feedback += "\nFull check: " + path
    except (ValueError, AttributeError, TypeError):
        pass
    correction = render_prompt(
        "responder",
        "reflection_check_correction",
        part="reflection.instructions",
        # The diagnostics are data of any length: over the budget they spill to a
        # details file the prompt points at instead of failing the turn.
        diagnostics=named_prompt("reflection.validation_feedback", feedback, required=False),
    )
    return render_prompt(
        "responder",
        "turn_01",
        part="reflection.instructions",
        idempotency_key=idempotency_key,
        prompt=correction,
    )


class CodexSessionBackend:
    """Continue the responder's own Codex session for the closing turn."""

    provider: ClassVar[AgentProvider] = "codex"

    def __init__(
        self,
        *,
        model: str | None = None,
        reasoning_effort: str,
        timeout_seconds: float | None = None,
        executor: CommandExecutor | None = None,
        memory_checker: Callable[[Path, ReflectionResult, float], ValidationResult] = check_reflection_result,
    ) -> None:
        self.model = model
        self.reasoning_effort = reasoning_effort
        self.timeout_seconds = timeout_seconds
        self.executor = executor
        self.memory_checker = memory_checker

    def resume(
        self,
        *,
        session_id: str,
        worktree: Path,
        prompt: RenderedPrompt,
        idempotency_key: str,
        check: bool = True,
    ) -> ReflectionTurn:
        # A private per-turn latch carries a terminal check timeout back to the final checker.
        with tempfile.TemporaryDirectory(prefix="sdo-check-timeout-") as temporary:
            previous = os.environ.get(VALIDATION_TIMEOUT_FILE_ENV)
            os.environ[VALIDATION_TIMEOUT_FILE_ENV] = os.path.join(temporary, "timeout.json")
            try:
                return self._checked_turn(
                    worktree=worktree,
                    prompt=prompt,
                    idempotency_key=idempotency_key,
                    session_id=session_id,
                    check=check,
                )
            finally:
                if previous is None:
                    os.environ.pop(VALIDATION_TIMEOUT_FILE_ENV, None)
                else:
                    os.environ[VALIDATION_TIMEOUT_FILE_ENV] = previous

    def _checked_turn(
        self, *, worktree: Path, prompt: RenderedPrompt, idempotency_key: str, session_id: str, check: bool
    ) -> ReflectionTurn:
        budget = reflection_budget_seconds() if self.timeout_seconds is None else self.timeout_seconds
        deadline = time.monotonic() + budget
        check_limit = memory_check_timeout_seconds()
        reserve = min(check_limit, budget / 3) if check else 0.0

        def agent_remaining() -> float:
            left = deadline - time.monotonic() - reserve
            if left <= 0:
                raise ReflectionDeadlineExceeded("closing turn budget used; no agent time remains")
            return left

        turn = self._run(
            worktree=worktree,
            prompt=render_prompt(
                "responder", "turn_01", part="reflection.instructions", idempotency_key=idempotency_key, prompt=prompt
            ),
            session_id=session_id,
            timeout_seconds=agent_remaining(),
        )
        usage: dict[str, int | float] = {}
        for correction in range(MAX_CORRECTION_TURNS + 1):
            for name, value in turn_usage(turn).items():
                usage[name] = usage.get(name, 0) + value
            try:
                payload = unwrap_branch(turn.output_json, "reflection")
            except WrongBranchError:
                turn = self._run(
                    worktree=worktree,
                    prompt=branch_correction("reflection"),
                    session_id=turn.session_id,
                    timeout_seconds=agent_remaining(),
                )
                for name, value in turn_usage(turn).items():
                    usage[name] = usage.get(name, 0) + value
                payload = unwrap_branch(turn.output_json, "reflection")
            reply = ReflectionTurn.from_model_output(payload)
            if not check:
                return reply.with_usage(usage)
            result = reply.to_result(worktree)
            verdict = self.memory_checker(worktree, result, max(0.0, min(check_limit, deadline - time.monotonic())))
            if verdict.timed_out:
                raise MemoryCheckTimeout(verdict)
            if verdict.exit_code == 0:
                return reply.with_usage(usage)
            if correction == MAX_CORRECTION_TURNS or time.monotonic() >= deadline:
                raise RuntimeError(f"closing turn memory check failed: {verdict.stdout}\n{verdict.stderr}")
            turn = self._run(
                worktree=worktree,
                prompt=memory_check_correction(verdict, idempotency_key),
                session_id=turn.session_id,
                timeout_seconds=agent_remaining(),
            )
        raise AssertionError("unreachable closing turn state")

    def _run(
        self, *, worktree: Path, prompt: RenderedPrompt, session_id: str, timeout_seconds: float
    ) -> StructuredTurn:
        if os.environ.get("SDO_API_HISTORY_DIR"):
            prompt = prompt + render_prompt("responder", "run_01", part="reflection.instructions")
        try:
            return run_structured_turn(
                self.provider,
                prompt,
                cwd=worktree,
                access="danger-full-access",
                model=self.model,
                reasoning_effort=self.reasoning_effort,
                timeout_seconds=timeout_seconds,
                resume_session_id=session_id,
                executor=self.executor,
                **session_turn_options(self.provider, worktree, phase="reflection"),
            )
        except StructuredTurnError as exc:
            if str(exc) == f"{self.provider} cannot resume session {session_id!r}" or (
                isinstance(exc.__cause__, SessionResumeError)
                and (
                    self.provider == "codex"
                    or "no conversation found" in exc.__cause__.stderr.lower()
                    or "session not found" in exc.__cause__.stderr.lower()
                )
            ):
                raise ResponderSessionUnavailable(
                    f"{self.provider} cannot resume session {session_id!r}: {exc}"
                ) from exc
            raise RuntimeError(f"{self.provider} closing turn failed: {exc}") from exc


class ClaudeSessionBackend(CodexSessionBackend):
    """Continue the responder's own Claude Code session for the closing turn."""

    provider: ClassVar[AgentProvider] = "claude"
