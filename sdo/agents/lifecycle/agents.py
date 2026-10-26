"""Structured, fresh-session agents used during SDO lifecycle bootstrap."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shlex
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar, Protocol

from pydantic import BaseModel, ConfigDict, Field

from libs.agent_cli.structured import (
    AccessMode,
    AgentProvider,
    StructuredTurnError,
    StructuredTurnTimeout,
    run_structured_turn,
)
from sdo.agents.code_paths import SKILLS_ROOT, agent_guidance
from sdo.agents.prompt_templates import named_prompt, render_prompt

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from agentshim import CommandExecutor


#: The health-objective detector watches every Kubernetes kind the detector SDK's
#: snapshot exposes except high-churn Events, in this order. The set follows the SDK,
#: not any fault class, so a judge can check any object the snapshot offers.
HEALTH_OBJECTIVE_WATCHES: tuple[tuple[str, str], ...] = (
    ("v1", "Pod"),
    ("v1", "ConfigMap"),
    ("v1", "Service"),
    ("apps/v1", "Deployment"),
    ("apps/v1", "ReplicaSet"),
    ("networking.k8s.io/v1", "NetworkPolicy"),
    ("v1", "Endpoints"),
    ("discovery.k8s.io/v1", "EndpointSlice"),
)

#: Kinds whose source-backed objects count as health-judge coverage provenance: the
#: application configuration kinds the deployment adapter observes as active topology.
#: Coverage is controller-canonicalized metadata; it selects no check.
COVERED_RESOURCE_KINDS: frozenset[str] = frozenset({"ConfigMap", "Deployment", "NetworkPolicy", "Service"})


class LifecycleAgentError(RuntimeError):
    """Raised when a lifecycle agent cannot produce a valid structured handoff."""


class TopologyResourceDTO(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: str = Field(min_length=1)
    name: str = Field(min_length=1)
    namespace: str = Field(min_length=1)
    source: str = Field(min_length=1)
    dependencies: list[str]


class ActiveTopologyResourceDTO(BaseModel):
    """A resource selected by the deployment backend for the active variant."""

    model_config = ConfigDict(extra="forbid")

    kind: str = Field(min_length=1)
    name: str = Field(min_length=1)


class DeployerDraft(BaseModel):
    """Structured deployer output.

    The resource inventory is deliberately absent: it is a deterministic controller
    fact derived from tracked manifests, so the controller attaches it instead of
    asking the model to transcribe it.
    """

    model_config = ConfigDict(extra="forbid")

    source_commit: str = Field(min_length=1)
    topology_fingerprint: str = Field(min_length=64, max_length=64)
    architecture_summary_markdown: str = Field(min_length=20)


class DeployerHandoff(DeployerDraft):
    """A deployer draft attributed to the fresh agent session that produced it."""

    session_id: str = Field(min_length=1)


class DeployerAssessment(DeployerHandoff):
    """A published deployer handoff with the controller-derived resource inventory."""

    resources: list[TopologyResourceDTO]


class HealthJudgeDraft(BaseModel):
    model_config = ConfigDict(extra="forbid")

    round: int = Field(ge=1)
    objective_digest: str = Field(min_length=64, max_length=64)
    source_commit: str = Field(min_length=1)
    covered_resources: list[TopologyResourceDTO]
    failure_patterns: list[str] = Field(min_length=1)
    detector_ids: list[str] = Field(min_length=1)


class HealthJudgeArtifact(HealthJudgeDraft):
    session_id: str = Field(min_length=1)


_WATCH_LIST = ", ".join(f"{api_version} {kind}" for api_version, kind in HEALTH_OBJECTIVE_WATCHES)
_COVERED_KINDS = ", ".join(sorted(COVERED_RESOURCE_KINDS))

#: Health-judge instructions for direct synthetic traffic edits.
TRAFFIC_AUTHORING = render_prompt("health_judge", "traffic_authoring", part="health_judge.traffic_authoring")

#: Metrics are an optional detector input, present only when an operator configured a metrics source.
METRICS_AUTHORING = render_prompt("health_judge", "metrics_authoring", part="health_judge.metrics_authoring")


class LifecycleAgentBackend(Protocol):
    def run_deployer(
        self,
        *,
        repository: Path,
        application: str,
        correction_feedback: str | None,
    ) -> DeployerHandoff: ...

    def run_health_judge(
        self,
        *,
        repository: Path,
        application: str,
        health_objective: str,
        deployer: DeployerAssessment,
        round_index: int,
        previous: HealthJudgeArtifact | None,
        correction_feedback: str | None,
        active_resources: list[ActiveTopologyResourceDTO] | None = None,
    ) -> HealthJudgeArtifact: ...


_ABSOLUTE_PATH = re.compile(r"(?<![A-Za-z0-9_.$~*?{}-])(/[A-Za-z0-9_./*?{}$@%+=:,~-]+)")
_PARENT_PATH = re.compile(r"(?:^|[\s'\"=;(])\.\.(?:/[^\s'\";|&)]*)?(?=$|[\s'\";|&)])")
_WRITE_REDIRECT_ABSOLUTE_PATH = re.compile(r"(?:^|[ \t])(?:\d*>>?|&>)\s*['\"]?(/[A-Za-z0-9_./*?{}$@%+=:,~-]+)")
_GIT_OBJECT_PATH = re.compile(r"\b[0-9a-fA-F]{7,64}:(/[A-Za-z0-9_./*?{}$@%+=,~-]+)")
_SYSTEM_COMMAND_ROOTS = tuple(Path(path) for path in ("/bin", "/usr/bin", "/usr/local/bin"))
# One literal path segment: no globs, variables, or ``.``/``..`` traversal.
_LITERAL_SEGMENT = r"[A-Za-z0-9_-][A-Za-z0-9_.-]*"


@dataclass(frozen=True)
class ClaudeTaskOutputs:
    """Background-task output files that one Claude Code session may read back.

    Claude Code writes the output of a backgrounded shell command to
    ``<tmp>/claude-<uid>/<project-slug>/<session-id>/tasks/<task-id>.output`` and
    reads it back with ordinary shell commands. Only this session's output files
    are exempt from the repository audit; the rest of the per-uid directory, and
    other sessions' outputs, stay outside the application repository.
    """

    root: Path
    session_id: str

    def __post_init__(self) -> None:
        if not isinstance(self.root, Path):
            raise TypeError(f"root must be a Path, got {type(self.root).__name__}")
        if not self.root.is_absolute():
            raise ValueError(f"root must be absolute, got {self.root}")
        if self.session_id in {"", ".", ".."} or "/" in self.session_id:
            raise ValueError(f"session_id must be one path segment, got {self.session_id!r}")

    @classmethod
    def for_session(cls, session_id: str, *, environ: Mapping[str, str], uid: int) -> ClaudeTaskOutputs:
        """Resolve the per-uid root the way Claude Code does.

        Claude Code uses ``$CLAUDE_CODE_TMPDIR`` when set and otherwise Node's
        ``os.tmpdir()`` (``$TMPDIR``, ``$TMP``, ``$TEMP``, then ``/tmp``).
        """
        tmpdir = next(
            (
                value.rstrip("/") or "/"
                for name in ("CLAUDE_CODE_TMPDIR", "TMPDIR", "TMP", "TEMP")
                if (value := environ.get(name))
            ),
            "/tmp",
        )
        return cls(root=Path(tmpdir) / f"claude-{uid}", session_id=session_id)

    def allows(self, raw_path: str) -> bool:
        pattern = (
            re.escape(str(self.root))
            + f"/{_LITERAL_SEGMENT}/{re.escape(self.session_id)}/tasks/{_LITERAL_SEGMENT}\\.output"
        )
        return re.fullmatch(pattern, raw_path) is not None


class CodexLifecycleBackend:
    """Run each lifecycle handoff as a new confined Codex CLI session."""

    provider: ClassVar[AgentProvider] = "codex"

    def __init__(
        self,
        *,
        model: str | None = None,
        reasoning_effort: str = "medium",
        timeout_seconds: int = 900,
        executor: CommandExecutor | None = None,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("lifecycle agent timeout must be positive")
        self.model = model
        self.reasoning_effort = reasoning_effort
        self.timeout_seconds = timeout_seconds
        self.executor = executor

    def run_deployer(
        self,
        *,
        repository: Path,
        application: str,
        correction_feedback: str | None,
    ) -> DeployerHandoff:
        feedback = correction_feedback or render_prompt("lifecycle", "run_deployer_01", part="lifecycle.instructions")
        prompt = render_prompt(
            "lifecycle",
            "architecture",
            part="lifecycle.instructions",
            application=repr(application),
            feedback=feedback,
        )
        draft, session_id = self._execute(repository, prompt, DeployerDraft)
        return DeployerHandoff(**draft.model_dump(), session_id=session_id)

    def run_health_judge(
        self,
        *,
        repository: Path,
        application: str,
        health_objective: str,
        deployer: DeployerAssessment,
        round_index: int,
        previous: HealthJudgeArtifact | None,
        correction_feedback: str | None,
        active_resources: list[ActiveTopologyResourceDTO] | None = None,
    ) -> HealthJudgeArtifact:
        objective_digest = hashlib.sha256(health_objective.strip().encode()).hexdigest()
        previous_payload = previous.model_dump_json(indent=2) if previous else "null"
        active_payload = (
            json.dumps([resource.model_dump(mode="json") for resource in active_resources], indent=2)
            if active_resources is not None
            else "null"
        )
        feedback = correction_feedback or render_prompt(
            "health_judge", "run_health_judge_01", part="health_judge.instructions"
        )
        prompt = render_prompt(
            "health_judge",
            "health_judge_shell" if agent_guidance() == "shell" else "health_judge",
            part="health_judge.instructions",
            skills_root=SKILLS_ROOT,
            source_commit=deployer.source_commit,
            application=repr(application),
            round_index=round_index,
            health_objective=named_prompt("health_judge.health_objective", health_objective),
            objective_digest=objective_digest,
            deployer=named_prompt("health_judge.deployer", deployer.model_dump_json(indent=2)),
            active_payload=named_prompt("health_judge.active_payload", active_payload),
            previous_payload=named_prompt("health_judge.previous_payload", previous_payload),
            feedback=feedback,
            watch_list=_WATCH_LIST,
            round_index_2=round_index,
            covered_kinds=_COVERED_KINDS,
            traffic_authoring=TRAFFIC_AUTHORING,
            metrics_authoring=METRICS_AUTHORING,
        )
        draft, session_id = self._execute(repository, prompt, HealthJudgeDraft, sandbox="workspace-write")
        return HealthJudgeArtifact(**draft.model_dump(), session_id=session_id)

    def _execute(
        self,
        repository: Path,
        prompt: str,
        output_type: type[DeployerDraft] | type[HealthJudgeDraft],
        *,
        sandbox: AccessMode = "read-only",
    ) -> tuple[DeployerDraft | HealthJudgeDraft, str]:
        role = f"{self.provider.capitalize()} lifecycle session"
        try:
            turn = run_structured_turn(
                self.provider,
                prompt,
                output_schema=output_type.model_json_schema(),
                cwd=repository,
                access=sandbox,
                model=self.model,
                reasoning_effort=self.reasoning_effort,
                timeout_seconds=self.timeout_seconds,
                executor=self.executor,
            )
        except StructuredTurnTimeout as exc:
            raise LifecycleAgentError(f"{role} timed out after {self.timeout_seconds}s") from exc
        except StructuredTurnError as exc:
            raise LifecycleAgentError(f"{role} failed: {exc}") from exc
        task_outputs = ClaudeTaskOutputs.for_session(turn.session_id, environ=os.environ, uid=os.getuid())
        home_variable = "CODEX_HOME" if self.provider == "codex" else "CLAUDE_CONFIG_DIR"
        skills = {home_variable: turn.skill_directory} if turn.skill_directory is not None else {}
        escaped_command = _first_repository_escape(
            turn.shell_commands, repository.resolve(), task_outputs=task_outputs, skill_directories=skills
        )
        if escaped_command is not None:
            raise LifecycleAgentError(
                f"{role} read outside the application repository; discarding its output: {escaped_command[:300]}"
            )
        try:
            output = output_type.model_validate_json(turn.output_json)
        except ValueError as exc:
            raise LifecycleAgentError(f"invalid structured {role} output: {exc}") from exc
        return output, turn.session_id


class ClaudeLifecycleBackend(CodexLifecycleBackend):
    """Run lifecycle handoffs as fresh structured Claude Code sessions."""

    provider: ClassVar[AgentProvider] = "claude"


def _first_repository_escape(
    commands: Sequence[str],
    repository: Path,
    *,
    task_outputs: ClaudeTaskOutputs | None = None,
    skill_directories: Mapping[str, Path] | None = None,
) -> str | None:
    """Return the first shell command that reaches outside *repository*, if any."""
    return next(
        (
            command
            for command in commands
            if _command_escapes_repository(
                command, repository, task_outputs=task_outputs, skill_directories=skill_directories
            )
        ),
        None,
    )


_SHELL_WRAPPERS = frozenset({"bash", "sh", "/bin/bash", "/bin/sh", "/usr/bin/bash", "/usr/bin/sh"})


def _unwrap_shell_command(command: str) -> str:
    """Return the script of a ``bash -c``/``-lc`` wrapper, which Codex puts around every command."""
    try:
        argv = shlex.split(command)
    except ValueError:
        return command
    if len(argv) == 3 and argv[0] in _SHELL_WRAPPERS and argv[1] in {"-c", "-lc"}:
        return argv[2]
    return command


def _mask_quoted_pattern_alternatives(script: str) -> str:
    """Hide ``/route`` alternatives inside quoted strings, such as ``rg 'HandleFunc|/items'``.

    Inside a quoted argument a ``/`` right after ``|`` or ``(`` starts a regular-expression
    alternative or group, not a path the shell opens. Unquoted text, including a pipe into an
    absolute command, is left for the path audit.
    """
    masked: list[str] = []
    quote: str | None = None
    escaped = False
    for char in script:
        if escaped:
            escaped = False
        elif char == "\\" and quote != "'":
            escaped = True
        elif quote is None and char in {"'", '"'}:
            quote = char
        elif char == quote:
            quote = None
        elif quote is not None and char == "/" and masked and masked[-1] in {"|", "("}:
            masked.append(" ")
            continue
        masked.append(char)
    return "".join(masked)


def _readonly_skill_access(command: str, raw_path: str) -> bool:
    """Admit ordinary skill readers, never writes or find's execution actions."""
    try:
        lexer = shlex.shlex(command, posix=True, punctuation_chars=";&|")
        lexer.whitespace_split = True
        chunks: list[list[str]] = [[]]
        for token in lexer:
            if token and all(character in ";&|" for character in token):
                chunks.append([])
            else:
                chunks[-1].append(token)
    except ValueError:
        return False
    readers = {"cat", "find", "ls", "head", "tail", "sed", "rg", "grep", "stat", "wc", "test", "["}
    matching = [chunk for chunk in chunks if raw_path in chunk]
    forbidden = {"-delete", "-exec", "-execdir", "-ok", "-okdir", "-fprint", "-fprint0", "-fprintf", "-fls"}
    return bool(matching) and all(
        chunk
        and Path(chunk[0]).name in readers
        and not any(token in forbidden or token.startswith((">", "-i", "--in-place")) for token in chunk[1:])
        for chunk in matching
    )


def _command_escapes_repository(
    command: str,
    repository: Path,
    *,
    task_outputs: ClaudeTaskOutputs | None = None,
    skill_directories: Mapping[str, Path] | None = None,
) -> bool:
    if _PARENT_PATH.search(command):
        return True
    # ``git show <object>:/path`` addresses a path inside this repository's object
    # database. Mask only the path portion so unrelated absolute paths in the same
    # compound command remain subject to the confinement audit.
    audited_command = _GIT_OBJECT_PATH.sub(
        lambda match: match.group(0).replace(match.group(1), ".git-object-path"), command
    )
    audited_command = _mask_quoted_pattern_alternatives(_unwrap_shell_command(audited_command))
    if skill_directories is None:
        skill_directories = {
            variable: Path(home).expanduser() / "skills"
            for variable in ("CODEX_HOME", "CLAUDE_CONFIG_DIR")
            if (home := os.environ.get(variable))
        }
    for variable in ("CODEX_HOME", "CLAUDE_CONFIG_DIR"):
        skills = skill_directories.get(variable)
        home = skills.parent if skills is not None else Path("/__untrusted_cli_home__")
        audited_command = re.sub(r"\$(?:" + variable + r"\b|\{" + variable + r"\})", str(home), audited_command)
    write_only_paths = {match.group(1) for match in _WRITE_REDIRECT_ABSOLUTE_PATH.finditer(audited_command)}
    for raw_path in _ABSOLUTE_PATH.findall(audited_command):
        candidate = Path(raw_path)
        if ".." in candidate.parts:
            return True
        if any(candidate == root or root in candidate.parents for root in (SKILLS_ROOT, *skill_directories.values())):
            if raw_path in write_only_paths or not _readonly_skill_access(audited_command, raw_path):
                return True
            continue
        if raw_path in write_only_paths:
            continue
        if task_outputs is not None and task_outputs.allows(raw_path):
            continue
        # ``Path`` keeps ``..`` segments, so ``<repository>/../x`` would look contained.
        if ".." in candidate.parts:
            return True
        if candidate == repository or repository in candidate.parents:
            continue
        if any(candidate == root or root in candidate.parents for root in _SYSTEM_COMMAND_ROOTS):
            continue
        return True
    return False
