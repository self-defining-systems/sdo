"""Any external agent command as a fast-loop agent and a per-cell executor: the external arm.

SDO knows nothing about the agent behind the command. One invocation is one phase of one cell, with the
arguments below, and the command writes the result file the contract names; the arm gates the cell on that file,
the transcripts it lists and the request logs, and accounts the tokens.

    COMMAND... --phase incident|closing --cell-dir D --prompt-file F --kubeconfig K --deadline-epoch T
               [--state-in S]

- ``--prompt-file``: the instruction (the SREGym notice for the incident, a fixed neutral text for the closing
  phase; no outcome or grade information ever reaches the command);
- ``--kubeconfig``: the cluster for the incident phase; an *empty* file for the closing phase (no cluster access);
- ``--deadline-epoch``: when the command must stop the agent itself and still export its evidence;
- ``--state-in``: the agent state a previous pass kept (``state_map``), copied, never moved;
- environment: the arm's environment plus ``SDO_EXTERNAL_PHASE``, ``SDO_EXTERNAL_SUBMIT_URL`` (incident phase),
  ``SDO_EXTERNAL_STATE_OUT`` (where the agent's state belongs, ``D/agent``) and ``KUBECONFIG``.

The result file ``<D>/<phase>/agent-result.json`` (schema ``sdo.external-agent-result/v1``) names the request log
(and an optional independent one), the evidence files that must exist and the problems the command found. Paths
in it are relative to ``D`` (the phase directory is searched too). The command's group is killed only as a
backstop, ``evidence_grace_seconds`` after the deadline.
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.adapter import WaitKind, wait_for
from benchmarks.sregym.fastloop.external_accounting import (
    ACCOUNTING_FILE,
    ACCOUNTING_SCHEMA,
    PhaseAccounting,
    account_phase,
)
from benchmarks.sregym.fastloop.loop import AgentOutcome
from benchmarks.sregym.fastloop.records import AgentName, TokenCounts

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from benchmarks.sregym.fastloop.codex_agent import SubmissionStub
    from benchmarks.sregym.fastloop.loop import InjectionWindow

RESULT_SCHEMA = "sdo.external-agent-result/v1"
RESULT_FILE = "agent-result.json"
CLOCK_FILE = "clock.json"
PHASES = ("incident", "closing")
KILL_GRACE_SECONDS = wait_for(WaitKind.SNAPSHOT_READ)
DEFAULT_EVIDENCE_GRACE_SECONDS = 1.5 * wait_for(WaitKind.MINUTE)
#: The closing phase's fixed text: no outcome, no grade, no cluster.
DEFAULT_CLOSING_PROMPT = (
    "This incident session is over and you no longer have cluster access. If anything from it would help you "
    "handle future incidents in this environment, keep it now with your own memory, skills or tools. Reply with "
    "one line when done."
)


class ExternalResultError(ValueError):
    """The result file a command wrote is missing, unreadable or breaks the contract."""


@dataclass(frozen=True)
class ExternalAgentSettings:
    command: tuple[str, ...]
    label: str
    results_dir: Path
    #: The incident-phase kubeconfig (the filtered proxy's); the closing phase never gets it.
    kubeconfig: Path
    incident_timeout_seconds: float
    #: ``None`` runs no closing phase.
    closing_timeout_seconds: float | None = None
    closing_prompt: str = DEFAULT_CLOSING_PROMPT
    evidence_grace_seconds: float = DEFAULT_EVIDENCE_GRACE_SECONDS
    #: Problem id -> the agent state directory a previous pass kept; that cell starts from a copy of it.
    state_map: Mapping[str, Path] = field(default_factory=dict[str, Path])
    #: Recorded for the row only; the command decides what runs.
    model: str = "unspecified"
    provider: str = "external"
    #: Base environment (default: this process's) and the variables the arm config adds on top of it.
    environ: Mapping[str, str] = field(default_factory=lambda: dict(os.environ))
    extra_env: Mapping[str, str] = field(default_factory=dict[str, str])

    def __post_init__(self) -> None:
        if not self.command or not all(isinstance(part, str) for part in self.command) or not self.command[0]:
            raise ValueError("command must be a non-empty argument vector")
        if not self.label.strip():
            raise ValueError("label must not be empty")
        if self.incident_timeout_seconds <= 0:
            raise ValueError("incident_timeout_seconds must be positive")
        if self.closing_timeout_seconds is not None and self.closing_timeout_seconds <= 0:
            raise ValueError("closing_timeout_seconds must be positive")
        if not self.closing_prompt.strip():
            raise ValueError("closing_prompt must not be empty")
        if self.evidence_grace_seconds < 0:
            raise ValueError("evidence_grace_seconds must not be negative")
        for problem, path in self.state_map.items():
            if not Path(path).is_dir():
                raise ValueError(f"state_map[{problem!r}]: {path} is not a directory")
        if not self.model.strip() or not self.provider.strip():
            raise ValueError("model and provider must not be empty")

    def state_for(self, problem_id: str) -> Path | None:
        found = self.state_map.get(problem_id) or self.state_map.get(problem_id.split("+", 1)[0])
        return Path(found) if found else None


@dataclass(frozen=True)
class AgentResult:
    """The command's own account of one phase, parsed and validated."""

    phase: str
    label: str | None
    agent_version: str | None
    timed_out: bool
    requests: Path
    independent_requests: Path | None
    required_evidence: tuple[Path, ...]
    problems: tuple[str, ...]
    state_dir: Path | None


@dataclass(frozen=True)
class PhaseClock:
    #: The moment the instruction is handed over: right after the fault is injected, for the incident phase.
    notice_at: datetime
    started_at: datetime
    ended_at: datetime


@dataclass(frozen=True)
class PhaseResult:
    phase: str
    clock: PhaseClock
    returncode: int | None
    timed_out: bool
    backstop_killed: bool
    error: str | None
    transcript_problems: tuple[str, ...]
    accounting: PhaseAccounting | None
    result: AgentResult | None
    cell_dir: Path

    @property
    def transcripts_complete(self) -> bool:
        return not self.transcript_problems


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _kill_group(process: subprocess.Popen[bytes]) -> None:
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(process.pid, sig)
        except (ProcessLookupError, PermissionError):
            return
        try:
            process.wait(timeout=KILL_GRACE_SECONDS if sig == signal.SIGTERM else 5)
            return
        except subprocess.TimeoutExpired:
            continue


def _fsync_tree(root: Path) -> None:
    for path in root.rglob("*"):
        if path.is_file():
            try:
                with path.open("rb") as handle:
                    os.fsync(handle.fileno())
            except OSError:
                continue


def _resolve(cell_dir: Path, phase_dir: Path, relative: object, what: str) -> Path:
    """*relative* under the cell directory, else under the phase directory; never outside the cell directory."""
    if not isinstance(relative, str) or not relative.strip():
        raise ExternalResultError(f"{what} must be a non-empty relative path, got {relative!r}")
    candidate = Path(relative)
    if candidate.is_absolute() or ".." in candidate.parts:
        raise ExternalResultError(f"{what} must stay inside the cell directory, got {relative!r}")
    for base in (cell_dir, phase_dir):
        if (base / candidate).exists():
            return base / candidate
    return cell_dir / candidate


def load_result(cell_dir: Path, phase: str) -> AgentResult:
    """Parse ``<cell_dir>/<phase>/agent-result.json``; a missing or invalid file raises ``ExternalResultError``."""
    phase_dir = cell_dir / phase
    path = phase_dir / RESULT_FILE
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise ExternalResultError(f"the command wrote no {phase}/{RESULT_FILE}") from None
    except (OSError, ValueError) as exc:
        raise ExternalResultError(f"{phase}/{RESULT_FILE} is unreadable: {type(exc).__name__}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ExternalResultError(f"{phase}/{RESULT_FILE} is not a JSON object")
    if raw.get("schema", RESULT_SCHEMA) != RESULT_SCHEMA:
        raise ExternalResultError(f"{phase}/{RESULT_FILE} has schema {raw.get('schema')!r}, not {RESULT_SCHEMA!r}")
    if raw.get("phase") != phase:
        raise ExternalResultError(f"{phase}/{RESULT_FILE} names phase {raw.get('phase')!r}")
    if not isinstance(raw.get("timed_out"), bool):
        raise ExternalResultError(f"{phase}/{RESULT_FILE}: timed_out must be a boolean")
    evidence, problems = raw.get("required_evidence"), raw.get("problems")
    if not isinstance(evidence, list) or not isinstance(problems, list):
        raise ExternalResultError(f"{phase}/{RESULT_FILE}: required_evidence and problems must be lists")
    independent = raw.get("independent_requests")
    state = raw.get("state_dir")
    return AgentResult(
        phase=phase,
        label=raw.get("label") if isinstance(raw.get("label"), str) else None,
        agent_version=raw.get("agent_version") if isinstance(raw.get("agent_version"), str) else None,
        timed_out=raw["timed_out"],
        requests=_resolve(cell_dir, phase_dir, raw.get("requests"), "requests"),
        independent_requests=(
            None if independent is None else _resolve(cell_dir, phase_dir, independent, "independent_requests")
        ),
        required_evidence=tuple(_resolve(cell_dir, phase_dir, item, "required_evidence") for item in evidence),
        problems=tuple(str(item) for item in problems),
        state_dir=None if state is None else _resolve(cell_dir, phase_dir, state, "state_dir"),
    )


def transcript_problems(result: AgentResult, cell_dir: Path, accounting: PhaseAccounting | None) -> tuple[str, ...]:
    """Why the phase cannot be graded: the command's own problems, missing evidence, missing or failed accounting."""
    found = [f"{result.phase}: {item}" for item in result.problems]
    found.extend(
        f"{result.phase}: required evidence {path.relative_to(cell_dir)} is missing or empty"
        for path in result.required_evidence
        if not path.is_file() or path.stat().st_size == 0
    )
    if not result.requests.is_file():
        found.append(f"{result.phase}: request log {result.requests.relative_to(cell_dir)} is missing")
    if result.independent_requests is not None and not result.independent_requests.is_file():
        found.append(
            f"{result.phase}: independent request log {result.independent_requests.relative_to(cell_dir)} is missing"
        )
    if result.state_dir is not None and not result.state_dir.is_dir():
        found.append(f"{result.phase}: state directory {result.state_dir.relative_to(cell_dir)} is missing")
    if accounting is not None:
        if accounting.requests == 0:
            found.append(f"{result.phase}: no model request was recorded")
        found.extend(
            f"{result.phase}: accounting cross-check failed: {item}" for item in accounting.crosscheck_problems
        )
    return tuple(found)


def _write_json(path: Path, data: dict[str, Any]) -> None:
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def _record_accounting(cell_dir: Path, label: str, row: PhaseAccounting) -> None:
    """``accounting.json``: one row per phase, the incident phase online and the closing phase offline."""
    path = cell_dir / ACCOUNTING_FILE
    rows: dict[str, Any] = {}
    try:
        rows = {item["phase"]: item for item in json.loads(path.read_text(encoding="utf-8"))["phases"]}
    except (OSError, ValueError, KeyError, TypeError):
        rows = {}
    rows[row.phase] = row.to_json()
    _write_json(path, {"schema": ACCOUNTING_SCHEMA, "label": label, "phases": list(rows.values())})


def _record_clock(cell_dir: Path, phase: str, entry: dict[str, Any], label: str) -> None:
    """``clock.json``: the incident phase's notice, start and end at the top, every phase under ``phases``."""
    path = cell_dir / CLOCK_FILE
    try:
        phases = json.loads(path.read_text(encoding="utf-8"))["phases"]
    except (OSError, ValueError, KeyError, TypeError):
        phases = {}
    phases[phase] = entry
    top = phases.get("incident", entry)
    _write_json(
        path,
        {k: top[k] for k in ("notice_at", "started_at", "ended_at")} | {"label": label, "phases": phases},
    )


def run_external_cell(
    settings: ExternalAgentSettings,
    *,
    cell_dir: Path,
    prompt: str,
    phase: str = "incident",
    state_in: Path | None = None,
    submit_url: str | None = None,
    on_notice: Callable[[], None] | None = None,
    now: Callable[[], datetime] = _now,
) -> PhaseResult:
    """Run the configured command once for *phase*; never lose evidence, whatever the exit.

    ``on_notice`` runs right before the command starts (the fault is injected when the page is the instruction);
    the clock's notice is taken right after it. The closing phase gets an empty kubeconfig.
    """
    if phase not in PHASES:
        raise ValueError(f"phase must be one of {PHASES}, got {phase!r}")
    closing = phase == "closing"
    timeout = settings.closing_timeout_seconds if closing else settings.incident_timeout_seconds
    if timeout is None:
        raise ValueError("no closing phase is configured")
    phase_dir = cell_dir / phase
    phase_dir.mkdir(parents=True, exist_ok=True)
    prompt_file = phase_dir / "prompt.txt"
    prompt_file.write_text(prompt, encoding="utf-8")
    if state_in is not None and not Path(state_in).is_dir():
        raise ValueError(f"state_in {state_in} is not a directory")

    scratch = Path(tempfile.mkdtemp(prefix="agent-"))
    kubeconfig = scratch / "kubeconfig"
    if closing:
        kubeconfig.write_text("", encoding="utf-8")
    else:
        shutil.copyfile(settings.kubeconfig, kubeconfig)
    state_out = cell_dir / "agent"
    env = {**settings.environ, **settings.extra_env}
    env.pop("SREGYM_SUMMARY_FILE", None)
    env.update(KUBECONFIG=str(kubeconfig), SDO_EXTERNAL_PHASE=phase, SDO_EXTERNAL_STATE_OUT=str(state_out))
    env.pop("SDO_EXTERNAL_SUBMIT_URL", None)
    if submit_url and not closing:
        env["SDO_EXTERNAL_SUBMIT_URL"] = submit_url
    returncode: int | None = None
    backstop = False
    error: str | None = None
    process: subprocess.Popen[bytes] | None = None
    started = ended = notice = now()
    deadline = 0.0
    try:
        if on_notice is not None:
            on_notice()
        notice = now()
        deadline = time.time() + timeout
        command = [
            *settings.command,
            "--phase",
            phase,
            "--cell-dir",
            str(cell_dir),
            "--prompt-file",
            str(prompt_file),
            "--kubeconfig",
            str(kubeconfig),
            "--deadline-epoch",
            f"{deadline:.3f}",
            *(["--state-in", str(state_in)] if state_in is not None else []),
        ]
        with (phase_dir / "command.stdout").open("wb") as out, (phase_dir / "command.stderr").open("wb") as errors:
            try:
                process = subprocess.Popen(
                    command,
                    cwd=scratch,
                    env=env,
                    stdin=subprocess.DEVNULL,
                    stdout=out,
                    stderr=errors,
                    start_new_session=True,
                )
                started = now()
                try:
                    returncode = process.wait(timeout=timeout + settings.evidence_grace_seconds)
                except subprocess.TimeoutExpired:
                    backstop = True
                    error = (
                        f"{settings.label} {phase} still running {settings.evidence_grace_seconds:.0f}s after its "
                        f"{timeout:.0f}s deadline: group killed"
                    )
                    _kill_group(process)
                    returncode = process.returncode
            except BaseException as exc:
                error = error or f"{settings.label} {phase} failed to run: {type(exc).__name__}: {exc}"
                if process is not None and process.poll() is None:
                    _kill_group(process)
                    returncode = process.returncode
                if isinstance(exc, KeyboardInterrupt):
                    raise
            finally:
                out.flush()
                errors.flush()
    finally:
        ended = now()
        shutil.rmtree(scratch, ignore_errors=True)
        _fsync_tree(cell_dir)

    result: AgentResult | None = None
    accounting: PhaseAccounting | None = None
    problems: tuple[str, ...]
    try:
        result = load_result(cell_dir, phase)
    except ExternalResultError as exc:
        problems = (f"{phase}: {exc}",)
    else:
        accounting = account_phase(
            phase, result.requests, result.independent_requests, timed_out=result.timed_out or backstop
        )
        problems = transcript_problems(result, cell_dir, accounting)
    timed_out = backstop or bool(result and result.timed_out)
    if error is None and timed_out:
        error = f"{settings.label} {phase} timed out after {timeout:.0f}s"
    if error is None and returncode != 0:
        error = f"{settings.label} {phase} exited with code {returncode}"
    clock = PhaseClock(notice, started, ended)
    entry = {
        "notice_at": notice.isoformat(),
        "started_at": started.isoformat(),
        "ended_at": ended.isoformat(),
        "deadline_epoch": deadline,
        "returncode": returncode,
        "timed_out": timed_out,
        "backstop_killed": backstop,
        "error": error,
        "agent_version": result.agent_version if result else None,
        "transcript_problems": list(problems),
    }
    _write_json(phase_dir / CLOCK_FILE, entry | {"label": settings.label, "command": list(settings.command)})
    _record_clock(cell_dir, phase, entry, settings.label)
    if accounting is not None:
        _record_accounting(cell_dir, settings.label, accounting)
    return PhaseResult(phase, clock, returncode, timed_out, backstop, error, problems, accounting, result, cell_dir)


def _row_error(result: PhaseResult) -> str | None:
    """What the phase adds to its row's error: its own failure and, apart, missing or failed evidence."""
    parts = [result.error] if result.error else []
    if not result.transcripts_complete:
        parts.append("external transcripts incomplete: " + "; ".join(result.transcript_problems))
    return "; ".join(parts) or None


class ExternalBaselineAgent:
    """The external command as a fast-loop agent: ``resolve`` is the incident phase, ``learn`` the closing phase."""

    name: AgentName = "external"

    def __init__(
        self,
        settings: ExternalAgentSettings,
        *,
        stub: SubmissionStub,
        prompt_for: Callable[[str, str], str],
    ) -> None:
        self._settings = settings
        self._stub = stub
        self._prompt_for = prompt_for

    @property
    def model(self) -> str:
        return self._settings.model

    @property
    def provider(self) -> str:
        return self._settings.provider

    @property
    def reasoning_effort(self) -> str | None:
        return None

    def resolve(self, index: int, problem_id: str, inject: Callable[[], InjectionWindow]) -> AgentOutcome:
        self._stub.reset()
        prompt = self._prompt_for(problem_id, self._stub.url)
        cell_dir = self._settings.results_dir / f"{index:03d}_{problem_id}"
        windows: list[InjectionWindow] = []
        result = run_external_cell(
            self._settings,
            cell_dir=cell_dir,
            prompt=prompt,
            phase="incident",
            state_in=self._settings.state_for(problem_id),
            submit_url=self._stub.url,
            on_notice=lambda: windows.append(inject()),
        )
        submissions = self._stub.submissions()
        diagnosis = next((item.solution for item in submissions if item.stage == "diagnosis"), "")
        mitigation = next((item for item in submissions if item.stage == "mitigation"), None)
        error = _row_error(result)
        if error is None and mitigation is None:
            error = "external agent exited without submitting a mitigation"
        accounting = result.accounting
        state_dir = result.result.state_dir if result.result is not None else None
        return AgentOutcome(
            injection=windows[0],
            dispatched_at=result.clock.notice_at,
            responder_started_at=result.clock.started_at,
            responder_finished_at=result.clock.ended_at,
            mitigation_applied_at=mitigation.received_at if mitigation is not None else result.clock.ended_at,
            resolved_at=result.clock.ended_at,
            diagnosis=diagnosis,
            mitigation=mitigation.solution if mitigation is not None else "",
            responder_tokens=accounting.tokens if accounting else TokenCounts(),
            setup_seconds=None,
            artifacts_dir=str(cell_dir),
            incident_id=f"external-{index:03d}-{int(time.time())}",
            agent_label=self._settings.label,
            agent_state_dir=str(state_dir) if state_dir is not None else None,
            error=error,
        )

    def learn(self, outcome: AgentOutcome) -> AgentOutcome:
        """The closing phase: runs after the oracle grade, so it never affects the grade; no outcome reaches it."""
        if self._settings.closing_timeout_seconds is None or outcome.artifacts_dir is None:
            return outcome
        cell_dir = Path(outcome.artifacts_dir)
        try:
            load_result(cell_dir, "incident")
        except ExternalResultError:
            return outcome  # no incident session to close; the incident phase's error is already on the row
        closing = run_external_cell(
            self._settings, cell_dir=cell_dir, prompt=self._settings.closing_prompt, phase="closing"
        )
        error = _row_error(closing)
        state_dir = closing.result.state_dir if closing.result is not None else None
        return outcome.with_learning(
            reflection_started_at=closing.clock.started_at,
            reflection_finished_at=closing.clock.ended_at,
            reflection_tokens=closing.accounting.tokens if closing.accounting else TokenCounts(),
            agent_state_dir=str(state_dir) if state_dir is not None else outcome.agent_state_dir,
            error="; ".join(item for item in (outcome.error, error) if item) or None,
        )

    def prepare_recovery(self, outcome: AgentOutcome | None) -> None:
        """The external agent has no long-running incident intake."""

    def close(self) -> None:
        """The stub belongs to the caller."""
