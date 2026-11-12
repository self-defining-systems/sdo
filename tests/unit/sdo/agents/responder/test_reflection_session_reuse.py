"""Reflection runs in the responder's own agent session; no setting or default starts a new one."""

from __future__ import annotations

import hashlib
import os
import re
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from google.protobuf import json_format

from sdo.agents.responder.reflection import CodexSessionBackend, SessionReflector
from sdo.agents.responder.reflection_worker import run_reflection_task
from sdo.contracts import AgentTask, IncidentClosure, OutcomeRecord, ReflectionRequest, to_canonical_json
from tests.structured_turns import ScriptedAgent, failure, reply

if TYPE_CHECKING:
    from agentshim import CommandRequest
    from agentshim.testing import FakeRun

ROOT = Path(__file__).resolve().parents[5]
INCIDENT = "incident"
RESPONDER_SESSION = "responder-session-0001"


class Publisher:
    def publish_payload(self, *args: object) -> None:
        pass


def _task(root: Path, *, session_mode: str, responder_session: str = RESPONDER_SESSION) -> Path:
    base = os.environ["SDO_BASE_COMMIT"]
    closure = IncidentClosure()
    closure.request.incident_id = INCIDENT
    closure.result.responder_session_id = responder_session
    outcome_path = root / ".sdo/outcomes" / f"{hashlib.sha256(INCIDENT.encode()).hexdigest()}.json"
    outcome = json_format.Parse(outcome_path.read_text(), OutcomeRecord())
    request = ReflectionRequest(closure=closure, outcome=outcome, outcome_commit=base, session_mode=session_mode)
    request.memory_snapshot.accepted_commit = base
    task = AgentTask(
        schema_version="sdo.dev/v1alpha1",
        task_id="reflection-task",
        agent_id="sdo-reflector",
        task_type="outcome-reflection-direct-v1",
        workspace={"proposal_id": "proposal", "checkout_path": str(root), "base_commit": base},
    )
    task.request.Pack(request)
    path = root.with_suffix(f".{session_mode or 'unset'}.task.json")
    path.write_text(to_canonical_json(task))
    return path


def _no_change(_: CommandRequest) -> FakeRun:
    return reply(
        "codex",
        {"summary": "covered", "learning_decision": "no_change", "no_change_reason": "No new verified repair"},
        session_id=RESPONDER_SESSION,
        branch="reflection",
    )


@pytest.mark.parametrize("session_mode", ["resume", ""])
def test_worker_reflection_resumes_the_responder_session_end_to_end(
    tmp_path: Path, prepare_reflection_memory, session_mode: str
) -> None:
    """The first agent invocation is `codex exec resume <responder session>`; an unset mode resumes too."""
    prepare_reflection_memory(tmp_path)
    agent = ScriptedAgent(_no_change)
    result = run_reflection_task(
        _task(tmp_path, session_mode=session_mode),
        "result",
        Publisher(),
        reflector=SessionReflector(CodexSessionBackend(reasoning_effort="medium", executor=agent.executor)),
    )
    assert result.status == "completed"
    assert agent.requests, "reflection never invoked the agent"
    for request in agent.requests:
        argv = list(request.argv)
        assert argv[1:4] == ["exec", "resume", RESPONDER_SESSION], argv


def test_worker_records_a_failed_resume_instead_of_starting_a_new_session(
    tmp_path: Path, prepare_reflection_memory
) -> None:
    """A provider that cannot resume yields a typed failure; no second, new-session invocation follows."""
    prepare_reflection_memory(tmp_path)
    from agentshim.testing import scripted_resume_failure

    agent = ScriptedAgent(lambda _request: scripted_resume_failure("codex"))
    result = run_reflection_task(
        _task(tmp_path, session_mode="resume"),
        "result",
        Publisher(),
        reflector=SessionReflector(CodexSessionBackend(reasoning_effort="medium", executor=agent.executor)),
    )
    assert (result.status, result.failure_reason) == ("failed", "session_unavailable")
    assert RESPONDER_SESSION in result.error_summary
    assert len(agent.requests) == 1
    assert list(agent.requests[0].argv)[1:4] == ["exec", "resume", RESPONDER_SESSION]


def test_worker_refuses_a_fresh_reflection_when_a_responder_session_exists(
    tmp_path: Path, prepare_reflection_memory
) -> None:
    prepare_reflection_memory(tmp_path)
    agent = ScriptedAgent(_no_change)
    with pytest.raises(ValueError, match="responder session"):
        run_reflection_task(
            _task(tmp_path, session_mode="fresh"),
            "result",
            Publisher(),
            reflector=SessionReflector(CodexSessionBackend(reasoning_effort="medium", executor=agent.executor)),
        )
    assert agent.requests == []


def test_worker_never_defaults_a_missing_session_to_fresh(tmp_path: Path, prepare_reflection_memory) -> None:
    prepare_reflection_memory(tmp_path)
    agent = ScriptedAgent(lambda _request: failure())
    result = run_reflection_task(
        _task(tmp_path, session_mode="", responder_session=""),
        "result",
        Publisher(),
        reflector=SessionReflector(CodexSessionBackend(reasoning_effort="medium", executor=agent.executor)),
    )
    assert (result.status, result.failure_reason) == ("failed", "session_unavailable")
    assert agent.requests == []


SCANNED_SUFFIXES = {".py", ".go", ".md", ".toml", ".yaml", ".yml", ".json", ".sh", ".proto"}
SCANNED_ROOTS = ("sdo", "benchmarks", "docs", "scripts", ".agents", ".github", "tests")
GENERATED = ("_gen/", "/gen/", "third_party/", "tests/fixtures/sregym/phase1_rejected_receipts/")


def _repo_files() -> list[Path]:
    files = []
    for name in SCANNED_ROOTS:
        for path in (ROOT / name).rglob("*"):
            relative = path.relative_to(ROOT).as_posix()
            if (
                path.is_file()
                and path.suffix in SCANNED_SUFFIXES
                and not any(skip in f"/{relative}" for skip in GENERATED)
            ):
                files.append(path)
    return files


def test_no_reflection_session_setting_or_flag_exists() -> None:
    """Resume is not a choice: the `--reflection-session` flag and `reflection_session` setting are gone."""
    # The proto keeps the retired name reserved; the adapter test asserts the flag is rejected.
    absent_by_design = {
        Path(__file__).resolve(),
        ROOT / "tests/unit/benchmarks/sregym/adapter/test_driver.py",
        ROOT / "sdo/operator/contracts/settings/config_test.go",
    }
    pattern = re.compile(
        r"--reflection-session\b|(?<!\w)reflection_session(?!\w)|SDO_LEARNING_REFLECTION_SESSION|(?<!\w)ReflectionSession(?!\w)"
    )
    offenders = []
    for path in _repo_files():
        if path.resolve() in absent_by_design:
            continue
        for number, line in enumerate(path.read_text(encoding="utf-8", errors="ignore").splitlines(), 1):
            if pattern.search(line) and "reserved" not in line:
                offenders.append(f"{path.relative_to(ROOT)}:{number}: {line.strip()[:100]}")
    assert not offenders, "\n".join(offenders)


def test_no_default_selects_a_fresh_reflection() -> None:
    """Every session-mode default in the worker and the Go launcher resumes."""
    worker = (ROOT / "sdo/agents/responder/reflection_worker.py").read_text()
    assert 'request.session_mode or "fresh"' not in worker
    assert 'request.session_mode or "resume"' in worker
    reflector = (ROOT / "sdo/agents/responder/reflection.py").read_text()
    assert 'session_mode: ReflectionSessionMode = "fresh"' not in reflector
    go = (ROOT / "sdo/operator/runtime/registered_reflection.go").read_text()
    assert 'mode := "resume"' in go
    assert "w.SessionMode" not in go
