"""A provider cannot return unchecked memory, even when it omits its check tool."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from sdo.agents.responder.reflection import ClaudeSessionBackend, CodexSessionBackend
from tests.structured_turns import ScriptedAgent, reply

if TYPE_CHECKING:
    from agentshim import CommandRequest
    from agentshim.testing import FakeRun

ROOT = Path(__file__).resolve().parents[5]


@pytest.fixture
def accepted_reflection(tmp_path: Path, memory_binary: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    def git(*args: str) -> str:
        return subprocess.check_output(["git", "-C", str(tmp_path), *args], text=True).strip()

    git("init", "-q")
    git("config", "user.name", "Fixture")
    git("config", "user.email", "fixture@example.test")
    # Minimal accepted memory; the only missing requirement is coverage of the verified cause.
    (tmp_path / "app.txt").write_text("synthetic application\n")
    git("add", ".")
    git("commit", "-qm", "accepted source")
    outcome = tmp_path / ".sdo/outcomes" / f"{hashlib.sha256(b'incident').hexdigest()}.json"
    outcome.parent.mkdir(parents=True)
    outcome.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "incident_id": "incident",
                "classification": "success",
                "findings": [
                    {
                        "detector_id": "synthetic-catalog",
                        "rule_id": "catalog",
                        "status": "active",
                        "severity": "warn",
                        "summary": "catalog fault",
                        "evidence": "controller observed catalog fault",
                        "fingerprint": "catalog",
                        "primary_resource": {
                            "api_version": "v1",
                            "kind": "Service",
                            "namespace": "demo",
                            "name": "api",
                        },
                    }
                ],
                "repair_actions": [
                    {
                        "action_id": "repair-catalog",
                        "kind": "kubectl",
                        "target": "Service/demo/api",
                        "summary": "restore catalog",
                        "details": "catalog restored and observed",
                        "success": True,
                        "resources": [{"api_version": "v1", "kind": "Service", "namespace": "demo", "name": "api"}],
                        "started_at": "2026-01-01T00:00:01Z",
                        "completed_at": "2026-01-01T00:00:02Z",
                    }
                ],
                "detector_history": [
                    {
                        "detector_id": "synthetic-catalog",
                        "status": "clear",
                        "evaluated_at": "2026-01-01T00:00:03Z",
                    }
                ],
                "confirmed_root_causes": [
                    {
                        "summary": "synthetic catalog repair",
                        "resources": [{"api_version": "v1", "kind": "Service", "namespace": "demo", "name": "api"}],
                    }
                ],
            }
        )
    )
    git("add", "-f", ".sdo")
    git("commit", "-qm", "accepted outcome\n\nSDO-Incident: incident\nSDO-Phase: outcome")
    monkeypatch.setenv("PATH", f"{memory_binary.parent}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("SDO_ACTOR", "responder")
    monkeypatch.setenv("SDO_PHASE", "reflection")
    monkeypatch.setenv("SDO_BASE_COMMIT", git("rev-parse", "HEAD"))
    monkeypatch.setenv("SDO_INCIDENT_ID", "incident")
    monkeypatch.setenv("SDO_OPERATOR_ROOT", str(ROOT / "sdo/operator"))
    monkeypatch.setenv("SDO_HEALTHY_BASELINE", "")
    return tmp_path


@pytest.mark.parametrize(
    ("provider", "backend_type"), [("codex", CodexSessionBackend), ("claude", ClaudeSessionBackend)]
)
@pytest.mark.parametrize("fresh", [False, True])
def test_uncovered_repair_reprompts_once_for_a_scoped_sdk_gap(
    accepted_reflection: Path,
    provider: str,
    backend_type: type[CodexSessionBackend],
    fresh: bool,
) -> None:
    def respond(_request):
        if len(agent.requests) == 1:
            payload = {"summary": "covered", "learning_decision": "no_change", "no_change_reason": "covered"}
        else:
            assert "reflection.uncovered-repair" in agent.prompts[-1]
            assert "repair-catalog" in agent.prompts[-1]
            payload = {
                "summary": "missing repair capability",
                "learning_decision": "sdk_gap",
                "no_change_reason": "catalog write cannot be encoded",
                "sdk_gaps": [
                    {
                        "fault_class": "catalog",
                        "primitive": "catalog write",
                        "reason": "catalog write capability unavailable",
                        "scratch_script": "catalog repair",
                        "resources": [{"api_version": "v1", "kind": "Service", "namespace": "demo", "name": "api"}],
                    }
                ],
            }
        return reply(provider, payload, session_id="reflection-session", branch="reflection")

    agent = ScriptedAgent(respond)
    backend = backend_type(executor=agent.executor)
    kwargs = {"worktree": accepted_reflection, "prompt": "reflect", "idempotency_key": "one-dispatch"}
    result = backend.fresh(**kwargs) if fresh else backend.resume(session_id="responder-session", **kwargs)
    assert result.learning_decision == "sdk_gap"
    assert len(agent.requests) == 2
    assert result.usage["llm_calls"] == 2


def test_worker_does_not_publish_persistently_rejected_memory(
    accepted_reflection: Path,
) -> None:
    from google.protobuf import json_format

    from sdo.agents.responder.reflection import SessionReflector
    from sdo.agents.responder.reflection_worker import run_reflection_task
    from sdo.contracts import AgentTask, IncidentClosure, OutcomeRecord, ReflectionRequest, to_canonical_json

    incident = "incident"
    base = os.environ["SDO_BASE_COMMIT"]
    closure = IncidentClosure()
    closure.request.incident_id = incident
    closure.result.responder_session_id = "responder-session"
    outcome_path = accepted_reflection / ".sdo/outcomes" / f"{hashlib.sha256(incident.encode()).hexdigest()}.json"
    outcome = json_format.Parse(outcome_path.read_text(), OutcomeRecord())
    request = ReflectionRequest(closure=closure, outcome=outcome, outcome_commit=base, session_mode="resume")
    request.memory_snapshot.accepted_commit = base
    task = AgentTask(
        schema_version="sdo.dev/v1alpha1",
        task_id="one-dispatch",
        agent_id="sdo-reflector",
        task_type="outcome-reflection-direct-v1",
        workspace={"proposal_id": "proposal", "checkout_path": str(accepted_reflection), "base_commit": base},
    )
    task.request.Pack(request)
    task_path = accepted_reflection.with_suffix(".task.json")
    task_path.write_text(to_canonical_json(task))
    published = []

    class Publisher:
        def publish_payload(self, *args: object) -> None:
            published.append(args)

    def respond(_: CommandRequest) -> FakeRun:
        # An unowned path is a blocking rejection the agent never fixes.
        (accepted_reflection / ".sdo/notes.txt").write_text("not an owned artifact\n")
        return reply(
            "codex",
            {"summary": "covered", "learning_decision": "no_change", "no_change_reason": "covered"},
            session_id="reflection-session",
            branch="reflection",
        )

    agent = ScriptedAgent(respond)
    result = run_reflection_task(
        task_path,
        "result",
        Publisher(),
        reflector=SessionReflector(CodexSessionBackend(executor=agent.executor)),
    )
    assert len(agent.requests) == 2  # unchanged blocking diagnostics stop after one correction
    # The rejected memory is never published; the controller receives a typed failure instead.
    assert (result.status, result.failure_reason) == ("failed", "agent_failed")
    assert not result.HasField("response")
    assert "layout.unknown-path" in result.error_summary
    assert len(published) == 1
    assert all("reflection-session" in request.argv for request in agent.requests[1:])


def test_final_check_rechecks_edits_after_an_earlier_pass(
    tmp_path: Path,
    prepare_reflection_memory,
) -> None:
    prepare_reflection_memory(tmp_path)
    decision = {"summary": "covered", "learning_decision": "no_change", "no_change_reason": "covered"}
    decision_file = tmp_path.with_suffix(".decision.json")
    decision_file.write_text(json.dumps(decision))
    calls = 0

    def respond(_: CommandRequest) -> FakeRun:
        nonlocal calls
        calls += 1
        if calls == 1:
            # An earlier passing model-side check does not authorize later bytes.
            subprocess.run(
                ["sdo-operator", "memory", "check", "--json", "--decision", str(decision_file)],
                cwd=tmp_path,
                check=True,
                capture_output=True,
            )
            (tmp_path / ".sdo/notes.txt").write_text("not an owned artifact\n")
        else:
            (tmp_path / ".sdo/notes.txt").unlink()
        return reply("codex", decision, session_id="reflection-session", branch="reflection")

    agent = ScriptedAgent(respond)
    result = CodexSessionBackend(executor=agent.executor).fresh(
        worktree=tmp_path,
        prompt="reflect",
        idempotency_key="one-dispatch",
    )
    assert result.learning_decision == "no_change"
    assert calls == 2
    assert "layout.unknown-path" in agent.prompts[1]


def test_final_check_stops_at_original_deadline(
    accepted_reflection: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace

    from sdo.agents.responder import reflection
    from sdo.agents.responder.memory_check import check_reflection_result

    now = 10.0
    monkeypatch.setattr(reflection, "time", SimpleNamespace(monotonic=lambda: now))
    budgets = []

    def check(root, result, timeout):
        nonlocal now
        budgets.append(timeout)
        checked = check_reflection_result(root, result, timeout)
        now = 100.0  # The existing deadline expires during the rejected check.
        return checked

    def respond(_: CommandRequest) -> FakeRun:
        (accepted_reflection / ".sdo/notes.txt").write_text("not an owned artifact\n")
        return reply(
            "codex",
            {"summary": "covered", "learning_decision": "no_change", "no_change_reason": "covered"},
            session_id="reflection-session",
            branch="reflection",
        )

    agent = ScriptedAgent(respond)
    with pytest.raises(RuntimeError, match="layout.unknown-path"):
        CodexSessionBackend(executor=agent.executor, memory_checker=check, timeout_seconds=30).fresh(
            worktree=accepted_reflection,
            prompt="reflect",
            idempotency_key="one-dispatch",
        )
    assert budgets == [30.0]
    assert len(agent.requests) == 1


def test_final_check_timeout_kills_the_validator_process_group(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import sys
    import time

    from sdo.agents.responder.memory_check import check_reflection_result
    from sdo.contracts import ReflectionResult

    started = tmp_path / "validator-started"
    executable = tmp_path / "sdo-operator"
    executable.write_text(
        f"""#!{sys.executable}
import os, subprocess, sys, time
# The child does not hold the output pipes, so a surviving child fails the wait below, not communicate().
quiet = {{"stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL}}
child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(600)"], **quiet)
print("validation started", flush=True)
with open({str(started)!r} + ".tmp", "w") as handle:
    handle.write(str(child.pid))
os.replace({str(started)!r} + ".tmp", {str(started)!r})
time.sleep(600)
"""
    )
    executable.chmod(0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")

    def expire_once_started(process: subprocess.Popen[bytes], seconds: float) -> tuple[bytes, bytes]:
        # The deadline fires only after the validator has output and a child.
        while not started.exists() and process.poll() is None:
            time.sleep(0.01)
        raise subprocess.TimeoutExpired(process.args, seconds)

    result = check_reflection_result(tmp_path, ReflectionResult(), 0.1, communicate=expire_once_started)
    assert result.exit_code != 0
    assert result.timed_out
    assert "validation started" in result.stdout
    assert "timed out after 0.1s" in result.stderr
    child = Path("/proc") / started.read_text()
    deadline = time.monotonic() + 60  # Bounds only a failure: a killed child is already exiting.
    while True:
        try:
            if (child / "stat").read_text().split(") ")[-1][0] == "Z":
                break
        except FileNotFoundError:
            break
        assert time.monotonic() < deadline, "validator child survived the process-group kill"
        time.sleep(0.01)


@pytest.fixture
def learned_reflection(accepted_reflection: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A real executable proposal with controller captures and a verified repair."""
    import shutil

    root = accepted_reflection

    def git(*args: str) -> str:
        return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()

    good = ROOT / "sdo/operator/memory/validate/testdata/good/.sdo"
    shutil.copytree(good, root / ".sdo", dirs_exist_ok=True)
    (root / ".sdo/playbooks/base/scripts/fix.sh").chmod(0o755)
    fixture = ROOT / "tests/fixtures/sdo/playbook_program_app/.sdo"
    shutil.copyfile(fixture / "diagnostics/go.mod", root / ".sdo/diagnostics/go.mod")
    git("add", "-f", ".sdo")
    git("commit", "-qm", "accepted memory")
    outcome = next((root / ".sdo/outcomes").glob("*.json"))
    data = json.loads(outcome.read_text())
    data["confirmed_root_causes"][0]["resources"][0]["name"] = "front"
    data["findings"][0]["primary_resource"]["name"] = "front"
    data["repair_actions"][0]["resources"][0]["name"] = "front"
    data["repair_actions"][0]["target"] = "Service/demo/front"
    outcome.write_text(json.dumps(data))
    git("add", "-f", ".sdo")
    git("commit", "-qm", "accepted outcome\n\nSDO-Incident: incident\nSDO-Phase: outcome")
    monkeypatch.setenv("SDO_BASE_COMMIT", git("rev-parse", "HEAD"))
    for relative in [
        "playbooks/service-targetport-mismatch",
        "lib",
        "corpus/targetport-mismatch",
        "diagnostics/detectors/incidents/routing-relations",
    ]:
        shutil.copytree(fixture / relative, root / ".sdo" / relative, dirs_exist_ok=True)
    index = root / ".sdo/playbooks/README.md"
    index.write_text(index.read_text() + "\n- [service-targetport-mismatch](service-targetport-mismatch/README.md)\n")
    corpus = root / ".sdo/corpus/targetport-mismatch"
    fixed = json.loads((corpus / "fixed.json").read_text())
    for key, value in json.loads((corpus / "healthy.json").read_text()).items():
        if isinstance(value, list):
            fixed.setdefault(key, []).extend(value)
    for phase in ["fixed", "healthy"]:
        (corpus / f"{phase}.json").write_text(json.dumps(fixed))
    label = json.loads((corpus / "labels.json").read_text())
    label["provenance"] = {
        "incident_id": "incident",
        "open_at": "detect",
        "open_sha256": hashlib.sha256((corpus / "open.json").read_bytes()).hexdigest(),
        "fixed_sha256": hashlib.sha256((corpus / "fixed.json").read_bytes()).hexdigest(),
    }
    (corpus / "labels.json").write_text(json.dumps(label))
    capture = root.with_suffix(".capture")
    (capture / "healthy").mkdir(parents=True)
    (capture / "healthy/healthy.json").write_text(json.dumps(fixed))
    for target, source in {
        "open@detect.json": "open.json",
        "fixed.json": "fixed.json",
        "open@detect-recordings.json": "recordings.json",
    }.items():
        (capture / target).write_bytes((corpus / source).read_bytes())
    monkeypatch.setenv("SDO_CAPTURED_CORPUS_DIR", str(capture))
    return root


def test_repeat_chain_is_corrected_in_the_same_reflection_session(learned_reflection: Path) -> None:
    """The real compiler's rejection reaches the live correction loop."""
    root = learned_reflection
    path = root / ".sdo/diagnostics/detectors/incidents/routing-relations/detector.go"
    valid = path.read_text()
    calls = 0

    def respond(request: CommandRequest) -> FakeRun:
        nonlocal calls
        calls += 1
        if calls == 1:
            path.write_text(valid + "\nfunc broken(\n")
        else:
            assert calls == 2, agent.prompts[-1]
            assert "reflection-session" in request.argv
            path.write_text(valid)
        return reply(
            "codex",
            {
                "summary": "learned routing repair",
                "learning_decision": "updated",
                "proposed_artifact_ids": ["service-targetport-mismatch"],
            },
            session_id="reflection-session",
            branch="reflection",
        )

    agent = ScriptedAgent(respond)
    result = CodexSessionBackend(executor=agent.executor).fresh(
        worktree=root, prompt="reflect", idempotency_key="repeat-chain"
    )
    assert calls == 2
    assert result.learning_decision == "updated"
    assert "detector.go" in agent.prompts[1]


@pytest.mark.parametrize("backend_type", [CodexSessionBackend, ClaudeSessionBackend])
def test_missing_healthy_capture_keeps_the_learned_detector_without_reprompt(
    learned_reflection: Path, backend_type: type[CodexSessionBackend]
) -> None:
    import shutil

    from sdo.agents.responder.memory_check import check_reflection_result

    root = learned_reflection
    shutil.rmtree(Path(os.environ["SDO_CAPTURED_CORPUS_DIR"]) / "healthy")
    agent = ScriptedAgent(
        lambda _: reply(
            backend_type.provider,
            {
                "summary": "learned routing repair",
                "learning_decision": "updated",
                "proposed_artifact_ids": ["service-targetport-mismatch"],
            },
            session_id="reflection-session",
            branch="reflection",
        )
    )
    result = backend_type(executor=agent.executor).fresh(
        worktree=root, prompt="reflect", idempotency_key="baseline-missing"
    )
    assert len(agent.requests) == 1
    assert result.learning_decision == "updated"
    assert not result.sdk_gaps
    assert list(result.playbook_ids) == ["service-targetport-mismatch"]
    assert (root / ".sdo/diagnostics/detectors/incidents/routing-relations/detector.go").exists()
    checked = check_reflection_result(root, result.to_result(), 120)
    assert checked.exit_code == 0, checked.stdout + checked.stderr
