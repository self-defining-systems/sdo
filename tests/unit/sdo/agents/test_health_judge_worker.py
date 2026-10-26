import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest

from sdo.agents.code_paths import SKILLS_ROOT
from sdo.agents.health_judge import worker as judge_worker
from sdo.agents.health_judge.worker import _validate_judge_draft, run_judge_task
from sdo.agents.lifecycle.agents import CodexLifecycleBackend, HealthJudgeDraft
from sdo.contracts.proto import (
    AgentTask,
    HealthJudgeRequest,
    HealthJudgeResult,
    ProposalWorkspace,
    SourceResource,
    parse_json,
    to_canonical_json,
)
from tests.structured_turns import ScriptedAgent, fake_codex_login, reply


class Publisher:
    def __init__(self) -> None:
        self.payload: str | None = None

    def publish_payload(self, name: str, key: str, payload: str) -> None:
        assert name == "result"
        assert key == "agent-result.json"
        self.payload = payload


def judge_task(tmp_path: Path, architecture: str = "# Architecture\nFacts.") -> Path:
    request = HealthJudgeRequest(
        application="demo",
        namespace="demo",
        health_objective="stay healthy",
        accepted_source_commit="accepted-source",
        architecture_summary_markdown=architecture,
        source_topology_fingerprint="a" * 64,
        source_resources=[SourceResource(kind="Deployment", name="api", namespace="demo", source="app.yaml")],
        attempt=1,
    )
    task = AgentTask(
        schema_version="sdo.dev/v1alpha1",
        task_id="initial-judge-1",
        agent_id="custom-judge",
        task_type="health-judgment-direct-v1",
        workspace=ProposalWorkspace(
            proposal_id="proposal-judge", checkout_path=str(tmp_path), base_commit="accepted-architecture"
        ),
    )
    task.request.Pack(request)
    path = tmp_path / "task.json"
    path.write_text(to_canonical_json(task), encoding="utf-8")
    return path


def test_judge_worker_preserves_direct_edit_proposal_identity(tmp_path: Path) -> None:
    publisher = Publisher()

    def execute(request: HealthJudgeRequest, checkout: Path) -> HealthJudgeResult:
        assert request.accepted_source_commit == "accepted-source"
        assert checkout == tmp_path
        return HealthJudgeResult(
            authored=True,
            summary="authored",
            agent_session_id="judge-1",
            objective_digest="digest",
            detector_ids=["health-objective"],
        )

    result = run_judge_task(judge_task(tmp_path), "result", publisher, execute=execute)
    assert result.proposal_id == "proposal-judge"
    assert publisher.payload is not None
    wire = parse_json(publisher.payload, type(result)())
    typed = HealthJudgeResult()
    assert wire.response.Unpack(typed)
    assert typed.detector_ids == ["health-objective"]
    assert not typed.HasField("artifact_proposal")


def test_judge_worker_rejects_retired_proposal_field(tmp_path: Path) -> None:
    def execute(request: HealthJudgeRequest, checkout: Path) -> HealthJudgeResult:
        result = HealthJudgeResult(authored=True, detector_ids=["health-objective"])
        result.artifact_proposal.SetInParent()
        return result

    with pytest.raises(ValueError, match="rejects deprecated"):
        run_judge_task(judge_task(tmp_path), "result", Publisher(), execute=execute)


def test_judge_worker_requires_typed_architecture_before_agent_execution(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="requires accepted source and architecture context"):
        run_judge_task(judge_task(tmp_path, ""), "result", Publisher())


def test_native_judge_rejects_draft_with_wrong_objective_digest() -> None:
    artifact = SimpleNamespace(objective_digest="0" * 64)
    with pytest.raises(ValueError, match="objective digest"):
        _validate_judge_draft(artifact, health_objective="stay healthy", source_commit="source")


def test_judge_rejects_missing_go_prepared_source_inventory(tmp_path: Path) -> None:
    task_path = judge_task(tmp_path)
    task = parse_json(task_path.read_text(), AgentTask())
    request = HealthJudgeRequest()
    assert task.request.Unpack(request)
    request.source_topology_fingerprint = ""
    task.request.Pack(request)
    task_path.write_text(to_canonical_json(task))
    with pytest.raises(ValueError, match="source inventory"):
        run_judge_task(task_path, "result", Publisher())


@pytest.mark.parametrize("retry", [False, True])
def test_judge_authors_final_files_in_writable_session(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    retry: bool,
) -> None:
    task = parse_json(judge_task(tmp_path).read_text(), AgentTask())
    request = HealthJudgeRequest()
    assert task.request.Unpack(request)
    if retry:
        request.validation_attempt = 2
        request.previous_diagnostics.add(rule="detector.manifest", path="m.yaml", line=2, message="bad", hint="fix")
    output = {
        "round": 1,
        "objective_digest": hashlib.sha256(b"stay healthy").hexdigest(),
        "source_commit": "accepted-source",
        "covered_resources": [],
        "failure_patterns": ["unavailable"],
        "detector_ids": ["health-objective"],
    }

    def respond(command):
        # The model writes final files. Python forwards metadata without creating
        # or translating an artifact in its response.
        target = tmp_path / ".sdo/diagnostics/detectors/health/health-objective"
        target.mkdir(parents=True)
        (target / "detector.go").write_text("package objective\n// authored bytes\n")
        return reply(
            "codex",
            output,
            session_id="judge-1",
            commands=[f"cat {SKILLS_ROOT}/sdo-shell-authoring/SKILL.md", "sdo-operator memory check"],
        )

    agent = ScriptedAgent(respond)
    monkeypatch.setenv("SDO_AGENT_PROVIDER", "codex")
    monkeypatch.setattr(
        judge_worker, "CodexLifecycleBackend", lambda **kwargs: CodexLifecycleBackend(executor=agent.executor)
    )
    with fake_codex_login(monkeypatch):
        result = judge_worker.author_health_diagnostics(request, tmp_path)
    assert result.detector_ids == ["health-objective"]
    assert not result.HasField("artifact_proposal")
    assert any("workspace-write" in arg for arg in agent.argvs[0])
    prompt = agent.prompts[0]
    assert "Accepted source and developer changes express intent." in prompt
    assert "Operational memory is fallible advice." in prompt
    assert "Recovery requires evidence from the intended application behavior." in prompt
    assert "sdo-operator memory check" in prompt
    assert "SDO_ACTOR=health_judge" in prompt
    assert "SDO_PHASE=initial" in prompt
    assert "a" * 64 in prompt
    assert '"source": "app.yaml"' in prompt
    if retry:
        assert "[detector.manifest] m.yaml:2: bad (hint: fix)" in prompt
    assert (
        (tmp_path / ".sdo/diagnostics/detectors/health/health-objective/detector.go")
        .read_text()
        .endswith("// authored bytes\n")
    )
    assert set(HealthJudgeDraft.model_fields) == {
        "round",
        "objective_digest",
        "source_commit",
        "covered_resources",
        "failure_patterns",
        "detector_ids",
    }
