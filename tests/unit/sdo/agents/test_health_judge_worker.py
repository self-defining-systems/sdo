import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest

from sdo.agents.health_judge import worker as judge_worker
from sdo.agents.health_judge.worker import _judge_proposal, _validate_judge_draft, run_judge_task
from sdo.contracts.proto import (
    AgentTask,
    HealthJudgeRequest,
    HealthJudgeResult,
    OperationalArtifactProposal,
    ProposalWorkspace,
    ProposedDetector,
    SourceResource,
    parse_json,
    to_canonical_json,
)


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
        task_type="health-judgment",
        workspace=ProposalWorkspace(
            proposal_id="proposal-judge", checkout_path=str(tmp_path), base_commit="accepted-architecture"
        ),
    )
    task.request.Pack(request)
    path = tmp_path / "task.json"
    path.write_text(to_canonical_json(task), encoding="utf-8")
    return path


def test_judge_worker_preserves_typed_proposal_identity(tmp_path: Path) -> None:
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
            artifact_proposal=OperationalArtifactProposal(detectors=[ProposedDetector(id="health-objective")]),
        )

    result = run_judge_task(judge_task(tmp_path), "result", publisher, execute=execute)
    assert result.proposal_id == "proposal-judge"
    assert publisher.payload is not None
    wire = parse_json(publisher.payload, type(result)())
    typed = HealthJudgeResult()
    assert wire.response.Unpack(typed)
    assert typed.detector_ids == ["health-objective"]


def test_judge_worker_requires_typed_architecture_before_agent_execution(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="requires accepted source and architecture context"):
        run_judge_task(judge_task(tmp_path, ""), "result", Publisher())


def test_judge_returns_logical_artifacts_without_writing_memory_paths(tmp_path: Path) -> None:
    artifact = SimpleNamespace(
        detector_source=(
            "package objective\n"
            'import ("time"; "sdo.dev/operator/sdk")\n'
            "type detector struct{}\n"
            "func (d detector) Spec() sdk.DetectorSpec { return sdk.DetectorSpec{} }\n"
            "func New() sdk.Detector { return detector{} }\n"
        ),
        detector_test_source="package objective\nfunc TestObjective(t *testing.T) {}\n",
        traffic_files=[],
    )
    proposal = _judge_proposal(artifact)
    assert not (tmp_path / ".sdo").exists()
    assert proposal.detectors[0].id == "health-objective"
    assert proposal.detectors[0].possible_playbook_ids == ["health-objective"]
    assert proposal.detectors[0].sources[0].name == "detector.go"
    assert proposal.playbooks[0].id == "health-objective"
    assert ".sdo/" not in proposal.SerializeToString().decode("utf-8", errors="ignore")


def test_native_judge_rejects_draft_with_wrong_objective_digest() -> None:
    artifact = SimpleNamespace(
        round=1,
        objective_digest="0" * 64,
        source_commit="source",
        detector_source="package objective",
        detector_test_source="package objective",
        failure_patterns=["unavailable"],
    )
    with pytest.raises(ValueError, match="objective digest"):
        _validate_judge_draft(artifact, health_objective="stay healthy", source_commit="source")


def test_judge_consumes_go_prepared_source_inventory_without_reading_checkout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    observed = []

    class Backend:
        def run_health_judge(self, **kwargs):
            observed.append(kwargs["deployer"])
            return SimpleNamespace(
                round=1,
                objective_digest=hashlib.sha256(b"stay healthy").hexdigest(),
                source_commit="accepted-source",
                failure_patterns=["unavailable"],
                detector_source="package objective\nfunc New() sdk.Detector { return nil }",
                detector_test_source="package objective\nfunc TestObjective(t *testing.T) {}",
                traffic_files=[],
                session_id="judge-1",
            )

    monkeypatch.setattr(judge_worker, "CodexLifecycleBackend", lambda **kwargs: Backend())
    task = parse_json(judge_task(tmp_path).read_text(), AgentTask())
    request = HealthJudgeRequest()
    assert task.request.Unpack(request)
    result = judge_worker.author_health_diagnostics(request, tmp_path)
    assert result.artifact_proposal.detectors[0].id == "health-objective"
    assert observed[0].topology_fingerprint == "a" * 64
    assert [(row.kind, row.name, row.source) for row in observed[0].resources] == [("Deployment", "api", "app.yaml")]


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
