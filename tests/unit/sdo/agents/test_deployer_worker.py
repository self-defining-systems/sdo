from pathlib import Path

import pytest

from sdo.agents.source_deployment.worker import _workload_id, propose_source, run_deployment_task
from sdo.contracts.proto import (
    AgentTask,
    DeploymentPlan,
    ProposalWorkspace,
    RawManifestStrategy,
    SourceDeploymentRequest,
    SourceDeploymentResult,
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


def test_deployment_worker_preserves_typed_workspace_and_result(tmp_path: Path) -> None:
    request = SourceDeploymentRequest(
        application="demo",
        namespace="demo",
        health_objective="stay healthy",
        attempt=1,
        registry_host="registry.demo.svc.cluster.local:5000",
    )
    task = AgentTask(
        schema_version="sdo.dev/v1alpha1",
        task_id="initial-deployer-1",
        agent_id="custom-source-agent",
        task_type="source-deployment",
        workspace=ProposalWorkspace(proposal_id="proposal-1", checkout_path=str(tmp_path), base_commit="base"),
    )
    task.request.Pack(request)
    path = tmp_path / "task.json"
    path.write_text(to_canonical_json(task), encoding="utf-8")
    publisher = Publisher()

    def execute(received: SourceDeploymentRequest, checkout: Path) -> SourceDeploymentResult:
        assert received == request
        assert checkout == tmp_path
        plan = DeploymentPlan(strategy_id="raw-manifests-v1")
        plan.strategy_config.Pack(
            RawManifestStrategy(manifest_paths=["deploy/app.yaml"], expected_workload_ids=["Deployment/demo/demo"])
        )
        return SourceDeploymentResult(
            proposed=True,
            summary="source prepared",
            agent_session_id="session-1",
            plan=plan,
        )

    result = run_deployment_task(path, "result", publisher, execute=execute)
    assert result.task_id == task.task_id
    assert result.proposal_id == "proposal-1"
    assert publisher.payload is not None
    wire = parse_json(publisher.payload, type(result)())
    typed = SourceDeploymentResult()
    assert wire.response.Unpack(typed)
    raw = RawManifestStrategy()
    assert typed.plan.strategy_config.Unpack(raw)
    assert raw.manifest_paths == ["deploy/app.yaml"]


def test_deployment_worker_rejects_unscoped_workload_identity() -> None:
    with pytest.raises(ValueError, match="Kind/namespace/name"):
        _workload_id("Deployment/demo")


def test_deployer_receives_trusted_registry_and_rejects_other_image_host(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace

    prompts: list[str] = []

    def turn(_provider: str, prompt: str, **_kwargs: object) -> SimpleNamespace:
        prompts.append(prompt)
        return SimpleNamespace(
            output_json='{"proposed":true,"summary":"ready","image_builds":[{"image":"foreign.example/app:v1","context_path":".","dockerfile_path":"Dockerfile"}],"manifest_paths":["deploy/app.yaml"],"expected_workload_ids":["Deployment/demo/api"]}',
            session_id="session-1",
        )

    monkeypatch.setattr("sdo.agents.source_deployment.worker.run_structured_turn", turn)
    request = SourceDeploymentRequest(
        application="demo",
        namespace="demo",
        health_objective="healthy",
        attempt=1,
        registry_host="sdo-benchmark-registry.demo-sdo.svc.cluster.local:5000",
    )
    with pytest.raises(ValueError, match="trusted registry"):
        propose_source(request, tmp_path)
    assert request.registry_host in prompts[0]


def test_deployment_worker_rejects_wrong_payload_type(tmp_path: Path) -> None:
    task = AgentTask(
        schema_version="sdo.dev/v1alpha1",
        task_id="initial-deployer-1",
        agent_id="source-agent",
        task_type="source-deployment",
        workspace=ProposalWorkspace(proposal_id="proposal-1", checkout_path=str(tmp_path)),
    )
    task.request.Pack(SourceDeploymentResult(summary="wrong type"))
    path = tmp_path / "task.json"
    path.write_text(to_canonical_json(task), encoding="utf-8")
    with pytest.raises(ValueError, match="wrong type"):
        run_deployment_task(path, "result", Publisher())
