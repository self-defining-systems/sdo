from pathlib import Path

import pytest

from sdo.agents.source_deployment.worker import propose_source, run_deployment_task
from sdo.contracts.proto import (
    AgentTask,
    ProposalWorkspace,
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
        return SourceDeploymentResult(proposed=True, summary="source prepared", agent_session_id="session-1")

    result = run_deployment_task(path, "result", publisher, execute=execute)
    assert result.task_id == task.task_id
    assert result.proposal_id == "proposal-1"
    assert publisher.payload is not None
    wire = parse_json(publisher.payload, type(result)())
    typed = SourceDeploymentResult()
    assert wire.response.Unpack(typed)
    assert (typed.proposed, typed.summary, typed.agent_session_id) == (True, "source prepared", "session-1")


def test_deployer_reply_carries_no_plan(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The deployer's work is its last commit: the reply is proposed, a summary and the session."""
    from types import SimpleNamespace

    prompts: list[str] = []

    def turn(_provider: str, prompt: str, **kwargs: object) -> SimpleNamespace:
        assert kwargs["access"] == "danger-full-access"
        assert set(kwargs["output_schema"]["properties"]) == {"proposed", "summary"}
        prompts.append(prompt)
        return SimpleNamespace(output_json='{"proposed":true,"summary":"committed a kustomization"}', session_id="s-1")

    monkeypatch.setattr("sdo.agents.source_deployment.worker.run_structured_turn", turn)
    request = SourceDeploymentRequest(
        application="demo",
        namespace="demo",
        health_objective="healthy",
        attempt=1,
        registry_host="registry.synthetic:5000",
        correction_feedback="O1.render [render]: the tree has no deployment files",
    )
    result = propose_source(request, tmp_path)
    assert (result.proposed, result.summary, result.agent_session_id) == (True, "committed a kustomization", "s-1")
    assert not hasattr(result, "plan") or "plan" not in [field.name for field in result.DESCRIPTOR.fields]
    assert request.registry_host in prompts[0]
    assert "no deployment files" in prompts[0]
    assert "git commit" in prompts[0]


def test_deployer_reply_with_plan_fields_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from types import SimpleNamespace

    def turn(_provider: str, _prompt: str, **_kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(
            output_json='{"proposed":true,"summary":"ready","manifest_paths":["k8s"]}', session_id="s"
        )

    monkeypatch.setattr("sdo.agents.source_deployment.worker.run_structured_turn", turn)
    request = SourceDeploymentRequest(application="demo", namespace="demo", attempt=1, registry_host="r.synthetic:5000")
    with pytest.raises(ValueError, match="manifest_paths"):
        propose_source(request, tmp_path)


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


@pytest.mark.parametrize("provider", ["codex", "claude"])
@pytest.mark.parametrize("role", ["source", "health"])
def test_live_lifecycle_callers_use_the_pod_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, provider: str, role: str
) -> None:
    from sdo.agents import prompt_templates
    from sdo.agents.lifecycle import ClaudeLifecycleBackend, CodexLifecycleBackend

    class Observed(BaseException):
        pass

    def boundary(actual_provider: str, _prompt: str, **options: object) -> None:
        assert actual_provider == provider
        assert options["access"] == "danger-full-access"
        raise Observed

    monkeypatch.setattr(prompt_templates, "_run_structured_turn", boundary)
    monkeypatch.setenv("SDO_AGENT_PROVIDER", provider)
    backend = (ClaudeLifecycleBackend if provider == "claude" else CodexLifecycleBackend)(
        reasoning_effort="medium", timeout_seconds=900
    )

    def invoke() -> None:
        if role == "source":
            propose_source(
                SourceDeploymentRequest(
                    application="demo",
                    namespace="demo",
                    health_objective="healthy",
                    attempt=1,
                    registry_host="registry.synthetic:5000",
                ),
                tmp_path,
            )
        else:
            backend.run_health_judge(
                repository=tmp_path,
                application="demo",
                health_objective="healthy",
                architecture="Synthetic application for provider-boundary checks",
                resources=[],
                correction_feedback=None,
            )

    with pytest.raises(Observed):
        invoke()


def test_deployer_turn_uses_the_configured_lifecycle_limit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The deployer is a lifecycle session like the designer: its turn limit is agent.lifecycle_timeout.

    Builds run inside the turn, so a fixed shorter cap ends a turn that is still fixing an image.
    """
    from types import SimpleNamespace

    limits: list[object] = []

    def turn(_provider: str, _prompt: str, **kwargs: object) -> SimpleNamespace:
        limits.append(kwargs["timeout_seconds"])
        return SimpleNamespace(output_json='{"proposed":true,"summary":"ok"}', session_id="s-1")

    monkeypatch.setattr("sdo.agents.source_deployment.worker.run_structured_turn", turn)
    monkeypatch.setenv("SDO_LIFECYCLE_TIMEOUT_SECONDS", "5400")
    propose_source(SourceDeploymentRequest(application="demo", namespace="demo", attempt=1), tmp_path)
    assert limits == [5400]
