from pathlib import Path

import pytest

from sdo.agents.source_deployment.worker import _workload_id, propose_source, run_deployment_task
from sdo.contracts.proto import (
    AgentTask,
    BuildKitImageConfig,
    DeploymentPlan,
    DeploymentSource,
    ProposalWorkspace,
    RawRenderConfig,
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
        plan = DeploymentPlan(
            source=DeploymentSource(renderer_id="raw"), expected_workload_ids=["Deployment/demo/demo"]
        )
        plan.source.config.Pack(RawRenderConfig(paths=["deploy/app.yaml"]))
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
    raw = RawRenderConfig()
    assert typed.plan.source.config.Unpack(raw)
    assert raw.paths == ["deploy/app.yaml"]


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
        assert _kwargs["access"] == "danger-full-access"
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


def test_propose_source_emits_new_form_plan(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from types import SimpleNamespace

    registry = "registry.synthetic:5000"

    def turn(_provider: str, _prompt: str, **_kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(
            output_json=(
                '{"proposed":true,"summary":"ready","image_builds":[{"image":"registry.synthetic:5000/app:v1",'
                '"context_path":"app","dockerfile_path":"app/Dockerfile"}],"manifest_paths":["deploy/app.yaml"],'
                '"expected_workload_ids":["Deployment/demo/api"]}'
            ),
            session_id="session-1",
        )

    monkeypatch.setattr("sdo.agents.source_deployment.worker.run_structured_turn", turn)
    request = SourceDeploymentRequest(
        application="demo", namespace="demo", health_objective="healthy", attempt=1, registry_host=registry
    )
    plan = propose_source(request, tmp_path).plan
    assert not plan.strategy_id
    assert not plan.HasField("strategy_config")
    assert plan.source.renderer_id == "raw"
    raw = RawRenderConfig()
    assert plan.source.config.Unpack(raw)
    assert list(raw.paths) == ["deploy/app.yaml"]
    assert list(plan.expected_workload_ids) == ["Deployment/demo/api"]
    [image] = plan.images
    assert (image.image, image.source_id) == ("registry.synthetic:5000/app:v1", "buildkit")
    build = BuildKitImageConfig()
    assert image.config.Unpack(build)
    assert (build.context_path, build.dockerfile_path) == ("app", "app/Dockerfile")


def _plan_with_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, manifest: str, dockerfile: str, context: str = "app"
):
    import json
    from types import SimpleNamespace

    def turn(_provider: str, _prompt: str, **_kwargs: object) -> SimpleNamespace:
        draft = {
            "proposed": True,
            "summary": "ready",
            "image_builds": [
                {"image": "registry.synthetic:5000/app:v1", "context_path": context, "dockerfile_path": dockerfile}
            ],
            "manifest_paths": [manifest],
            "expected_workload_ids": ["Deployment/demo/api"],
        }
        return SimpleNamespace(output_json=json.dumps(draft), session_id="session-1")

    monkeypatch.setattr("sdo.agents.source_deployment.worker.run_structured_turn", turn)
    request = SourceDeploymentRequest(
        application="demo",
        namespace="demo",
        health_objective="healthy",
        attempt=1,
        registry_host="registry.synthetic:5000",
    )
    return propose_source(request, tmp_path).plan


def test_plan_paths_may_name_authored_deployment_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    plan = _plan_with_manifest(
        tmp_path, monkeypatch, ".sdo/deployment/manifests/app.yaml", ".sdo/deployment/build/app/Dockerfile"
    )
    raw = RawRenderConfig()
    assert plan.source.config.Unpack(raw)
    assert list(raw.paths) == [".sdo/deployment/manifests/app.yaml"]
    build = BuildKitImageConfig()
    assert plan.images[0].config.Unpack(build)
    assert build.dockerfile_path == ".sdo/deployment/build/app/Dockerfile"


def test_plan_build_context_may_be_the_repository_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    plan = _plan_with_manifest(tmp_path, monkeypatch, "kubernetes/app.yaml", "Dockerfile", context=".")
    build = BuildKitImageConfig()
    assert plan.images[0].config.Unpack(build)
    assert build.context_path == "."


@pytest.mark.parametrize(
    "manifest",
    [
        ".sdo/deployment/plan.yaml",
        ".sdo/deployment/patches/0001-x.patch",
        ".sdo/playbooks/x/README.md",
        ".sdo/deployment/manifests",
        ".sdo/deployment/other/app.yaml",
        "../app.yaml",
        "/app.yaml",
        " deploy/app.yaml",
    ],
)
def test_plan_paths_reject_non_effective_locations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, manifest: str
) -> None:
    with pytest.raises(ValueError, match="effective tree|must be .sdo/deployment"):
        _plan_with_manifest(tmp_path, monkeypatch, manifest, "app/Dockerfile")


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
    backend = (ClaudeLifecycleBackend if provider == "claude" else CodexLifecycleBackend)()

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
