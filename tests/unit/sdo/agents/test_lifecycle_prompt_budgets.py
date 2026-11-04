"""Controller-produced and generated text always fits the lifecycle prompts it is substituted into."""

from __future__ import annotations

import os
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from sdo.agents.lifecycle.agents import CodexLifecycleBackend
from sdo.agents.source_deployment.worker import propose_source
from sdo.contracts.proto import SourceDeploymentRequest

if TYPE_CHECKING:
    from pathlib import Path

    from sdo.agents.prompt_templates import RenderedPrompt

PLAN = (
    '{"proposed":true,"summary":"ready","image_builds":[],"manifest_paths":["deploy/app.yaml"],'
    '"expected_workload_ids":["Deployment/demo/api"]}'
)
FEEDBACK_317 = "verifier rejected: " + "x" * 298
OBJECTIVE_1399 = "keep the social network healthy; " + "y" * 1366


@pytest.fixture(autouse=True)
def _scratch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path / "scratch"))


def deployer_prompt(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, objective: str, feedback: str) -> RenderedPrompt:
    prompts: list[RenderedPrompt] = []

    def turn(_provider: str, prompt: RenderedPrompt, **_kwargs: Any) -> SimpleNamespace:
        prompts.append(prompt)
        return SimpleNamespace(output_json=PLAN, session_id="s")

    monkeypatch.setattr("sdo.agents.source_deployment.worker.run_structured_turn", turn)
    request = SourceDeploymentRequest(
        application="demo",
        namespace="demo",
        health_objective=objective,
        correction_feedback=feedback,
        attempt=2,
        registry_host="registry.demo.svc.cluster.local:5000",
    )
    propose_source(request, tmp_path)
    return prompts[0]


def judge_prompt(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, objective: str, feedback: str | None
) -> RenderedPrompt:
    prompts: list[RenderedPrompt] = []

    def execute(_self: object, _repository: Path, prompt: RenderedPrompt, _draft: type) -> tuple[Any, str]:
        prompts.append(prompt)
        return SimpleNamespace(model_dump=dict), "s"

    monkeypatch.setattr(CodexLifecycleBackend, "_execute", execute)
    monkeypatch.setattr("sdo.agents.lifecycle.agents.HealthJudgeArtifact", lambda **_kw: None)
    CodexLifecycleBackend().run_health_judge(
        repository=tmp_path,
        application="demo",
        health_objective=objective,
        architecture="arch",
        resources=[],
        correction_feedback=feedback,
    )
    return prompts[0]


def test_deployer_keeps_long_feedback_and_objective_inline_or_by_pointer(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    prompt = deployer_prompt(monkeypatch, tmp_path, OBJECTIVE_1399, FEEDBACK_317)
    assert FEEDBACK_317 in prompt
    assert OBJECTIVE_1399 in prompt


def test_oversized_deployer_values_spill_to_a_file_the_prompt_points_at(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    feedback = "rejected: " + "z" * 50_000
    prompt = deployer_prompt(monkeypatch, tmp_path, "goal", feedback)
    assert prompt.context_files
    assert any(path.read_text() == feedback for path in prompt.context_files)
    assert str(prompt.context_files[0].name) in prompt or "SDO_SESSION_SCRATCH" in prompt


@pytest.mark.parametrize("feedback", [FEEDBACK_317, "f" * 3000], ids=["317", "3000"])
def test_judge_accepts_long_objective_and_feedback(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, feedback: str
) -> None:
    prompt = judge_prompt(monkeypatch, tmp_path, OBJECTIVE_1399, feedback)
    assert OBJECTIVE_1399 in prompt


TEXT = st.builds(lambda n, c: c * n, st.integers(0, 30_000), st.characters(blacklist_categories=("Cs",)))


@settings(max_examples=60, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(objective=TEXT, feedback=TEXT)
def test_any_producer_output_fits_the_deployer_prompt(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, objective: str, feedback: str
) -> None:
    os.environ["SDO_SESSION_SCRATCH"] = str(tmp_path / "scratch")
    deployer_prompt(monkeypatch, tmp_path, objective, feedback)


@settings(max_examples=60, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(objective=TEXT, feedback=st.none() | TEXT)
def test_any_producer_output_fits_the_judge_prompt(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, objective: str, feedback: str | None
) -> None:
    os.environ["SDO_SESSION_SCRATCH"] = str(tmp_path / "scratch")
    judge_prompt(monkeypatch, tmp_path, objective, feedback)
