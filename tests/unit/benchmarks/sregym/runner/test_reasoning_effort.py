"""Both arms of an SDO-vs-Codex comparison run at one declared reasoning effort.

The stock Codex baseline used to inherit the CLI catalog's model default,
because SREGym clears ``AGENT_REASONING_EFFORT`` unless ``--reasoning-effort``
is passed. SDO's incident agents pin their effort in code.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from benchmarks.sregym.runner.experiment import (
    ExperimentConfig,
    _serialize_config,
    config_to_main_args,
    load_experiment_config,
)
from benchmarks.sregym.runner.pipeline import load_pipeline_config, merge_stage_config
from sdo.agents.responder import INCIDENT_REASONING_EFFORT

EXPERIMENTS = Path(__file__).resolve().parents[5] / "benchmarks" / "sregym" / "experiments"
COMPARED_AGENTS = {"codex", "sdo_codex"}


def _resolved_configs(path: Path) -> list[ExperimentConfig]:
    if "[pipeline]" in path.read_text(encoding="utf-8"):
        pipeline = load_pipeline_config(path)
        return [merge_stage_config(pipeline.defaults, stage.runner_overrides) for stage in pipeline.stages]
    return [load_experiment_config(path)]


def _compared_arms() -> dict[str, list[ExperimentConfig]]:
    arms: dict[str, list[ExperimentConfig]] = {}
    for path in sorted(EXPERIMENTS.glob("*.toml")):
        configs = [config for config in _resolved_configs(path) if config.agent in COMPARED_AGENTS]
        if configs:
            arms[path.name] = configs
    return arms


def test_the_reasoning_effort_reaches_sregym_as_its_cli_flag(tmp_path: Path) -> None:
    config = ExperimentConfig(agent="codex", model="gpt-6-luna", reasoning_effort="medium")

    args = config_to_main_args(config, tmp_path, None)

    assert args[args.index("--reasoning-effort") + 1] == "medium"
    assert "--reasoning-effort" not in config_to_main_args(ExperimentConfig(), tmp_path, None)


def test_the_reasoning_effort_is_loaded_and_survives_the_snapshot(tmp_path: Path) -> None:
    source = tmp_path / "exp.toml"
    source.write_text(
        textwrap.dedent(
            """
            [runner]
            agent = "codex"
            reasoning_effort = "medium"
            """
        ),
        encoding="utf-8",
    )
    config = load_experiment_config(source)
    snapshot = tmp_path / "snapshot.toml"
    snapshot.write_text(_serialize_config(config), encoding="utf-8")

    assert config.reasoning_effort == "medium"
    assert load_experiment_config(snapshot).reasoning_effort == "medium"


def test_a_pipeline_stage_inherits_the_default_reasoning_effort() -> None:
    config = merge_stage_config({"agent": "sdo_codex", "reasoning_effort": "medium"}, {"problems": ["p"]})

    assert config.reasoning_effort == "medium"


def test_an_unknown_reasoning_effort_is_rejected() -> None:
    with pytest.raises(ValueError, match="reasoning_effort"):
        ExperimentConfig(agent="codex", reasoning_effort="extreme")


@pytest.mark.parametrize("name", sorted(_compared_arms()))
def test_every_compared_arm_declares_its_reasoning_effort(name: str) -> None:
    assert all(config.reasoning_effort for config in _compared_arms()[name]), name


def test_sdo_and_codex_baseline_arms_declare_the_same_reasoning_effort() -> None:
    arms = _compared_arms()
    efforts = {
        (config.agent, config.reasoning_effort)
        for configs in arms.values()
        for config in configs
        if config.model == "gpt-6-luna"
    }

    assert {agent for agent, _ in efforts} == COMPARED_AGENTS
    assert {effort for _, effort in efforts} == {INCIDENT_REASONING_EFFORT}
