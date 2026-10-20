"""The runner consults the launch preflight before anything launches."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest

from benchmarks.sregym.runner import runner as runner_mod
from benchmarks.sregym.runner.pipeline import PipelineConfig, StageConfig
from benchmarks.sregym.runner.preflight import GB, LaunchAssurance, PreflightError

if TYPE_CHECKING:
    from sregym_fake_host import FakeHost

LUNA_DEFAULTS = {
    "agent": "codex",
    "model": "gpt-6-luna",
    "reasoning_effort": "medium",
    "parallel": 1,
    "env": {"judge_model_id": "codex-gpt-6-luna"},
}


@pytest.fixture
def sregym_dir(tmp_path: Path) -> Path:
    directory = tmp_path / "third_party" / "sregym"
    directory.mkdir(parents=True)
    (directory / "main.py").write_text("# fake", encoding="utf-8")
    (directory / "agents.yaml").write_text("agents:\n  - name: codex\n    agent_version: '0.163.0'\n", encoding="utf-8")
    return directory


def _failing_assurance(fake_host: FakeHost) -> LaunchAssurance:
    fake_host.default_free = 10 * GB
    return LaunchAssurance(host=fake_host)


def test_a_failed_preflight_aborts_a_pipeline_before_any_directory_or_stage(
    fake_host: FakeHost, sregym_dir: Path
) -> None:
    config = PipelineConfig(name="luna", defaults=LUNA_DEFAULTS, stages=[StageConfig(name="one")])

    with patch("subprocess.run") as launched, pytest.raises(PreflightError, match="disk"):
        runner_mod.run_pipeline(
            config,
            project_root=Path(__file__).resolve().parents[5],
            sregym_dir=sregym_dir,
            assurance=_failing_assurance(fake_host),
        )

    launched.assert_not_called()
    assert not (sregym_dir / "logs").exists() or not any((sregym_dir / "logs").iterdir())


def test_a_failed_preflight_aborts_a_single_experiment_before_its_directory(
    fake_host: FakeHost, sregym_dir: Path, tmp_path: Path
) -> None:
    toml = tmp_path / "luna.toml"
    toml.write_text(
        '[runner]\nagent = "codex"\nmodel = "gpt-6-luna"\nreasoning_effort = "medium"\nparallel = 1\n'
        'problems = ["p"]\n[runner.env]\njudge_model_id = "codex-gpt-6-luna"\n',
        encoding="utf-8",
    )

    with patch("os.execvpe") as launched, pytest.raises(PreflightError):
        runner_mod.run_single_experiment(
            toml,
            [],
            project_root=Path(__file__).resolve().parents[5],
            sregym_dir=sregym_dir,
            assurance=_failing_assurance(fake_host),
        )

    launched.assert_not_called()
    assert not (sregym_dir / "logs").exists() or not any((sregym_dir / "logs").iterdir())


def test_the_pipeline_preflight_covers_every_stage(fake_host: FakeHost, sregym_dir: Path) -> None:
    config = PipelineConfig(
        name="luna",
        defaults=LUNA_DEFAULTS,
        stages=[StageConfig(name="one"), StageConfig(name="two", runner_overrides={"model": "x"})],
    )

    with patch("subprocess.run") as launched, pytest.raises(PreflightError, match="model differs"):
        runner_mod.run_pipeline(
            config,
            project_root=Path(__file__).resolve().parents[5],
            sregym_dir=sregym_dir,
            assurance=LaunchAssurance(host=fake_host),
        )

    launched.assert_not_called()
