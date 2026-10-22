"""Tests for benchmarks.sregym.runner.pipeline."""

from __future__ import annotations

import dataclasses
import json
import textwrap
from pathlib import Path
from unittest.mock import patch

import pytest

from benchmarks.sregym.runner.pipeline import (
    PipelineConfig,
    PipelineState,
    StageConfig,
    StageState,
    _deep_merge,
    has_pipeline_state,
    is_pipeline_config,
    load_pipeline_config,
    merge_stage_config,
    read_pipeline_snapshot,
    read_pipeline_state,
    reset_stages_for_rerun,
    write_pipeline_snapshot,
    write_pipeline_state,
)
from benchmarks.sregym.runner.runner import _stage_results_error

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_toml(tmp_path: Path, content: str, name: str = "test.toml") -> Path:
    p = tmp_path / name
    p.write_text(textwrap.dedent(content))
    return p


PIPELINE_TOML = """\
[pipeline]
name = "test-pipeline"

[defaults]
agent = "sdo_codex"
model = "gemini-flash"
parallel = 4
app_filter = "hotel_reservation"
deploy_from_source = true
application_workspace = true
spec_names = ["wrong_service_selector"]

[defaults.env]
judge_model_id = "judge-model"
reuse_cluster = true
force_recreate_cluster = false
submit_done_returns_feedback = true

[defaults.variants]
seed = 99

[[stages]]
name = "build_kb"
chain_application_workspace = false

[stages.runner]
parallel = 8

[stages.runner.variants]
enabled = true
count = 16

[[stages]]
name = "evaluate"
chain_application_workspace = true

[stages.runner]
tasklist = "count_train"
"""


def test_stage_results_error_rejects_agent_crash_even_when_benchmark_exits_zero(tmp_path: Path) -> None:
    result = tmp_path / "problem_runs" / "run-1" / "results_1.csv"
    result.parent.mkdir(parents=True)
    result.write_text('"agent_error","agent_exit_code","problem_id"\nTrue,1,"problem"\n', encoding="utf-8")

    assert "agent_error" in (_stage_results_error(tmp_path) or "")


def _valid_strict_receipt() -> dict[str, object]:
    canary = {
        "passed": True,
        "observed_at": "2026-07-12T12:00:00+00:00",
        "details": "enforcement observed",
    }
    return {
        "schema_version": "sdo.production-receipt/v1",
        "pre_cutover": False,
        "validator_mode": "kubernetes-job",
        "lifecycle_provenance": True,
        "production_job_dispatch": True,
        "completed": True,
        "repair_policy": "commit",
        "repair_actions": [],
        "proposal_commit": "proposal",
        "outcome_commit": "outcome",
        "reflection_commit": "reflection",
        "validator_evidence_commit": "reflection",
        "same_session_reflection": True,
        "detector_clear": [{"status": "clear", "fingerprints": []}],
        "independent_verification": [{"passed": True}],
        "validator_network_policy_canaries": [
            {**canary, "mode": "allow", "job_name": "validator-allow"},
            {**canary, "mode": "deny", "job_name": "validator-deny"},
        ],
        "acknowledged": True,
        "cleaned": True,
        "remaining_worktrees": [],
        "responder_jobs": ["sdo-incident-job"],
        "controller_update_required": False,
    }


def test_stage_results_error_accepts_completed_semantic_result(tmp_path: Path) -> None:
    result = tmp_path / "problem_runs" / "run-1" / "results_1.csv"
    result.parent.mkdir(parents=True)
    result.write_text(
        '"Diagnosis.success","Mitigation.success","agent_error","agent_exit_code","problem_id"\n'
        'True,True,False,0,"problem"\n',
        encoding="utf-8",
    )

    assert _stage_results_error(tmp_path) is None


def test_stage_results_error_accepts_current_parallel_runner_layout_with_strict_receipt(tmp_path: Path) -> None:
    run = tmp_path / "runs" / "000000_problem" / "worker_0" / "results" / "sdo_codex" / "problem" / "run_1"
    run.mkdir(parents=True)
    (run / "problem_results.csv").write_text(
        '"Diagnosis.success","Mitigation.success","problem_id"\nTrue,True,"problem"\n',
        encoding="utf-8",
    )
    (run / "sdo_production_receipt_strict.json").write_text(
        json.dumps(_valid_strict_receipt()) + "\n",
        encoding="utf-8",
    )

    assert _stage_results_error(tmp_path, require_strict_receipt=True) is None


@pytest.mark.parametrize(
    ("receipts", "expected"),
    [
        ([], "produced no strict receipt"),
        (
            ["sdo_production_receipt_strict.json", "sdo_production_receipt_old.json"],
            "exactly one standalone strict receipt, found sdo_production_receipt_old.json, ",
        ),
    ],
)
def test_stage_results_error_distinguishes_zero_from_multiple_receipts(
    tmp_path: Path, receipts: list[str], expected: str
) -> None:
    run = tmp_path / "runs" / "000000_problem" / "worker_0" / "results" / "sdo_codex" / "problem" / "run_1"
    run.mkdir(parents=True)
    (run / "problem_results.csv").write_text(
        '"Diagnosis.success","Mitigation.success","problem_id"\nTrue,True,"problem"\n',
        encoding="utf-8",
    )
    for name in receipts:
        (run / name).write_text(json.dumps(_valid_strict_receipt()) + "\n", encoding="utf-8")

    error = _stage_results_error(tmp_path, require_strict_receipt=True)

    assert error is not None
    assert expected in error


@pytest.mark.parametrize(
    ("header", "values", "expected"),
    [
        ('"problem_id"', '"problem"', "Diagnosis.success"),
        (
            '"Diagnosis.success","Mitigation.success","problem_id"',
            ',True,"problem"',
            "Diagnosis.success",
        ),
        (
            '"Diagnosis.success","Mitigation.success","problem_id"',
            'definitely,True,"problem"',
            "Diagnosis.success",
        ),
        (
            '"Diagnosis.success","Mitigation.success","problem_id"',
            'True,False,"problem"',
            "Mitigation.success",
        ),
    ],
)
def test_stage_results_error_rejects_missing_blank_malformed_or_false_semantics(
    tmp_path: Path,
    header: str,
    values: str,
    expected: str,
) -> None:
    result = tmp_path / "problem_runs" / "run-1" / "results_1.csv"
    result.parent.mkdir(parents=True)
    result.write_text(f"{header}\n{values}\n", encoding="utf-8")

    assert expected in (_stage_results_error(tmp_path) or "")


def test_stage_results_error_accepts_nested_json_semantics(tmp_path: Path) -> None:
    result = tmp_path / "problem_runs" / "run-1" / "results_1.csv"
    result.parent.mkdir(parents=True)
    with result.open("w", newline="", encoding="utf-8") as stream:
        import csv

        writer = csv.DictWriter(stream, fieldnames=["Diagnosis", "Mitigation", "problem_id"])
        writer.writeheader()
        writer.writerow(
            {
                "Diagnosis": json.dumps({"success": True}),
                "Mitigation": json.dumps({"success": True}),
                "problem_id": "problem",
            }
        )

    assert _stage_results_error(tmp_path) is None


def test_stage_results_error_rejects_failed_diagnosis_or_mitigation(tmp_path: Path) -> None:
    result = tmp_path / "problem_runs" / "run-1" / "results_1.csv"
    result.parent.mkdir(parents=True)
    result.write_text(
        '"Diagnosis.success","Mitigation.success","problem_id"\nTrue,False,"problem"\n',
        encoding="utf-8",
    )

    assert "Mitigation.success=true" in (_stage_results_error(tmp_path) or "")


def test_stage_results_error_requires_one_strict_receipt_per_sdo_problem(tmp_path: Path) -> None:
    run = tmp_path / "problem_runs" / "run-1"
    run.mkdir(parents=True)
    (run / "results_1.csv").write_text(
        '"Diagnosis.success","Mitigation.success","problem_id"\nTrue,True,"problem"\n',
        encoding="utf-8",
    )

    assert "strict receipt" in (_stage_results_error(tmp_path, require_strict_receipt=True) or "")

    agent = run / "agent"
    agent.mkdir()
    (agent / "sdo_production_receipt_strict.json").write_text(
        json.dumps(_valid_strict_receipt()) + "\n",
        encoding="utf-8",
    )
    assert _stage_results_error(tmp_path, require_strict_receipt=True) is None


@pytest.mark.parametrize(
    "receipt_text",
    [
        "not-json\n",
        "[]\n",
        "{}\n",
        json.dumps({**_valid_strict_receipt(), "completed": False}) + "\n",
    ],
)
def test_stage_results_error_rejects_malformed_or_invalid_strict_receipt(
    tmp_path: Path,
    receipt_text: str,
) -> None:
    run = tmp_path / "problem_runs" / "run-1"
    agent = run / "agent"
    agent.mkdir(parents=True)
    (run / "results_1.csv").write_text(
        '"Diagnosis.success","Mitigation.success","problem_id"\nTrue,True,"problem"\n',
        encoding="utf-8",
    )
    (agent / "sdo_production_receipt_strict.json").write_text(receipt_text, encoding="utf-8")

    assert "strict receipt" in (_stage_results_error(tmp_path, require_strict_receipt=True) or "")


SINGLE_EXPERIMENT_TOML = """\
[runner]
agent = "sdo_codex"
model = "gemini-flash"
parallel = 4

[runner.variants]
enabled = true
count = 8

[runner.env]
judge_model_id = "judge-model"
worker_cpu_limit = "16"

[agent.sdo_codex]
persistent_controller = true
"""


# ---------------------------------------------------------------------------
# test_load_pipeline_config
# ---------------------------------------------------------------------------


class TestLoadPipelineConfig:
    def test_parse_two_stage_pipeline(self, tmp_path: Path) -> None:
        p = _write_toml(tmp_path, PIPELINE_TOML)
        config = load_pipeline_config(p)

        assert config.name == "test-pipeline"
        assert len(config.stages) == 2
        assert config.stages[0].name == "build_kb"
        assert config.stages[0].chain_application_workspace is False
        assert config.stages[0].runner_overrides["parallel"] == 8
        assert config.stages[0].runner_overrides["variants"]["enabled"] is True
        assert config.stages[1].name == "evaluate"
        assert config.stages[1].chain_application_workspace is True
        assert config.stages[1].runner_overrides["tasklist"] == "count_train"
        assert config.defaults["model"] == "gemini-flash"
        assert config.defaults["app_filter"] == "hotel_reservation"
        assert config.defaults["env"]["judge_model_id"] == "judge-model"

    def test_merge_stage_preserves_source_docker_builder(self) -> None:
        config = merge_stage_config(
            {"env": {"docker_builder": "sdo-example"}},
            {},
        )

        assert config.env.docker_builder == "sdo-example"

    def test_no_stages_raises(self) -> None:
        with pytest.raises(ValueError, match="at least one stage"):
            PipelineConfig(name="empty", stages=[])

    def test_duplicate_stage_names_raises(self) -> None:
        with pytest.raises(ValueError, match="unique"):
            PipelineConfig(
                name="dup",
                stages=[
                    StageConfig(name="a"),
                    StageConfig(name="a"),
                ],
            )


# ---------------------------------------------------------------------------
# test_is_pipeline_config
# ---------------------------------------------------------------------------


class TestIsPipelineConfig:
    def test_pipeline_toml_detected(self, tmp_path: Path) -> None:
        p = _write_toml(tmp_path, PIPELINE_TOML)
        assert is_pipeline_config(p) is True

    def test_single_experiment_not_detected(self, tmp_path: Path) -> None:
        p = _write_toml(tmp_path, SINGLE_EXPERIMENT_TOML)
        assert is_pipeline_config(p) is False


# ---------------------------------------------------------------------------
# test_merge_stage_config
# ---------------------------------------------------------------------------


class TestMergeStageConfig:
    def test_defaults_only(self) -> None:
        defaults = {
            "agent": "sdo_codex",
            "model": "gemini-flash",
            "parallel": 1,
            "agent_timeout": 3600,
            "app_filter": "hotel_reservation",
            "deploy_from_source": True,
            "application_workspace": True,
            "spec_names": ["wrong_service_selector"],
            "variants": {"seed": 99},
            "env": {
                "judge_model_id": "judge",
                "reuse_cluster": True,
                "force_recreate_cluster": False,
                "preserve_infrastructure": True,
            },
        }
        config = merge_stage_config(defaults, {})
        assert config.agent == "sdo_codex"
        assert config.model == "gemini-flash"
        assert config.parallel == 1
        assert config.agent_timeout == 3600
        assert config.app_filter == "hotel_reservation"
        assert config.deploy_from_source is True
        assert config.application_workspace is True
        assert config.spec_names == ["wrong_service_selector"]
        assert config.variants.seed == 99
        assert config.env.judge_model_id == "judge"
        assert config.env.reuse_cluster is True
        assert config.env.force_recreate_cluster is False
        assert config.env.preserve_infrastructure is True
        assert config.env.submit_done_returns_feedback is False

    def test_defaults_env_preserves_submit_done_feedback(self) -> None:
        defaults = {
            "env": {
                "judge_model_id": "judge",
                "reuse_cluster": True,
                "force_recreate_cluster": False,
                "submit_done_returns_feedback": True,
            },
        }
        config = merge_stage_config(defaults, {})
        assert config.env.submit_done_returns_feedback is True

    def test_defaults_env_preserves_fast_namespace_teardown(self) -> None:
        config = merge_stage_config({"env": {"fast_namespace_teardown": True}}, {})
        assert config.env.fast_namespace_teardown is True

    def test_defaults_env_preserves_lifecycle_validation_cache(self) -> None:
        config = merge_stage_config({"env": {"lifecycle_validation_cache": True}}, {})
        assert config.env.lifecycle_validation_cache is True

    def test_defaults_env_preserves_source_build_cache(self) -> None:
        config = merge_stage_config({"env": {"source_build_cache": True}}, {})
        assert config.env.source_build_cache is True

    def test_defaults_env_preserves_deferred_diagnosis_grading(self) -> None:
        config = merge_stage_config({"env": {"defer_diagnosis_grading": True}}, {})
        assert config.env.defer_diagnosis_grading is True

    def test_with_overrides(self) -> None:
        defaults = {
            "agent": "sdo_codex",
            "model": "gemini-flash",
            "parallel": 4,
            "variants": {"seed": 99, "enabled": False},
        }
        overrides = {"model": "gemini-pro", "parallel": 8}
        config = merge_stage_config(defaults, overrides)
        assert config.model == "gemini-pro"
        assert config.parallel == 8
        assert config.agent == "sdo_codex"  # inherited

    def test_workspace_related_fields_are_preserved(self) -> None:
        defaults = {
            "agent": "sdo_codex",
            "parallel": 1,
            "app_filter": "hotel_reservation",
            "deploy_from_source": True,
            "application_workspace": True,
            "spec_names": ["wrong_service_selector"],
            "env": {
                "reuse_cluster": True,
                "force_recreate_cluster": True,
                "submit_done_returns_feedback": True,
            },
        }
        overrides = {"model": "claude-sonnet-4-5"}
        config = merge_stage_config(defaults, overrides)
        assert config.agent == "sdo_codex"
        assert config.model == "claude-sonnet-4-5"
        assert config.parallel == 1
        assert config.app_filter == "hotel_reservation"
        assert config.deploy_from_source is True
        assert config.application_workspace is True
        assert config.spec_names == ["wrong_service_selector"]
        assert config.env.reuse_cluster is True
        assert config.env.force_recreate_cluster is True
        assert config.env.submit_done_returns_feedback is True

    def test_workspace_mode_is_preserved(self) -> None:
        defaults = {
            "agent": "sdo_codex",
            "parallel": 1,
            "app_filter": "hotel_reservation",
            "deploy_from_source": True,
            "application_workspace": "ephemeral",
        }
        config = merge_stage_config(defaults, {})
        assert config.application_workspace == "ephemeral"

    def test_nested_override_preserves_siblings(self) -> None:
        """Override variants.count but keep variants.seed from defaults."""
        defaults = {
            "variants": {"seed": 99, "enabled": False, "count": 4},
        }
        overrides = {
            "variants": {"count": 16, "enabled": True},
        }
        config = merge_stage_config(defaults, overrides)
        assert config.variants.count == 16
        assert config.variants.enabled is True
        assert config.variants.seed == 99  # preserved from defaults

    def test_agent_config_override(self) -> None:
        defaults = {
            "agent_config": {"sdo_codex": {"persistent_controller": True, "controller_image": "img:1"}},
        }
        overrides = {
            "agent_config": {"sdo_codex": {"persistent_controller": False}},
        }
        config = merge_stage_config(defaults, overrides)
        assert config.agent_config["sdo_codex"]["persistent_controller"] is False
        assert config.agent_config["sdo_codex"]["controller_image"] == "img:1"


# ---------------------------------------------------------------------------
# test_deep_merge
# ---------------------------------------------------------------------------


class TestDeepMerge:
    def test_flat(self) -> None:
        assert _deep_merge({"a": 1}, {"b": 2}) == {"a": 1, "b": 2}

    def test_override(self) -> None:
        assert _deep_merge({"a": 1}, {"a": 2}) == {"a": 2}

    def test_nested(self) -> None:
        base = {"x": {"a": 1, "b": 2}}
        over = {"x": {"b": 3, "c": 4}}
        assert _deep_merge(base, over) == {"x": {"a": 1, "b": 3, "c": 4}}

    def test_does_not_mutate_base(self) -> None:
        base = {"x": {"a": 1}}
        _deep_merge(base, {"x": {"b": 2}})
        assert base == {"x": {"a": 1}}


# ---------------------------------------------------------------------------
# test_pipeline_state_roundtrip
# ---------------------------------------------------------------------------


class TestPipelineState:
    def test_roundtrip(self, tmp_path: Path) -> None:
        state = PipelineState(
            stages=[
                StageState(index=0, name="a", status="completed", experiment_dir="/tmp/a"),
                StageState(index=1, name="b", status="pending"),
            ]
        )
        write_pipeline_state(state, tmp_path)
        loaded = read_pipeline_state(tmp_path)
        assert len(loaded.stages) == 2
        assert loaded.stages[0].status == "completed"
        assert loaded.stages[0].experiment_dir == "/tmp/a"
        assert loaded.stages[1].status == "pending"

    def test_snapshot_keeps_the_workspace_seed_and_the_agent_failure_policy(self, tmp_path: Path) -> None:
        """A resume reads the snapshot: dropping the seed would rerun stage 0 as a fresh lifecycle."""

        config = PipelineConfig(
            name="p",
            workspace_seed="/seeds/lifecycle",
            continue_on_agent_failure=True,
            stages=[StageConfig(name="s0")],
        )
        write_pipeline_snapshot(config, tmp_path)

        loaded = read_pipeline_snapshot(tmp_path)

        assert loaded.workspace_seed == "/seeds/lifecycle"
        assert loaded.continue_on_agent_failure is True

    def test_agent_failure_policy_loads_from_toml_and_defaults_off(self, tmp_path: Path) -> None:
        on = tmp_path / "on.toml"
        on.write_text('[pipeline]\nname = "p"\ncontinue_on_agent_failure = true\n\n[[stages]]\nname = "s"\n')
        off = tmp_path / "off.toml"
        off.write_text('[pipeline]\nname = "p"\n\n[[stages]]\nname = "s"\n')

        assert load_pipeline_config(on).continue_on_agent_failure is True
        assert load_pipeline_config(off).continue_on_agent_failure is False

    def test_has_pipeline_state(self, tmp_path: Path) -> None:
        assert has_pipeline_state(tmp_path) is False
        state = PipelineState(stages=[StageState(index=0, name="a")])
        write_pipeline_state(state, tmp_path)
        assert has_pipeline_state(tmp_path) is True


# ---------------------------------------------------------------------------
# test_pipeline_snapshot
# ---------------------------------------------------------------------------


class TestPipelineSnapshot:
    def test_write_and_read(self, tmp_path: Path) -> None:
        config = PipelineConfig(
            name="snap-test",
            defaults={"agent": "sdo_codex", "model": "gemini-flash"},
            stages=[
                StageConfig(name="s0", runner_overrides={"parallel": 8}),
                StageConfig(name="s1", chain_application_workspace=True),
            ],
        )
        write_pipeline_snapshot(config, tmp_path)
        from benchmarks.sregym.runner.pipeline import read_pipeline_snapshot

        loaded = read_pipeline_snapshot(tmp_path)
        assert loaded.name == "snap-test"
        assert len(loaded.stages) == 2
        assert loaded.stages[0].name == "s0"
        assert loaded.stages[0].chain_application_workspace is False
        assert loaded.stages[1].chain_application_workspace is True


# ---------------------------------------------------------------------------
# test_reset_stages_for_rerun
# ---------------------------------------------------------------------------


class TestResetStagesForRerun:
    def _make_state_and_config(self, tmp_path: Path) -> tuple[PipelineConfig, PipelineState]:
        """Create a 4-stage pipeline: stages 0,1,2 chain, stage 3 independent."""
        config = PipelineConfig(
            name="rerun-test",
            stages=[
                StageConfig(name="s0"),
                StageConfig(name="s1"),
                StageConfig(name="s2", chain_application_workspace=True),
                StageConfig(name="s3"),
            ],
        )

        # Create fake experiment dirs for all stages
        dirs = []
        for i, s in enumerate(config.stages):
            d = tmp_path / f"stage_{i}_{s.name}"
            d.mkdir()
            (d / "kb").mkdir()
            dirs.append(d)

        state = PipelineState(
            stages=[
                StageState(index=i, name=s.name, status="completed", experiment_dir=str(dirs[i]))
                for i, s in enumerate(config.stages)
            ]
        )

        return config, state

    def test_resets_chained_stages(self, tmp_path: Path) -> None:
        """Rerunning stage 1 should reset stages 1 and 2 (chained) but not 3."""
        config, state = self._make_state_and_config(tmp_path)
        reset_stages_for_rerun(config, state, from_stage=1, pipeline_dir=tmp_path)

        assert state.stages[0].status == "completed"
        assert state.stages[1].status == "pending"
        assert state.stages[2].status == "pending"
        assert state.stages[3].status == "completed"  # independent

    def test_renames_old_dir(self, tmp_path: Path) -> None:
        config, state = self._make_state_and_config(tmp_path)
        old_dir = Path(state.stages[1].experiment_dir)
        assert old_dir.exists()

        reset_stages_for_rerun(config, state, from_stage=1, pipeline_dir=tmp_path)

        assert not old_dir.exists()
        # Should have a timestamped backup
        backups = [p for p in tmp_path.iterdir() if p.name.startswith("stage_1_s1.")]
        assert len(backups) == 1

    def test_preserves_prior_kb(self, tmp_path: Path) -> None:
        """Rerunning stage 1 should not touch stage 0's experiment dir."""
        config, state = self._make_state_and_config(tmp_path)
        stage0_dir = Path(state.stages[0].experiment_dir)
        stage0_kb = stage0_dir / "kb"
        assert stage0_kb.exists()

        reset_stages_for_rerun(config, state, from_stage=1, pipeline_dir=tmp_path)

        assert stage0_dir.exists()
        assert stage0_kb.exists()
        assert state.stages[0].status == "completed"

    def test_invalid_stage_index(self, tmp_path: Path) -> None:
        config, state = self._make_state_and_config(tmp_path)
        with pytest.raises(ValueError, match="out of range"):
            reset_stages_for_rerun(config, state, from_stage=5, pipeline_dir=tmp_path)

    def test_extends_state_when_config_adds_new_stage(self, tmp_path: Path) -> None:
        config, state = self._make_state_and_config(tmp_path)
        state.stages = state.stages[:2]

        reset_stages_for_rerun(config, state, from_stage=2, pipeline_dir=tmp_path)

        assert len(state.stages) == 4
        assert state.stages[2].index == 2
        assert state.stages[2].name == "s2"
        assert state.stages[2].status == "pending"
        assert state.stages[2].experiment_dir == ""


# ---------------------------------------------------------------------------
# Pipeline runner tests (mocked subprocess)
# ---------------------------------------------------------------------------


from benchmarks.sregym.runner import runner as runner_mod  # noqa: E402


class TestPipelineRunner:
    """Tests for ``benchmarks.sregym.runner.runner.run_pipeline``."""

    @pytest.fixture
    def sregym_dir(self, tmp_path: Path):
        """Create a fake sregym dir with main.py."""
        d = tmp_path / "third_party" / "sregym"
        d.mkdir(parents=True)
        (d / "main.py").write_text("# fake")
        return d

    def _make_config(self) -> PipelineConfig:
        return PipelineConfig(
            name="test",
            defaults={"agent": "sdo_codex", "model": "gemini-flash"},
            stages=[
                StageConfig(name="build"),
                StageConfig(name="eval"),
            ],
        )

    @staticmethod
    def _write_success_result(argv: list[str]) -> None:
        experiment_dir = Path(argv[argv.index("--experiment-dir") + 1])
        problem_run = experiment_dir / "problem_runs" / "run"
        problem_run.mkdir(parents=True, exist_ok=True)
        (problem_run / "results_test.csv").write_text(
            '"Diagnosis.success","Mitigation.success","agent_error","agent_exit_code","problem_id"\n'
            'True,True,False,0,"problem"\n',
            encoding="utf-8",
        )

    def test_runs_stages_sequentially(self, sregym_dir, tmp_path: Path) -> None:
        config = self._make_config()
        calls = []

        def mock_run(argv, cwd=None, env=None):
            calls.append(argv)
            self._write_success_result(argv)
            return type("Result", (), {"returncode": 0})()

        pipeline_dir = tmp_path / "pipeline"
        pipeline_dir.mkdir()

        with patch("subprocess.run", side_effect=mock_run):
            state = PipelineState(
                stages=[
                    StageState(index=0, name="build"),
                    StageState(index=1, name="eval"),
                ]
            )
            write_pipeline_state(state, pipeline_dir)
            write_pipeline_snapshot(config, pipeline_dir)
            rc = runner_mod.run_pipeline(
                config,
                project_root=tmp_path,
                sregym_dir=sregym_dir,
                pipeline_dir=pipeline_dir,
                state=state,
            )

        assert rc == 0
        assert len(calls) == 2

    def test_abort_on_failure(self, sregym_dir, tmp_path: Path) -> None:
        config = self._make_config()
        call_count = 0

        def mock_run(argv, cwd=None, env=None):
            nonlocal call_count
            call_count += 1
            return type("Result", (), {"returncode": 1})()

        pipeline_dir = tmp_path / "pipeline"
        pipeline_dir.mkdir()

        with patch("subprocess.run", side_effect=mock_run):
            state = PipelineState(
                stages=[
                    StageState(index=0, name="build"),
                    StageState(index=1, name="eval"),
                ]
            )
            write_pipeline_state(state, pipeline_dir)
            write_pipeline_snapshot(config, pipeline_dir)
            rc = runner_mod.run_pipeline(
                config,
                project_root=tmp_path,
                sregym_dir=sregym_dir,
                pipeline_dir=pipeline_dir,
                state=state,
            )

        assert rc == 1
        assert call_count == 1  # stage 1 never ran

        loaded_state = read_pipeline_state(pipeline_dir)
        assert loaded_state.stages[0].status == "failed"
        assert loaded_state.stages[1].status == "pending"

    @staticmethod
    def _write_mitigation_failure(argv: list[str]) -> None:
        experiment_dir = Path(argv[argv.index("--experiment-dir") + 1])
        problem_run = experiment_dir / "problem_runs" / "run"
        problem_run.mkdir(parents=True, exist_ok=True)
        (problem_run / "results_test.csv").write_text(
            '"Diagnosis.success","Mitigation.success","agent_error","agent_exit_code","problem_id"\n'
            'True,False,False,0,"problem"\n',
            encoding="utf-8",
        )

    def _run(self, config, sregym_dir, tmp_path: Path, mock_run, state=None):
        pipeline_dir = tmp_path / "pipeline"
        pipeline_dir.mkdir(exist_ok=True)
        state = state or PipelineState(
            stages=[StageState(index=i, name=stage.name) for i, stage in enumerate(config.stages)]
        )
        with patch("subprocess.run", side_effect=mock_run):
            write_pipeline_state(state, pipeline_dir)
            write_pipeline_snapshot(config, pipeline_dir)
            rc = runner_mod.run_pipeline(
                config, project_root=tmp_path, sregym_dir=sregym_dir, pipeline_dir=pipeline_dir, state=state
            )
        return rc, read_pipeline_state(pipeline_dir)

    def test_an_oracle_failure_is_recorded_and_the_chain_continues_when_the_policy_allows_it(
        self, sregym_dir, tmp_path: Path
    ) -> None:
        """One agent failure must not discard every later stage of a measurement pipeline."""

        config = dataclasses.replace(self._make_config(), continue_on_agent_failure=True)
        calls: list[list[str]] = []

        def mock_run(argv, cwd=None, env=None):
            calls.append(argv)
            if len(calls) == 1:
                self._write_mitigation_failure(argv)
            else:
                self._write_success_result(argv)
            return type("Result", (), {"returncode": 0})()

        rc, loaded = self._run(config, sregym_dir, tmp_path, mock_run)

        assert rc == 0
        assert len(calls) == 2
        assert loaded.stages[0].status == "agent_failure"
        assert "Mitigation.success" in loaded.stages[0].error
        assert loaded.stages[1].status == "completed"

    def test_an_oracle_failure_still_aborts_by_default(self, sregym_dir, tmp_path: Path) -> None:
        calls: list[list[str]] = []

        def mock_run(argv, cwd=None, env=None):
            calls.append(argv)
            self._write_mitigation_failure(argv)
            return type("Result", (), {"returncode": 0})()

        rc, loaded = self._run(self._make_config(), sregym_dir, tmp_path, mock_run)

        assert rc == 1
        assert len(calls) == 1
        assert loaded.stages[0].status == "failed"

    def test_a_harness_crash_aborts_even_when_agent_failures_may_continue(self, sregym_dir, tmp_path: Path) -> None:
        config = dataclasses.replace(self._make_config(), continue_on_agent_failure=True)
        calls: list[list[str]] = []

        def mock_run(argv, cwd=None, env=None):
            calls.append(argv)
            return type("Result", (), {"returncode": 1})()

        rc, loaded = self._run(config, sregym_dir, tmp_path, mock_run)

        assert rc == 1
        assert len(calls) == 1
        assert loaded.stages[0].status == "failed"

    def test_resume_skips_a_recorded_agent_failure(self, sregym_dir, tmp_path: Path) -> None:
        config = dataclasses.replace(self._make_config(), continue_on_agent_failure=True)
        stage0_dir = tmp_path / "pipeline" / "stage_0_build"
        (stage0_dir / "kb").mkdir(parents=True)
        calls: list[list[str]] = []

        def mock_run(argv, cwd=None, env=None):
            calls.append(argv)
            self._write_success_result(argv)
            return type("Result", (), {"returncode": 0})()

        state = PipelineState(
            stages=[
                StageState(index=0, name="build", status="agent_failure", experiment_dir=str(stage0_dir)),
                StageState(index=1, name="eval"),
            ]
        )
        rc, loaded = self._run(config, sregym_dir, tmp_path, mock_run, state=state)

        assert rc == 0
        assert len(calls) == 1
        assert loaded.stages[0].status == "agent_failure"

    def test_resume_skips_completed(self, sregym_dir, tmp_path: Path) -> None:
        config = self._make_config()
        calls = []

        def mock_run(argv, cwd=None, env=None):
            calls.append(argv)
            self._write_success_result(argv)
            return type("Result", (), {"returncode": 0})()

        pipeline_dir = tmp_path / "pipeline"
        pipeline_dir.mkdir()

        stage0_dir = pipeline_dir / "stage_0_build"
        stage0_dir.mkdir()
        (stage0_dir / "kb").mkdir()

        with patch("subprocess.run", side_effect=mock_run):
            state = PipelineState(
                stages=[
                    StageState(index=0, name="build", status="completed", experiment_dir=str(stage0_dir)),
                    StageState(index=1, name="eval", status="pending"),
                ]
            )
            write_pipeline_state(state, pipeline_dir)
            write_pipeline_snapshot(config, pipeline_dir)
            rc = runner_mod.run_pipeline(
                config,
                project_root=tmp_path,
                sregym_dir=sregym_dir,
                pipeline_dir=pipeline_dir,
                state=state,
            )

        assert rc == 0
        assert len(calls) == 1  # only stage 1 ran

    def test_chain_application_workspace_sets_seed_env(self, sregym_dir, tmp_path: Path) -> None:
        config = PipelineConfig(
            name="test",
            defaults={
                "agent": "sdo_codex",
                "model": "claude-sonnet-4-5",
                "parallel": 1,
                "app_filter": "hotel_reservation",
                "deploy_from_source": True,
                "application_workspace": True,
            },
            stages=[
                StageConfig(name="build"),
                StageConfig(name="eval", chain_application_workspace=True),
            ],
        )
        captured_envs = []

        def mock_run_stage(exp_config, stage_exp_dir, tasklist_path, sregym_dir, project_root, extra_env=None):
            captured_envs.append(extra_env or {})
            if stage_exp_dir.name == "stage_0_build":
                workspace = stage_exp_dir / "application_workspace"
                workspace.mkdir(parents=True, exist_ok=True)
                (workspace / "README.md").write_text("seed\n", encoding="utf-8")
            return 0

        pipeline_dir = tmp_path / "pipeline"
        pipeline_dir.mkdir()

        with patch.object(runner_mod, "_run_stage", side_effect=mock_run_stage):
            state = PipelineState(
                stages=[
                    StageState(index=0, name="build"),
                    StageState(index=1, name="eval"),
                ]
            )
            write_pipeline_state(state, pipeline_dir)
            write_pipeline_snapshot(config, pipeline_dir)
            rc = runner_mod.run_pipeline(
                config,
                project_root=tmp_path,
                sregym_dir=sregym_dir,
                pipeline_dir=pipeline_dir,
                state=state,
            )

        assert rc == 0
        assert captured_envs[0] == {}
        assert captured_envs[1]["SREGYM_APP_WORKSPACE_SEED_DIR"].endswith("/stage_0_build/application_workspace")

    def test_runtime_retry_prefers_validated_lifecycle_seed_for_current_stage(self, sregym_dir, tmp_path: Path) -> None:
        config = PipelineConfig(
            name="test",
            defaults={
                "agent": "sdo_codex",
                "model": "gpt-5.4",
                "parallel": 1,
                "app_filter": "hotel_reservation",
                "deploy_from_source": True,
                "application_workspace": "persistent",
            },
            stages=[
                StageConfig(name="build", chain_application_workspace=False),
                StageConfig(name="eval", chain_application_workspace=True),
            ],
        )
        pipeline_dir = tmp_path / "pipeline"
        pipeline_dir.mkdir()
        stage0_dir = pipeline_dir / "stage_0_build"
        workspace = stage0_dir / "application_workspace"
        workspace.mkdir(parents=True)
        runner_mod.write_snapshot(merge_stage_config(config.defaults, {}), stage0_dir)
        seed = pipeline_dir / "lifecycle_seed_stage1"
        (seed / ".git").mkdir(parents=True)
        (seed / ".sdo").mkdir()
        (seed / ".sdo" / "lifecycle-provenance.yaml").write_text("validated\n", encoding="utf-8")
        captured_envs = []

        def mock_run_stage(exp_config, stage_exp_dir, tasklist_path, sregym_dir, project_root, extra_env=None):
            captured_envs.append(extra_env or {})
            return 0

        with patch.object(runner_mod, "_run_stage", side_effect=mock_run_stage):
            state = PipelineState(
                stages=[
                    StageState(index=0, name="build", status="completed", experiment_dir=str(stage0_dir)),
                    StageState(index=1, name="eval"),
                ]
            )
            write_pipeline_state(state, pipeline_dir)
            write_pipeline_snapshot(config, pipeline_dir)
            rc = runner_mod.run_pipeline(
                config,
                project_root=tmp_path,
                sregym_dir=sregym_dir,
                pipeline_dir=pipeline_dir,
                state=state,
            )

        assert rc == 0
        assert captured_envs[0]["SREGYM_APP_WORKSPACE_SEED_DIR"] == str(seed)

    def test_chain_application_workspace_missing_source_fails(self, sregym_dir, tmp_path: Path) -> None:
        config = PipelineConfig(
            name="test",
            defaults={
                "agent": "sdo_codex",
                "model": "claude-sonnet-4-5",
                "parallel": 1,
                "app_filter": "hotel_reservation",
                "deploy_from_source": True,
                "application_workspace": True,
            },
            stages=[
                StageConfig(name="build"),
                StageConfig(name="eval", chain_application_workspace=True),
            ],
        )

        pipeline_dir = tmp_path / "pipeline"
        pipeline_dir.mkdir()

        with patch.object(runner_mod, "_run_stage", return_value=0):
            state = PipelineState(
                stages=[
                    StageState(index=0, name="build"),
                    StageState(index=1, name="eval"),
                ]
            )
            write_pipeline_state(state, pipeline_dir)
            write_pipeline_snapshot(config, pipeline_dir)
            rc = runner_mod.run_pipeline(
                config,
                project_root=tmp_path,
                sregym_dir=sregym_dir,
                pipeline_dir=pipeline_dir,
                state=state,
            )

        assert rc == 1
        loaded_state = read_pipeline_state(pipeline_dir)
        assert loaded_state.stages[1].status == "failed"
        assert "application workspace is missing" in loaded_state.stages[1].error

    def test_chain_application_workspace_requires_workspace_mode(self, sregym_dir, tmp_path: Path) -> None:
        config = PipelineConfig(
            name="test",
            defaults={
                "agent": "sdo_codex",
                "model": "claude-sonnet-4-5",
                "parallel": 1,
                "app_filter": "hotel_reservation",
                "deploy_from_source": True,
                "application_workspace": True,
            },
            stages=[
                StageConfig(name="build"),
                StageConfig(
                    name="eval",
                    chain_application_workspace=True,
                    runner_overrides={"application_workspace": False},
                ),
            ],
        )

        pipeline_dir = tmp_path / "pipeline"
        pipeline_dir.mkdir()

        with patch.object(runner_mod, "_run_stage", return_value=0):
            state = PipelineState(
                stages=[
                    StageState(index=0, name="build"),
                    StageState(index=1, name="eval"),
                ]
            )
            write_pipeline_state(state, pipeline_dir)
            write_pipeline_snapshot(config, pipeline_dir)
            rc = runner_mod.run_pipeline(
                config,
                project_root=tmp_path,
                sregym_dir=sregym_dir,
                pipeline_dir=pipeline_dir,
                state=state,
            )

        assert rc == 1
        loaded_state = read_pipeline_state(pipeline_dir)
        assert loaded_state.stages[1].status == "failed"
        assert "requires application_workspace = 'persistent'" in loaded_state.stages[1].error

    def test_chain_application_workspace_requires_persistent_workspace(self, sregym_dir, tmp_path: Path) -> None:
        config = PipelineConfig(
            name="test",
            defaults={
                "agent": "sdo_codex",
                "model": "claude-sonnet-4-5",
                "parallel": 1,
                "app_filter": "hotel_reservation",
                "deploy_from_source": True,
                "application_workspace": "persistent",
            },
            stages=[
                StageConfig(name="build"),
                StageConfig(
                    name="eval",
                    chain_application_workspace=True,
                    runner_overrides={"application_workspace": "ephemeral"},
                ),
            ],
        )

        pipeline_dir = tmp_path / "pipeline"
        pipeline_dir.mkdir()

        with patch.object(runner_mod, "_run_stage", return_value=0):
            state = PipelineState(
                stages=[
                    StageState(index=0, name="build"),
                    StageState(index=1, name="eval"),
                ]
            )
            write_pipeline_state(state, pipeline_dir)
            write_pipeline_snapshot(config, pipeline_dir)
            rc = runner_mod.run_pipeline(
                config,
                project_root=tmp_path,
                sregym_dir=sregym_dir,
                pipeline_dir=pipeline_dir,
                state=state,
            )

        assert rc == 1
        loaded_state = read_pipeline_state(pipeline_dir)
        assert loaded_state.stages[1].status == "failed"
        assert "requires application_workspace = 'persistent'" in loaded_state.stages[1].error

    def test_chain_application_workspace_requires_matching_app_filter(self, sregym_dir, tmp_path: Path) -> None:
        config = PipelineConfig(
            name="test",
            defaults={
                "agent": "sdo_codex",
                "model": "claude-sonnet-4-5",
                "parallel": 1,
                "app_filter": "hotel_reservation",
                "deploy_from_source": True,
                "application_workspace": True,
            },
            stages=[
                StageConfig(name="build"),
                StageConfig(
                    name="eval",
                    chain_application_workspace=True,
                    runner_overrides={"app_filter": "social_network"},
                ),
            ],
        )

        pipeline_dir = tmp_path / "pipeline"
        pipeline_dir.mkdir()

        def mock_run_stage(exp_config, stage_exp_dir, tasklist_path, sregym_dir, project_root, extra_env=None):
            workspace = stage_exp_dir / "application_workspace"
            workspace.mkdir(parents=True, exist_ok=True)
            return 0

        with patch.object(runner_mod, "_run_stage", side_effect=mock_run_stage):
            state = PipelineState(
                stages=[
                    StageState(index=0, name="build"),
                    StageState(index=1, name="eval"),
                ]
            )
            write_pipeline_state(state, pipeline_dir)
            write_pipeline_snapshot(config, pipeline_dir)
            rc = runner_mod.run_pipeline(
                config,
                project_root=tmp_path,
                sregym_dir=sregym_dir,
                pipeline_dir=pipeline_dir,
                state=state,
            )

        assert rc == 1
        loaded_state = read_pipeline_state(pipeline_dir)
        assert loaded_state.stages[1].status == "failed"
        assert "matching app_filter values" in loaded_state.stages[1].error

    def test_resume_extends_state_for_new_stage(self, sregym_dir, tmp_path: Path) -> None:
        config = PipelineConfig(
            name="test",
            defaults={"agent": "sdo_codex", "model": "gemini-flash"},
            stages=[
                StageConfig(name="build"),
                StageConfig(name="eval"),
                StageConfig(name="eval_again"),
            ],
        )

        pipeline_dir = tmp_path / "pipeline"
        pipeline_dir.mkdir()

        with patch.object(runner_mod, "_run_stage", return_value=0):
            state = PipelineState(
                stages=[
                    StageState(index=0, name="build"),
                    StageState(index=1, name="eval"),
                ]
            )
            write_pipeline_state(state, pipeline_dir)
            write_pipeline_snapshot(config, pipeline_dir)
            rc = runner_mod.run_pipeline(
                config,
                project_root=tmp_path,
                sregym_dir=sregym_dir,
                pipeline_dir=pipeline_dir,
                state=state,
            )

        assert rc == 0
        loaded_state = read_pipeline_state(pipeline_dir)
        assert [stage.name for stage in loaded_state.stages] == ["build", "eval", "eval_again"]


class TestPersistentControllerPipeline:
    """Persistent mode: one controller per application across stages, receipts after the drain."""

    @pytest.fixture
    def sregym_dir(self, tmp_path: Path):
        d = tmp_path / "third_party" / "sregym"
        d.mkdir(parents=True)
        (d / "main.py").write_text("# fake")
        return d

    @staticmethod
    def _config(*, continue_on_agent_failure: bool = False) -> PipelineConfig:
        return PipelineConfig(
            name="persistent",
            continue_on_agent_failure=continue_on_agent_failure,
            defaults={
                "agent": "sdo_codex",
                "model": "gpt-test",
                "require_strict_receipt": True,
                "agent_config": {"sdo_codex": {"persistent_controller": True}},
            },
            stages=[StageConfig(name="first"), StageConfig(name="second")],
        )

    def _run(
        self,
        sregym_dir: Path,
        tmp_path: Path,
        *,
        teardown_writes_receipts: bool,
        receipt_stages: tuple[int, ...] = (0, 1),
        teardown_returncode: int = 0,
        continue_on_agent_failure: bool = False,
    ) -> tuple[int, list[list[str]], list[dict[str, str]], Path]:
        config = self._config(continue_on_agent_failure=continue_on_agent_failure)
        pipeline_dir = tmp_path / "pipeline"
        pipeline_dir.mkdir()
        calls: list[list[str]] = []
        envs: list[dict[str, str]] = []
        agent_dirs: list[Path] = []

        def mock_run(argv, cwd=None, env=None, **_kwargs):
            calls.append(list(argv))
            envs.append(dict(env or {}))
            if "benchmarks.sregym.adapter.persistent" in argv:
                if teardown_writes_receipts:
                    for index, agent_dir in enumerate(agent_dirs):
                        if index in receipt_stages:
                            (agent_dir / "sdo_production_receipt_strict.json").write_text(
                                json.dumps(_valid_strict_receipt()) + "\n", encoding="utf-8"
                            )
                return type("Result", (), {"returncode": teardown_returncode})()
            experiment_dir = Path(argv[argv.index("--experiment-dir") + 1])
            problem_run = experiment_dir / "problem_runs" / "run"
            agent_dir = problem_run / "agent"
            agent_dir.mkdir(parents=True, exist_ok=True)
            agent_dirs.append(agent_dir)
            (problem_run / "results_test.csv").write_text(
                '"Diagnosis.success","Mitigation.success","agent_error","problem_id"\nTrue,True,False,"problem"\n',
                encoding="utf-8",
            )
            # Only a resolution record exists at stage end; the strict receipt follows the drain.
            (agent_dir / "sdo_incident_resolution.json").write_text("{}\n", encoding="utf-8")
            Path(env["SDO_PERSISTENT_CONTROLLER_STATE"]).write_text('{"controllers": {}}\n', encoding="utf-8")
            return type("Result", (), {"returncode": 0})()

        with patch("subprocess.run", side_effect=mock_run):
            state = PipelineState(stages=[StageState(index=0, name="first"), StageState(index=1, name="second")])
            write_pipeline_state(state, pipeline_dir)
            write_pipeline_snapshot(config, pipeline_dir)
            rc = runner_mod.run_pipeline(
                config, project_root=tmp_path, sregym_dir=sregym_dir, pipeline_dir=pipeline_dir, state=state
            )
        return rc, calls, envs, pipeline_dir

    def test_stages_share_one_state_file_and_teardown_runs_last(self, sregym_dir, tmp_path: Path) -> None:
        rc, calls, envs, pipeline_dir = self._run(sregym_dir, tmp_path, teardown_writes_receipts=True)

        assert rc == 0
        stage_envs = [env for call, env in zip(calls, envs, strict=True) if "main.py" in call]
        assert len(stage_envs) == 2
        for env in stage_envs:
            assert env["SDO_PERSISTENT_CONTROLLER_STATE"] == str(pipeline_dir / "sdo_persistent_controller.json")
            assert env["SREGYM_PRESERVE_NAMESPACE_LABEL"] == "sdo.dev/controller-namespace"
        assert "benchmarks.sregym.adapter.persistent" in calls[-1]
        assert calls[-1][calls[-1].index("--state") + 1] == str(pipeline_dir / "sdo_persistent_controller.json")
        # Deferred receipts are published into the harness's results tree.
        assert calls[-1][calls[-1].index("--publish-root") + 1] == str(pipeline_dir)
        assert read_pipeline_state(pipeline_dir).stages[1].status == "completed"

    def test_a_failed_last_drain_does_not_blame_stages_with_valid_receipts(self, sregym_dir, tmp_path: Path) -> None:
        rc, _calls, _envs, pipeline_dir = self._run(
            sregym_dir, tmp_path, teardown_writes_receipts=True, receipt_stages=(0,), teardown_returncode=1
        )

        assert rc == 1
        states = read_pipeline_state(pipeline_dir).stages
        assert states[0].status == "completed"
        assert states[1].status == "failed"
        assert "strict receipt" in states[1].error
        assert "teardown exited with code 1" in states[1].error

    def test_missing_deferred_strict_receipt_fails_the_pipeline(self, sregym_dir, tmp_path: Path) -> None:
        rc, _calls, _envs, pipeline_dir = self._run(sregym_dir, tmp_path, teardown_writes_receipts=False)

        assert rc == 1
        states = read_pipeline_state(pipeline_dir).stages
        assert states[0].status == "failed"
        assert "strict receipt" in states[0].error

    def test_a_rejected_receipt_is_an_agent_failure_for_that_stage_only(self, sregym_dir, tmp_path: Path) -> None:
        rc, _calls, _envs, pipeline_dir = self._run(
            sregym_dir,
            tmp_path,
            teardown_writes_receipts=True,
            receipt_stages=(1,),
            continue_on_agent_failure=True,
        )

        assert rc == 0
        states = read_pipeline_state(pipeline_dir).stages
        assert states[0].status == "agent_failure"
        assert "strict receipt" in states[0].error
        assert states[1].status == "completed"

    def test_a_rejected_receipt_still_fails_the_pipeline_unless_agent_failures_continue(
        self, sregym_dir, tmp_path: Path
    ) -> None:
        rc, _calls, _envs, pipeline_dir = self._run(
            sregym_dir, tmp_path, teardown_writes_receipts=True, receipt_stages=(1,)
        )

        assert rc == 1
        assert read_pipeline_state(pipeline_dir).stages[0].status == "failed"


def test_persistent_mode_is_off_by_default_for_sdo_codex(tmp_path: Path) -> None:
    from benchmarks.sregym.runner.experiment import ExperimentConfig, config_to_env

    env = config_to_env(ExperimentConfig(agent="sdo_codex"), tmp_path)

    assert "SREGYM_PRESERVE_NAMESPACE_LABEL" not in env


# ---------------------------------------------------------------------------
# Stage-0 application-workspace seed
# ---------------------------------------------------------------------------

SEEDED_TOML = """\
[pipeline]
name = "seeded"
workspace_seed = "{seed}"

[defaults]
agent = "sdo_codex"
application_workspace = "persistent"

[[stages]]
name = "first"
chain_application_workspace = false
"""


def test_pipeline_without_a_workspace_seed_adds_no_seed_environment(tmp_path: Path) -> None:
    from benchmarks.sregym.runner.pipeline import initial_workspace_seed_env

    config = load_pipeline_config(_write_toml(tmp_path, PIPELINE_TOML))

    assert config.workspace_seed == ""
    assert initial_workspace_seed_env(config) == {}


def test_pipeline_refuses_to_launch_on_the_placeholder_workspace_seed(tmp_path: Path) -> None:
    from benchmarks.sregym.runner.pipeline import WORKSPACE_SEED_PLACEHOLDER, initial_workspace_seed_env

    config = load_pipeline_config(_write_toml(tmp_path, SEEDED_TOML.format(seed=WORKSPACE_SEED_PLACEHOLDER)))

    with pytest.raises(ValueError, match="fresh lifecycle"):
        initial_workspace_seed_env(config)


def test_pipeline_seeds_stage_zero_from_an_existing_workspace_seed(tmp_path: Path) -> None:
    from benchmarks.sregym.runner.pipeline import initial_workspace_seed_env

    seed = tmp_path / "seed"
    seed.mkdir()
    config = load_pipeline_config(_write_toml(tmp_path, SEEDED_TOML.format(seed=seed)))

    assert initial_workspace_seed_env(config) == {"SREGYM_APP_WORKSPACE_SEED_DIR": str(seed.resolve())}

    missing = load_pipeline_config(_write_toml(tmp_path, SEEDED_TOML.format(seed=tmp_path / "absent"), "m.toml"))
    with pytest.raises(FileNotFoundError, match="absent"):
        initial_workspace_seed_env(missing)


def _write_result(tmp_path: Path, header: str, values: str) -> None:
    result = tmp_path / "problem_runs" / "run-1" / "results_1.csv"
    result.parent.mkdir(parents=True)
    result.write_text(f"{header}\n{values}\n", encoding="utf-8")


def test_stage_results_error_rejects_a_graded_failure_by_default(tmp_path: Path) -> None:
    _write_result(tmp_path, '"Diagnosis.success","Mitigation.success","problem_id"', 'False,True,"problem"')

    assert "Diagnosis.success" in (_stage_results_error(tmp_path) or "")


def test_stage_results_error_accepts_a_graded_failure_when_failed_verdicts_are_allowed(tmp_path: Path) -> None:
    _write_result(tmp_path, '"Diagnosis.success","Mitigation.success","problem_id"', 'False,False,"problem"')

    assert _stage_results_error(tmp_path, allow_failed_verdicts=True) is None


@pytest.mark.parametrize(
    ("header", "values"),
    [
        ('"problem_id"', '"problem"'),
        ('"Diagnosis.success","Mitigation.success","problem_id"', ',,"problem"'),
        ('"Diagnosis.success","Mitigation.success","problem_id"', 'maybe,True,"problem"'),
        ('"Diagnosis.success","Mitigation.success","agent_error","problem_id"', 'True,True,True,"problem"'),
    ],
)
def test_allowing_failed_verdicts_still_rejects_ungraded_or_errored_rows(
    tmp_path: Path, header: str, values: str
) -> None:
    _write_result(tmp_path, header, values)

    assert _stage_results_error(tmp_path, allow_failed_verdicts=True) is not None


def test_allow_failed_verdicts_survives_the_pipeline_config_and_snapshot(tmp_path: Path) -> None:
    config_path = tmp_path / "p.toml"
    config_path.write_text(
        '[pipeline]\nname = "p"\n[defaults]\nallow_failed_verdicts = true\n[[stages]]\nname = "a"\n',
        encoding="utf-8",
    )
    config = load_pipeline_config(config_path)
    assert merge_stage_config(config.defaults, {}).allow_failed_verdicts is True

    write_pipeline_snapshot(config, tmp_path)
    restored = read_pipeline_snapshot(tmp_path)
    assert merge_stage_config(restored.defaults, {}).allow_failed_verdicts is True
    assert (
        merge_stage_config(load_pipeline_config(_pipeline_without_flag(tmp_path)).defaults, {}).allow_failed_verdicts
        is False
    )


def _pipeline_without_flag(tmp_path: Path) -> Path:
    path = tmp_path / "plain.toml"
    path.write_text('[pipeline]\nname = "p"\n[defaults]\n[[stages]]\nname = "a"\n', encoding="utf-8")
    return path


@pytest.mark.parametrize(
    ("section", "key"),
    [("stage", "chain_kb"), ("defaults", "enable_summary"), ("defaults.env", "crucible_seed_kb_dir")],
)
def test_pipeline_rejects_keys_of_the_removed_crucible_agent(tmp_path: Path, section: str, key: str) -> None:
    stage = {"stage": f"{key} = true\n", "defaults": "", "defaults.env": ""}[section]
    defaults = {"stage": "", "defaults": f"{key} = true\n", "defaults.env": ""}[section]
    env = {"stage": "", "defaults": "", "defaults.env": f'{key} = "x"\n'}[section]
    path = tmp_path / "pipeline.toml"
    path.write_text(
        f'[pipeline]\nname = "p"\n\n[defaults]\nagent = "sdo_codex"\n{defaults}\n[defaults.env]\n{env}\n'
        f'[[stages]]\nname = "s1"\n{stage}',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=key):
        load_pipeline_config(path)
