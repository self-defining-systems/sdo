"""Tests for benchmarks.sregym.runner.experiment."""

from __future__ import annotations

import json
import textwrap
from pathlib import Path

import pytest
import yaml

from benchmarks.sregym.runner.experiment import (
    ExperimentConfig,
    RunnerEnv,
    VariantConfig,
    _serialize_config,
    config_to_env,
    config_to_main_args,
    load_experiment_config,
    resolve_config,
    resolve_tasklist,
)


def _write_toml(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "exp.toml"
    p.write_text(textwrap.dedent(content))
    return p


def test_spec_names_loaded_from_toml(tmp_path: Path) -> None:
    toml = _write_toml(
        tmp_path,
        """
        [runner]
        spec_names = ["service_dns_resolution_failure", "wrong_dns_policy"]

        [runner.variants]
        enabled = false
    """,
    )
    config = load_experiment_config(toml)
    assert config.spec_names == ["service_dns_resolution_failure", "wrong_dns_policy"]


def test_spec_names_defaults_to_empty(tmp_path: Path) -> None:
    toml = _write_toml(
        tmp_path,
        """
        [runner]
        agent = "sdo_codex"

        [runner.variants]
        enabled = false
    """,
    )
    config = load_experiment_config(toml)
    assert config.spec_names == []


def test_spec_names_mutually_exclusive_with_variants() -> None:
    with pytest.raises(ValueError, match="runner.spec_names cannot be used with runner.variants.enabled"):
        ExperimentConfig(
            spec_names=["service_dns_resolution_failure"],
            variants=VariantConfig(enabled=True, count=5),
        )


def test_spec_names_mutually_exclusive_with_tasklist() -> None:
    with pytest.raises(ValueError, match="runner.spec_names is mutually exclusive"):
        ExperimentConfig(
            spec_names=["service_dns_resolution_failure"],
            tasklist="count_train",
        )


def test_spec_names_mutually_exclusive_with_problems() -> None:
    with pytest.raises(ValueError, match="runner.spec_names is mutually exclusive"):
        ExperimentConfig(
            spec_names=["service_dns_resolution_failure"],
            problems=["some_problem"],
        )


def test_spec_names_allowed_alone() -> None:
    config = ExperimentConfig(spec_names=["service_dns_resolution_failure"])
    assert config.spec_names == ["service_dns_resolution_failure"]


def test_config_to_main_args_emits_problem_spec(tmp_path: Path) -> None:
    config = ExperimentConfig(
        spec_names=["service_dns_resolution_failure", "wrong_dns_policy"],
    )
    args = config_to_main_args(config, exp_dir=tmp_path, tasklist_path=None)
    assert "--problem-spec" in args
    idx = args.index("--problem-spec")
    assert args[idx + 1] == "service_dns_resolution_failure"
    remaining = args[idx + 2 :]
    assert "--problem-spec" in remaining
    assert remaining[remaining.index("--problem-spec") + 1] == "wrong_dns_policy"


def test_config_to_main_args_no_problem_spec_when_empty(tmp_path: Path) -> None:
    config = ExperimentConfig()
    args = config_to_main_args(config, exp_dir=tmp_path, tasklist_path=None)
    assert "--problem-spec" not in args


def test_config_to_main_args_emits_agent_timeout(tmp_path: Path) -> None:
    config = ExperimentConfig(agent_timeout=3600)

    args = config_to_main_args(config, exp_dir=tmp_path, tasklist_path=None)

    index = args.index("--agent-timeout")
    assert args[index + 1] == "3600"


def test_config_to_main_args_emits_explicit_judge_model(tmp_path: Path) -> None:
    config = ExperimentConfig(env=RunnerEnv(judge_model_id="vertex-ai-gemini-2.5-pro"))

    args = config_to_main_args(config, exp_dir=tmp_path, tasklist_path=None)

    index = args.index("--judge-model")
    assert args[index + 1] == "vertex-ai-gemini-2.5-pro"


def test_sdo_codex_exports_required_kind_images_for_prefault_preflight(tmp_path: Path) -> None:
    config = ExperimentConfig(
        agent="sdo_codex",
        agent_config={
            "sdo_codex": {
                "controller_image": "controller:run-123",
                "responder_image": "responder:run-123",
            }
        },
    )

    env = config_to_env(config, tmp_path)

    assert json.loads(env["SREGYM_KIND_REQUIRED_IMAGES"]) == [
        "controller:run-123",
        "responder:run-123",
    ]
    assert env["SREGYM_KIND_REQUIRE_NETWORK_POLICY"] == "1"
    assert env["SREGYM_KIND_NETWORK_POLICY_CANARY_IMAGE"] == "controller:run-123"


def test_every_arm_gets_an_enforcing_cni_on_the_same_kind_topology(tmp_path: Path, monkeypatch) -> None:
    """SREGym's base kind config disables the default CNI and installs Calico only on request.

    A baseline arm that did not ask for it got a cluster with no CNI (nodes
    NotReady, every deploy timing out), and a network-policy fault would not
    be enforced for the baseline while it is for SDO. Every arm must ask.
    """

    monkeypatch.delenv("SREGYM_KIND_REQUIRE_NETWORK_POLICY", raising=False)
    for agent in ("codex", "sdo_codex"):
        env = config_to_env(ExperimentConfig(agent=agent), tmp_path)
        assert env["SREGYM_KIND_REQUIRE_NETWORK_POLICY"] == "1", agent


def test_deploy_from_source_loaded_from_toml(tmp_path: Path) -> None:
    toml = _write_toml(
        tmp_path,
        """
        [runner]
        deploy_from_source = true

        [runner.variants]
        enabled = false
    """,
    )
    config = load_experiment_config(toml)
    assert config.deploy_from_source is True


def test_app_filter_loaded_from_toml(tmp_path: Path) -> None:
    toml = _write_toml(
        tmp_path,
        """
        [runner]
        app_filter = "hotel_reservation"

        [runner.variants]
        enabled = false
    """,
    )
    config = load_experiment_config(toml)
    assert config.app_filter == "hotel_reservation"


def test_application_workspace_loaded_from_toml(tmp_path: Path) -> None:
    toml = _write_toml(
        tmp_path,
        """
        [runner]
        parallel = 1
        app_filter = "hotel_reservation"
        deploy_from_source = true
        application_workspace = true

        [runner.variants]
        enabled = false
    """,
    )
    config = load_experiment_config(toml)
    assert config.application_workspace is True


def test_application_workspace_mode_loaded_from_toml(tmp_path: Path) -> None:
    toml = _write_toml(
        tmp_path,
        """
        [runner]
        parallel = 1
        app_filter = "hotel_reservation"
        deploy_from_source = true
        application_workspace = "ephemeral"

        [runner.variants]
        enabled = false
    """,
    )
    config = load_experiment_config(toml)
    assert config.application_workspace == "ephemeral"


def test_config_to_main_args_emits_deploy_from_source_flag(tmp_path: Path) -> None:
    config = ExperimentConfig(deploy_from_source=True)
    args = config_to_main_args(config, exp_dir=tmp_path, tasklist_path=None)
    assert "--deploy-from-source" in args


def test_config_to_main_args_emits_app_filter(tmp_path: Path) -> None:
    config = ExperimentConfig(app_filter="social_network")
    args = config_to_main_args(config, exp_dir=tmp_path, tasklist_path=None)
    assert "--app-filter" in args
    assert "social_network" in args


def test_config_to_main_args_emits_application_workspace_flag(tmp_path: Path) -> None:
    config = ExperimentConfig(
        parallel=1,
        app_filter="hotel_reservation",
        deploy_from_source=True,
        application_workspace=True,
    )
    args = config_to_main_args(config, exp_dir=tmp_path, tasklist_path=None)
    assert "--application-workspace" in args


def test_config_to_main_args_ephemeral_emits_bare_flag_no_value(tmp_path: Path) -> None:
    """Ephemeral mode must render the BARE ``--application-workspace`` flag.

    ``main.py`` declares ``--application-workspace`` with ``action="store_true"``
    (boolean), so a trailing mode token like ``ephemeral`` is an argparse usage
    error (exit code 2) — which previously aborted the ephemeral control stage
    of a pipeline. The mode remains runner-side state and is never emitted as
    a positional value on the harness CLI.
    """
    config = ExperimentConfig(
        parallel=1,
        app_filter="hotel_reservation",
        deploy_from_source=True,
        application_workspace="ephemeral",
    )
    args = config_to_main_args(config, exp_dir=tmp_path, tasklist_path=None)
    assert "--application-workspace" in args
    # No bare mode token may follow the flag (would break main.py argparse).
    idx = args.index("--application-workspace")
    after = args[idx + 1] if idx + 1 < len(args) else None
    assert after != "ephemeral"
    assert "ephemeral" not in args


def test_config_to_main_args_omits_deploy_from_source_when_disabled(tmp_path: Path) -> None:
    config = ExperimentConfig()
    args = config_to_main_args(config, exp_dir=tmp_path, tasklist_path=None)
    assert "--deploy-from-source" not in args


def test_serialize_includes_deploy_from_source() -> None:
    config = ExperimentConfig(deploy_from_source=True)
    serialized = _serialize_config(config)
    assert "deploy_from_source = true" in serialized


def test_serialize_includes_app_filter() -> None:
    config = ExperimentConfig(app_filter="hotel_reservation")
    serialized = _serialize_config(config)
    assert 'app_filter = "hotel_reservation"' in serialized


def test_serialize_includes_application_workspace() -> None:
    config = ExperimentConfig(
        parallel=1,
        app_filter="hotel_reservation",
        deploy_from_source=True,
        application_workspace=True,
    )
    serialized = _serialize_config(config)
    assert "application_workspace = true" in serialized


def test_serialize_includes_application_workspace_mode() -> None:
    config = ExperimentConfig(
        parallel=1,
        app_filter="hotel_reservation",
        deploy_from_source=True,
        application_workspace="ephemeral",
    )
    serialized = _serialize_config(config)
    assert 'application_workspace = "ephemeral"' in serialized


def test_roundtrip_deploy_from_source(tmp_path: Path) -> None:
    config = ExperimentConfig(deploy_from_source=True)
    toml_path = tmp_path / "snap.toml"
    toml_path.write_text(_serialize_config(config))
    loaded = load_experiment_config(toml_path)
    assert loaded.deploy_from_source is True


def test_roundtrip_app_filter(tmp_path: Path) -> None:
    config = ExperimentConfig(app_filter="social_network")
    toml_path = tmp_path / "snap.toml"
    toml_path.write_text(_serialize_config(config))
    loaded = load_experiment_config(toml_path)
    assert loaded.app_filter == "social_network"


def test_roundtrip_application_workspace(tmp_path: Path) -> None:
    config = ExperimentConfig(
        parallel=1,
        app_filter="hotel_reservation",
        deploy_from_source=True,
        application_workspace=True,
    )
    toml_path = tmp_path / "snap.toml"
    toml_path.write_text(_serialize_config(config))
    loaded = load_experiment_config(toml_path)
    assert loaded.application_workspace is True


def test_roundtrip_application_workspace_mode(tmp_path: Path) -> None:
    config = ExperimentConfig(
        parallel=1,
        app_filter="hotel_reservation",
        deploy_from_source=True,
        application_workspace="ephemeral",
    )
    toml_path = tmp_path / "snap.toml"
    toml_path.write_text(_serialize_config(config))
    loaded = load_experiment_config(toml_path)
    assert loaded.application_workspace == "ephemeral"


def test_application_workspace_requires_app_filter() -> None:
    with pytest.raises(ValueError, match="application_workspace requires runner.app_filter"):
        ExperimentConfig(deploy_from_source=True, application_workspace=True)


def test_application_workspace_requires_deploy_from_source() -> None:
    with pytest.raises(ValueError, match="application_workspace requires runner.deploy_from_source = true"):
        ExperimentConfig(app_filter="hotel_reservation", application_workspace=True)


def test_application_workspace_allows_parallel_workers() -> None:
    config = ExperimentConfig(
        app_filter="hotel_reservation",
        deploy_from_source=True,
        application_workspace=True,
        parallel=2,
    )

    assert config.parallel == 2


def test_application_workspace_mode_allows_parallel_workers() -> None:
    config = ExperimentConfig(
        app_filter="hotel_reservation",
        deploy_from_source=True,
        application_workspace="ephemeral",
        parallel=2,
    )

    assert config.parallel == 2


def test_serialize_includes_spec_names(tmp_path: Path) -> None:
    config = ExperimentConfig(spec_names=["service_dns_resolution_failure"])
    serialized = _serialize_config(config)
    assert 'spec_names = ["service_dns_resolution_failure"]' in serialized


def test_serialize_omits_spec_names_when_empty() -> None:
    config = ExperimentConfig()
    serialized = _serialize_config(config)
    assert "spec_names" not in serialized


def test_roundtrip_spec_names(tmp_path: Path) -> None:
    config = ExperimentConfig(spec_names=["service_dns_resolution_failure", "wrong_dns_policy"])
    toml_path = tmp_path / "snap.toml"
    toml_path.write_text(_serialize_config(config))
    loaded = load_experiment_config(toml_path)
    assert loaded.spec_names == config.spec_names


def test_reuse_cluster_defaults_to_false() -> None:
    config = ExperimentConfig()
    assert config.env.reuse_cluster is False
    assert config.env.force_recreate_cluster is False
    assert config.env.preserve_infrastructure is False
    assert config.env.submit_done_returns_feedback is False


def test_reuse_cluster_loaded_from_toml(tmp_path: Path) -> None:
    toml = _write_toml(
        tmp_path,
        """
        [runner]
        agent = "sdo_codex"

        [runner.variants]
        enabled = false

        [runner.env]
        reuse_cluster = true
        force_recreate_cluster = false
        preserve_infrastructure = true
        submit_done_returns_feedback = true
    """,
    )
    config = load_experiment_config(toml)
    assert config.env.reuse_cluster is True
    assert config.env.force_recreate_cluster is False
    assert config.env.preserve_infrastructure is True
    assert config.env.submit_done_returns_feedback is True


def test_reuse_cluster_env_override_enables() -> None:
    config = ExperimentConfig(env=RunnerEnv(reuse_cluster=False))
    resolved = resolve_config(config, env_overrides={"SREGYM_REUSE_CLUSTER": "1"})
    assert resolved.env.reuse_cluster is True


def test_reuse_cluster_env_override_disables() -> None:
    config = ExperimentConfig(env=RunnerEnv(reuse_cluster=True))
    resolved = resolve_config(config, env_overrides={"SREGYM_REUSE_CLUSTER": "false"})
    assert resolved.env.reuse_cluster is False


def test_force_recreate_env_override() -> None:
    config = ExperimentConfig(env=RunnerEnv(reuse_cluster=True))
    resolved = resolve_config(config, env_overrides={"SREGYM_FORCE_RECREATE_CLUSTER": "yes"})
    assert resolved.env.reuse_cluster is True
    assert resolved.env.force_recreate_cluster is True


def test_submit_done_feedback_env_override() -> None:
    config = ExperimentConfig(env=RunnerEnv(submit_done_returns_feedback=False))
    resolved = resolve_config(config, env_overrides={"SREGYM_SUBMIT_DONE_RETURNS_FEEDBACK": "1"})
    assert resolved.env.submit_done_returns_feedback is True


def test_cleanup_timeout_process_env_overrides_stale_persisted_config(tmp_path: Path) -> None:
    stale = ExperimentConfig(env=RunnerEnv(cleanup_defer_timeout_seconds=600))

    resolved = resolve_config(stale, env_overrides={"SREGYM_CLEANUP_DEFER_TIMEOUT_SECONDS": "1800"})

    assert resolved.env.cleanup_defer_timeout_seconds == 1800
    assert config_to_env(resolved, project_root=tmp_path)["SREGYM_CLEANUP_DEFER_TIMEOUT_SECONDS"] == "1800"


def test_deferred_diagnosis_grading_is_opt_in_and_reaches_the_conductor(tmp_path: Path) -> None:
    assert ExperimentConfig().env.defer_diagnosis_grading is False
    assert "SREGYM_DEFER_DIAGNOSIS_GRADING" not in config_to_env(ExperimentConfig(), project_root=tmp_path)

    toml = _write_toml(
        tmp_path,
        """
        [runner]
        agent = "sdo-codex"

        [runner.env]
        defer_diagnosis_grading = true
    """,
    )
    config = load_experiment_config(toml)

    assert config.env.defer_diagnosis_grading is True
    assert config_to_env(config, project_root=tmp_path)["SREGYM_DEFER_DIAGNOSIS_GRADING"] == "1"
    snapshot = tmp_path / "snapshot.toml"
    snapshot.write_text(_serialize_config(config))
    assert load_experiment_config(snapshot).env.defer_diagnosis_grading is True
    disabled = resolve_config(config, env_overrides={"SREGYM_DEFER_DIAGNOSIS_GRADING": "0"})
    assert disabled.env.defer_diagnosis_grading is False


def test_fast_namespace_teardown_is_opt_in_and_reaches_the_conductor(tmp_path: Path) -> None:
    assert "SREGYM_FAST_NAMESPACE_TEARDOWN" not in config_to_env(ExperimentConfig(), project_root=tmp_path)
    toml = _write_toml(
        tmp_path,
        """
        [runner.env]
        fast_namespace_teardown = true
    """,
    )
    config = load_experiment_config(toml)

    assert config_to_env(config, project_root=tmp_path)["SREGYM_FAST_NAMESPACE_TEARDOWN"] == "1"
    snapshot = tmp_path / "snapshot.toml"
    snapshot.write_text(_serialize_config(config))
    assert load_experiment_config(snapshot).env.fast_namespace_teardown is True
    disabled = resolve_config(config, env_overrides={"SREGYM_FAST_NAMESPACE_TEARDOWN": "0"})
    assert disabled.env.fast_namespace_teardown is False


def test_source_build_cache_is_opt_in_and_reaches_the_conductor(tmp_path: Path) -> None:
    assert "SREGYM_SOURCE_BUILD_CACHE" not in config_to_env(ExperimentConfig(), project_root=tmp_path)
    toml = _write_toml(
        tmp_path,
        """
        [runner.env]
        source_build_cache = true
    """,
    )
    config = load_experiment_config(toml)

    assert config_to_env(config, project_root=tmp_path)["SREGYM_SOURCE_BUILD_CACHE"] == "1"
    snapshot = tmp_path / "snapshot.toml"
    snapshot.write_text(_serialize_config(config))
    assert load_experiment_config(snapshot).env.source_build_cache is True
    disabled = resolve_config(config, env_overrides={"SREGYM_SOURCE_BUILD_CACHE": "0"})
    assert disabled.env.source_build_cache is False


def test_deploy_from_source_env_override_enables() -> None:
    config = ExperimentConfig(deploy_from_source=False)
    resolved = resolve_config(config, env_overrides={"SREGYM_DEPLOY_FROM_SOURCE": "1"})
    assert resolved.deploy_from_source is True


def test_deploy_from_source_env_override_disables() -> None:
    config = ExperimentConfig(deploy_from_source=True)
    resolved = resolve_config(config, env_overrides={"SREGYM_DEPLOY_FROM_SOURCE": "false"})
    assert resolved.deploy_from_source is False


def test_config_to_env_emits_reuse_flags(tmp_path: Path) -> None:
    config = ExperimentConfig(env=RunnerEnv(reuse_cluster=True, force_recreate_cluster=True))
    env = config_to_env(config, project_root=tmp_path)
    assert env["SREGYM_REUSE_CLUSTER"] == "1"
    assert env["SREGYM_FORCE_RECREATE_CLUSTER"] == "1"
    assert env["SREGYM_SUBMIT_DONE_RETURNS_FEEDBACK"] == "0"


def test_config_to_env_emits_submit_done_feedback_flag_when_enabled(tmp_path: Path) -> None:
    config = ExperimentConfig(env=RunnerEnv(submit_done_returns_feedback=True))
    env = config_to_env(config, project_root=tmp_path)
    assert env["SREGYM_SUBMIT_DONE_RETURNS_FEEDBACK"] == "1"


def test_config_to_env_emits_cleanup_deferral_timeout(tmp_path: Path) -> None:
    config = ExperimentConfig(env=RunnerEnv(cleanup_defer_timeout_seconds=1800))

    env = config_to_env(config, project_root=tmp_path)

    assert env["SREGYM_CLEANUP_DEFER_TIMEOUT_SECONDS"] == "1800"


def test_config_to_env_emits_source_docker_builder(tmp_path: Path) -> None:
    config = ExperimentConfig(env=RunnerEnv(docker_builder="sdo-example"))

    env = config_to_env(config, tmp_path)

    assert env["SREGYM_DOCKER_BUILDER"] == "sdo-example"


def test_config_to_env_omits_reuse_flags_when_false(tmp_path: Path) -> None:
    config = ExperimentConfig()
    env = config_to_env(config, project_root=tmp_path)
    assert "SREGYM_REUSE_CLUSTER" not in env
    assert "SREGYM_FORCE_RECREATE_CLUSTER" not in env


def test_config_to_env_serializes_effective_agent_config(tmp_path: Path) -> None:
    config = ExperimentConfig(
        agent="sdo_codex",
        agent_config={
            "sdo_codex": {
                "controller_image": "controller:v1",
                "persistent_controller": True,
            }
        },
        env=RunnerEnv(judge_model_id="judge-model"),
    )
    env = config_to_env(config, project_root=tmp_path, exp_dir=tmp_path / "exp")
    decoded = json.loads(env["SREGYM_EXPERIMENT_AGENT_CONFIG"])
    assert decoded["controller_image"] == "controller:v1"
    assert decoded["persistent_controller"] is True
    assert "judge_model_id" not in decoded
    assert env["JUDGE_MODEL_ID"] == "judge-model"
    assert env["SREGYM_EXPERIMENT_DIR"] == str(tmp_path / "exp")


def test_roundtrip_reuse_cluster(tmp_path: Path) -> None:
    config = ExperimentConfig(env=RunnerEnv(reuse_cluster=True))
    toml_path = tmp_path / "snap.toml"
    toml_path.write_text(_serialize_config(config))
    loaded = load_experiment_config(toml_path)
    assert loaded.env.reuse_cluster is True
    assert loaded.env.force_recreate_cluster is False


def test_roundtrip_submit_done_returns_feedback(tmp_path: Path) -> None:
    config = ExperimentConfig(env=RunnerEnv(submit_done_returns_feedback=True))
    toml_path = tmp_path / "snap.toml"
    toml_path.write_text(_serialize_config(config))
    loaded = load_experiment_config(toml_path)
    assert loaded.env.submit_done_returns_feedback is True


def test_inline_problems_keep_order_and_duplicates(tmp_path: Path) -> None:
    """Repeated inline problems each become a run in SREGym's tasklist."""
    path = _write_toml(
        tmp_path,
        """\
        [runner]
        problems = ["a", "b", "a", "b"]
        """,
    )
    config = load_experiment_config(path)

    tasklist = resolve_tasklist(config, tmp_path, tmp_path)

    assert tasklist is not None
    document = yaml.safe_load(tasklist.read_text())
    # SREGym's parallel runner accepts all.problems as a list of strings and
    # expands it in order; a mapping would collapse the repeated keys.
    assert document == {"all": {"problems": ["a", "b", "a", "b"]}}


def test_repeat_maps_to_sregym_n_attempts(tmp_path: Path) -> None:
    """SREGym main.py has no --repeat flag; independent attempts are --n-attempts."""
    config = ExperimentConfig(problems=["a"], repeat=5)

    args = config_to_main_args(config, tmp_path, None)

    assert "--repeat" not in args
    assert args[args.index("--n-attempts") + 1] == "5"


def test_default_repeat_emits_no_attempt_flag(tmp_path: Path) -> None:
    args = config_to_main_args(ExperimentConfig(problems=["a"]), tmp_path, None)

    assert "--n-attempts" not in args
    assert "--repeat" not in args


@pytest.mark.parametrize("repeat", [0, -1])
def test_repeat_must_be_positive(repeat: int) -> None:
    with pytest.raises(ValueError, match="repeat"):
        ExperimentConfig(problems=["a"], repeat=repeat)


def test_kind_worker_nodes_is_opt_in_and_reaches_the_cluster_bootstrap(tmp_path: Path) -> None:
    assert ExperimentConfig().env.kind_worker_nodes == 0
    assert "SREGYM_KIND_WORKER_NODES" not in config_to_env(ExperimentConfig(), project_root=tmp_path)
    toml = _write_toml(
        tmp_path,
        """
        [runner.env]
        kind_worker_nodes = 1
    """,
    )
    config = load_experiment_config(toml)

    assert config_to_env(config, project_root=tmp_path)["SREGYM_KIND_WORKER_NODES"] == "1"
    snapshot = tmp_path / "snapshot.toml"
    snapshot.write_text(_serialize_config(config))
    assert load_experiment_config(snapshot).env.kind_worker_nodes == 1
    assert resolve_config(config, env_overrides={"SREGYM_KIND_WORKER_NODES": "2"}).env.kind_worker_nodes == 2


def test_pipeline_stages_inherit_the_kind_worker_nodes() -> None:
    from benchmarks.sregym.runner.pipeline import merge_stage_config

    config = merge_stage_config({"env": {"kind_worker_nodes": 1}}, {"problems": ["a"]})

    assert config.env.kind_worker_nodes == 1


def test_kind_worker_nodes_must_not_be_negative() -> None:
    with pytest.raises(ValueError, match="kind_worker_nodes"):
        RunnerEnv(kind_worker_nodes=-1)


def test_submit_done_returns_feedback_stays_unread_outside_its_own_plumbing() -> None:
    """Audit finding #17: ``SREGYM_SUBMIT_DONE_RETURNS_FEEDBACK`` is written into the
    subprocess env by ``config_to_env`` (and round-tripped by ``resolve_config`` and TOML
    load/serialize), but no adapter, protocol, SDO, or controller code reads it back — the
    config field promises a "submit done returns feedback" knob that has no effect on
    anything. This is dead plumbing, tracked rather than silently expanded: it fails if a
    consumer starts reading the variable (or the field) without updating this test to say
    the gap was closed, and fails if the name drifts between the env var and the field.
    """
    root = Path(__file__).resolve().parents[5]
    needles = ("SREGYM_SUBMIT_DONE_RETURNS_FEEDBACK", "submit_done_returns_feedback")
    consumer_dirs = (
        root / "benchmarks" / "sregym" / "adapter",
        root / "benchmarks" / "sregym" / "protocol",
        root / "benchmarks" / "sregym" / "fastloop",
        root / "sdo",
        root / "controller",
        root / "libs",
    )
    readers = [
        path
        for directory in consumer_dirs
        for path in directory.rglob("*.py")
        if any(needle in path.read_text(encoding="utf-8") for needle in needles)
    ]
    assert readers == [], f"{needles} is meant to be dead outside runner/experiment.py and pipeline.py: {readers}"

    # The only two places that may legitimately name it: the field definition/plumbing in
    # experiment.py, and pipeline.py's RunnerEnv construction from a stage's [env] table.
    owners = (
        root / "benchmarks" / "sregym" / "runner" / "experiment.py",
        root / "benchmarks" / "sregym" / "runner" / "pipeline.py",
    )
    for owner in owners:
        assert any(needle in owner.read_text(encoding="utf-8") for needle in needles), owner


@pytest.mark.parametrize(("table", "key"), [("runner", "enable_summary"), ("runner.env", "crucible_seed_kb_dir")])
def test_experiment_rejects_keys_of_the_removed_crucible_agent(tmp_path: Path, table: str, key: str) -> None:
    path = tmp_path / "exp.toml"
    path.write_text(f'[{table}]\n{key} = "x"\n', encoding="utf-8")

    with pytest.raises(ValueError, match=key):
        load_experiment_config(path)
