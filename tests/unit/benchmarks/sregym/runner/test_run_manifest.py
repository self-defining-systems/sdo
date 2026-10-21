"""Every run directory carries a manifest of exactly what produced it."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest

from benchmarks.sregym.runner import runner as runner_mod
from benchmarks.sregym.runner.codex_baseline import CONCISE_VERIFY_PROMPT, EXEC_DISCLOSURE_PROMPT, FULL_VERIFY_PROMPT
from benchmarks.sregym.runner.experiment import ExperimentConfig, RunnerEnv, write_snapshot
from benchmarks.sregym.runner.manifest import MANIFEST_NAME, build_run_manifest, read_run_manifest, write_run_manifest
from benchmarks.sregym.runner.pipeline import PipelineConfig, StageConfig
from benchmarks.sregym.runner.preflight import LaunchAssurance, run_preflight

if TYPE_CHECKING:
    from sregym_fake_host import FakeHost

REPO_ROOT = Path(__file__).resolve().parents[5]


def _config() -> ExperimentConfig:
    return ExperimentConfig(
        agent="sdo_codex",
        model="gpt-6-luna",
        reasoning_effort="medium",
        parallel=1,
        env=RunnerEnv(judge_model_id="codex-gpt-6-luna", kind_worker_nodes=1),
        agent_config={"sdo_codex": {"provider": "codex", "model": "gpt-6-luna"}},
    )


@pytest.fixture
def sregym_dir(tmp_path: Path) -> Path:
    directory = tmp_path / "third_party" / "sregym"
    (directory / "logs").mkdir(parents=True)
    (directory / "main.py").write_text("# fake", encoding="utf-8")
    (directory / "agents.yaml").write_text("agents:\n  - name: codex\n    agent_version: '0.163.0'\n", encoding="utf-8")
    return directory


def _git(fake_host: FakeHost, *, dirty: str = "", submodule_dirty: str = "") -> None:
    fake_host.git_answers = {
        ("rev-parse", "HEAD"): "a" * 40,
        ("status", "--porcelain", "--untracked-files=no"): dirty,
        ("ls-tree", "HEAD", "third_party/sregym"): f"160000 commit {'c' * 40}\tthird_party/sregym",
    }


def test_manifest_records_provenance_versions_models_topology_config_and_host(
    fake_host: FakeHost, sregym_dir: Path, tmp_path: Path
) -> None:
    _git(fake_host, dirty=" M benchmarks/sregym/runner/runner.py")
    fake_host.clusters = ["luna-w0"]
    fake_host.nodes = {"luna-w0": ["luna-w0-control-plane", "luna-w0-worker"]}
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    snapshot = write_snapshot(_config(), run_dir)
    env = {"SREGYM_KIND_CLUSTER_PREFIX": "luna-w"}
    report = run_preflight([_config()], project_root=REPO_ROOT, sregym_dir=sregym_dir, env=env, host=fake_host)

    manifest = build_run_manifest(
        run_dir=run_dir,
        configs=[_config()],
        snapshot=snapshot,
        report=report,
        host=fake_host,
        project_root=REPO_ROOT,
        sregym_dir=sregym_dir,
        env=env,
    )

    assert manifest["git"]["sha"] == "a" * 40
    assert manifest["git"]["dirty"] is True
    assert manifest["git"]["submodule"]["recorded_sha"] == "c" * 40
    assert manifest["versions"]["codex_cli"]["pin"] == "0.163.0"
    assert manifest["versions"]["agentshim"]["host"] == "0.7.0"
    responder = manifest["images"]["responder_image:sdo-sregym-responder:v0.1.0"]
    assert responder["id"].startswith("sha256:")
    assert responder["agentshim"] == "0.7.0"
    assert manifest["models"][0]["responder"] == {
        "provider": "codex",
        "model": "gpt-6-luna",
        "effort": "medium",
        "source": "agent.sdo_codex",
    }
    assert manifest["judge"] == {"model": "codex-gpt-6-luna", "effort": "xhigh"}
    assert manifest["kind_topology"] == {
        "clusters": ["luna-w0"],
        "worker_nodes": 1,
        "nodes": {"luna-w0": ["luna-w0-control-plane", "luna-w0-worker"]},
    }
    assert manifest["config"]["sha256"] == hashlib.sha256(snapshot.read_bytes()).hexdigest()
    assert manifest["host"]["load_average"] == [1.0, 2.0, 3.0]
    assert manifest["host"]["disk_free_bytes"]["logs"] > 0
    assert manifest["preflight"]["ok"] is True


def _codex_config(agent_config: dict | None = None) -> ExperimentConfig:
    return ExperimentConfig(
        agent="codex",
        model="gpt-6-luna",
        reasoning_effort="medium",
        parallel=1,
        env=RunnerEnv(judge_model_id="codex-gpt-6-luna", kind_worker_nodes=1),
        agent_config=agent_config or {},
    )


def test_manifest_records_whether_the_exec_disclosure_was_on(
    fake_host: FakeHost, sregym_dir: Path, tmp_path: Path
) -> None:
    _git(fake_host)
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    config = _codex_config({"codex": {"verify_protocol": "concise", "exec_disclosure": True}})
    snapshot = write_snapshot(config, run_dir)
    env = {"SREGYM_KIND_CLUSTER_PREFIX": "luna-w"}
    report = run_preflight([config], project_root=REPO_ROOT, sregym_dir=sregym_dir, env=env, host=fake_host)

    manifest = build_run_manifest(
        run_dir=run_dir,
        configs=[config],
        snapshot=snapshot,
        report=report,
        host=fake_host,
        project_root=REPO_ROOT,
        sregym_dir=sregym_dir,
        env=env,
    )

    (entry,) = manifest["codex_prompt_appendix"]
    assert entry["mode"] == "concise"
    assert entry["exec_disclosure"] is True
    expected = CONCISE_VERIFY_PROMPT + "\n" + EXEC_DISCLOSURE_PROMPT
    assert entry["sha256"] == hashlib.sha256(expected.encode("utf-8")).hexdigest()


def test_manifest_records_whether_exec_through_the_proxy_was_on(
    fake_host: FakeHost, sregym_dir: Path, tmp_path: Path
) -> None:
    _git(fake_host)
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    config = _codex_config({"codex": {"verify_protocol": "concise", "allow_exec": True}})
    snapshot = write_snapshot(config, run_dir)
    env = {"SREGYM_KIND_CLUSTER_PREFIX": "luna-w"}
    report = run_preflight([config], project_root=REPO_ROOT, sregym_dir=sregym_dir, env=env, host=fake_host)

    manifest = build_run_manifest(
        run_dir=run_dir,
        configs=[config],
        snapshot=snapshot,
        report=report,
        host=fake_host,
        project_root=REPO_ROOT,
        sregym_dir=sregym_dir,
        env=env,
    )

    (entry,) = manifest["codex_prompt_appendix"]
    assert entry["allow_exec"] is True


@pytest.mark.parametrize(
    ("agent_config", "expected_mode", "expected_text"),
    [
        ({}, "concise", CONCISE_VERIFY_PROMPT),
        ({"codex": {"verify_protocol": "full"}}, "full", FULL_VERIFY_PROMPT),
        ({"codex": {"verify_protocol": True}}, "full", FULL_VERIFY_PROMPT),
        ({"codex": {"verify_protocol": "none"}}, "none", ""),
    ],
)
def test_manifest_records_the_codex_verify_protocol_mode_and_a_hash_of_its_text(
    fake_host: FakeHost,
    sregym_dir: Path,
    tmp_path: Path,
    agent_config: dict,
    expected_mode: str,
    expected_text: str,
) -> None:
    _git(fake_host)
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    config = _codex_config(agent_config)
    snapshot = write_snapshot(config, run_dir)
    env = {"SREGYM_KIND_CLUSTER_PREFIX": "luna-w"}
    report = run_preflight([config], project_root=REPO_ROOT, sregym_dir=sregym_dir, env=env, host=fake_host)

    manifest = build_run_manifest(
        run_dir=run_dir,
        configs=[config],
        snapshot=snapshot,
        report=report,
        host=fake_host,
        project_root=REPO_ROOT,
        sregym_dir=sregym_dir,
        env=env,
    )

    (entry,) = manifest["codex_prompt_appendix"]
    assert entry["mode"] == expected_mode
    if expected_text:
        assert entry["sha256"] == hashlib.sha256(expected_text.encode("utf-8")).hexdigest()
    else:
        assert entry["sha256"] is None


def test_manifest_records_no_codex_prompt_appendix_for_a_non_codex_agent(
    fake_host: FakeHost, sregym_dir: Path, tmp_path: Path
) -> None:
    _git(fake_host)
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    snapshot = write_snapshot(_config(), run_dir)
    env = {"SREGYM_KIND_CLUSTER_PREFIX": "luna-w"}
    report = run_preflight([_config()], project_root=REPO_ROOT, sregym_dir=sregym_dir, env=env, host=fake_host)

    manifest = build_run_manifest(
        run_dir=run_dir,
        configs=[_config()],
        snapshot=snapshot,
        report=report,
        host=fake_host,
        project_root=REPO_ROOT,
        sregym_dir=sregym_dir,
        env=env,
    )

    assert manifest["codex_prompt_appendix"] is None


def test_a_resumed_run_keeps_its_first_manifest_and_appends_the_resume(tmp_path: Path) -> None:
    first = {"schema_version": 1, "git": {"sha": "a"}}
    second = {"schema_version": 1, "git": {"sha": "b"}}

    write_run_manifest(tmp_path, first)
    path = write_run_manifest(tmp_path, second)

    document = json.loads(path.read_text(encoding="utf-8"))
    assert path.name == MANIFEST_NAME
    assert document["git"]["sha"] == "a"
    assert [resume["git"]["sha"] for resume in document["resumes"]] == ["b"]
    assert read_run_manifest(tmp_path) == document


def test_a_launched_pipeline_writes_a_manifest_into_the_pipeline_and_every_stage(
    fake_host: FakeHost, sregym_dir: Path
) -> None:
    config = PipelineConfig(
        name="luna",
        defaults={"agent": "codex", "model": "gpt-6-luna", "reasoning_effort": "medium", "parallel": 1},
        stages=[StageConfig(name="one"), StageConfig(name="two")],
    )

    def launched(argv: list[str], cwd: object = None, env: object = None) -> object:
        return type("Result", (), {"returncode": 1})()

    with patch("subprocess.run", side_effect=launched):
        runner_mod.run_pipeline(
            config,
            project_root=REPO_ROOT,
            sregym_dir=sregym_dir,
            assurance=LaunchAssurance(host=fake_host, mode="warn"),
        )

    (pipeline_dir,) = (sregym_dir / "logs").iterdir()
    pipeline = read_run_manifest(pipeline_dir)
    stage = read_run_manifest(pipeline_dir / "stage_0_one")
    assert pipeline is not None
    assert stage is not None
    assert pipeline["kind"] == "pipeline"
    assert stage["kind"] == "stage"
    assert (
        stage["preflight"]["waived"] is True
    )  # the stub config names no judge, so preflight fails and warn mode waives it
    assert stage["config"]["snapshot"] == "experiment_config.toml"


def test_a_launched_experiment_writes_its_manifest_before_exec(
    fake_host: FakeHost, sregym_dir: Path, tmp_path: Path
) -> None:
    toml = tmp_path / "luna.toml"
    toml.write_text(
        '[runner]\nagent = "codex"\nmodel = "gpt-6-luna"\nreasoning_effort = "medium"\nparallel = 1\n'
        'problems = ["p"]\n[runner.env]\njudge_model_id = "codex-gpt-6-luna"\n',
        encoding="utf-8",
    )

    with patch("os.execvpe") as launched, patch("os.chdir"):
        runner_mod.run_single_experiment(
            toml, [], project_root=REPO_ROOT, sregym_dir=sregym_dir, assurance=LaunchAssurance(host=fake_host)
        )

    launched.assert_called_once()
    (exp_dir,) = (sregym_dir / "logs").iterdir()
    manifest = read_run_manifest(exp_dir)
    assert manifest is not None
    assert manifest["kind"] == "experiment"
    assert manifest["config"]["source"] == str(toml)
    assert manifest["config"]["source_sha256"] == hashlib.sha256(toml.read_bytes()).hexdigest()
    assert manifest["models"][0]["baseline_agent"]["model"] == "gpt-6-luna"
