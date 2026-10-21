"""The launch preflight refuses an experiment whose numbers could not be trusted.

Each test breaks one thing on an otherwise healthy fake host and checks that
the preflight names it with a remedy, before anything launches.
"""

from __future__ import annotations

import dataclasses
import inspect
import json
import subprocess
import time
from pathlib import Path

import pytest
import yaml
from sregym_fake_host import CODEX_PIN, FakeHost

from benchmarks.sregym.runner.experiment import ExperimentConfig, RunnerEnv, load_experiment_config
from benchmarks.sregym.runner.preflight import (
    GB,
    ImageVersions,
    LaunchAssurance,
    ModelPolicy,
    PreflightError,
    PreflightSettings,
    SystemHost,
    flock_owner,
    lane_clusters,
    load_arm_configs,
    parse_codex_version,
    read_quota_snapshot,
    run_preflight,
)
from sdo.agents.lifecycle.agents import CodexLifecycleBackend
from sdo.agents.responder import INCIDENT_REASONING_EFFORT

REPO_ROOT = Path(__file__).resolve().parents[5]
EXPERIMENTS = REPO_ROOT / "benchmarks" / "sregym" / "experiments"
NOW = 1_790_000_000.0


def _luna(agent: str = "sdo_codex", **overrides: object) -> ExperimentConfig:
    env = RunnerEnv(judge_model_id="codex-gpt-6-luna", worker_cpu_limit="3", kind_worker_nodes=1, reuse_cluster=True)
    agent_config = {"sdo_codex": {"provider": "codex", "model": "gpt-6-luna"}} if agent == "sdo_codex" else {}
    config = ExperimentConfig(
        agent=agent,
        model="gpt-6-luna",
        reasoning_effort="medium",
        parallel=1,
        agent_timeout=3600,
        app_filter="hotel_reservation",
        deploy_from_source=True,
        env=env,
        agent_config=agent_config,
    )
    return dataclasses.replace(config, **overrides)  # type: ignore[arg-type]


@pytest.fixture
def sregym_dir(tmp_path: Path) -> Path:
    directory = tmp_path / "sregym"
    (directory / "logs").mkdir(parents=True)
    (directory / "SREGym-applications" / "hotelReservation" / "helm-chart").mkdir(parents=True)
    _write_agents_yaml(directory, CODEX_PIN)
    return directory


def _write_agents_yaml(sregym_dir: Path, version: str | None) -> None:
    agents = {"agents": [{"name": "codex", "install_script": "install-codex.sh", "agent_version": version}]}
    (sregym_dir / "agents.yaml").write_text(yaml.safe_dump(agents), encoding="utf-8")


def _write_rollout(home: Path, *, used: float, observed: float, resets: float) -> None:
    sessions = home / ".codex" / "sessions" / "2026" / "09" / "28"
    sessions.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(observed))
    event = {
        "timestamp": stamp,
        "type": "event_msg",
        "payload": {
            "type": "token_count",
            "rate_limits": {"primary": {"used_percent": used, "window_minutes": 10080, "resets_at": resets}},
        },
    }
    (sessions / "rollout-2026-09-28T07-48-49-x.jsonl").write_text(json.dumps(event) + "\n", encoding="utf-8")


def _preflight(configs: list[ExperimentConfig], host: FakeHost, sregym_dir: Path, **env: str):
    if not (host.home / ".codex").exists():
        _write_rollout(host.home, used=40, observed=NOW - 60, resets=NOW + 86_400)
    return run_preflight(configs, project_root=REPO_ROOT, sregym_dir=sregym_dir, env=env, host=host, now=NOW)


def _check(report, name: str):
    return next(check for check in report.checks if check.name == name)


def test_luna_arms_on_a_healthy_host_pass_and_resolve_every_role(fake_host: FakeHost, sregym_dir: Path) -> None:
    report = _preflight([_luna("sdo_codex"), _luna("codex")], fake_host, sregym_dir)

    assert report.ok, [check for check in report.checks if check.status != "pass"]
    sdo_roles, codex_roles = report.facts["roles"]
    assert {role: (r["provider"], r["model"]) for role, r in sdo_roles.items()} == {
        "sregym_judge": ("codex", "codex-gpt-6-luna"),
        "responder": ("codex", "gpt-6-luna"),
        "reflection": ("codex", "gpt-6-luna"),
        "lifecycle_deployer": ("codex", "gpt-6-luna"),
        "health_judge": ("codex", "gpt-6-luna"),
    }
    assert sdo_roles["sregym_judge"]["effort"] == "xhigh"
    assert codex_roles["baseline_agent"]["model"] == "gpt-6-luna"
    assert {"sdo-controller:v0.1.0", "sdo-sregym-responder:v0.1.0", "sdo-detector-validator:v0.1.0"} <= set(
        fake_host.probed
    )


def _empty_applications(sregym_dir: Path) -> Path:
    applications = sregym_dir / "SREGym-applications"
    for child in sorted(applications.rglob("*"), reverse=True):
        child.rmdir()
    return applications


def test_an_uninitialized_applications_submodule_aborts_a_source_deploy(fake_host: FakeHost, sregym_dir: Path) -> None:
    """A fresh worktree whose nested submodules were never initialized has an empty
    ``SREGym-applications``; every source deploy then fails on a missing Helm chart."""

    applications = _empty_applications(sregym_dir)

    report = _preflight([_luna()], fake_host, sregym_dir)

    check = _check(report, "application-sources")
    assert check.status == "fail"
    assert str(applications) in check.detail
    assert "--recursive" in check.remedy
    assert not report.ok


def test_application_sources_are_not_checked_when_no_arm_deploys_from_source(
    fake_host: FakeHost, sregym_dir: Path
) -> None:
    _empty_applications(sregym_dir)

    report = _preflight([_luna("codex", deploy_from_source=False)], fake_host, sregym_dir)

    assert all(check.name != "application-sources" for check in report.checks)


def test_less_than_100_gb_free_on_the_logs_disk_aborts_with_the_path(fake_host: FakeHost, sregym_dir: Path) -> None:
    fake_host.free = {str(sregym_dir / "logs"): 40 * GB}

    report = _preflight([_luna()], fake_host, sregym_dir)

    check = _check(report, "disk")
    assert check.status == "fail"
    assert str(sregym_dir / "logs") in check.detail
    assert "100 GB" in check.detail
    with pytest.raises(PreflightError, match=r"\[disk\].*40 GB free"):
        LaunchAssurance(host=fake_host).preflight([_luna()], project_root=REPO_ROOT, sregym_dir=sregym_dir, env={})


def test_a_full_docker_data_disk_also_aborts(fake_host: FakeHost, sregym_dir: Path) -> None:
    fake_host.docker_root_dir = Path("/var/lib/docker")
    fake_host.free = {"/var/lib/docker": 5 * GB}

    assert _check(_preflight([_luna()], fake_host, sregym_dir), "disk").status == "fail"


@pytest.mark.parametrize("version", [None, "latest"])
def test_a_floating_stock_codex_cli_aborts(fake_host: FakeHost, sregym_dir: Path, version: str | None) -> None:
    _write_agents_yaml(sregym_dir, version)

    check = _check(_preflight([_luna("codex")], fake_host, sregym_dir), "codex-cli-pins")

    assert check.status == "fail"
    assert "floating" in check.detail
    assert f"agent_version: {CODEX_PIN}" in check.remedy


def test_a_pin_whose_platform_package_404s_aborts(fake_host: FakeHost, sregym_dir: Path) -> None:
    fake_host.npm_missing = {f"@openai/codex@{CODEX_PIN}-linux-x64"}

    check = _check(_preflight([_luna("codex")], fake_host, sregym_dir), "codex-cli-pins")

    assert check.status == "fail"
    assert "404" in check.detail


def test_npm_unreachable_records_the_pin_as_unverified_without_aborting(fake_host: FakeHost, sregym_dir: Path) -> None:
    fake_host.npm_offline = True

    report = _preflight([_luna("codex")], fake_host, sregym_dir)

    assert _check(report, "codex-cli-pins").status == "unknown"
    assert report.ok


def test_a_host_judge_cli_off_the_pin_aborts(fake_host: FakeHost, sregym_dir: Path) -> None:
    fake_host.codex_version = "0.158.0"

    assert _check(_preflight([_luna()], fake_host, sregym_dir), "codex-cli-pins").status == "fail"


def test_an_image_with_a_stale_agentshim_aborts(fake_host: FakeHost, sregym_dir: Path) -> None:
    fake_host.versions["sdo-sregym-responder:v0.1.0"] = ImageVersions(
        codex=CODEX_PIN, agentshim="0.6.8", import_error="libs.agent_cli: ImportError: cannot import name 'usage'"
    )

    check = _check(_preflight([_luna()], fake_host, sregym_dir), "sdo-images")

    assert check.status == "fail"
    assert "agentshim '0.6.8'" in check.detail
    assert "ImportError" in check.detail
    assert "build_sdo_images.sh" in check.remedy


def test_go_only_images_probe_the_native_binary_without_python_or_codex() -> None:
    commands: list[list[str]] = []

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        return subprocess.CompletedProcess(command, 0, "sdo.dev/v1alpha1+sdk.test\n", "")

    versions = SystemHost(run=run).image_versions("sdo-controller:test", ())

    assert versions is not None
    assert versions.import_error is None
    assert len(commands) == 1
    assert commands[0] == [
        "docker",
        "run",
        "--rm",
        "--workdir",
        "/opt/sdo/sdo/operator",
        "--entrypoint",
        "/usr/local/bin/sdo-operator",
        "sdo-controller:test",
        "schema-identity",
    ]


def test_a_missing_sdo_image_aborts(fake_host: FakeHost, sregym_dir: Path) -> None:
    fake_host.missing_images = {"sdo-controller:v0.1.0"}

    check = _check(_preflight([_luna()], fake_host, sregym_dir), "sdo-images")

    assert check.status == "fail"
    assert "sdo-controller:v0.1.0 is not present" in check.detail


def test_host_agentshim_off_the_lockfile_aborts(fake_host: FakeHost, sregym_dir: Path) -> None:
    fake_host.packages = {"agentshim": "0.6.8"}

    assert _check(_preflight([_luna()], fake_host, sregym_dir), "agentshim").status == "fail"


def _stable_kubeconfig(home: Path, cluster: str, *, context: str, port: int) -> Path:
    path = home / ".cache" / "sregym" / "kubeconfigs" / f"{cluster}.kubeconfig"
    path.parent.mkdir(parents=True, exist_ok=True)
    document = {
        "clusters": [{"name": context, "cluster": {"server": f"https://127.0.0.1:{port}"}}],
        "contexts": [{"name": context, "context": {"cluster": context, "user": context}}],
        "current-context": context,
    }
    path.write_text(yaml.safe_dump(document), encoding="utf-8")
    return path


def test_lane_clusters_follow_prefix_offset_and_parallelism() -> None:
    config = _luna(parallel=2)

    env = {"SREGYM_KIND_CLUSTER_PREFIX": "luna-w", "SREGYM_WORKER_ID_OFFSET": "3"}
    assert lane_clusters(config, env) == ["luna-w3", "luna-w4"]
    assert lane_clusters(config, {}) == ["sregym-w0", "sregym-w1"]


def test_a_stable_kubeconfig_reaching_another_cluster_aborts(fake_host: FakeHost, sregym_dir: Path) -> None:
    _stable_kubeconfig(fake_host.home, "luna-w0", context="kind-luna-w1", port=40001)

    check = _check(_preflight([_luna()], fake_host, sregym_dir, SREGYM_KIND_CLUSTER_PREFIX="luna-w"), "lane-isolation")

    assert check.status == "fail"
    assert "kind-luna-w1" in check.detail


def test_a_stale_stable_kubeconfig_port_aborts(fake_host: FakeHost, sregym_dir: Path) -> None:
    _stable_kubeconfig(fake_host.home, "luna-w0", context="kind-luna-w0", port=40001)
    fake_host.clusters = ["luna-w0"]
    fake_host.ports = {"luna-w0": 40999}

    check = _check(_preflight([_luna()], fake_host, sregym_dir, SREGYM_KIND_CLUSTER_PREFIX="luna-w"), "lane-isolation")

    assert check.status == "fail"
    assert "40999" in check.detail


def test_a_lane_locked_by_another_experiment_aborts(fake_host: FakeHost, sregym_dir: Path) -> None:
    fake_host.locks = {"luna-w0": "pid 4242 (lock pid 4242)"}

    check = _check(_preflight([_luna()], fake_host, sregym_dir, SREGYM_KIND_CLUSTER_PREFIX="luna-w"), "lane-isolation")

    assert check.status == "fail"
    assert "pid 4242" in check.detail


def test_an_ambient_kubeconfig_selecting_another_lane_aborts(
    fake_host: FakeHost, sregym_dir: Path, tmp_path: Path
) -> None:
    ambient = _stable_kubeconfig(tmp_path, "elsewhere", context="kind-luna-w2", port=40002)

    report = _preflight([_luna()], fake_host, sregym_dir, SREGYM_KIND_CLUSTER_PREFIX="luna-w", KUBECONFIG=str(ambient))

    assert _check(report, "lane-isolation").status == "fail"


def test_a_matching_stable_kubeconfig_passes(fake_host: FakeHost, sregym_dir: Path) -> None:
    _stable_kubeconfig(fake_host.home, "luna-w0", context="kind-luna-w0", port=40001)
    fake_host.clusters = ["luna-w0"]
    fake_host.ports = {"luna-w0": 40001}

    report = _preflight([_luna()], fake_host, sregym_dir, SREGYM_KIND_CLUSTER_PREFIX="luna-w")

    assert _check(report, "lane-isolation").status == "pass"


@pytest.mark.parametrize(
    ("config", "expected"),
    [
        (_luna(agent_config={"sdo_codex": {"provider": "claude", "model": "claude-haiku-4-5"}}), "responder resolves"),
        (_luna(agent_config={"sdo_codex": {"provider": "codex", "model": "gpt-5.4"}}), "codex:gpt-5.4"),
        (_luna(env=RunnerEnv(judge_model_id="codex-gpt-6-astra")), "judge is 'codex-gpt-6-astra'"),
        (_luna("codex", model="gpt-5.4"), "runner.model is 'gpt-5.4'"),
        (_luna("codex", reasoning_effort="high"), "reasoning_effort is 'high'"),
        (_luna("legacy_agent", model="gpt-6-luna"), "agent 'legacy_agent' is outside"),
    ],
)
def test_any_role_or_judge_off_the_luna_policy_aborts(
    fake_host: FakeHost, sregym_dir: Path, config: ExperimentConfig, expected: str
) -> None:
    check = _check(_preflight([config], fake_host, sregym_dir), "model-policy")

    assert check.status == "fail"
    assert expected in check.detail


def test_an_sdo_codex_config_without_a_model_uses_the_luna_driver_default(
    fake_host: FakeHost, sregym_dir: Path
) -> None:
    config = _luna(agent_config={"sdo_codex": {"provider": "codex"}})

    assert _check(_preflight([config], fake_host, sregym_dir), "model-policy").status == "pass"


@pytest.mark.parametrize(
    ("env", "expected"),
    [
        ({"JUDGE_REASONING_EFFORT": "high"}, "judge effort is 'high'"),
        ({"SDO_RESPONDER_MODEL": "gpt-5.4"}, "SDO_RESPONDER_MODEL=gpt-5.4"),
        ({"MODEL_ID": "gpt-5.4"}, "codex:gpt-5.4"),
    ],
)
def test_env_overrides_off_the_policy_abort(
    fake_host: FakeHost, sregym_dir: Path, env: dict[str, str], expected: str
) -> None:
    config = _luna(agent_config={"sdo_codex": {"provider": "codex"}}) if "MODEL_ID" in env else _luna()

    check = _check(_preflight([config], fake_host, sregym_dir, **env), "model-policy")

    assert check.status == "fail"
    assert expected in check.detail


def test_compared_arms_that_differ_in_grading_settings_abort(fake_host: FakeHost, sregym_dir: Path) -> None:
    baseline = _luna("codex", env=dataclasses.replace(_luna().env, worker_cpu_limit="4"))

    check = _check(_preflight([_luna(), baseline], fake_host, sregym_dir), "arm-parity")

    assert check.status == "fail"
    assert "worker_cpu_limit" in check.detail


def test_quota_at_the_limit_aborts_and_names_the_reset(fake_host: FakeHost, sregym_dir: Path) -> None:
    _write_rollout(fake_host.home, used=90, observed=NOW - 120, resets=NOW + 5 * 86_400)

    check = _check(_preflight([_luna()], fake_host, sregym_dir), "codex-quota")

    assert check.status == "fail"
    assert "90% used" in check.detail


@pytest.mark.parametrize(
    ("observed", "resets"),
    [(NOW - 7 * 3600, NOW + 86_400), (NOW - 60, NOW - 30)],
    ids=["stale", "past-reset"],
)
def test_an_old_quota_snapshot_is_unknown_not_a_failure(
    fake_host: FakeHost, sregym_dir: Path, observed: float, resets: float
) -> None:
    _write_rollout(fake_host.home, used=99, observed=observed, resets=resets)

    report = _preflight([_luna()], fake_host, sregym_dir)

    assert _check(report, "codex-quota").status == "unknown"
    assert report.facts["codex_quota"]["status"] == "unknown"


def test_no_codex_sessions_leaves_the_quota_unknown(tmp_path: Path) -> None:
    assert read_quota_snapshot(tmp_path / "nothing") is None


def test_warn_mode_launches_but_marks_the_report_waived(fake_host: FakeHost, sregym_dir: Path) -> None:
    fake_host.free = {str(sregym_dir / "logs"): GB}
    _write_rollout(fake_host.home, used=10, observed=time.time() - 60, resets=time.time() + 86_400)

    report = LaunchAssurance(host=fake_host, mode="warn").preflight(
        [_luna()], project_root=REPO_ROOT, sregym_dir=sregym_dir, env={}
    )

    assert not report.ok
    assert report.waived
    assert report.to_dict()["waived"] is True


def test_policy_effort_is_the_effort_sdo_pins_in_code() -> None:
    lifecycle_default = inspect.signature(CodexLifecycleBackend.__init__).parameters["reasoning_effort"].default

    assert ModelPolicy().agent_effort == INCIDENT_REASONING_EFFORT == lifecycle_default


def test_settings_reject_nonsense() -> None:
    with pytest.raises(ValueError, match="max_quota_used_percent"):
        PreflightSettings(max_quota_used_percent=0)
    with pytest.raises(ValueError, match="model"):
        ModelPolicy(model=" ")


LUNA_COMPARISONS = [
    ("sdo_example.toml", "codex_baseline_example.toml"),
]


@pytest.mark.parametrize("names", LUNA_COMPARISONS, ids=lambda names: names[0])
def test_checked_in_luna_comparisons_satisfy_policy_and_parity(
    fake_host: FakeHost, sregym_dir: Path, names: tuple[str, ...]
) -> None:
    configs = load_arm_configs([EXPERIMENTS / name for name in names], {})

    report = _preflight(configs, fake_host, sregym_dir)

    assert _check(report, "model-policy").status == "pass", _check(report, "model-policy").detail
    assert _check(report, "arm-parity").status == "pass", _check(report, "arm-parity").detail


def test_the_committed_stock_codex_pin_matches_the_image_pin(committed_sregym_registry: str) -> None:
    path = REPO_ROOT / "third_party" / "sregym" / "agents.yaml"
    if not path.is_file():
        pytest.skip("requires the optional external SREGym registry fixture")
    agents = yaml.safe_load(committed_sregym_registry)
    codex = next(entry for entry in agents["agents"] if entry["name"] == "codex")

    assert codex["agent_version"] == CODEX_PIN
    assert load_experiment_config(EXPERIMENTS / "codex_baseline_example.toml").agent == "codex"


def test_system_host_parses_codex_versions_behind_warnings() -> None:
    output = "WARNING: proceeding, even though we could not create PATH aliases\ncodex-cli 0.163.0\n"

    assert parse_codex_version(output) == "0.163.0"
    assert parse_codex_version("command not found") is None


def test_system_host_treats_npm_404_as_missing_and_other_errors_as_unknown() -> None:
    answers = {
        "@openai/codex@0.163.0": (0, "0.163.0\n", ""),
        "@openai/codex@0.158.0-linux-x64": (1, "", "npm error code E404\n"),
        "@openai/codex@9.9.9": (1, "", "npm error network ETIMEDOUT\n"),
    }

    def run(argv: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        returncode, stdout, stderr = answers[argv[2]]
        return subprocess.CompletedProcess(argv, returncode, stdout, stderr)

    host = SystemHost(run=run)

    assert host.npm_resolves("@openai/codex@0.163.0") is True
    assert host.npm_resolves("@openai/codex@0.158.0-linux-x64") is False
    assert host.npm_resolves("@openai/codex@9.9.9") is None


def test_flock_owner_reads_proc_locks_without_taking_the_lock(tmp_path: Path) -> None:
    lock = tmp_path / "luna-w0.lock"
    lock.write_text("pid 777", encoding="utf-8")
    inode = lock.stat().st_ino
    proc = tmp_path / "locks"
    proc.write_text(f"1: FLOCK  ADVISORY  WRITE 777 fd:01:{inode} 0 EOF\n", encoding="utf-8")
    free = tmp_path / "free"
    free.write_text("1: POSIX  ADVISORY  WRITE 9 fd:01:1 0 EOF\n", encoding="utf-8")

    assert flock_owner(lock, proc_locks=proc) == "pid 777 (lock pid 777)"
    assert flock_owner(lock, proc_locks=free) is None
