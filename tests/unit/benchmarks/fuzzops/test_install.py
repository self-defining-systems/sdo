from pathlib import Path

import pytest

from benchmarks.fuzzops.config import BENCH_PROFILE
from benchmarks.fuzzops.install import InstallConfig, main


def _config(**overrides: object) -> InstallConfig:
    values: dict[str, object] = {
        "pack": Path("/synthetic/pack"),
        "work": Path("/synthetic/work"),
        "namespace": "app",
        "control_namespace": "app-sdo",
        "objective": "users are served",
        "controller_image": "sdo-controller:test",
        "responder_image": "sdo-responder:test",
        "operator_binary": Path("/synthetic/sdo-operator"),
        "kubeconfig": Path("/synthetic/kubeconfig"),
        "auth_file": Path("/synthetic/auth.json"),
        "model": "synthetic-model",
    }
    values.update(overrides)
    return InstallConfig(**values)  # type: ignore[arg-type]


def test_watch_only_installs_a_watch_only_controller_through_the_mode_variable() -> None:
    config = _config(watch_only=True)
    assert config.host_env()["SDO_MODE"] == "watch-only"
    assert not any(arg in ("--watch-only", "--frozen-memory") for arg in config.host_args(Path("/w/app")))


def test_default_install_responds() -> None:
    assert "SDO_MODE" not in _config().host_env()


def test_host_passes_the_benchmark_profile_then_the_overlays() -> None:
    args = _config(config_files=(Path("/synthetic/lane.yaml"),)).host_args(Path("/synthetic/work/app"))
    files = [args[i + 1] for i, arg in enumerate(args) if arg == "--config"]
    assert files == [str(BENCH_PROFILE), "/synthetic/lane.yaml"]


def test_max_attempts_is_configured_only_when_given() -> None:
    assert "SDO_DEPLOYMENT_MAX_ATTEMPTS" not in _config().host_env()
    assert _config(max_attempts=2).host_env()["SDO_DEPLOYMENT_MAX_ATTEMPTS"] == "2"


def test_the_model_is_a_required_setting_not_a_default() -> None:
    with pytest.raises(ValueError, match="model is required"):
        InstallConfig(
            pack=Path("/synthetic/pack"),
            work=Path("/synthetic/work"),
            namespace="app",
            control_namespace="app-sdo",
            objective="users are served",
            controller_image="sdo-controller:test",
            responder_image="sdo-responder:test",
            operator_binary=Path("/synthetic/sdo-operator"),
            kubeconfig=Path("/synthetic/kubeconfig"),
            auth_file=Path("/synthetic/auth.json"),
        )


def test_the_command_line_requires_a_model(capsys: pytest.CaptureFixture[str]) -> None:
    required = [
        "--pack", "/synthetic/pack", "--work", "/synthetic/work", "--namespace", "app",
        "--control-namespace", "app-sdo", "--objective", "o", "--controller-image", "c",
        "--responder-image", "r", "--operator-binary", "/synthetic/b", "--kubeconfig", "/synthetic/k",
    ]  # fmt: skip
    with pytest.raises(SystemExit):
        main(required)
    assert "--model" in capsys.readouterr().err


def test_journeys_file_is_passed_to_host_only_when_given() -> None:
    assert "--journeys-file" not in _config().host_args(Path("/w/app"))
    args = _config(journeys_file=Path("/synthetic/journeys.txt")).host_args(Path("/w/app"))
    assert args[args.index("--journeys-file") + 1] == "/synthetic/journeys.txt"
    assert args.index("--journeys-file") < args.index("--config")


def test_journeys_file_must_be_absolute() -> None:
    with pytest.raises(ValueError, match="journeys_file"):
        _config(journeys_file=Path("journeys.txt"))


def test_no_launch_passes_a_plan_flag() -> None:
    """The override is the repository's committed file; host takes no plan flag in either mode."""
    assert "--deployment-plan" not in _config().host_args(Path("/w/app"))
    assert "--deployment-plan" not in _config(provided_plan=False, registry_host="registry.local:5000").host_args(
        Path("/w/app")
    )


def test_the_deployer_launch_names_the_registry_its_builds_push_to() -> None:
    # the deployer task was once refused before any model call: the launch named no registry host
    args = _config(provided_plan=False, registry_host="registry.local:5000", registry_insecure=True).host_args(
        Path("/w/app")
    )
    assert args[args.index("--registry-host") + 1] == "registry.local:5000"
    assert "--registry-insecure" in args
    assert args.index("--registry-insecure") < args.index("--config")
    with pytest.raises(ValueError, match="registry_host"):
        _config(provided_plan=False)
    assert "--registry-host" not in _config().host_args(Path("/w/app"))


def test_the_command_line_passes_the_deployer_registry(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from benchmarks.fuzzops import install as module

    seen: list[InstallConfig] = []
    monkeypatch.setattr(module, "install", lambda cfg, description="": seen.append(cfg) or 0)
    required = [
        "--pack", "/synthetic/pack", "--work", str(tmp_path / "work"), "--namespace", "app",
        "--control-namespace", "app-sdo", "--objective", "o", "--controller-image", "c",
        "--responder-image", "r", "--operator-binary", "/synthetic/b", "--kubeconfig", "/synthetic/k",
        "--model", "m", "--deployer", "--registry-host", "registry.local:5000", "--registry-insecure",
    ]  # fmt: skip
    assert main(required) == 0
    assert seen[0].registry_host == "registry.local:5000"
    assert seen[0].registry_insecure
