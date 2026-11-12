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
    assert not any(arg in ("--watch-only", "--frozen-memory") for arg in config.host_args(Path("/w/app"), {}))


def test_default_install_responds() -> None:
    assert "SDO_MODE" not in _config().host_env()


def test_host_passes_the_benchmark_profile_then_the_overlays() -> None:
    args = _config(config_files=(Path("/synthetic/lane.yaml"),)).host_args(Path("/synthetic/work/app"), {})
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
