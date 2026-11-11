from pathlib import Path

from benchmarks.fuzzops.install import InstallConfig


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
    }
    values.update(overrides)
    return InstallConfig(**values)  # type: ignore[arg-type]


def test_watch_only_installs_a_frozen_watch_only_controller() -> None:
    args = _config(watch_only=True).host_args(Path("/synthetic/work/app"), {})
    assert "--watch-only" in args
    assert "--frozen-memory" in args


def test_default_install_responds() -> None:
    args = _config().host_args(Path("/synthetic/work/app"), {})
    assert "--watch-only" not in args
    assert "--frozen-memory" not in args
