"""Launchers name the evidence sink as installer flags; the default is no sink (evidence stays on the PVC)."""

from __future__ import annotations

from pathlib import Path

import pytest

from benchmarks.sregym.adapter import RuntimeConfig
from benchmarks.sregym.adapter.native_operator import NativeInstallConfig
from benchmarks.sregym.adapter.persistent import controller_settings
from benchmarks.sregym.adapter.runtime import native_install_config


def _install(**overrides: object) -> NativeInstallConfig:
    root = Path("/fixture")
    return NativeInstallConfig(
        repository=root / "app",
        namespace="app",
        control_namespace="app-sdo",
        application="app",
        controller_image="controller:go",
        responder_image="responder:python",
        repository_pvc="repository",
        credentials_secret="agent-credentials",
        model="model",
        **overrides,  # type: ignore[arg-type]
    )


def test_no_sink_adds_no_flag() -> None:
    assert not any(arg.startswith("--evidence") for arg in _install().common_args())


def test_sink_flags_follow_the_configuration() -> None:
    args = _install(evidence_sink_host_path="/opt/sdo-evidence", evidence_run_id="lane-1").common_args()
    assert args[-4:] == ["--evidence-sink-host-path", "/opt/sdo-evidence", "--evidence-run-id", "lane-1"]


@pytest.mark.parametrize(
    "overrides",
    [{"evidence_sink_host_path": "relative/dir"}, {"evidence_run_id": "lane-1"}],
)
def test_invalid_sink_shapes_are_refused(overrides: dict[str, str]) -> None:
    with pytest.raises(ValueError, match="evidence"):
        _install(**overrides)


def test_runtime_config_carries_the_sink_to_the_installer(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SREGYM_BASE_KUBECONFIG", raising=False)
    monkeypatch.setenv("KUBECONFIG", "/fixture/lane.kubeconfig")
    config = RuntimeConfig(
        repository=Path("/fixture/app"),
        namespace="app",
        application="app",
        controller_image="controller:go",
        responder_image="responder:python",
        repository_pvc="repository",
        credentials_secret="agent-credentials",
        model="model",
        timeout_seconds=600,
        evidence_sink_host_path="/opt/sdo-evidence",
        evidence_run_id="lane-1",
    )
    install = native_install_config(config)
    assert (install.evidence_sink_host_path, install.evidence_run_id) == ("/opt/sdo-evidence", "lane-1")


def test_controllers_recorded_before_the_sink_were_installed_without_one() -> None:
    settings = controller_settings({"model": "m"})
    assert settings["evidence_sink_host_path"] == ""
    assert settings["evidence_run_id"] == ""
