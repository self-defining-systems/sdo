"""The active benchmark lifecycle is launched through the Go host boundary."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.adapter import driver

if TYPE_CHECKING:
    from pathlib import Path


def test_native_host_prepares_benchmark_registry_and_go_settings(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    events: list[str] = []
    monkeypatch.setenv("SREGYM_KIND_CLUSTER_NAME", "sregym-w1")
    monkeypatch.setenv("SREGYM_BASE_KUBECONFIG", str(tmp_path / "config"))
    monkeypatch.setattr(driver, "prepare_benchmark_resources", lambda config, **_kwargs: events.append("prepare"))
    monkeypatch.setattr(driver, "wait_submission_bridge", lambda *_args: events.append("bridge"))

    def launch(config: object) -> dict[str, object]:
        events.append("host")
        assert config.install.control_namespace == "demo-sdo"
        assert config.install.repair_policy == "recorded-actions"
        assert config.install.reflection_session == "resume"
        assert config.install.exit_after_closure is True
        assert config.registry_host == "sdo-benchmark-registry.demo-sdo.svc.cluster.local:5000"
        assert "SDO_SREGYM_API_BASE=http://sdo-sregym-bridge:8000" in config.install.responder_env
        return {"installed": True, "operation_id": config.operation_id}

    monkeypatch.setattr(driver, "launch_host", launch)
    args = driver._parse_args(["--reflection-session", "resume"])
    driver._launch_native_lifecycle(
        args,
        repository=tmp_path,
        namespace="demo",
        application="demo",
        health_objective="deployment stays healthy",
        api_base="http://localhost:8000",
        persistent=False,
    )
    assert events == ["prepare", "bridge", "host"]


def test_native_host_requires_explicit_kind_lane(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("SREGYM_KIND_CLUSTER_NAME", raising=False)
    monkeypatch.setenv("SREGYM_BASE_KUBECONFIG", str(tmp_path / "config"))
    args = driver._parse_args([])
    with pytest.raises(RuntimeError, match="SREGYM_KIND_CLUSTER_NAME"):
        driver._launch_native_lifecycle(
            args,
            repository=tmp_path,
            namespace="demo",
            application="demo",
            health_objective="healthy",
            api_base="http://localhost:8000",
            persistent=True,
        )
