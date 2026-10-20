"""The active benchmark lifecycle is launched through the Go host boundary."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.adapter import driver
from benchmarks.sregym.adapter.runtime import RuntimeConfig

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


def test_seeded_workspace_installs_controller_without_running_the_lifecycle(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    sdo = tmp_path / ".sdo"
    (sdo / "diagnostics").mkdir(parents=True)
    (sdo / "diagnostics" / "manifest.yaml").write_text("detectors: []\n", encoding="utf-8")
    (sdo / "lifecycle-provenance.yaml").write_text("{}\n", encoding="utf-8")
    events: list[str] = []
    monkeypatch.setenv("SREGYM_KIND_CLUSTER_NAME", "sregym-w1")
    monkeypatch.setenv("SREGYM_BASE_KUBECONFIG", str(tmp_path / "config"))
    monkeypatch.setattr(driver, "prepare_benchmark_resources", lambda config, **_kwargs: events.append("prepare"))
    monkeypatch.setattr(driver, "wait_submission_bridge", lambda *_args: events.append("bridge"))
    monkeypatch.setattr(driver, "launch_host", lambda config: events.append("host") or {})

    def install(config: object) -> object:
        events.append("install-existing")
        return type("Receipt", (), {"reused": False, "accepted_commit": "a" * 40})()

    monkeypatch.setattr(driver, "install_existing", install)
    inspections: list[int] = []

    def inspect(config: object) -> object:
        inspections.append(1)
        if len(inspections) < 3:
            raise driver.NativeOperatorError("existing controller has no ready owned Pod")
        events.append("controller-ready")
        return object()

    monkeypatch.setattr(driver, "inspect_existing", inspect)
    monkeypatch.setattr(driver.time, "sleep", lambda _seconds: None)
    result = driver._launch_native_lifecycle(
        driver._parse_args([]),
        repository=tmp_path,
        namespace="demo",
        application="demo",
        health_objective="deployment stays healthy",
        api_base="http://localhost:8000",
        persistent=True,
    )
    assert events == ["prepare", "bridge", "install-existing", "controller-ready"]
    assert result["seeded"] is True


def test_unseeded_workspace_has_no_accepted_lifecycle(tmp_path: Path) -> None:
    assert driver._has_accepted_lifecycle(tmp_path) is False


def test_fastloop_native_launch_has_recorded_action_repairs_and_no_submission_transport(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("SREGYM_KIND_CLUSTER_NAME", "fastloop-w0")
    monkeypatch.setenv("SREGYM_BASE_KUBECONFIG", str(tmp_path / "config"))
    monkeypatch.setattr(driver, "prepare_benchmark_resources", lambda *_args, **_kwargs: None)
    seen: list[object] = []
    monkeypatch.setattr(driver, "launch_host", lambda config: seen.append(config) or {"installed": True})

    driver._launch_native_lifecycle(
        driver._parse_args([]),
        repository=tmp_path,
        namespace="demo",
        application="demo",
        health_objective="healthy",
        api_base="",
        persistent=True,
    )

    assert seen[0].install.repair_policy == "recorded-actions"
    assert seen[0].install.responder_env == ()


def test_native_launch_passes_configured_repair_policy_to_installer(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    policies: list[str] = []

    def launch(args: object, **_kwargs: object) -> dict[str, object]:
        policies.append(args.repair_policy)
        return {}

    monkeypatch.setattr(driver, "_launch_native_lifecycle", launch)
    base = {
        "repository": tmp_path,
        "namespace": "demo",
        "application": "demo",
        "controller_image": "controller:pbprog",
        "responder_image": "responder:pbprog",
        "repository_pvc": "repository",
        "credentials_secret": "credentials",
        "model": "gpt-6-luna",
        "timeout_seconds": 30,
    }
    for policy in ("recorded-actions", "commit"):
        driver.launch_native_lifecycle(RuntimeConfig(**base, repair_policy=policy), "healthy", "")

    assert policies == ["recorded-actions", "commit"]
