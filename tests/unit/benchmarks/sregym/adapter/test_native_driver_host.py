"""The active benchmark lifecycle is launched through the Go host boundary."""

from __future__ import annotations

import json
from dataclasses import fields
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.adapter import driver, native_operator
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


@pytest.mark.parametrize("schema_version", [1, 3])
def test_seeded_workspace_installs_controller_without_running_the_lifecycle(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, schema_version: int
) -> None:
    sdo = tmp_path / ".sdo"
    diagnostics = sdo / "diagnostics"
    if schema_version == 3:
        diagnostics /= "detectors/health/health-objective"
    diagnostics.mkdir(parents=True)
    (diagnostics / "manifest.yaml").write_text("id: health-objective\n", encoding="utf-8")
    (sdo / "schema-version").write_text(f"{schema_version}\n", encoding="utf-8")
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


@pytest.mark.parametrize("health", [False, True])
def test_incomplete_current_memory_does_not_skip_lifecycle(tmp_path: Path, health: bool) -> None:
    sdo = tmp_path / ".sdo"
    detector = sdo / "diagnostics/detectors" / ("health" if health else "incidents") / "example"
    detector.mkdir(parents=True)
    (detector / "manifest.yaml").write_text("id: example\n", encoding="utf-8")
    if not health:
        (sdo / "lifecycle-provenance.yaml").write_text("{}\n", encoding="utf-8")
    assert driver._has_accepted_lifecycle(tmp_path) is False


def test_scripted_worker_override_requires_accepted_lifecycle_before_cluster_access(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("SREGYM_KIND_CLUSTER_NAME", "sregym-w1")
    monkeypatch.setenv("SREGYM_BASE_KUBECONFIG", str(tmp_path / "config"))
    monkeypatch.setattr(
        driver,
        "prepare_benchmark_resources",
        lambda *_args, **_kwargs: pytest.fail("cluster was accessed without a lifecycle seed"),
    )
    with pytest.raises(ValueError, match="accepted lifecycle seed"):
        driver._launch_native_lifecycle(
            driver._parse_args(["--agent-registrations", str(tmp_path / "registrations")]),
            repository=tmp_path,
            namespace="demo",
            application="demo",
            health_objective="healthy",
            api_base="",
            persistent=True,
        )


@pytest.mark.parametrize("schema_version", [1, 3])
def test_seeded_workspace_publishes_scripted_worker_registrations(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, schema_version: int
) -> None:
    sdo = tmp_path / ".sdo"
    diagnostics = sdo / "diagnostics"
    if schema_version == 3:
        diagnostics /= "detectors/health/health-objective"
    diagnostics.mkdir(parents=True)
    (diagnostics / "manifest.yaml").write_text("id: health-objective\n", encoding="utf-8")
    (sdo / "schema-version").write_text(f"{schema_version}\n", encoding="utf-8")
    (sdo / "lifecycle-provenance.yaml").write_text("{}\n", encoding="utf-8")
    registrations = tmp_path / "registrations"
    registrations.mkdir()
    (registrations / "responder.json").write_text('{"agent_id":"scripted"}', encoding="utf-8")
    monkeypatch.setenv("SREGYM_KIND_CLUSTER_NAME", "sregym-w1")
    monkeypatch.setenv("SREGYM_BASE_KUBECONFIG", str(tmp_path / "config"))
    monkeypatch.setattr(driver, "prepare_benchmark_resources", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(driver, "wait_submission_bridge", lambda *_args: None)
    monkeypatch.setattr(
        driver, "install_existing", lambda *_args: type("Receipt", (), {"reused": False, "accepted_commit": "a" * 40})()
    )
    monkeypatch.setattr(driver, "inspect_existing", lambda *_args: object())
    applied: list[dict[str, object]] = []

    def apply(args: list[str], **kwargs: object) -> object:
        assert args == ["apply", "-f", "-"]
        applied.append(json.loads(str(kwargs["input_text"])))
        return object()

    monkeypatch.setattr(driver, "kubectl", apply)
    driver._launch_native_lifecycle(
        driver._parse_args(["--agent-registrations", str(registrations)]),
        repository=tmp_path,
        namespace="demo",
        application="demo",
        health_objective="healthy",
        api_base="",
        persistent=True,
    )
    assert applied[0]["metadata"] == {"name": "sdo-agent-registrations", "namespace": "demo-sdo"}
    assert applied[0]["data"] == {"responder.json": '{"agent_id":"scripted"}'}


def test_published_scripted_registration_uses_loaded_responder_image(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    registrations = tmp_path / "registrations"
    registrations.mkdir()
    (registrations / "responder.json").write_text(
        '{"agent_id":"scripted","kubernetes_job":{"image":"${RESPONDER_IMAGE}"}}',
        encoding="utf-8",
    )
    applied: list[dict[str, object]] = []
    monkeypatch.setattr(
        driver, "kubectl", lambda _args, **kwargs: applied.append(json.loads(str(kwargs["input_text"])))
    )
    driver._publish_agent_registrations(
        registrations, "demo-sdo", tmp_path / "kubeconfig", "responder:loaded", {"responder:loaded"}
    )
    published = json.loads(applied[0]["data"]["responder.json"])
    assert published["kubernetes_job"]["image"] == "responder:loaded"


def test_registration_rejects_unloaded_worker_image_before_publication(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    registrations = tmp_path / "registrations"
    registrations.mkdir()
    (registrations / "responder.json").write_text(
        '{"agent_id":"scripted","kubernetes_job":{"image":"missing:v1"}}', encoding="utf-8"
    )
    monkeypatch.setattr(driver, "kubectl", lambda *_args, **_kwargs: pytest.fail("published invalid image"))
    with pytest.raises(ValueError, match="not loaded into kind"):
        driver._publish_agent_registrations(
            registrations, "demo-sdo", tmp_path / "kubeconfig", "responder:loaded", {"responder:loaded"}
        )


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
    monkeypatch.setenv("SREGYM_BASE_KUBECONFIG", str(tmp_path / "config"))

    def launch(args: object, **_kwargs: object) -> dict[str, object]:
        policies.append(_kwargs["install_config"].repair_policy)
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


@pytest.mark.parametrize("seeded", [False, True])
def test_runtime_install_preserves_shared_settings_on_every_lifecycle_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, seeded: bool
) -> None:
    if seeded:
        sdo = tmp_path / ".sdo"
        (sdo / "diagnostics").mkdir(parents=True)
        (sdo / "diagnostics" / "manifest.yaml").write_text("detectors: []\n")
        (sdo / "lifecycle-provenance.yaml").write_text("{}\n")
    registrations = tmp_path / "registrations"
    registrations.mkdir()
    config = RuntimeConfig(
        repository=tmp_path,
        namespace="demo",
        application="demo",
        controller_image="controller:test",
        responder_image="responder:test",
        validator_image="validator:test",
        repository_pvc="repository",
        credentials_secret="credentials",
        model="gpt-6-luna",
        timeout_seconds=41,
        verification_timeout_seconds=43,
        repair_policy="recorded-actions",
        agent_provider="claude",
        reflection_session="fresh",
        reflection_guidance="generalize",
        force_reflection=True,
        late_findings="pull",
        max_follow_ups=2,
        max_response_loops=4,
        follow_up_cooldown_seconds=17,
        healthy_baseline=True,
        controller_namespace="demo-sdo",
        kind_cluster_name="fastloop-w0",
        persistent=True,
        agent_registrations=registrations if seeded else None,
        deployed_manifest_paths=("kubernetes",),
    )
    monkeypatch.setenv("SREGYM_BASE_KUBECONFIG", str(tmp_path / "config"))
    monkeypatch.setattr(driver, "prepare_benchmark_resources", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(driver, "_publish_agent_registrations", lambda *_args: None)
    monkeypatch.setattr(driver, "inspect_existing", lambda *_args: object())
    installed: list[native_operator.NativeInstallConfig] = []
    controller_args: list[str] = []

    def run(_command: str, args: list[str], **_kwargs: object) -> dict[str, object]:
        controller_args.extend(args)
        return {"installed": True, "reused": False, "fingerprint": "abc", "accepted_commit": "a" * 40}

    def install(value: native_operator.NativeInstallConfig) -> object:
        installed.append(value)
        return native_operator.install_existing(value)

    monkeypatch.setattr(native_operator, "_run", run)
    monkeypatch.setattr(driver, "install_existing", install)
    monkeypatch.setattr(driver, "launch_host", lambda host: installed.append(host.install) or {"installed": True})

    driver.launch_native_lifecycle(config, "healthy", "")

    assert len(installed) == 1
    actual = installed[0]
    for field in fields(native_operator.NativeInstallConfig):
        if hasattr(config, field.name):
            assert getattr(actual, field.name) == getattr(config, field.name), field.name
    assert actual.control_namespace == config.controller_namespace
    assert actual.response_timeout_seconds == config.timeout_seconds
    assert actual.verification_timeout_seconds == config.verification_timeout_seconds
    assert actual.deployed_manifest_paths == ("kubernetes",)
    if seeded:
        assert controller_args[controller_args.index("--deployed-manifest-path") + 1] == "kubernetes"
