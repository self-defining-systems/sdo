"""Launchers name the install-time deployment policy as config ``deployment.policy``; the default is no policy."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from benchmarks.sregym.adapter import DeploymentPolicyConfig, RuntimeConfig
from benchmarks.sregym.adapter.native_operator import NativeInstallConfig


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
        kubeconfig=root / "lane.kubeconfig",
        **overrides,
    )


def test_no_policy_adds_no_config() -> None:
    assert "deployment" not in _install().overlay()
    assert DeploymentPolicyConfig().settings() == {}


def test_policy_config_follows_the_configuration() -> None:
    policy = DeploymentPolicyConfig(
        allowed_registries=("registry.gitlab.com", "docker.io"),
        scope_blocking=True,
        granted_namespaces=("monitoring", "ingress"),
        granted_cluster_kinds=("ClusterRole",),
    )
    assert _install(deployment_policy=policy).overlay()["deployment"] == {
        "policy": {
            "allowed_registries": ["registry.gitlab.com", "docker.io"],
            "scope_blocking": True,
            "granted_namespaces": ["monitoring", "ingress"],
            "granted_cluster_kinds": ["ClusterRole"],
        }
    }
    assert DeploymentPolicyConfig(allowed_registries=("docker.io",)).settings() == {"allowed_registries": ["docker.io"]}


def test_content_rules_have_one_owner() -> None:
    # Go's deploymentpolicy.Validate refuses these when the installer resolves the config
    # (sdo/operator/deploymentpolicy TestInvalidPoliciesAreRefused); Python adds no second copy.
    assert DeploymentPolicyConfig(allowed_registries=("Docker.io",)).settings() == {"allowed_registries": ["Docker.io"]}
    with pytest.raises(TypeError):
        DeploymentPolicyConfig(scope_blocking="yes")  # type: ignore[arg-type]


def test_runtime_config_policy_reaches_the_native_install_config(monkeypatch: pytest.MonkeyPatch) -> None:
    from benchmarks.sregym.adapter.runtime import native_install_config

    monkeypatch.setenv("KUBECONFIG", "/fixture/lane.kubeconfig")
    monkeypatch.delenv("SREGYM_BASE_KUBECONFIG", raising=False)
    config = _runtime_config()
    assert native_install_config(config).deployment_policy == DeploymentPolicyConfig()
    policy = DeploymentPolicyConfig(allowed_registries=("docker.io",))
    assert native_install_config(replace(config, deployment_policy=policy)).deployment_policy == policy


def _runtime_config() -> RuntimeConfig:
    return RuntimeConfig(
        repository=Path("/fixture/app"),
        namespace="app",
        application="app",
        controller_image="c:1",
        responder_image="r:1",
        repository_pvc="repository",
        credentials_secret="credentials",
        model="m",
        timeout_seconds=600,
    )


def test_policy_survives_the_persistent_state_round_trip_and_changes_the_controller_identity() -> None:
    import json

    from benchmarks.sregym.adapter.persistent import (
        controller_settings,
        runtime_config_from_payload,
        runtime_config_payload,
    )

    config = replace(
        _runtime_config(),
        deployment_policy=DeploymentPolicyConfig(allowed_registries=("docker.io",), scope_blocking=True),
    )
    stored = json.loads(json.dumps(runtime_config_payload(config)))
    assert runtime_config_from_payload(stored) == config
    plain = replace(config, deployment_policy=DeploymentPolicyConfig())
    assert controller_settings(runtime_config_payload(plain)) != controller_settings(runtime_config_payload(config))


def test_a_controller_recorded_before_the_policy_existed_is_the_no_policy_controller() -> None:
    import json

    from benchmarks.sregym.adapter.persistent import (
        controller_settings,
        runtime_config_from_payload,
        runtime_config_payload,
    )

    config = _runtime_config()
    old = json.loads(json.dumps(runtime_config_payload(config)))
    del old["deployment_policy"]
    assert runtime_config_from_payload(old) == config
    assert controller_settings(old) == controller_settings(runtime_config_payload(config))
