"""Launchers name the install-time deployment policy as flags; the default is no policy (today's behavior)."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from benchmarks.sregym.adapter import DeploymentPolicyConfig, RuntimeConfig
from benchmarks.sregym.adapter.native_operator import NativeInstallConfig

POLICY_FLAGS = ("--allow-registry", "--scope-blocking", "--grant-namespace", "--grant-cluster-kind")


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


def test_no_policy_adds_no_flag() -> None:
    args = _install().common_args()
    assert not set(POLICY_FLAGS) & set(args)
    assert DeploymentPolicyConfig().args() == []


def test_policy_flags_follow_the_configuration() -> None:
    policy = DeploymentPolicyConfig(
        allowed_registries=("registry.gitlab.com", "docker.io"),
        scope_blocking=True,
        granted_namespaces=("monitoring", "ingress"),
        granted_cluster_kinds=("ClusterRole",),
    )
    args = _install(deployment_policy=policy).common_args()
    assert args[-7:] == [
        "--allow-registry",
        "registry.gitlab.com,docker.io",
        "--scope-blocking",
        "--grant-namespace",
        "monitoring,ingress",
        "--grant-cluster-kind",
        "ClusterRole",
    ]
    assert DeploymentPolicyConfig(allowed_registries=("docker.io",)).args() == ["--allow-registry", "docker.io"]


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"allowed_registries": ("Docker.io",)}, "lowercase host"),
        ({"allowed_registries": ("registry/path",)}, "lowercase host"),
        ({"allowed_registries": ("a,b",)}, "lowercase host"),
        ({"granted_namespaces": ("x",)}, "grants need scope_blocking"),
        ({"granted_cluster_kinds": ("ClusterRole",)}, "grants need scope_blocking"),
        ({"scope_blocking": True, "granted_namespaces": ("a b",)}, "invalid granted namespace"),
        ({"scope_blocking": True, "granted_cluster_kinds": ("",)}, "invalid granted cluster kind"),
    ],
)
def test_invalid_policy_is_refused_at_construction(kwargs: dict[str, object], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        DeploymentPolicyConfig(**kwargs)


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
