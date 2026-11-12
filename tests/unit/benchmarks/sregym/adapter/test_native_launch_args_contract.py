"""The adapter's emitted Go-operator arguments are pinned against the Go flag parsers.

The fixture is the exact argv the adapter emits with every option set, so an
adapter change that adds or removes a flag fails this suite. Regenerate with
``SDO_UPDATE_LAUNCH_ARGS_FIXTURE=1``.

Operator configuration crosses as ``--config`` files and ``SDO_*`` variables. In
the fixture a ``--config`` path is repository-relative (the benchmark profile) or
``<command>.config.yaml`` (the lane overlay, whose content is the fixture's
``<command>.config`` entry), and ``<command>.env`` lists the launch variables.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING

from benchmarks.sregym.adapter.deployment_source import (
    DeploymentPolicyConfig,
    buildkit_image,
    deployment_plan,
    helm_source,
    raw_source,
)
from benchmarks.sregym.adapter.native_operator import (
    NativeHostConfig,
    NativeInstallConfig,
    NativeOperatorError,
    export_runtime_artifacts,
    inspect_existing,
    install_existing,
    launch_host,
    read_incident_evidence,
    reset_control_state,
    set_maintenance,
    stop_runtime,
    sync_existing,
)
from tests.unit.benchmarks.sregym.adapter.launch_capture import config_files, overlay, stub_resolution

if TYPE_CHECKING:
    import pytest

ADAPTER = Path(__file__).resolve().parents[5] / "benchmarks" / "sregym" / "adapter" / "native_operator.py"
ADAPTER_COMMANDS = {
    "install-existing",
    "host",
    "sync-existing",
    "maintenance",
    "inspect-existing",
    "incident-evidence",
    "runtime-artifacts",
    "observe-controller",
    "stop",
    "reset-control-state",
}
REPOSITORY = Path(__file__).resolve().parents[5]
FIXTURE = REPOSITORY / "tests" / "fixtures" / "sdo" / "contracts" / "adapter_launch_args.json"


def _install() -> NativeInstallConfig:
    root = Path("/fixture")
    return NativeInstallConfig(
        repository=root / "app",
        namespace="hotel",
        control_namespace="hotel-sdo",
        application="hotel",
        controller_image="controller:go",
        responder_image="responder:python",
        repository_pvc="repository",
        credentials_secret="agent-credentials",
        model="model",
        kubeconfig=root / "lane.kubeconfig",
        agent_provider="claude",
        agent_effort="high",
        replay="on",
        lifecycle_timeout_seconds=900,
        repair_policy="recorded-actions",
        reflection_guidance="generalize-spec",
        force_reflection=True,
        max_response_loops=3,
        response_timeout_seconds=600,
        verification_timeout_seconds=90,
        duration_seconds=1200,
        exit_after_closure=True,
        healthy_baseline=True,
        registration_configmap="registrations",
        responder_env=("SDO_SUBMISSION_MODE=relay",),
        evidence_sink_host_path="/opt/sdo-evidence",
        evidence_run_id="run-1",
        deployment_source=raw_source(("kubernetes",)),
        deployment_policy=DeploymentPolicyConfig(
            allowed_registries=("registry:5000", "docker.io"),
            scope_blocking=True,
            granted_namespaces=("monitoring",),
            granted_cluster_kinds=("ClusterRole",),
        ),
    )


def _emitted(monkeypatch: pytest.MonkeyPatch) -> dict[str, list[str]]:
    monkeypatch.setenv("SDO_OPERATOR_BIN", "/tmp/sdo-operator")
    stub_resolution(monkeypatch)
    argv: dict[str, list[str]] = {}

    def run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        name = command[1]
        argv[name] = _normalized(name, command[1:])
        if "--config" in command:
            argv[f"{name}.config"] = [json.dumps(overlay(command), sort_keys=True)]
            launch_env = kwargs["env"]
            assert isinstance(launch_env, dict)
            argv[f"{name}.env"] = sorted(
                f"{key}={value}"
                for key, value in launch_env.items()
                if key.startswith("SDO_") and key not in ("SDO_OPERATOR_BIN", "SDO_UPDATE_LAUNCH_ARGS_FIXTURE")
            )
        receipt = {
            "installed": True,
            "reused": False,
            "fingerprint": "f",
            "accepted_commit": "a" * 40,
            "operation_id": "op",
        }
        receipt.update({f"{name}_commit": "b" * 40 for name in ("goal", "schema", "source", "architecture", "judge")})
        return subprocess.CompletedProcess(command, 0, json.dumps(receipt), "")

    monkeypatch.setattr(subprocess, "run", run)
    install = _install()
    install_existing(install)
    # The remaining commands validate their receipts after the call; the argv is
    # captured before that, so a receipt mismatch is irrelevant here.
    for call in (
        lambda: sync_existing(install),
        lambda: set_maintenance("hotel-sdo", paused=True, generation="g1", kubeconfig=install.kubeconfig),
        lambda: inspect_existing(install),
        lambda: read_incident_evidence(install, "incident-1", sync_repository=True),
        lambda: export_runtime_artifacts(install, Path("/fixture/out")),
        lambda: stop_runtime("hotel-sdo", kubeconfig=str(install.kubeconfig), inspect=True),
        lambda: reset_control_state("hotel-sdo", kubeconfig=str(install.kubeconfig)),
    ):
        try:
            call()
        except NativeOperatorError:
            pass
    launch_host(
        NativeHostConfig(
            install=replace(install, duration_seconds=None),
            health_objective="requests succeed",
            operation_id="op",
            buildkit_image="buildkit@sha256:" + "0" * 64,
            registry_push_secret="registry-push",
            registry_host="registry:5000",
            registry_insecure=True,
            max_attempts=2,
            agent_registrations=Path("/fixture/registrations.json"),
            deployment_plan=deployment_plan(
                helm_source("chart"),
                images=(buildkit_image("app:v1", ".", "Dockerfile"),),
                expected_workload_ids=["Deployment/hotel/web"],
            ),
            source_kind="git",
            source_config='{"poll_interval":"5s","ref":"main","repository":"git://upstream/app.git"}',
        )
    )
    return argv


def _normalized(name: str, args: list[str]) -> list[str]:
    files = config_files(args)
    normal = list(args)
    for index, item in enumerate(args):
        if item == "--config":
            path = Path(args[index + 1])
            normal[index + 1] = (
                str(path.relative_to(REPOSITORY)) if path.is_relative_to(REPOSITORY) else f"{name}.config.yaml"
            )
    assert len(files) <= 2
    return normal


def test_adapter_emitted_launch_args_match_fixture(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in [
        name
        for name in os.environ
        if name.startswith("SDO_") and name not in ("SDO_OPERATOR_BIN", "SDO_UPDATE_LAUNCH_ARGS_FIXTURE")
    ]:
        monkeypatch.delenv(name)
    emitted = _emitted(monkeypatch)
    launched = {name for name in emitted if "." not in name}
    assert launched == ADAPTER_COMMANDS - {"observe-controller"}
    assert {name.split(".")[0] for name in emitted if "." in name} == {"install-existing", "host"}
    if os.environ.get("SDO_UPDATE_LAUNCH_ARGS_FIXTURE") == "1":
        FIXTURE.write_text(json.dumps(emitted, indent=2) + "\n", encoding="utf-8")
    assert json.loads(FIXTURE.read_text(encoding="utf-8")) == emitted


def test_every_go_operator_command_the_adapter_runs_is_covered() -> None:
    # observe-controller takes JSON on stdin, not flags; every other command is
    # in the fixture and parsed by the Go test.
    called = set(re.findall(r'(?:execute_operator|_run)\(\s*"([a-z-]+)"', ADAPTER.read_text(encoding="utf-8")))
    assert called == ADAPTER_COMMANDS
