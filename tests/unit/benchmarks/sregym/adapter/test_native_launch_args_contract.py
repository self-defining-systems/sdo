"""The adapter's emitted Go-operator arguments are pinned against the Go flag parsers.

The fixture is the exact argv the adapter emits with every option set. The Go test
``TestAdapterEmittedLaunchArgsParse`` (``sdo/operator/operatorcli``) feeds it to the
real flag parsers, so a flag that one side adds or removes without the other fails
both suites. Regenerate with ``SDO_UPDATE_LAUNCH_ARGS_FIXTURE=1``.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING

from benchmarks.sregym.adapter.native_operator import (
    NativeHostConfig,
    NativeInstallConfig,
    NativeOperatorError,
    export_runtime_artifacts,
    inspect_existing,
    install_existing,
    launch_host,
    read_incident_evidence,
    set_maintenance,
    stop_runtime,
    sync_existing,
)

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
}
FIXTURE = Path(__file__).resolve().parents[5] / "tests" / "fixtures" / "sdo" / "contracts" / "adapter_launch_args.json"


def _install() -> NativeInstallConfig:
    root = Path("/fixture")
    return NativeInstallConfig(
        repository=root / "app",
        namespace="hotel",
        control_namespace="hotel-sdo",
        application="hotel",
        controller_image="controller:go",
        responder_image="responder:python",
        validator_image="validator:go",
        repository_pvc="repository",
        credentials_secret="agent-credentials",
        model="model",
        kubeconfig=root / "lane.kubeconfig",
        agent_provider="claude",
        repair_policy="recorded-actions",
        late_findings="pull",
        reflection_session="fresh",
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
        deployed_manifest_paths=("kubernetes",),
    )


def _emitted(monkeypatch: pytest.MonkeyPatch) -> dict[str, list[str]]:
    monkeypatch.setenv("SDO_OPERATOR_BIN", "/tmp/sdo-operator")
    argv: dict[str, list[str]] = {}

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        argv[command[1]] = command[1:]
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
        )
    )
    return argv


def test_adapter_emitted_launch_args_match_fixture(monkeypatch: pytest.MonkeyPatch) -> None:
    emitted = _emitted(monkeypatch)
    assert set(emitted) == ADAPTER_COMMANDS - {"observe-controller"}
    if os.environ.get("SDO_UPDATE_LAUNCH_ARGS_FIXTURE") == "1":
        FIXTURE.write_text(json.dumps(emitted, indent=2) + "\n", encoding="utf-8")
    assert json.loads(FIXTURE.read_text(encoding="utf-8")) == emitted


def test_every_go_operator_command_the_adapter_runs_is_covered() -> None:
    # observe-controller takes JSON on stdin, not flags; every other command is
    # in the fixture and parsed by the Go test.
    called = set(re.findall(r'(?:execute_operator|_run)\(\s*"([a-z-]+)"', ADAPTER.read_text(encoding="utf-8")))
    assert called == ADAPTER_COMMANDS
