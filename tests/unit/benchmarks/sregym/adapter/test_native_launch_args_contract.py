"""The adapter's emitted Go-operator arguments are pinned against the Go flag parsers.

The fixture is the exact argv the adapter emits with every option set. The Go test
``TestAdapterEmittedLaunchArgsParse`` (``sdo/operator/operatorcli``) feeds it to the
real flag parsers, so a flag that one side adds or removes without the other fails
both suites. Regenerate with ``SDO_UPDATE_LAUNCH_ARGS_FIXTURE=1``.
"""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING

from benchmarks.sregym.adapter.native_operator import (
    NativeHostConfig,
    NativeInstallConfig,
    install_existing,
    launch_host,
)

if TYPE_CHECKING:
    import pytest

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
        program_fast_path=True,
        program_trust_threshold=4,
        max_follow_ups=2,
        max_response_loops=3,
        follow_up_cooldown_seconds=25,
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
    assert set(emitted) == {"install-existing", "host"}
    if os.environ.get("SDO_UPDATE_LAUNCH_ARGS_FIXTURE") == "1":
        FIXTURE.write_text(json.dumps(emitted, indent=2) + "\n", encoding="utf-8")
    assert json.loads(FIXTURE.read_text(encoding="utf-8")) == emitted
