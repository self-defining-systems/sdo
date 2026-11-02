"""The benchmark's active installer calls only the compiled Go operator."""

from __future__ import annotations

import json
import subprocess
from dataclasses import replace
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path

from benchmarks.sregym.adapter.native_operator import (
    NativeHostConfig,
    NativeInstallConfig,
    NativeOperatorError,
    collect_native_receipt,
    export_runtime_artifacts,
    inspect_existing,
    install_existing,
    launch_host,
    read_incident_evidence,
    set_maintenance,
    sync_existing,
)


def _install(tmp_path: Path) -> NativeInstallConfig:
    return NativeInstallConfig(
        repository=tmp_path / "app",
        namespace="hotel",
        control_namespace="hotel-sdo",
        application="hotel",
        controller_image="controller:go",
        responder_image="responder:python",
        repository_pvc="repository",
        credentials_secret="agent-credentials",
        model="gpt-6-luna",
        kubeconfig=tmp_path / "lane.kubeconfig",
        repair_policy="recorded-actions",
        late_findings="pull",
        responder_env=("SDO_SREGYM_API_BASE=http://sdo-sregym-bridge:8000",),
    )


@pytest.mark.parametrize("code_path", ["go", "shell"])
def test_install_existing_invokes_go_cli_and_parses_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, code_path: str
) -> None:
    calls: list[list[str]] = []
    monkeypatch.setenv("SDO_OPERATOR_BIN", "/tmp/sdo-operator")

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(
            command,
            0,
            json.dumps({"installed": True, "reused": True, "fingerprint": "abc", "accepted_commit": "a" * 40}),
            "",
        )

    monkeypatch.setattr(subprocess, "run", run)
    receipt = install_existing(
        replace(
            _install(tmp_path),
            code_path=code_path,
            healthy_baseline=True,
            permissive_responder=True,
            responder_namespaces=("policy-system",),
            max_response_loops=3,
            deployed_manifest_paths=("kubernetes",),
        )
    )

    if code_path == "shell":
        assert calls[0][calls[0].index("--code-path") + 1] == "shell"
    else:
        assert "--code-path" not in calls[0]
    assert receipt.reused
    assert receipt.accepted_commit == "a" * 40
    assert calls[0][0:2] == ["/tmp/sdo-operator", "install-existing"]
    assert "--repair-policy" in calls[0]
    assert "recorded-actions" in calls[0]
    assert "--responder-env" in calls[0]
    assert "--late-findings" in calls[0]
    assert "--permissive-responder" in calls[0]
    assert calls[0][calls[0].index("--responder-namespace") + 1] == "policy-system"
    assert "--healthy-baseline" in calls[0]
    assert calls[0][calls[0].index("--max-response-loops") + 1] == "3"
    assert calls[0][calls[0].index("--deployed-manifest-path") + 1] == "kubernetes"
    assert "--drift-tokens-per-hour" not in calls[0]
    assert "pull" in calls[0]
    assert calls[0][0] != "python3"
    assert "-m" not in calls[0]


def test_host_requires_typed_registry_and_strict_json(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SDO_OPERATOR_BIN", "/tmp/sdo-operator")
    calls: list[list[str]] = []

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, "warning\n{}", "")

    monkeypatch.setattr(subprocess, "run", run)
    host = NativeHostConfig(
        install=replace(_install(tmp_path), healthy_baseline=True),
        health_objective="hotel serves requests",
        operation_id="benchmark-stage-1",
        buildkit_image="moby/buildkit@sha256:" + "b" * 64,
        registry_push_secret="registry-push",
        registry_host="registry.hotel-sdo.svc.cluster.local:5000",
        registry_insecure=True,
    )
    with pytest.raises(NativeOperatorError, match="JSON object"):
        launch_host(host)
    assert calls[0][0:2] == ["/tmp/sdo-operator", "host"]
    assert "--registry-insecure" in calls[0]
    assert "--registry-push-secret" in calls[0]
    assert "--registry-host" in calls[0]
    assert "--healthy-baseline" in calls[0]
    assert "--late-findings" in calls[0]
    assert "pull" in calls[0]


def test_sync_and_maintenance_use_go_cli_without_python_broker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SDO_OPERATOR_BIN", "/tmp/sdo-operator")
    calls: list[list[str]] = []

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(subprocess, "run", run)
    config = _install(tmp_path)
    sync_existing(config)
    set_maintenance(config.control_namespace, paused=True, generation="stage-2", kubeconfig=config.kubeconfig)
    assert calls[0] == [
        "/tmp/sdo-operator",
        "sync-existing",
        "--repository",
        str(config.repository),
        "--control-namespace",
        "hotel-sdo",
        "--repository-pvc",
        "repository",
        "--controller-image",
        "controller:go",
        "--kubeconfig",
        str(config.kubeconfig),
    ]
    assert calls[1] == [
        "/tmp/sdo-operator",
        "maintenance",
        "--control-namespace",
        "hotel-sdo",
        "--paused=true",
        "--generation",
        "stage-2",
        "--kubeconfig",
        str(config.kubeconfig),
    ]
    set_maintenance(config.control_namespace, paused=True, generation="barrier", preserve_baseline=True)
    assert "--preserve-baseline" in calls[2]


def test_incident_evidence_requires_correlated_native_ledger(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SDO_OPERATOR_BIN", "/tmp/sdo-operator")
    calls: list[list[str]] = []

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(
            command,
            0,
            json.dumps(
                {
                    "incident_id": "incident-1",
                    "responder_job_name": "sdo-incident-1-hash",
                    "ledger": {"incident_id": "incident-1", "acknowledged": True},
                }
            ),
            "",
        )

    monkeypatch.setattr(subprocess, "run", run)
    assert read_incident_evidence(_install(tmp_path), "incident-1")["ledger"]["acknowledged"] is True
    assert calls[0] == [
        "/tmp/sdo-operator",
        "incident-evidence",
        "--control-namespace",
        "hotel-sdo",
        "--repository-pvc",
        "repository",
        "--controller-image",
        "controller:go",
        "--incident-id",
        "incident-1",
        "--kubeconfig",
        str(tmp_path / "lane.kubeconfig"),
    ]
    with pytest.raises(ValueError, match="incident ID"):
        read_incident_evidence(_install(tmp_path), "")


@pytest.mark.parametrize("job_name", ["sdo-incident-1-hash", "sdo-standby-bound-identity"])
def test_collect_native_receipt_survives_retired_responder_job(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    job_name: str,
) -> None:
    from tests.unit.benchmarks.sregym.protocol.test_native_receipt import _ledger

    monkeypatch.setenv("SDO_OPERATOR_BIN", "/tmp/sdo-operator")
    calls: list[list[str]] = []

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        if command[0] == "/tmp/sdo-operator":
            payload: object = {
                "incident_id": "incident-1",
                "responder_job_name": job_name,
                "ledger": {
                    **_ledger(),
                    "closure": {
                        **_ledger()["closure"],
                        "dispatched_at": "2026-10-18T03:00:00Z",
                        "responder_completed_at": "2026-10-18T03:01:00Z",
                    },
                },
            }
        elif f"job/{job_name}" in command:
            raise AssertionError("receipt queried a retired responder Job")
        elif "job/sdo-controller-run" in command:
            payload = {"metadata": {"uid": "controller-job-uid"}}
        else:
            payload = {
                "items": [
                    {
                        "metadata": {"uid": "old-controller-uid", "ownerReferences": [{"uid": "old-job-uid"}]},
                        "status": {"phase": "Succeeded"},
                    },
                    {
                        "metadata": {"uid": "controller-uid", "ownerReferences": [{"uid": "controller-job-uid"}]},
                        "status": {"phase": "Running"},
                    },
                ]
            }
        return subprocess.CompletedProcess(command, 0, json.dumps(payload), "")

    monkeypatch.setattr(subprocess, "run", run)
    receipt = collect_native_receipt(replace(_install(tmp_path), namespace="demo"), "incident-1")
    assert receipt["schema_version"] == "sdo.production-receipt/v2"
    assert receipt["controller_pod_uid"] == "controller-uid"
    assert receipt["responder_job_evidence"] == []
    assert not any(f"job/{job_name}" in call for call in calls)


def test_runtime_artifacts_export_is_go_owned(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SDO_OPERATOR_BIN", "/tmp/sdo-operator")
    output_dir = tmp_path / "results"
    calls: list[list[str]] = []

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, json.dumps({"directory": str(output_dir / "sdo_runtime")}), "")

    monkeypatch.setattr(subprocess, "run", run)
    assert export_runtime_artifacts(_install(tmp_path), output_dir) == output_dir / "sdo_runtime"
    assert calls[0][0:2] == ["/tmp/sdo-operator", "runtime-artifacts"]
    assert "--output-dir" in calls[0]
    assert str(output_dir) in calls[0]


def test_inspect_existing_reuses_live_go_controller_without_local_checkout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SDO_OPERATOR_BIN", "/tmp/sdo-operator")
    calls: list[list[str]] = []

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(
            command,
            0,
            json.dumps(
                {
                    "job_uid": "job-uid",
                    "pod_uid": "pod-uid",
                    "fingerprint": "fp",
                    "accepted_commit": "a" * 40,
                }
            ),
            "",
        )

    monkeypatch.setattr(subprocess, "run", run)
    result = inspect_existing(_install(tmp_path))
    assert result.accepted_commit == "a" * 40
    assert result.pod_uid == "pod-uid"
    assert calls[0][0:2] == ["/tmp/sdo-operator", "inspect-existing"]
    assert "--repository" not in calls[0]


def test_install_records_abort_target_before_transport_call(tmp_path, monkeypatch):
    from benchmarks.sregym.adapter import native_operator
    from benchmarks.sregym.adapter.persistent import PersistentState

    path = tmp_path / "abort-state.json"
    monkeypatch.setenv("SDO_RUN_REGISTRY_STATE", str(path))
    config = _install(tmp_path)

    def interrupted(*args, **kwargs):
        target = PersistentState.load(path).cleanup_targets["hotel"]
        assert target.control_namespace == "hotel-sdo"
        assert target.kubeconfig == str(config.kubeconfig)
        raise NativeOperatorError("interrupted install")

    monkeypatch.setattr(native_operator, "execute_operator", interrupted)
    with pytest.raises(NativeOperatorError, match="interrupted install"):
        install_existing(config)


def test_host_accepts_optional_goal_and_absent_goal_commit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from benchmarks.sregym.adapter import native_operator

    host = NativeHostConfig(
        install=_install(tmp_path),
        health_objective="",
        operation_id="no-goal",
        buildkit_image="buildkit@sha256:" + "b" * 64,
        registry_push_secret="registry",
        registry_host="registry:5000",
    )
    receipt = {"installed": True, "operation_id": "no-goal"}
    receipt.update(dict.fromkeys(("schema_commit", "source_commit", "architecture_commit", "judge_commit"), "a" * 40))
    monkeypatch.setattr(native_operator, "_run", lambda *args, **kwargs: receipt)
    assert launch_host(host) == receipt


def test_adapter_agent_efforts_match_the_production_provider() -> None:
    # The adapter reads the shared manifest so SREGym's venv can import it without agentshim.
    from benchmarks.sregym.adapter import native_operator
    from sdo.agents import provider

    assert native_operator.AGENT_EFFORTS == provider.AGENT_EFFORTS
    assert native_operator.DEFAULT_AGENT_EFFORT == provider.DEFAULT_AGENT_EFFORT


@pytest.mark.parametrize("failure", ["not_ready", "exec_failed", "timeout", "mismatch"])
def test_shared_identity_distinguishes_transport_readiness_and_mismatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    from benchmarks.sregym.adapter import native_operator as native

    config = replace(_install(tmp_path), shared_accepted_id="a" * 32)
    identity = config.repository / ".git" / "sdo-shared-id"
    identity.parent.mkdir(parents=True)
    identity.write_text(config.shared_accepted_id)
    monkeypatch.setattr(native, "refresh_memory_mirror", lambda *_: None)
    monkeypatch.setattr(
        native,
        "_kubectl_json",
        lambda *_: {
            "items": [
                {
                    "metadata": {"name": "controller-pod"},
                    "status": {
                        "phase": "Running",
                        "conditions": [{"type": "Ready", "status": "False" if failure == "not_ready" else "True"}],
                    },
                }
            ]
        },
    )
    calls = []

    def run(args, **kwargs):
        calls.append(args)
        if failure == "timeout":
            raise subprocess.TimeoutExpired(args, 30)
        return subprocess.CompletedProcess(args, 1 if failure == "exec_failed" else 0, "b" * 32, "SENSITIVE_CANARY")

    monkeypatch.setattr(native.subprocess, "run", run)
    expected = {
        "not_ready": "SharedRepositoryControllerNotReady",
        "exec_failed": "SharedRepositoryExecFailed",
        "timeout": "SharedRepositoryExecFailed",
        "mismatch": "SharedRepositoryIdentityMismatch",
    }
    with pytest.raises(NativeOperatorError) as caught:
        native.assert_shared_repository(config)
    assert type(caught.value).__name__ == expected[failure]
    assert "hotel-sdo" in str(caught.value)
    assert "CANARY" not in str(caught.value)
    assert bool(calls) is (failure != "not_ready")
