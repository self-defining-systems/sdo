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
        validator_image="validator:go",
        repository_pvc="repository",
        credentials_secret="agent-credentials",
        model="gpt-6-luna",
        kubeconfig=tmp_path / "lane.kubeconfig",
        repair_policy="recorded-actions",
        late_findings="pull",
        responder_env=("SDO_SREGYM_API_BASE=http://sdo-sregym-bridge:8000",),
    )


def test_install_existing_invokes_go_cli_and_parses_receipt(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
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
            healthy_baseline=True,
            max_response_loops=3,
            deployed_manifest_paths=("kubernetes",),
            program_fast_path=True,
            program_trust_threshold=4,
            drift_tokens_per_hour=250000,
        )
    )

    assert receipt.reused
    assert receipt.accepted_commit == "a" * 40
    assert calls[0][0:2] == ["/tmp/sdo-operator", "install-existing"]
    assert "--repair-policy" in calls[0]
    assert "recorded-actions" in calls[0]
    assert "--responder-env" in calls[0]
    assert "--late-findings" in calls[0]
    assert "--healthy-baseline" in calls[0]
    assert calls[0][calls[0].index("--max-response-loops") + 1] == "3"
    assert calls[0][calls[0].index("--deployed-manifest-path") + 1] == "kubernetes"
    assert "--program-fast-path" in calls[0]
    assert calls[0][calls[0].index("--program-trust-threshold") + 1] == "4"
    assert calls[0][calls[0].index("--drift-tokens-per-hour") + 1] == "250000"
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


def test_collect_native_receipt_binds_go_ledger_to_cluster_job_and_pod(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tests.unit.benchmarks.sregym.protocol.test_native_receipt import _ledger

    monkeypatch.setenv("SDO_OPERATOR_BIN", "/tmp/sdo-operator")
    calls: list[list[str]] = []

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        if command[0] == "/tmp/sdo-operator":
            payload: object = {
                "incident_id": "incident-1",
                "responder_job_name": "sdo-incident-1-hash",
                "ledger": _ledger(),
            }
        elif "job/sdo-incident-1-hash" in command:
            payload = {"metadata": {"uid": "worker-uid"}, "status": {"succeeded": 1}}
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
    assert receipt["responder_job_evidence"] == [
        {"incident_id": "incident-1", "job_name": "sdo-incident-1-hash", "job_uid": "worker-uid", "succeeded": True}
    ]
    assert any("job/sdo-incident-1-hash" in call for call in calls)


def test_collect_native_fast_path_receipt_has_no_responder_job(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from tests.unit.benchmarks.sregym.protocol.test_native_receipt import _ledger

    ledger = _ledger()
    ledger["proposal_commit"] = ""
    ledger["receipt"]["proposal_commit"] = ""
    ledger["receipt"]["reflection_commit"] = ""
    ledger["reflection_commit"] = ""
    ledger["reflection_decision"] = None
    ledger["reflection_processed"] = False
    ledger["reflection_skipped_reason"] = "trusted program fast path"
    ledger["requires_reflection"] = False
    ledger["outcome_document"]["repair_commit"] = ""
    ledger["outcome_document"]["resolution_path"] = "fast_path"
    ledger["outcome_document"]["fast_path_programs"] = ["targetport"]
    ledger["closure"]["result"]["resolution_path"] = "fast_path"
    ledger["closure"]["result"]["fast_path_programs"] = ["targetport"]
    application = {"applied": True, "program_ids": ["targetport"], "verification": {"result": "RESULT_NO_MATCH"}}
    ledger["closure"]["result"]["plan_applications"] = [application]
    ledger["outcome_document"]["plan_applications"] = [application]
    monkeypatch.setenv("SDO_OPERATOR_BIN", "/tmp/sdo-operator")
    calls: list[list[str]] = []

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        if command[0] == "/tmp/sdo-operator":
            payload: object = {
                "incident_id": "incident-1",
                "responder_job_name": "sdo-incident-1-hash",
                "ledger": ledger,
            }
        elif "job/sdo-controller-run" in command:
            payload = {"metadata": {"uid": "controller-job-uid"}}
        else:
            payload = {
                "items": [
                    {
                        "metadata": {"uid": "controller-uid", "ownerReferences": [{"uid": "controller-job-uid"}]},
                        "status": {"phase": "Running"},
                    }
                ]
            }
        return subprocess.CompletedProcess(command, 0, json.dumps(payload), "")

    monkeypatch.setattr(subprocess, "run", run)
    receipt = collect_native_receipt(replace(_install(tmp_path), namespace="demo"), "incident-1")
    assert receipt["responder_job_evidence"] == []
    assert not any("job/sdo-incident-1-hash" in call for call in calls)


def test_collect_native_receipt_rejects_missing_successful_worker_job(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tests.unit.benchmarks.sregym.protocol.test_native_receipt import _ledger

    monkeypatch.setenv("SDO_OPERATOR_BIN", "/tmp/sdo-operator")

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        if command[0] == "/tmp/sdo-operator":
            payload: object = {
                "incident_id": "incident-1",
                "responder_job_name": "sdo-incident-1-hash",
                "ledger": _ledger(),
            }
        else:
            payload = {"metadata": {"uid": "worker-uid"}, "status": {"failed": 1}}
        return subprocess.CompletedProcess(command, 0, json.dumps(payload), "")

    monkeypatch.setattr(subprocess, "run", run)
    with pytest.raises(NativeOperatorError, match="durably successful"):
        collect_native_receipt(replace(_install(tmp_path), namespace="demo"), "incident-1")


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
