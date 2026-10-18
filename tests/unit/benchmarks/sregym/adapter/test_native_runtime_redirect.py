"""Active benchmark installs and evidence must use the Go operator boundary."""

from __future__ import annotations

from typing import TYPE_CHECKING

from benchmarks.sregym.adapter import runtime
from benchmarks.sregym.adapter.native_operator import NativeInstallReceipt

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def _config(tmp_path: Path) -> runtime.RuntimeConfig:
    return runtime.RuntimeConfig(
        repository=tmp_path / "accepted",
        namespace="demo",
        application="demo",
        controller_image="controller:go",
        responder_image="worker:python",
        validator_image="validator:go",
        repository_pvc="repository",
        credentials_secret="agent-credentials",
        model="gpt-6-luna",
        timeout_seconds=90,
        verification_timeout_seconds=45,
        repair_policy="recorded-actions",
        controller_namespace="demo-sdo",
        late_findings="pull",
        persistent=True,
    )


def test_persistent_install_calls_go_reuse_with_typed_settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SREGYM_BASE_KUBECONFIG", str(tmp_path / "kubeconfig"))
    config = _config(tmp_path)
    captured: list[object] = []

    def install(value: object) -> NativeInstallReceipt:
        captured.append(value)
        return NativeInstallReceipt(True, True, "fingerprint", "a" * 40)

    monkeypatch.setattr(runtime, "install_existing", install)
    assert runtime.install_persistent_controller(config) is True
    native = captured[0]
    assert native.control_namespace == "demo-sdo"
    assert native.repair_policy == "recorded-actions"
    assert native.late_findings == "pull"
    assert native.response_timeout_seconds == 90


def test_persistent_receipt_reads_native_go_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SREGYM_BASE_KUBECONFIG", str(tmp_path / "kubeconfig"))
    calls: list[str] = []

    def sync(_config: object) -> None:
        calls.append("sync")

    def collect(_config: object, incident_id: str) -> dict[str, object]:
        calls.append(incident_id)
        return {"schema_version": "sdo.production-receipt/v2", "incident_id": incident_id}

    monkeypatch.setattr(runtime, "sync_existing", sync)
    monkeypatch.setattr(runtime, "collect_native_receipt", collect)
    receipt = runtime.collect_production_receipt(_config(tmp_path), "incident-1", tmp_path)
    assert receipt["schema_version"] == "sdo.production-receipt/v2"
    assert calls == ["sync", "incident-1"]
