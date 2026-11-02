"""Persistent benchmark side effects route through the Go operator CLI."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.adapter import persistent
from benchmarks.sregym.adapter.runtime import RuntimeConfig

if TYPE_CHECKING:
    from pathlib import Path


def test_replacing_controller_preserves_externally_provisioned_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from benchmarks.sregym.adapter import native_operator

    calls = []

    def stop(namespace: str, **kwargs: object) -> dict[str, list[str]]:
        calls.append((namespace, kwargs))
        return {"removed": ["job/demo-sdo/sdo-controller-run"], "leftovers": []}

    monkeypatch.setattr(native_operator, "stop_runtime", stop)
    monkeypatch.setattr(persistent, "kubectl", lambda *_args, **_kwargs: pytest.fail("stop must delegate to Go"))
    persistent.KubectlClusterOps().delete_controller("demo-sdo")
    assert calls == [("demo-sdo", {"kubeconfig": None})]


def test_persistent_maintenance_and_receipt_do_not_start_python_sync_pod(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = RuntimeConfig(
        repository=tmp_path / "app",
        namespace="demo",
        application="demo",
        controller_image="controller:go",
        responder_image="worker:python",
        repository_pvc="repository",
        credentials_secret="agent-credentials",
        model="gpt-6-luna",
        timeout_seconds=60,
        controller_namespace="demo-sdo",
        persistent=True,
    )
    called: list[str] = []

    def maintenance(*_args: object, **_kwargs: object) -> None:
        called.append("maintenance")

    def receipt(*_args: object, **_kwargs: object) -> dict[str, object]:
        called.append("receipt")
        return {"schema_version": "sdo.production-receipt/v2"}

    monkeypatch.setattr(persistent, "go_set_maintenance", maintenance)
    monkeypatch.setattr(persistent, "collect_production_receipt", receipt)
    ops = persistent.KubectlClusterOps()
    # Maintenance now proves the controller has no pending closure first.
    # This transport-only fixture supplies that read independently of kubectl.
    monkeypatch.setattr(ops, "runtime_state", lambda _namespace: {"incident_open": False})
    ops.set_maintenance("demo-sdo", paused=True, generation="stage-2")
    assert ops.collect_receipt(config, "incident-1", tmp_path)["schema_version"] == "sdo.production-receipt/v2"
    assert called == ["maintenance", "receipt"]


def test_native_closeout_settles_on_acknowledged_go_ledger() -> None:
    assert persistent._closeout_settled(
        {
            "schema_version": "sdo.production-receipt/v2",
            "incident_id": "incident-1",
            "native_ledger": {"incident_id": "incident-1", "acknowledged": True},
        }
    )
    assert not persistent._closeout_settled(
        {
            "schema_version": "sdo.production-receipt/v2",
            "incident_id": "incident-1",
            "native_ledger": {"incident_id": "incident-1", "acknowledged": False},
        }
    )


def test_persistent_artifact_export_uses_go_cli(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SREGYM_BASE_KUBECONFIG", str(tmp_path / "kubeconfig"))
    config = RuntimeConfig(
        repository=tmp_path / "app",
        namespace="demo",
        application="demo",
        controller_image="controller:go",
        responder_image="worker:python",
        repository_pvc="repository",
        credentials_secret="agent-credentials",
        model="gpt-6-luna",
        timeout_seconds=60,
        controller_namespace="demo-sdo",
        persistent=True,
    )
    called: list[Path] = []

    def export(_config: object, output: Path) -> Path:
        called.append(output)
        return output / "sdo_runtime"

    monkeypatch.setattr(persistent, "go_export_runtime_artifacts", export)
    result = persistent.KubectlClusterOps().export_runtime_artifacts(config, tmp_path)
    assert result == {"directory": str(tmp_path / "sdo_runtime"), "error": None}
    assert called == [tmp_path]


def test_reusable_stage_inspects_live_go_controller_without_reinstall(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SREGYM_BASE_KUBECONFIG", str(tmp_path / "kubeconfig"))
    config = RuntimeConfig(
        repository=tmp_path / "stale-stage-checkout",
        namespace="demo",
        application="demo",
        controller_image="controller:go",
        responder_image="worker:python",
        repository_pvc="repository",
        credentials_secret="agent-credentials",
        model="gpt-6-luna",
        timeout_seconds=60,
        controller_namespace="demo-sdo",
        persistent=True,
    )
    called: list[str] = []

    def inspect(_config: object) -> object:
        called.append("inspect")
        return object()

    monkeypatch.setattr(persistent, "go_inspect_existing", inspect)
    assert persistent.KubectlClusterOps().install(replace(config, reuse_existing=True))
    assert called == ["inspect"]


def test_reset_stage_syncs_accepted_pvc_before_recreating_controller(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from benchmarks.sregym.adapter import native_operator

    monkeypatch.setenv("SREGYM_BASE_KUBECONFIG", str(tmp_path / "kubeconfig"))
    config = RuntimeConfig(
        repository=tmp_path / "stale-stage-checkout",
        namespace="demo",
        application="demo",
        controller_image="controller:go",
        responder_image="worker:python",
        repository_pvc="repository",
        credentials_secret="agent-credentials",
        model="gpt-6-luna",
        timeout_seconds=60,
        controller_namespace="demo-sdo",
        persistent=True,
    )
    config.repository.mkdir()
    head = config.repository / "accepted-head"
    head.write_text("stale")

    def inspect(_config: object) -> object:
        raise AssertionError("reset deleted the controller Job; inspection cannot restart it")

    def sync(native: native_operator.NativeInstallConfig) -> None:
        assert native.repository == config.repository
        head.write_text("learned-pvc-head")

    def install(native: native_operator.NativeInstallConfig) -> SimpleNamespace:
        assert native.repository == config.repository
        assert head.read_text() == "learned-pvc-head"
        return SimpleNamespace(reused=False)

    monkeypatch.setattr(persistent, "go_inspect_existing", inspect)
    monkeypatch.setattr(native_operator, "sync_existing", sync)
    monkeypatch.setattr(native_operator, "install_existing", install)

    assert persistent.KubectlClusterOps().install(config) is False


def test_controller_inventory_failure_is_not_controller_loss(monkeypatch: pytest.MonkeyPatch) -> None:
    def kubectl(*_args: object, **_kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(returncode=1, stdout="", stderr="connection refused")

    monkeypatch.setattr(persistent, "kubectl", kubectl)
    with pytest.raises(persistent.PersistentControllerError, match="connection refused"):
        persistent.KubectlClusterOps().controller_pod("demo-sdo")


@pytest.mark.parametrize("output", ["", '{"items": []}'])
def test_controller_inventory_confirms_absence(monkeypatch: pytest.MonkeyPatch, output: str) -> None:
    def kubectl(*_args: object, **_kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(returncode=0, stdout=output, stderr="")

    monkeypatch.setattr(persistent, "kubectl", kubectl)
    assert persistent.KubectlClusterOps().controller_pod("demo-sdo") is None


def test_graceful_teardown_stops_benchmark_transport_after_go_runtime(tmp_path, monkeypatch):
    from tests.unit.benchmarks.sregym.adapter.test_abort import seed_state

    path = tmp_path / "state.json"
    seed_state(path)
    state = persistent.PersistentState.load(path)
    state.controllers["demo"].pending = None
    state.controllers["demo"].kubeconfig = "/recorded/lane"
    state.save(path)
    events = []
    ops = persistent.KubectlClusterOps()
    monkeypatch.setattr(ops, "controller_pod", lambda *_: None)
    monkeypatch.setattr(ops, "stop_runtime", lambda target: events.append(("go-stop", target.kubeconfig)))
    monkeypatch.setattr(ops, "stop_transport", lambda target: events.append(("benchmark-stop", target.kubeconfig)))
    monkeypatch.setattr(
        ops, "retire_controller_state", lambda target, _archive: events.append(("retire-state", target.kubeconfig))
    )
    assert persistent.teardown(path, ops=ops) == []
    assert events == [
        ("go-stop", "/recorded/lane"),
        ("benchmark-stop", "/recorded/lane"),
        ("retire-state", "/recorded/lane"),
    ]
