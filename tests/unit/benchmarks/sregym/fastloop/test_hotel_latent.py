from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from benchmarks.sregym.fastloop.baseline_recovery import ApplicationBaseline
from benchmarks.sregym.fastloop.fault_tracker import FaultSpec


def test_tracing_seed_uses_the_healthy_external_name_without_changing_health(tmp_path: Path) -> None:
    from benchmarks.sregym.fastloop.hotel_latent import align_tracing_alias

    service = tmp_path / "kubernetes/jaeger/jaeger-service.yaml"
    service.parent.mkdir(parents=True)
    service.write_text("apiVersion: v1\nkind: Service\nmetadata: {name: jaeger}\nspec: {ports: [{port: 16686}]}\n")
    health = tmp_path / ".sdo/diagnostics/manifest.yaml"
    health.parent.mkdir(parents=True)
    health.write_text("structural health only\n")
    alias = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "jaeger", "labels": {"app": "jaeger"}},
        "spec": {"type": "ExternalName", "externalName": "otel-collector.observe.svc.cluster.local"},
    }
    assert align_tracing_alias(tmp_path, ApplicationBaseline("hotel-reservation", [alias]))
    assert yaml.safe_load(service.read_text()) == alias
    assert health.read_text() == "structural health only\n"
    assert not align_tracing_alias(tmp_path, ApplicationBaseline("hotel-reservation", [alias]))


def test_sweep_preparation_occurs_before_targetport_injection(monkeypatch: pytest.MonkeyPatch) -> None:
    from benchmarks.sregym.fastloop import hotel_latent
    from benchmarks.sregym.fastloop.fault_driver import SregymFaultDriver

    calls: list[str] = []

    def kubectl(args: list[str], **kwargs: object) -> SimpleNamespace:
        assert kwargs["namespace"] == "hotel-reservation"
        assert args == [
            "patch",
            "service",
            "recommendation",
            "--type=merge",
            "-p",
            '{"metadata":{"managedFields":[{}]}}',
        ]
        calls.append("ownership-cleared")
        return SimpleNamespace(returncode=0)

    class Worker:
        def request(self, op: str, **kwargs: object) -> dict[str, float]:
            assert op == "inject"
            calls.append(str(kwargs["problem_id"]))
            return {"started_at": 1790000000.0, "finished_at": 1790000001.0}

    monkeypatch.setattr(hotel_latent, "kubectl", kubectl)
    prepare = hotel_latent.sweep_preparer(
        ("recommendation",),
        {"first": (FaultSpec("target_port", "reservation"),), "repeat": (FaultSpec("target_port", "recommendation"),)},
        namespace="hotel-reservation",
        kubeconfig=Path("/lane53"),
        sleep=lambda _seconds: calls.append("informer-wait"),
    )
    driver = SregymFaultDriver(Worker(), namespace="hotel-reservation", before_inject=prepare)
    driver.inject("first")
    driver.inject("repeat")
    assert calls == ["first", "ownership-cleared", "informer-wait", "repeat"]


def test_sweep_preparation_rejects_targets_outside_targetport_stream() -> None:
    from benchmarks.sregym.fastloop.hotel_latent import sweep_preparer

    with pytest.raises(ValueError, match="targetPort"):
        sweep_preparer(
            ("rate",), {"image": (FaultSpec("incorrect_image", "rate"),)}, namespace="hotel", kubeconfig=Path("/lane53")
        )


def test_sweep_services_require_a_stream_before_cluster_access(tmp_path: Path) -> None:
    from benchmarks.sregym.fastloop.cli import main

    with pytest.raises(SystemExit, match="requires an SDO --stream"):
        main(["run", "--run-dir", str(tmp_path), "--agent", "sdo", "--sweep-service", "recommendation"])
