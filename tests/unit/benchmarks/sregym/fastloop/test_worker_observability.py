"""Deployment setup follows resolved applications, including legacy problem IDs."""

from types import SimpleNamespace

import pytest

from benchmarks.sregym import adapter
from benchmarks.sregym.fastloop import worker_bridge


def test_legacy_astronomy_problem_gets_telemetry_before_deployment(monkeypatch: pytest.MonkeyPatch):
    calls = []

    class Worker:
        def _problem_factory(self, _problem_id):
            return SimpleNamespace(namespace="astronomy-shop")

        def handle(self, request):
            calls.append("deploy")
            return {"deployed": True}

    module = SimpleNamespace(
        __file__="/harness/sregym/fastloop/worker.py",
        FastloopWorker=Worker,
        main=lambda: Worker().handle({"op": "deploy", "problem_id": "ad_service_high_cpu"}),
    )
    monkeypatch.setenv("KUBECONFIG", "/lane/kubeconfig")
    monkeypatch.setenv("SREGYM_KIND_CLUSTER_NAME", "lane52")
    monkeypatch.setattr(adapter, "prepare_oracle_observability", lambda **_: calls.append("telemetry"))
    monkeypatch.setattr(adapter, "route_astronomy_traces", lambda *_: calls.append("traces"))
    monkeypatch.setattr(
        worker_bridge.importlib, "import_module", lambda name: module if name.endswith(".worker") else SimpleNamespace()
    )
    assert worker_bridge.main() == {"deployed": True}
    assert calls == ["telemetry", "deploy", "traces"]
