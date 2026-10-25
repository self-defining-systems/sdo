"""Admission invariants apply to every app, including real harness manifests."""

from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest

from benchmarks.sregym.fastloop import clean_start


@pytest.mark.parametrize("namespace", ["hotel-reservation", "astronomy-shop", "social-network"])
def test_probe_placement_preserves_app_dns(namespace):
    value = {
        "kind": "Pod",
        "metadata": {"namespace": namespace, "labels": {"app.kubernetes.io/managed-by": "sregym-composite-probe"}},
        "spec": {"containers": [{"command": ["sh", "-c", f"wget http://frontend.{namespace}.svc.cluster.local"]}]},
    }
    original = deepcopy(value)
    isolated = clean_start.isolate_benchmark_probe(value)
    assert isolated["metadata"]["namespace"] == "sregym"
    assert isolated["spec"] == original["spec"]
    assert value == original


@pytest.mark.parametrize("kind", ["Pod", "Job", "ConfigMap"])
def test_admission_rejects_any_benchmark_labelled_object(monkeypatch, kind):
    monkeypatch.setattr(clean_start, "_injected_objects", lambda _: [])

    def api(args, **kwargs):
        if args[args.index("-o") + 1] == "name":
            return SimpleNamespace(stdout="")
        return SimpleNamespace(
            stdout='{"items":[{"kind":"'
            + kind
            + '","metadata":{"name":"foreign","labels":{"app.kubernetes.io/managed-by":"sregym-composite-probe"}}}]}'
        )

    monkeypatch.setattr(clean_start, "kubectl", api)
    with pytest.raises(RuntimeError, match="benchmark objects"):
        clean_start.assert_clean_baseline(Path("unused"), namespace="any-app")
