"""Oracle infrastructure comes from the pinned harness, outside the application."""

import json
import subprocess
from pathlib import Path

import pytest

from benchmarks.sregym.adapter.oracle_observability import prepare_oracle_observability


def test_installs_vendored_stack_without_dependency_updates(tmp_path: Path):
    calls = []

    def run(argv, **kwargs):
        calls.append(argv)
        out = "{}"
        if argv[:3] == ["kind", "get", "nodes"]:
            out = "lane-control-plane\nlane-worker\n"
        elif argv[:2] == ["docker", "inspect"]:
            out = "3000000000\n3000000000\n"
        return subprocess.CompletedProcess(argv, 0, out, "")

    prepare_oracle_observability(kubeconfig=tmp_path / "kube", sregym_dir=Path("/harness"), cluster="lane", run=run)
    helm = next(c for c in calls if c[0] == "helm")
    assert "/harness/sregym/observer/prometheus/prometheus" in helm
    assert not any("dependency" in c or "latest" in str(c) for c in calls)
    assert any("otel-collector.yaml" in str(c) for c in calls)
    assert any("jaeger.yaml" in str(c) for c in calls)


def test_uncapped_nodes_fail_before_install(tmp_path: Path):
    def run(argv, **kwargs):
        out = "node\n" if argv[0] == "kind" else "0\n"
        return subprocess.CompletedProcess(argv, 0, out, "")

    with pytest.raises(ValueError, match="capped"):
        prepare_oracle_observability(kubeconfig=tmp_path / "kube", sregym_dir=Path("/harness"), cluster="lane", run=run)


def test_trace_routing_keeps_existing_external_name_services(tmp_path: Path):
    from benchmarks.sregym.adapter import route_astronomy_traces

    calls = []

    def run(argv, **kwargs):
        calls.append(argv)
        return subprocess.CompletedProcess(
            argv,
            0,
            json.dumps({"spec": {"type": "ExternalName", "externalName": "otel-collector.observe.svc.cluster.local"}}),
            "",
        )

    route_astronomy_traces(tmp_path / "kube", run=run)
    assert not any("delete" in c and "service" in c for c in calls)
    assert not any("create" in c for c in calls)
