"""Oracle infrastructure comes from the pinned harness, outside the application."""

import subprocess
from pathlib import Path

import pytest

from benchmarks.sregym.adapter.oracle_observability import prepare_oracle_observability


def _harness(tmp_path: Path) -> Path:
    observer = tmp_path / "harness/sregym/observer"
    observer.mkdir(parents=True)
    return tmp_path / "harness"


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

    harness = _harness(tmp_path)
    prepare_oracle_observability(kubeconfig=tmp_path / "kube", sregym_dir=harness, cluster="lane", run=run)
    helm = next(c for c in calls if c[0] == "helm")
    assert f"{harness}/sregym/observer/prometheus/prometheus" in helm
    assert not any("dependency" in c or "latest" in str(c) for c in calls)
    assert any("otel-collector.yaml" in str(c) for c in calls)
    assert any("jaeger.yaml" in str(c) for c in calls)


def test_uncapped_nodes_fail_before_install(tmp_path: Path):
    def run(argv, **kwargs):
        out = "node\n" if argv[0] == "kind" else "0\n"
        return subprocess.CompletedProcess(argv, 0, out, "")

    with pytest.raises(ValueError, match="capped"):
        prepare_oracle_observability(
            kubeconfig=tmp_path / "kube", sregym_dir=_harness(tmp_path), cluster="lane", run=run
        )


def test_an_unchecked_out_harness_fails_before_any_cluster_command(tmp_path: Path):
    calls = []

    def run(argv, **kwargs):
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 0, "", "")

    with pytest.raises(ValueError, match="submodule is not checked out"):
        prepare_oracle_observability(
            kubeconfig=tmp_path / "kube", sregym_dir=tmp_path / "empty", cluster="lane", run=run
        )
    assert calls == []


def test_a_failed_command_reports_its_stderr(tmp_path: Path):
    def run(argv, **kwargs):
        if argv[0] == "kubectl" and "prometheus-pvc.yml" in str(argv):
            raise subprocess.CalledProcessError(1, argv, "", "error: the path does not exist")
        out = "node\n" if argv[0] == "kind" else "1000000000\n"
        return subprocess.CompletedProcess(argv, 0, out, "")

    with pytest.raises(RuntimeError, match="the path does not exist"):
        prepare_oracle_observability(
            kubeconfig=tmp_path / "kube", sregym_dir=_harness(tmp_path), cluster="lane", run=run
        )
