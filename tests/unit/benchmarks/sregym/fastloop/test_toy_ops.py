"""A completed evidence export need not wait out its transport Pod's sleep grace."""

import json
from subprocess import CompletedProcess
from types import SimpleNamespace

import pytest

from benchmarks.sregym.adapter.persistent import KubectlClusterOps
from benchmarks.sregym.fastloop.toy_ops import ToyClusterOps


@pytest.mark.parametrize("deleting", [True, False])
def test_only_retired_sync_pods_are_removed_after_real_export(tmp_path, monkeypatch, deleting):
    calls = []
    artifact = {"directory": "real-export"}
    monkeypatch.setattr(KubectlClusterOps, "export_runtime_artifacts", lambda *args: artifact)

    def kubectl(args, **kwargs):
        calls.append(args)
        metadata = {"deletionTimestamp": "2026-10-21T00:00:00Z"} if deleting else {}
        return CompletedProcess(args, 0, json.dumps({"metadata": metadata}), "")

    monkeypatch.setattr("benchmarks.sregym.fastloop.toy_ops.kubectl", kubectl)
    result = ToyClusterOps(tmp_path / "kubeconfig").export_runtime_artifacts(
        SimpleNamespace(control_namespace="sdo-toy-sdo"), tmp_path
    )
    assert result is artifact
    assert any(args[0] == "delete" for args in calls) is deleting
