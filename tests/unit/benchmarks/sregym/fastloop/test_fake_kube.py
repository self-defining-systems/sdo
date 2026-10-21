from __future__ import annotations

import json
import subprocess
from typing import TYPE_CHECKING

from benchmarks.sregym.fastloop.fake_kube import FakeKubernetesAPIServer

if TYPE_CHECKING:
    from pathlib import Path


def test_kubectl_reads_captured_snapshot_without_a_cluster(tmp_path: Path) -> None:
    snapshot = {
        "namespace": "hotel",
        "services": [
            {"metadata": {"name": "front", "namespace": "hotel"}, "spec": {"ports": [{"port": 80, "targetPort": 9999}]}}
        ],
    }
    with FakeKubernetesAPIServer(snapshot) as server:
        kubeconfig = server.write_kubeconfig(tmp_path / "kubeconfig")
        result = subprocess.run(
            ["kubectl", "--kubeconfig", str(kubeconfig), "-n", "hotel", "get", "service", "front", "-o", "json"],
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        )

    assert json.loads(result.stdout)["spec"]["ports"][0]["targetPort"] == 9999
