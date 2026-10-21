"""Fast cleanup of completed read-only evidence transport in a dedicated toy lane."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from benchmarks.sregym.adapter import KubectlClusterOps, kubectl

if TYPE_CHECKING:
    from pathlib import Path

    from benchmarks.sregym.adapter import RuntimeConfig


class ToyClusterOps(KubectlClusterOps):
    def __init__(self, kubeconfig: Path) -> None:
        self.kubeconfig = kubeconfig

    def export_runtime_artifacts(self, config: RuntimeConfig, artifacts_dir: Path) -> dict[str, str | None]:
        result = super().export_runtime_artifacts(config, artifacts_dir)
        # The production exporter has completed all reads and requested deletion.
        # Its sleeping transport Pod otherwise delays the next export by 30 s.
        pod = kubectl(
            ["get", "pod", "sdo-repository-sync", "-o", "json"],
            namespace=config.control_namespace,
            kubeconfig=self.kubeconfig,
            check=False,
        )
        if pod.returncode == 0 and json.loads(pod.stdout).get("metadata", {}).get("deletionTimestamp"):
            kubectl(
                ["delete", "pod", "sdo-repository-sync", "--grace-period=0", "--force", "--wait=false"],
                namespace=config.control_namespace,
                kubeconfig=self.kubeconfig,
                check=False,
            )
        return result
