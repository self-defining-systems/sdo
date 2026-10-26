"""Run model-free observation conformance under the installed controller identity."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

    from benchmarks.sregym.fastloop.environment import FastloopEnvironment


def check_observations(environment: FastloopEnvironment, directory: Path) -> dict[str, Any]:
    from benchmarks.sregym.adapter import kubectl

    result = kubectl(
        [
            "exec",
            "job/sdo-controller-run",
            "-c",
            "controller",
            "--",
            "/usr/local/bin/sdo-observation-check",
            "--namespace",
            environment.namespace,
        ],
        namespace=environment.namespace + "-sdo",
        kubeconfig=environment.kubeconfig,
        check=False,
    )
    try:
        report = json.loads(result.stdout)
    except (ValueError, TypeError) as exc:
        raise ValueError(
            "SDO: observation conformance returned no valid report; check installed probe/transport"
        ) from exc
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "observation-conformance.json"
    path.write_text(json.dumps(report, indent=2) + "\n")
    if not isinstance(report, dict) or not isinstance(report.get("observations"), list):
        raise ValueError("SDO: observation conformance returned an invalid report")
    failures = [row for row in report["observations"] if row.get("passed") is not True]
    if result.returncode or report.get("passed") is not True or failures:
        causes = [
            f"{row.get('primitive')} {row.get('target', {}).get('pod', {}).get('name')}/"
            f"{row.get('target', {}).get('container')}: {row.get('failure_stage')}"
            for row in failures
        ]
        raise ValueError("SDO: observation conformance failed: " + ("; ".join(causes) or "probe/transport"))
    return {"passed": True, "evidence": str(path), "observations": len(report["observations"])}
