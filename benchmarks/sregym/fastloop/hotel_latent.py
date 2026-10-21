"""Prepare the historical hotel seed and explicit imported-Service sweep trials."""

from __future__ import annotations

import argparse
import copy
import logging
import subprocess
import time
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

from benchmarks.sregym.adapter import kubectl
from benchmarks.sregym.fastloop.baseline_recovery import ApplicationBaseline, load_application_baseline
from benchmarks.sregym.fastloop.environment import FastloopEnvironment

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from benchmarks.sregym.fastloop.fault_tracker import FaultSpec

TRACING_SERVICE = "kubernetes/jaeger/jaeger-service.yaml"


def align_tracing_alias(workspace: Path, baseline: ApplicationBaseline) -> bool:
    aliases = [
        value
        for value in baseline.objects
        if value.get("kind") == "Service" and value.get("metadata", {}).get("name") == "jaeger"
    ]
    if len(aliases) != 1 or aliases[0].get("spec", {}).get("type") != "ExternalName":
        raise ValueError("hotel latent seed requires the captured healthy Jaeger ExternalName alias")
    alias = copy.deepcopy(aliases[0])
    if not alias["spec"].get("externalName"):
        raise ValueError("Jaeger alias requires externalName")
    target = workspace / TRACING_SERVICE
    prior = yaml.safe_load(target.read_text(encoding="utf-8"))
    if prior.get("kind") != "Service" or prior.get("metadata", {}).get("name") != "jaeger":
        raise ValueError("expected the existing Jaeger Service source manifest")
    if prior == alias:
        return False
    target.write_text(yaml.safe_dump(alias, sort_keys=False), encoding="utf-8")
    return True


def sweep_preparer(
    services: tuple[str, ...],
    catalog: Mapping[str, tuple[FaultSpec, ...]],
    *,
    namespace: str,
    kubeconfig: Path,
    sleep: Callable[[float], None] = time.sleep,
) -> Callable[[str], None]:
    targets = {
        problem_id: faults[0].component
        for problem_id, faults in catalog.items()
        if len(faults) == 1 and faults[0].kind == "target_port" and faults[0].component in services
    }
    if set(services) != set(targets.values()):
        raise ValueError("sweep Services must be standalone targetPort targets in the stream")

    def prepare(problem_id: str) -> None:
        service = targets.get(problem_id)
        if service is None:
            return
        kubectl(
            ["patch", "service", service, "--type=merge", "-p", '{"metadata":{"managedFields":[{}]}}'],
            namespace=namespace,
            kubeconfig=kubeconfig,
        )
        # Adoption runs at startup and after accepted commits. Prepare only at
        # the injection boundary, and let the informer see the healthy object.
        sleep(1)
        logging.getLogger(__name__).info("prepared imported Service/%s for periodic sweep", service)

    return prepare


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    args = parser.parse_args()
    environment = FastloopEnvironment.load(args.run_dir.resolve())
    baselines = load_application_baseline(environment.baseline_path, environment.namespace) or ()
    baseline = next(item for item in baselines if item.namespace == environment.namespace)
    if align_tracing_alias(environment.workspace, baseline):
        subprocess.run(["git", "-C", str(environment.workspace), "add", TRACING_SERVICE], check=True)
        subprocess.run(
            ["git", "-C", str(environment.workspace), "commit", "-qm", "fix: align tracing source with hotel baseline"],
            check=True,
        )
    environment.seed_commit = subprocess.check_output(
        ["git", "-C", str(environment.workspace), "rev-parse", "HEAD"], text=True
    ).strip()
    environment.save()
    print(f"prepared hotel seed {environment.seed_commit}: structural health unchanged")


if __name__ == "__main__":
    main()
