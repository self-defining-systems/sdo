"""Restart a reset toy lane from its trusted, production-installed Job template."""

from __future__ import annotations

import json
import subprocess
from copy import deepcopy
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from apps.toy.faults import command
from benchmarks.sregym.adapter import PersistentState, runtime_config_from_payload
from benchmarks.sregym.fastloop.toy import record_controller, wait_for_baseline

if TYPE_CHECKING:
    from benchmarks.sregym.adapter import RuntimeConfig
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment


def restart_job(job: dict[str, Any]) -> dict[str, Any]:
    """Preserve the installed runtime/security spec; let Kubernetes assign new identity."""
    result = deepcopy(job)
    result.pop("status", None)
    result["metadata"] = {
        key: value for key, value in result["metadata"].items() if key in {"name", "namespace", "labels", "annotations"}
    }
    result["spec"].pop("selector", None)
    result["spec"].pop("manualSelector", None)
    labels = result["spec"]["template"].get("metadata", {}).get("labels", {})
    for key in ("controller-uid", "job-name", "batch.kubernetes.io/controller-uid", "batch.kubernetes.io/job-name"):
        labels.pop(key, None)
    return result


@dataclass(frozen=True)
class ToyControllerTemplate:
    job: dict[str, Any]
    config: RuntimeConfig

    @classmethod
    def capture(cls, environment: FastloopEnvironment) -> ToyControllerTemplate | None:
        record = PersistentState.load(environment.state_path).controllers.get(environment.namespace)
        if record is None:
            return None
        job = json.loads(
            command(
                str(environment.kubeconfig), record.control_namespace, "get", "job", "sdo-controller-run", "-o", "json"
            )
        )
        return cls(restart_job(job), runtime_config_from_payload(record.runtime_config))

    def restart(self, environment: FastloopEnvironment) -> None:
        clear_runtime_state(environment, self.config.control_namespace)
        subprocess.run(
            ["kubectl", "--kubeconfig", str(environment.kubeconfig), "create", "-f", "-"],
            input=json.dumps(self.job),
            text=True,
            check=True,
            capture_output=True,
        )
        wait_for_baseline(environment, environment.seed_commit)
        record_controller(environment, self.config)


def clear_runtime_state(environment: FastloopEnvironment, control: str) -> None:
    """Discard state only after reset has stopped every controller and worker."""
    command(
        str(environment.kubeconfig),
        control,
        "delete",
        "configmap",
        "sdo-controller-state",
        "sdo-controller-maintenance",
        "sdo-controller-state-drift-verdicts",
        "--ignore-not-found=true",
    )
    # The old Job has stopped; its 60-second leadership lease must not delay this restart.
    command(str(environment.kubeconfig), control, "delete", "lease", "sdo-controller", "--ignore-not-found=true")
