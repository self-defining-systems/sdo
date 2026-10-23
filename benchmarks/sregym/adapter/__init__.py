"""SDO lifecycle adapter for Codex-backed SREGym incidents.

The adapter's modules also run as entry points (``python -m
benchmarks.sregym.adapter.persistent``, and ``...adapter.submission`` inside the
responder image), so the facade loads a module only when a caller asks for one
of its names.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from benchmarks.sregym.adapter.cluster_resources import kubectl
    from benchmarks.sregym.adapter.driver import (
        DeployedLifecycle,
        DeployedLifecycleContext,
        deployed_lifecycle,
        launch_native_lifecycle,
        publish_agent_registrations,
    )
    from benchmarks.sregym.adapter.healthy_baseline import HealthyBaselineCaptureOps, kubectl_capture_ops
    from benchmarks.sregym.adapter.native_operator import execute_operator, inspect_existing, install_existing
    from benchmarks.sregym.adapter.persistent import (
        REJECTED_RECEIPT_FILENAME,
        STRICT_RECEIPT_FILENAME,
        Clock,
        ClosureFailedError,
        ClusterOps,
        ControllerRecord,
        DetectionMissError,
        KubectlClusterOps,
        PersistentControllerError,
        PersistentState,
        StageInputs,
        collect_followup_incident,
        control_namespace_for,
        drain_pending_incident,
        pause_controller,
        run_persistent_stage,
        runtime_config_from_payload,
        runtime_config_payload,
        teardown,
        wait_for_maintenance_ack,
    )
    from benchmarks.sregym.adapter.processes import bind_state
    from benchmarks.sregym.adapter.runtime import (
        ControllerInstallError,
        RuntimeConfig,
        native_install_config,
    )

_EXPORTS: dict[str, str] = {
    "bind_state": "processes",
    "runtime_config_payload": "persistent",
    "runtime_config_from_payload": "persistent",
    "ControllerRecord": "persistent",
    "install_existing": "native_operator",
    "publish_agent_registrations": "driver",
    "DeployedLifecycle": "driver",
    "DeployedLifecycleContext": "driver",
    "deployed_lifecycle": "driver",
    "launch_native_lifecycle": "driver",
    "native_install_config": "runtime",
    "ControllerInstallError": "runtime",
    "execute_operator": "native_operator",
    "inspect_existing": "native_operator",
    "kubectl": "cluster_resources",
    "REJECTED_RECEIPT_FILENAME": "persistent",
    "STRICT_RECEIPT_FILENAME": "persistent",
    "Clock": "persistent",
    "ClusterOps": "persistent",
    "ClosureFailedError": "persistent",
    "DetectionMissError": "persistent",
    "PersistentControllerError": "persistent",
    "wait_for_maintenance_ack": "persistent",
    "KubectlClusterOps": "persistent",
    "PersistentState": "persistent",
    "StageInputs": "persistent",
    "control_namespace_for": "persistent",
    "collect_followup_incident": "persistent",
    "drain_pending_incident": "persistent",
    "pause_controller": "persistent",
    "run_persistent_stage": "persistent",
    "teardown": "persistent",
    "RuntimeConfig": "runtime",
    "HealthyBaselineCaptureOps": "healthy_baseline",
    "kubectl_capture_ops": "healthy_baseline",
}

__all__ = [
    "bind_state",
    "runtime_config_payload",
    "runtime_config_from_payload",
    "ControllerRecord",
    "install_existing",
    "publish_agent_registrations",
    "REJECTED_RECEIPT_FILENAME",
    "STRICT_RECEIPT_FILENAME",
    "Clock",
    "ClusterOps",
    "ClosureFailedError",
    "DetectionMissError",
    "DeployedLifecycle",
    "DeployedLifecycleContext",
    "KubectlClusterOps",
    "PersistentControllerError",
    "PersistentState",
    "RuntimeConfig",
    "StageInputs",
    "HealthyBaselineCaptureOps",
    "kubectl_capture_ops",
    "control_namespace_for",
    "deployed_lifecycle",
    "launch_native_lifecycle",
    "native_install_config",
    "ControllerInstallError",
    "execute_operator",
    "inspect_existing",
    "kubectl",
    "collect_followup_incident",
    "drain_pending_incident",
    "pause_controller",
    "run_persistent_stage",
    "teardown",
    "wait_for_maintenance_ack",
]


def __getattr__(name: str) -> Any:
    """Import the owning adapter module on first use of one of its public names."""

    module = _EXPORTS.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(importlib.import_module(f"{__name__}.{module}"), name)
    globals()[name] = value
    return value
