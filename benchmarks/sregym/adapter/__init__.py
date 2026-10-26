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
    from benchmarks.sregym.adapter.eval_guards import (
        Deadline,
        assert_stable_workloads,
        closure_pending,
        controller_proof,
        qualified_scenarios,
        setup_lineage,
        wait_for_proof,
    )
    from benchmarks.sregym.adapter.healthy_baseline import HealthyBaselineCaptureOps, kubectl_capture_ops
    from benchmarks.sregym.adapter.image_entrypoints import (
        BENCHMARK_IMAGE_ENTRY_POINTS,
        assert_image_entrypoints,
        image_entrypoint_script,
    )
    from benchmarks.sregym.adapter.image_retention import ImageRetentionError, retain_lane_images
    from benchmarks.sregym.adapter.lane_resources import (
        LaneResourcePolicy,
        assert_host_disk_headroom,
        assert_host_headroom,
        assert_lane_resources,
    )
    from benchmarks.sregym.adapter.native_operator import (
        NativeInstallConfig,
        assert_shared_repository,
        execute_operator,
        inspect_existing,
        install_existing,
        reset_control_state,
        stop_runtime,
        sync_existing,
    )
    from benchmarks.sregym.adapter.oracle_observability import prepare_oracle_observability, route_astronomy_traces
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
        pause_for_recovery,
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
    from benchmarks.sregym.adapter.scripted_replay import replay_recorded_repair
    from benchmarks.sregym.adapter.scripted_replay_catalog import SCRIPTED_REPLAY_SLOTS

_EXPORTS: dict[str, str] = {
    "BENCHMARK_IMAGE_ENTRY_POINTS": "image_entrypoints",
    "assert_image_entrypoints": "image_entrypoints",
    "image_entrypoint_script": "image_entrypoints",
    "NativeInstallConfig": "native_operator",
