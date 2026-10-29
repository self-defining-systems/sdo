"""Kubernetes shell for the benchmark-owned warm-lane transaction."""

from __future__ import annotations

import json
from dataclasses import replace
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from benchmarks.sregym.adapter import (
    PersistentState,
    allowed_namespaces,
    incident_work_open,
    kubectl,
    native_install_config,
    sync_existing,
)
from benchmarks.sregym.fastloop.accepted_source import AcceptedSource
from benchmarks.sregym.fastloop.admission import LaneAdmission
from benchmarks.sregym.fastloop.clean_start import assert_clean_baseline
from benchmarks.sregym.fastloop.reset import RestoreChecks, require_terminal_reflection, restore_application
from benchmarks.sregym.fastloop.warm_lane import LaneIdentity, WarmLaneBusy
from benchmarks.sregym.fastloop.wipe import (
    AttemptMarker,
    KubectlLaneCluster,
    LaneInjectionState,
    harness_leftovers,
    inventory_path,
    load_inventory,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime
    from pathlib import Path

    from benchmarks.sregym.adapter import ClusterOps, RuntimeConfig
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment
    from benchmarks.sregym.fastloop.reset import HealthWorker


class WarmEnvironment:
    def __init__(
        self,
        environment: FastloopEnvironment,
        ops: ClusterOps,
        worker: HealthWorker,
        config: RuntimeConfig,
        directory: Path,
        *,
        incoming_data_faults: tuple[str, ...],
        full_reset: Callable[[], None],
        planned_commit: str | None = None,
    ) -> None:
        self.environment, self.ops, self.worker = environment, ops, worker
        self.config, self.directory = replace(config, reuse_existing=True), directory
        self.incoming_data_faults = incoming_data_faults
        self._full_reset = full_reset
        self.planned_commit = planned_commit
        self.control = config.control_namespace
        self.generation = "warm-" + uuid4().hex
        self.admission = LaneAdmission(
            environment,
            ops,
            directory / "admission",
            install_config=native_install_config(self.config),
            code_path=config.code_path,
        )

    def _head(self) -> str:
        # The installed controller owns this fixed accepted checkout. No worker
        # worktree or host mirror is authority for its current revision.
        return kubectl(
            [
                "exec",
                "job/sdo-controller-run",
                "-c",
                "controller",
                "--",
                "git",
                "-C",
                "/workspace/application",
                "rev-parse",
                "HEAD",
            ],
            namespace=self.control,
            kubeconfig=self.environment.kubeconfig,
        ).stdout.strip()

    def inspect(self) -> LaneIdentity:
        env = self.environment
        if env.shared_accepted_id:
            raise ValueError("warm transaction currently requires a private accepted repository")
        if self.incoming_data_faults or LaneInjectionState(env.kubeconfig, env.namespace).reason() is not None:
            raise ValueError("warm transaction cannot prove data-plane isolation")
        if AttemptMarker(env.run_dir).pending() is not None:
            raise ValueError("previous attempt did not finish cleanly")
        try:
            require_terminal_reflection(env)
        except RuntimeError as exc:
            raise WarmLaneBusy("prior native reflection still owns the lane") from exc
        record = PersistentState.load(env.state_path).controllers.get(env.namespace)
        pod = self.ops.controller_pod(self.control)
        if record is None or pod is None or record.controller_pod_uid != pod.uid:
            raise ValueError("warm lane has no recorded live controller")
        state = self.ops.runtime_state(self.control)
        if not state or incident_work_open(state):
            raise WarmLaneBusy("warm lane has unclosed controller work")
        if not self.ops.install(self.config):
            raise ValueError("warm controller settings cannot be reused")
        standby = json.loads(
            kubectl(
                ["get", "pods", "-l", "sdo.dev/standby=true", "-o", "json"],
                namespace=self.control,
                kubeconfig=env.kubeconfig,
            ).stdout
        )["items"]
        running = [
            p
            for p in standby
            if p.get("status", {}).get("phase") == "Running" and not p["metadata"].get("deletionTimestamp")
        ]
        if len(running) != 1:
            raise ValueError("warm lane requires exactly one live standby responder")
        self._clean_scope()
        return LaneIdentity(pod.uid, running[0]["metadata"]["uid"], self._head())

    def _clean_scope(self) -> None:
        env = self.environment
        inventory = load_inventory(inventory_path(env.kubeconfig))
        if inventory is None:
            raise ValueError("warm lane has no installed cluster inventory")
        leftovers = harness_leftovers(
            KubectlLaneCluster(env.kubeconfig).list_objects(),
            inventory,
            keep_namespaces=allowed_namespaces({env.namespace}),
            control_namespace="",
        )
        if leftovers:
            raise ValueError("warm lane has resources outside its installed inventory")
        assert_clean_baseline(env.kubeconfig, namespace=env.namespace)

    def pause(self) -> None:
        self.ops.set_maintenance(self.control, paused=True, generation=self.generation)
        # Sync back the accepted checkout, never seed or change it. Restoration
        # must use the accepted render, not the prior row's stale host mirror.
        sync_existing(native_install_config(self.config))
        if self.planned_commit and AcceptedSource.of(self.environment).revision != self.planned_commit:
            raise ValueError("requested revision differs from current accepted memory")

    def reset_changed(self) -> None:
        from benchmarks.sregym.fastloop.baseline_recovery import KubectlApplicationCluster

        checks = RestoreChecks(self.environment)
        checks.validate()
        restore_application(
            self.environment,
            KubectlApplicationCluster(self.environment.kubeconfig),
            self.worker,
            checks=checks,
        )

    def adopt(self) -> str:
        revision = self._head()
        if AcceptedSource.of(self.environment).revision != revision:
            raise ValueError("accepted memory changed during restoration")
        return revision

    def prove(self, not_before: datetime, revision: str) -> dict[str, Any]:
        self.ops.set_maintenance(self.control, paused=False, generation=self.generation + "-proof")
        proof = self.admission.prove(self.control)
        # The existing admission requires all detector samples after its own
        # boundary (later than not_before). Require the controller to have
        # evaluated this exact revision, not merely mounted its Git checkout.
        state = self.ops.runtime_state(self.control)
        if state.get("setup_evaluated_revision") != revision:
            raise ValueError("controller has not evaluated the accepted revision")
        return proof

    def full_reset(self) -> None:
        self._full_reset()
