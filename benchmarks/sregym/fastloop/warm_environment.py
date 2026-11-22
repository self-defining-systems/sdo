"""Kubernetes shell for the benchmark-owned warm-lane transaction."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import replace
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from benchmarks.devswap.controller import SWAP_STANDBY_WAIT_S, release_leader_lease, wait_standby
from benchmarks.devswap.kube import Kubectl
from benchmarks.devswap.observability import await_observability, restart_observability
from benchmarks.sregym.adapter import (
    PersistentState,
    WaitKind,
    allowed_namespaces,
    controller_settings,
    deployed_lifecycle,
    incident_work_open,
    install_existing,
    kubectl,
    native_install_config,
    pause_for_recovery,
    reset_control_state,
    runtime_config_payload,
    stop_runtime,
    sync_existing,
    wait_for,
)
from benchmarks.sregym.fastloop.accepted_source import AcceptedSource
from benchmarks.sregym.fastloop.admission import LaneAdmission
from benchmarks.sregym.fastloop.clean_start import assert_clean_baseline
from benchmarks.sregym.fastloop.reseed import ReseedSteps, reseed_lane
from benchmarks.sregym.fastloop.reset import (
    KubectlResetCluster,
    RestoreChecks,
    is_worker_record,
    require_terminal_reflection,
    restore_application,
)
from benchmarks.sregym.fastloop.resident_start import INSTALL_CONFIGMAPS
from benchmarks.sregym.fastloop.warm_lane import LaneIdentity, WarmLaneBusy, WarmLaneRefusal
from benchmarks.sregym.fastloop.wipe import (
    AttemptMarker,
    KubectlLaneCluster,
    LaneInjectionState,
    harness_leftovers,
    inventory_path,
    load_inventory,
    reseed_workspace,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime
    from pathlib import Path

    from benchmarks.sregym.adapter import ClusterOps, RuntimeConfig
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment
    from benchmarks.sregym.fastloop.reset import HealthWorker


logger = logging.getLogger(__name__)

#: Settings that belong to one row and are applied again by every reseed (the controller Job is installed anew from the
#: row's own configuration): this row's evidence run and the path of its config overlay. They are not install identity.
PER_ROW_SETTINGS = frozenset({"evidence_run_id", "config_files"})


def _session_state(configmap: dict[str, Any]) -> bool:
    """Whether a control-namespace ConfigMap is an earlier row's incident, session, reflection or worker record: an
    SDO-managed ConfigMap that is none of the ones every install has (the fresh-install comparison names the rest)."""
    metadata = configmap.get("metadata") or {}
    name = str(metadata.get("name", ""))
    if name in INSTALL_CONFIGMAPS:
        return False
    return is_worker_record(configmap) or (
        name.startswith("sdo-") and (metadata.get("labels") or {}).get("app.kubernetes.io/managed-by") == "sdo"
    )


def identity_settings(payload: dict[str, Any]) -> dict[str, Any]:
    """What must be the same for a lane's install to serve a row: the controller's settings but the per-row ones."""
    return {key: value for key, value in controller_settings(payload).items() if key not in PER_ROW_SETTINGS}


def planned_application_unchanged(environment: Any, planned_commit: str | None) -> None:
    """A warm lane keeps its app only while it is the deploy of the planned source; otherwise fall back."""
    from benchmarks.sregym.fastloop.deployed_application import deployed_application_change

    changed = deployed_application_change(environment, planned_commit or "HEAD")
    if changed is not None:
        raise WarmLaneRefusal(
            f"warm lane application is not the planned deploy: {changed.trigger.value} ({changed.detail})"
        )


class WarmEnvironment:
    #: Set when the reseed began: the controller may be stopped and its state gone, so the ordinary idle check (which
    #: would call that "unknown controller work") no longer describes the lane.
    _reseeding = False
    #: Digest of the install identity the lane held when the transaction began (what the row is held against).
    _installed_digest = ""

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

    def _assert_idle(self) -> None:
        try:
            require_terminal_reflection(self.environment)
            state = self.ops.runtime_state(self.control)
            if incident_work_open(state) or (not state and self.ops.controller_pod(self.control) is not None):
                raise WarmLaneBusy("warm lane has unclosed or unknown controller work")
        except WarmLaneBusy:
            raise
        except Exception:
            raise WarmLaneBusy("prior native incident ownership could not be checked") from None

    def inspect(self) -> LaneIdentity:
        env = self.environment
        self._assert_idle()
        if env.shared_accepted_id:
            raise WarmLaneRefusal("warm transaction currently requires a private accepted repository")
        injected = LaneInjectionState(env.kubeconfig, env.namespace).reason()
        if self.incoming_data_faults or injected is not None:
            why = "; ".join(self.incoming_data_faults) or (injected.detail if injected else "")
            raise WarmLaneRefusal(f"warm transaction cannot prove data-plane isolation: {why}")
        if (unclean := AttemptMarker(env.run_dir).pending()) is not None:
            raise WarmLaneRefusal(f"previous attempt did not finish cleanly: {unclean.detail}")
        planned_application_unchanged(env, self.planned_commit)
        record = PersistentState.load(env.state_path).controllers.get(env.namespace)
        pod = self.ops.controller_pod(self.control)
        if record is None or pod is None or record.controller_pod_uid != pod.uid:
            raise WarmLaneRefusal("warm lane has no recorded live controller")
        from benchmarks.sregym.fastloop.setup_proof import digest

        installed = identity_settings(record.runtime_config)
        if installed != identity_settings(runtime_config_payload(self.config)):
            raise WarmLaneRefusal("warm controller settings differ from the requested stage")
        self._installed_digest = digest(installed)
        if record.lifecycle_fingerprint != deployed_lifecycle(env.namespace).fingerprint:
            raise WarmLaneRefusal("warm controller topology differs from the requested stage")
        if not self.ops.install(self.config):
            raise WarmLaneRefusal("warm controller settings cannot be reused")
        standby = self._standby_uid()
        self._clean_scope()
        return LaneIdentity(pod.uid, standby, self._head())

    def _standby_uid(self) -> str:
        pods = json.loads(
            kubectl(
                ["get", "pods", "-l", "sdo.dev/standby=true", "-o", "json"],
                namespace=self.control,
                kubeconfig=self.environment.kubeconfig,
            ).stdout
        )["items"]
        running = [
            p
            for p in pods
            if p.get("status", {}).get("phase") == "Running" and not p["metadata"].get("deletionTimestamp")
        ]
        if len(running) != 1:
            raise WarmLaneRefusal("warm lane requires exactly one live standby responder")
        return str(running[0]["metadata"]["uid"])

    def reidentify(self) -> LaneIdentity:
        """The controller and standby running now, without ``inspect``'s idle checks (a new controller is not idle)."""
        pod = self.ops.controller_pod(self.control)
        if pod is None:
            raise WarmLaneRefusal("warm lane has no live controller")
        return LaneIdentity(pod.uid, self._standby_uid(), self._head())

    def _clean_scope(self) -> None:
        env = self.environment
        inventory = load_inventory(inventory_path(env.kubeconfig))
        if inventory is None:
            raise WarmLaneRefusal("warm lane has no installed cluster inventory")
        leftovers = harness_leftovers(
            KubectlLaneCluster(env.kubeconfig, env.host_owned_kinds).list_objects(),
            inventory,
            keep_namespaces=allowed_namespaces({env.namespace}),
            control_namespace="",
        )
        if leftovers:
            raise WarmLaneRefusal("warm lane has resources outside its installed inventory")
        assert_clean_baseline(env.kubeconfig, namespace=env.namespace)

    def pause(self) -> None:
        self._assert_idle()
        pause_for_recovery(self.ops, self.control)
        if self.planned_commit:
            # The row's own memory: the application below is restored from this source, and the reseed seeds the
            # controller's volume from it. The earlier row's memory on the volume is replaced there, not read here.
            reseed_workspace(self.environment.workspace, self.planned_commit)
        else:
            # No planned memory: the lane keeps what it holds. Sync back the accepted checkout, never seed or change
            # it; restoration must use the accepted render, not the prior row's stale host mirror.
            sync_existing(native_install_config(self.config))
        from benchmarks.sregym.fastloop.setup_proof import RECEIPTS, collect_fingerprint
        from benchmarks.sregym.fastloop.warm_scope import check_cluster_scope

        check_cluster_scope(
            self.environment.run_dir / RECEIPTS, collect_fingerprint(self.environment), self.environment.namespace
        )

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

    def _kube(self) -> Kubectl:
        return Kubectl("", str(self.environment.kubeconfig))

    def reseed(self) -> None:
        """Swap the install's state for a fresh install's at the planned memory (see ``reseed.py`` for the order).

        The Job, namespace, volume, credentials, RBAC, transport and observability Deployments stay; the controller
        process does not: Go ``install-existing`` replaces the Job with this row's configuration and evidence run and
        seeds the volume from the host copy of the accepted repository, which the transaction put at the planned commit.
        """
        self._reseeding = True
        seconds = reseed_lane(self._reseed_steps())
        self.directory.mkdir(parents=True, exist_ok=True)
        (self.directory / "reseed-phases.json").write_text(json.dumps(seconds, indent=2) + "\n")

    def _reseed_steps(self) -> ReseedSteps:
        env, control = self.environment, self.control
        kube = self._kube()

        def settle_evidence() -> None:
            from benchmarks.sregym.fastloop.evidence_queue import settle_lane_exports
            from benchmarks.sregym.fastloop.evidence_sink import settle_lane_evidence

            settle_lane_exports(env)
            if failures := settle_lane_evidence(env):
                raise ValueError("the earlier row's evidence is not copied out: " + "; ".join(failures))

        def stop_controller(head: str) -> None:
            KubectlResetCluster(env.kubeconfig, control).reset_controller(head, preserve_repository=False)
            stop_runtime(control, kubeconfig=str(env.kubeconfig))
            # No controller process is left, so nobody holds the leader Lease for real; the next controller would
            # otherwise wait out the rest of its 60 s (devswap measured 41-60 s of every restart).
            release_leader_lease(kube, control)

        def install_controller() -> None:
            install_existing(native_install_config(replace(self.config, reuse_existing=False)))

        return ReseedSteps(
            settle_evidence=settle_evidence,
            read_head=self._head,
            stop_controller=stop_controller,
            retire_control_state=self._retire_control_state,
            fresh_observability=lambda: restart_observability(kube, control),
            install_controller=install_controller,
            wait_started=self._wait_started,
            record_controller=self._record_controller,
            await_observability=lambda: await_observability(kube, control),
        )

    def _retire_control_state(self) -> None:
        """Delete every record of the earlier row's controller: Go's control state (controller state, onboarding,
        maintenance, holds), then the incident, reflection and worker ConfigMaps a fresh install does not have."""
        env = self.environment
        reset_control_state(self.control, kubeconfig=str(env.kubeconfig))
        listing = json.loads(
            kubectl(["get", "configmaps", "-o", "json"], namespace=self.control, kubeconfig=env.kubeconfig).stdout
            or "{}"
        )
        leftovers = sorted(item["metadata"]["name"] for item in listing.get("items", []) if _session_state(item))
        if leftovers:
            kubectl(
                ["delete", "configmap", *leftovers, "--ignore-not-found=true"],
                namespace=self.control,
                kubeconfig=env.kubeconfig,
            )

    def _wait_started(self) -> None:
        """The new controller is running and its responder standby is ready (the controller starts it)."""
        deadline = time.monotonic() + wait_for(WaitKind.ROLLOUT)
        while self.ops.controller_pod(self.control) is None:
            if time.monotonic() >= deadline:
                raise TimeoutError("no running controller pod after the reseed")
            time.sleep(wait_for(WaitKind.SAMPLE))
        wait_standby(self._kube(), self.control, SWAP_STANDBY_WAIT_S)

    def _record_controller(self) -> None:
        """The lane's registry names the controller now running, installed from this row's configuration."""
        env = self.environment
        pod = self.ops.controller_pod(self.control)
        state = PersistentState.load(env.state_path)
        record = state.controllers.get(env.namespace)
        if pod is None or record is None:
            raise WarmLaneRefusal("warm lane has no recorded controller to update")
        record.controller_pod_uid, record.controller_pod_name = pod.uid, pod.name
        record.runtime_config = runtime_config_payload(self.config)
        record.pending = None
        state.save(env.state_path)

    def adopt(self) -> str:
        revision = self._head()
        if AcceptedSource.of(self.environment).revision != revision:
            raise WarmLaneRefusal("accepted memory changed during restoration")
        return revision

    def prove(self, not_before: datetime, revision: str) -> dict[str, Any]:
        self.ops.set_maintenance(self.control, paused=False, generation=self.generation + "-proof")
        proof = self.admission.prove(self.control)
        # The existing admission requires all detector samples after its own
        # boundary (later than not_before). Require the controller to have
        # evaluated this exact revision, not merely mounted its Git checkout.
        state = self.ops.runtime_state(self.control)
        if state.get("setup_evaluated_revision") != revision:
            raise WarmLaneRefusal("controller has not evaluated the accepted revision")
        self._require_fresh_equivalent(state, not_before, revision)
        return proof

    def _require_fresh_equivalent(self, state: dict[str, Any], not_before: datetime, revision: str) -> None:
        """The resident install starts this row only from what a fresh install would hold at injection."""
        from benchmarks.sregym.fastloop.resident_start import (
            ResidentStart,
            ResidentStartMismatch,
            resident_start_violations,
        )
        from benchmarks.sregym.fastloop.setup_proof import digest

        env = self.environment
        names = kubectl(["get", "configmaps", "-o", "name"], namespace=self.control, kubeconfig=env.kubeconfig).stdout
        reference = env.run_dir / "fresh-onboarding" / f"{self.planned_commit or revision}.json"
        if not reference.is_file():
            # Memory does not enter the objects an onboarding names: the lane's latest fresh install is the reference.
            reference = env.run_dir / "fresh-onboarding" / "latest.json"
        violations = resident_start_violations(
            ResidentStart(
                state=state,
                planned_commit=self.planned_commit or revision,
                accepted_head=revision,
                boundary=not_before.isoformat().replace("+00:00", "Z"),
                control_configmaps=tuple(name.removeprefix("configmap/") for name in names.split()),
                # What the install held when the transaction began against what the row asks for. (Both were equal for
                # the transaction to begin; the record now names the row's own configuration, so only the earlier
                # reading can disagree with it.)
                config_digest=self._installed_digest,
                expected_config_digest=digest(identity_settings(runtime_config_payload(self.config))),
                onboarding_reference=json.loads(reference.read_text()) if reference.is_file() else None,
            )
        )
        if violations:
            raise ResidentStartMismatch(violations)

    def full_reset(self) -> None:
        if self.environment.shared_accepted_id:
            raise WarmLaneBusy("shared accepted memory cannot use destructive lane reset")
        if self._reseeding:
            # The reseed began (idle was proven when the transaction opened) and failed part way: the controller may be
            # stopped and its state gone. The row's planned memory is the only memory that may follow.
            if not self.planned_commit:
                raise WarmLaneBusy("a reseed failed with no planned memory; refusing to rewind accepted memory")
            self._full_reset()
            return
        self._assert_idle()
        if self.ops.controller_pod(self.control) is not None:
            pause_for_recovery(self.ops, self.control)
            sync_existing(native_install_config(self.config))
        elif (
            self.environment.namespace in PersistentState.load(self.environment.state_path).controllers
            and not self.planned_commit
        ):
            raise WarmLaneBusy("accepted memory has no paused live owner; refusing to rewind it")
        self._full_reset()
