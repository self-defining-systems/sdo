"""Kubernetes shell for the benchmark-owned warm-lane transaction."""

from __future__ import annotations

import json
import logging
import subprocess
import threading
import time
from contextlib import contextmanager
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
    incident_work_pending,
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
from benchmarks.sregym.fastloop.warm_lane import DeferredCheck, LaneIdentity, WarmLaneBusy, WarmLaneRefusal
from benchmarks.sregym.fastloop.wipe import (
    AttemptMarker,
    KubectlLaneCluster,
    LaneInjectionState,
    harness_leftovers,
    install_owned_objects,
    inventory_path,
    load_inventory,
    reseed_workspace,
)
from benchmarks.sregym.repository_git import host_git_command

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from datetime import datetime
    from pathlib import Path

    from benchmarks.sregym.adapter import ClusterOps, RuntimeConfig
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment
    from benchmarks.sregym.fastloop.reset import HealthWorker


logger = logging.getLogger(__name__)

#: Settings that belong to one row and are applied again by every reseed (the controller Job is installed anew from the
#: row's own configuration): this row's evidence run and the path of its config overlay. They are not install identity.
#: Settings that are a row's, not its install's: the reseed installs every row's controller from the row's own
#: configuration. ``healthy_baseline`` is on only for a row whose closing turn runs (the snapshots serve its learned-
#: detector validation), so it differs between the arms that share a lane; as an install setting it made every
#: cold/p2 row after a learning row fall back ("warm controller settings differ", fast-eks10).
PER_ROW_SETTINGS = frozenset({"evidence_run_id", "config_files", "healthy_baseline"})

#: The seconds each step of the transaction's phases took (``{phase: {step: seconds}}``), beside the warm receipt:
#: written after every step, so a phase that failed names the step it failed in and what came before.
STEP_SECONDS_FILE = "step-seconds.json"


#: The runtime's per-incident state store (an incident Job's ``<job>-tools-state``) carries no managed-by label; its
#: compare-and-swap annotation and data key identify it (the same pair Go's reset uses for the controller state).
STATE_REVISION_ANNOTATION = "sdo.dev/state-revision"
STATE_DATA_KEY = "runtime-state.json"


def _session_state(configmap: dict[str, Any]) -> bool:
    """Whether a control-namespace ConfigMap is an earlier row's incident, session, reflection or worker record: an
    SDO-managed ConfigMap that is none of the ones every install has (the fresh-install comparison names the rest),
    or an incident's own state store."""
    metadata = configmap.get("metadata") or {}
    name = str(metadata.get("name", ""))
    if name in INSTALL_CONFIGMAPS:
        return False
    incident_state = (
        name.startswith("sdo-")
        and STATE_REVISION_ANNOTATION in (metadata.get("annotations") or {})
        and STATE_DATA_KEY in (configmap.get("data") or {})
    )
    return (
        is_worker_record(configmap)
        or incident_state
        or (name.startswith("sdo-") and (metadata.get("labels") or {}).get("app.kubernetes.io/managed-by") == "sdo")
    )


#: The trailer Go ``install-existing`` puts on the commits with which it prepares the accepted tree for the install
#: (the deployment plan record, the generic detectors). A fresh install makes the same commits.
INSTALL_ORIGIN_TRAILER = "SDO-Origin: install-existing"


def install_prepared_head(workspace: Path, planned: str, head: str) -> bool:
    """Whether ``head`` is ``planned`` plus only the install's own tree preparation: ``planned`` is an ancestor and
    every commit after it carries the install-existing origin trailer."""
    if head == planned:
        return True

    def git(*args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            host_git_command(workspace, *args),
            capture_output=True,
            text=True,
            check=False,
            timeout=wait_for(WaitKind.COMMAND),
        )

    if git("merge-base", "--is-ancestor", planned, head).returncode != 0:
        return False
    listed = git("log", "--format=%H%x00%B%x1e", f"{planned}..{head}")
    if listed.returncode != 0:
        return False
    commits = [entry for entry in listed.stdout.split("\x1e") if entry.strip()]
    return bool(commits) and all(INSTALL_ORIGIN_TRAILER in entry.partition("\x00")[2].splitlines() for entry in commits)


def identity_settings(payload: dict[str, Any]) -> dict[str, Any]:
    """What must be the same for a lane's install to serve a row: the controller's settings but the per-row ones."""
    return {key: value for key, value in controller_settings(payload).items() if key not in PER_ROW_SETTINGS}


def planned_application_unchanged(environment: Any, planned_commit: str | None, *, live_content: bool = True) -> None:
    """A warm lane keeps its app only while it is the deploy of the planned source; otherwise fall back.

    ``live_content=False``: the previous row left its fault for this start's restore, which replaces the live objects
    from the baseline, so only their comparison with it is skipped."""
    from benchmarks.sregym.fastloop.deployed_application import deployed_application_change

    changed = deployed_application_change(environment, planned_commit or "HEAD", live_content=live_content)
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
    #: ``{phase: {step: seconds}}`` of this transaction (``STEP_SECONDS_FILE``).
    _step_seconds: dict[str, dict[str, float]] | None = None
    #: The restore and the reseed run concurrently and both time their steps into ``STEP_SECONDS_FILE``.
    _step_lock = threading.Lock()
    #: The restore's sustained-health window, running beside the reseed and the proof until ``join_sustained``.
    _sustained: DeferredCheck | None = None

    def __init__(
        self,
        environment: FastloopEnvironment,
        ops: ClusterOps,
        worker: HealthWorker,
        config: RuntimeConfig,
        directory: Path,
        *,
        incoming_data_faults: tuple[str, ...] | Callable[[], tuple[str, ...]],
        full_reset: Callable[[], None],
        planned_commit: str | None = None,
    ) -> None:
        self.environment, self.ops, self.worker = environment, ops, worker
        self.config, self.directory = replace(config, reuse_existing=True), directory
        # A callable is resolved when ``inspect`` has read the lane: the row's fault description may still be running
        # beside it, and only the refusal (never a write) needs its answer.
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

    @contextmanager
    def _step(self, phase: str, name: str) -> Iterator[None]:
        """Time one step of ``phase`` into ``STEP_SECONDS_FILE``, failed or not."""
        started = time.monotonic()
        try:
            yield
        finally:
            with self._step_lock:
                if self._step_seconds is None:
                    self._step_seconds = {}
                self._step_seconds.setdefault(phase, {})[name] = round(time.monotonic() - started, 3)
                self.directory.mkdir(parents=True, exist_ok=True)
                (self.directory / STEP_SECONDS_FILE).write_text(json.dumps(self._step_seconds, indent=2) + "\n")

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

    def _assert_idle(self, *, destructive: bool = False) -> None:
        """Refuse only what is real incident work of an earlier row (or what cannot be read).

        A controller's start-up hold also publishes ``busy_until``, with no incident and none able to open: a row that
        failed before injection leaves exactly that, and it is not incident work. A controller that has published no
        state has no incident either (opening one publishes first): it is no reason to keep the lane for the warm
        start (``WarmLaneRefusal``: fall back) and none to refuse replacing it (``destructive``: the full reset).
        """
        try:
            require_terminal_reflection(self.environment)
            state = self.ops.runtime_state(self.control)
            if incident_work_pending(state):
                raise WarmLaneBusy("warm lane has unclosed controller work")
            if not state and self.ops.controller_pod(self.control) is not None:
                # `runtime_state` answers {} for an unreadable ConfigMap too: only an absent one is "no state yet".
                absent = not kubectl(
                    ["get", "configmap", "sdo-controller-state", "--ignore-not-found", "-o", "name"],
                    namespace=self.control,
                    kubeconfig=self.environment.kubeconfig,
                ).stdout.strip()
                if not absent:
                    raise WarmLaneBusy("warm lane controller state could not be read")
                if not destructive:
                    raise WarmLaneRefusal("warm lane controller has published no state yet")
        except (WarmLaneBusy, WarmLaneRefusal):
            raise
        except Exception:
            raise WarmLaneBusy("prior native incident ownership could not be checked") from None

    def inspect(self) -> LaneIdentity:
        env = self.environment
        with self._step("inspect", "idle"):
            self._assert_idle()
        if env.shared_accepted_id:
            raise WarmLaneRefusal("warm transaction currently requires a private accepted repository")
        with self._step("inspect", "injection_state"):
            injected = LaneInjectionState(env.kubeconfig, env.namespace).reason()
        if injected is not None:
            raise WarmLaneRefusal(f"warm transaction cannot prove data-plane isolation: {injected.detail}")
        # The reset below restores the app and grades it afresh (``restore_application``'s baseline oracle) before the
        # reseed and admission: the proof a previous row may have left to this start.
        # A fault the previous row left (one the restore reverts) is reverted by that same reset.
        marker = AttemptMarker(env.run_dir)
        if (unclean := marker.pending(start_proves_baseline=True, start_reverts_fault=True)) is not None:
            raise WarmLaneRefusal(f"previous attempt did not finish cleanly: {unclean.detail}")
        with self._step("inspect", "planned_application"):
            planned_application_unchanged(env, self.planned_commit, live_content=not marker.owed_faults())
        with self._step("inspect", "controller_record"):
            record = PersistentState.load(env.state_path).controllers.get(env.namespace)
            pod = self.ops.controller_pod(self.control)
        if record is None or pod is None or record.controller_pod_uid != pod.uid:
            raise WarmLaneRefusal("warm lane has no recorded live controller")
        from benchmarks.sregym.fastloop.setup_proof import digest

        installed = identity_settings(record.runtime_config)
        if installed != identity_settings(runtime_config_payload(self.config)):
            raise WarmLaneRefusal("warm controller settings differ from the requested stage")
        self._installed_digest = digest(installed)
        with self._step("inspect", "lifecycle"):
            lifecycle = deployed_lifecycle(env.namespace).fingerprint
        if record.lifecycle_fingerprint != lifecycle:
            raise WarmLaneRefusal("warm controller topology differs from the requested stage")
        with self._step("inspect", "operator_inspect"):
            reusable = self.ops.install(self.config)
        if not reusable:
            raise WarmLaneRefusal("warm controller settings cannot be reused")
        with self._step("inspect", "standby"):
            standby = self._standby_uid()
        with self._step("inspect", "clean_scope"):
            self._clean_scope()
        with self._step("inspect", "head"):
            head = self._head()
        incoming = self.incoming_data_faults() if callable(self.incoming_data_faults) else self.incoming_data_faults
        if incoming:
            raise WarmLaneRefusal(f"warm transaction cannot prove data-plane isolation: {'; '.join(incoming)}")
        return LaneIdentity(pod.uid, standby, head)

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
            # The lane inventory predates the install, so the resident install's own namespace and RBAC are "new"
            # since it. They are the lane's, not residue: exempt exactly that set and nothing else.
            bindings = json.loads(
                kubectl(["get", "clusterrolebindings", "-o", "json"], namespace=None, kubeconfig=env.kubeconfig).stdout
                or "{}"
            ).get("items", [])
            leftovers = leftovers - install_owned_objects(self.control, bindings)
        if leftovers:
            raise WarmLaneRefusal("warm lane has resources outside its installed inventory")
        assert_clean_baseline(env.kubeconfig, namespace=env.namespace)

    def pause(self) -> None:
        with self._step("pause", "idle"):
            self._assert_idle()
        with self._step("pause", "maintenance_pause"):
            pause_for_recovery(self.ops, self.control)
        # Sync back the accepted checkout first, planned memory or not: the setup fingerprint below refuses a host copy
        # that differs from the memory the controller's volume holds ("installed accepted-memory commit differs from
        # accepted workspace"), which is exactly what moving the host copy to the planned commit makes of it.
        with self._step("pause", "sync_existing"):
            sync_existing(native_install_config(self.config))
        from benchmarks.sregym.fastloop.setup_proof import (
            APPLICATION_RECEIPTS,
            RECEIPTS,
            collect_application_fingerprint,
            collect_fingerprint,
        )
        from benchmarks.sregym.fastloop.warm_scope import check_application_scope, check_cluster_scope

        # The full fingerprint also proves the host copy holds the memory on the controller's volume.
        with self._step("pause", "fingerprint"):
            fingerprint = collect_fingerprint(self.environment)
        receipts = self.environment.run_dir / RECEIPTS
        with self._step("pause", "scope"):
            if (receipts / "latest.json").is_file():
                check_cluster_scope(receipts, fingerprint, self.environment.namespace)
            else:
                # A lane that never ran the no-fault setup rehearsal (every SREGym lane) proves its cluster scope
                # against the application setup it sealed after its last passing admission.
                check_application_scope(
                    self.environment.run_dir / APPLICATION_RECEIPTS,
                    collect_application_fingerprint(self.environment),
                    self.environment.namespace,
                )
        if self.planned_commit:
            # The row's own memory: the application below is restored from this source, and the reseed seeds the
            # controller's volume from it. The earlier row's memory on the volume is replaced there, not read here.
            with self._step("pause", "planned_workspace"):
                reseed_workspace(self.environment.workspace, self.planned_commit)

    def reset_changed(self, restored: Callable[[], None] | None = None) -> None:
        from benchmarks.sregym.fastloop.app_observability import (
            TRACE_STORE_MAX_AGE_SECONDS,
            restart_in_memory_trace_stores,
        )
        from benchmarks.sregym.fastloop.baseline_recovery import KubectlApplicationCluster, application_namespaces

        checks = RestoreChecks(self.environment)
        checks.validate()
        # An app trace store that grew while the lane idled goes before the reseed's install records the Pods; the
        # restore's health wait covers the replacement.
        env = self.environment
        restart_in_memory_trace_stores(
            env.kubeconfig,
            application_namespaces(env.baseline_path, env.kubeconfig, env.namespace),
            # The earlier row's end replaced it; a store that young has been Ready longer than the baseline check's
            # sustained-health window, while a fresh one would make that check wait the whole window.
            older_than=TRACE_STORE_MAX_AGE_SECONDS,
        )
        restore_application(
            self.environment,
            KubectlApplicationCluster(self.environment.kubeconfig),
            self.worker,
            checks=checks,
            restored=restored,
            sustained=self._deferred().start,
        )

    def _deferred(self) -> DeferredCheck:
        if self._sustained is None:
            self._sustained = DeferredCheck()
        return self._sustained

    def join_sustained(self) -> None:
        """Wait for the restore's sustained-health window and raise when the pods were not healthy across it."""
        self._deferred().join()

    def _kube(self) -> Kubectl:
        return Kubectl("", str(self.environment.kubeconfig))

    def reseed(self, application_restored: Callable[[], None] | None = None) -> None:
        """Swap the install's state for a fresh install's at the planned memory (see ``reseed.py`` for the order).

        The Job, namespace, volume, credentials, RBAC, transport and observability Deployments stay; the controller
        process does not: Go ``install-existing`` replaces the Job with this row's configuration and evidence run and
        seeds the volume from the host copy of the accepted repository, which the transaction put at the planned commit.
        ``application_restored`` (the transaction's join with the concurrent restore) runs right before that install.
        """
        self._reseeding = True
        seconds = reseed_lane(self._reseed_steps(application_restored or (lambda: None)))
        self.directory.mkdir(parents=True, exist_ok=True)
        (self.directory / "reseed-phases.json").write_text(json.dumps(seconds, indent=2) + "\n")

    def _reseed_steps(self, application_restored: Callable[[], object]) -> ReseedSteps:
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
            await_application=application_restored,
            install_controller=install_controller,
            wait_started=self._wait_started,
            record_controller=self._record_controller,
            await_observability=lambda: await_observability(kube, control),
        )

    def _retire_control_state(self) -> None:
        """Delete every record of the earlier row's controller: Go's control state (controller state, onboarding,
        maintenance, holds), then the incident, reflection and worker ConfigMaps a fresh install does not have."""
        env = self.environment
        # The same install restarts from memory: it keeps its established start-up period (a restart starts none).
        reset_control_state(self.control, kubeconfig=str(env.kubeconfig), keep_install_startup=True)
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
        with self._step("reseed.wait_started", "controller_running"):
            while self.ops.controller_pod(self.control) is None:
                if time.monotonic() >= deadline:
                    raise TimeoutError("no running controller pod after the reseed")
                time.sleep(wait_for(WaitKind.SAMPLE))
        # The controller creates the standby after its start-up (memory check, build): this is that time plus the
        # standby's own start.
        with self._step("reseed.wait_started", "standby_ready"):
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
        with self._step("prove", "maintenance_resume"):
            self.ops.set_maintenance(self.control, paused=False, generation=self.generation + "-proof")
        with self._step("prove", "admission"):
            proof = self.admission.prove(self.control)
        # The existing admission requires all detector samples after its own
        # boundary (later than not_before). Require the controller to have
        # evaluated this exact revision, not merely mounted its Git checkout.
        with self._step("prove", "runtime_state"):
            state = self.ops.runtime_state(self.control)
        if state.get("setup_evaluated_revision") != revision:
            raise WarmLaneRefusal("controller has not evaluated the accepted revision")
        with self._step("prove", "fresh_equivalence"):
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
        planned = self.planned_commit or revision
        if revision != planned and install_prepared_head(env.workspace, planned, revision):
            # Go install-existing prepares the planned tree for the install with its own commits (as a fresh install
            # does): the row's memory is the planned commit plus exactly that preparation.
            planned = revision
        violations = resident_start_violations(
            ResidentStart(
                state=state,
                planned_commit=planned,
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
        self._assert_idle(destructive=True)
        if self.ops.controller_pod(self.control) is not None:
            pause_for_recovery(self.ops, self.control)
            sync_existing(native_install_config(self.config))
        elif (
            self.environment.namespace in PersistentState.load(self.environment.state_path).controllers
            and not self.planned_commit
        ):
            raise WarmLaneBusy("accepted memory has no paused live owner; refusing to rewind it")
        self._full_reset()
