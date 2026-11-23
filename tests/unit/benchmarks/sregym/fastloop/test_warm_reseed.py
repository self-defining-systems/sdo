"""The Kubernetes shell of the in-place reseed: what each step asks of the cluster, and what a failure leaves."""

from __future__ import annotations

import json
from dataclasses import replace
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.sregym.adapter import PersistentState, RuntimeConfig, runtime_config_payload
from benchmarks.sregym.adapter.persistent import ControllerRecord
from benchmarks.sregym.fastloop import warm_environment
from benchmarks.sregym.fastloop.reseed import ReseedError
from benchmarks.sregym.fastloop.warm_environment import WarmEnvironment, _session_state, identity_settings
from benchmarks.sregym.fastloop.warm_lane import WarmLaneBusy

if TYPE_CHECKING:
    from pathlib import Path

HEAD = "b" * 40
PLANNED = "a" * 40


def config(**changes: Any) -> RuntimeConfig:
    base = RuntimeConfig(
        repository=__import__("pathlib").Path("/ws"),
        namespace="app",
        application="synthetic",
        controller_image="controller:1",
        responder_image="responder:1",
        repository_pvc="pvc",
        credentials_secret="reference-only",
        model="scripted",
        timeout_seconds=300,
        controller_namespace="app-sdo",
    )
    return replace(base, **changes)


def configmap(name: str, *, managed: bool = True, data: dict[str, str] | None = None) -> dict[str, Any]:
    labels = {"app.kubernetes.io/managed-by": "sdo"} if managed else {}
    return {"metadata": {"name": name, "labels": labels}, "data": data or {}}


def test_per_row_settings_are_not_install_identity_but_everything_else_is():
    from pathlib import Path

    first = runtime_config_payload(config(evidence_run_id="lane-row-1", config_files=(Path("/row1/overlay.yaml"),)))
    second = runtime_config_payload(config(evidence_run_id="lane-row-2", config_files=(Path("/row2/overlay.yaml"),)))
    assert identity_settings(first) == identity_settings(second)
    for other in (config(controller_image="controller:2"), config(model="other"), config(agent_effort="high")):
        assert identity_settings(runtime_config_payload(other)) != identity_settings(first)


@pytest.mark.parametrize(
    ("item", "stale"),
    [
        (configmap("sdo-config"), False),
        (configmap("sdo-controller-state"), False),
        (configmap("kube-root-ca.crt", managed=False), False),
        (configmap("sdo-app-1-abc-request"), True),
        (configmap("sdo-reflection-app-1-result"), True),
        (configmap("sdo-something-unlabelled", managed=False), False),
        (configmap("cache-of-a-worker", managed=False, data={"worker-record.json": "{}"}), True),
    ],
)
def test_only_an_earlier_rows_incident_session_and_worker_records_are_retired(item, stale, monkeypatch):
    monkeypatch.setattr(warm_environment, "is_worker_record", lambda value: "worker-record.json" in value["data"])
    assert _session_state(item) is stale


class Cluster:
    """A recording stand-in for every cluster call the reseed makes."""

    def __init__(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        self.events: list[str] = []
        self.configmaps = [configmap("sdo-config"), configmap("sdo-app-1-abc-request"), configmap("sdo-reflection-1")]
        self.deleted: list[str] = []
        self.installs: list[RuntimeConfig] = []
        self.pod = SimpleNamespace(uid="pod-new", name="controller-new")
        self.pod_ready = True
        kube = tmp_path / "kube"
        kube.write_text("")
        state_path = tmp_path / "controllers.json"
        record = ControllerRecord(
            application="synthetic",
            namespace="app",
            control_namespace="app-sdo",
            lifecycle_fingerprint="fp",
            controller_pod_uid="pod-old",
            controller_pod_name="controller-old",
            installed_by_stage="row-1",
            installed_at="2026-01-01T00:00:00Z",
            runtime_config=runtime_config_payload(config(evidence_run_id="row-1")),
        )
        state = PersistentState()
        state.controllers["app"] = record
        state.save(state_path)
        self.environment = SimpleNamespace(
            kubeconfig=kube,
            namespace="app",
            state_path=state_path,
            run_dir=tmp_path,
            workspace=tmp_path / "ws",
            shared_accepted_id="",
        )

        def kubectl(args, **kwargs):
            if args[:2] == ["get", "configmaps"]:
                return SimpleNamespace(stdout=json.dumps({"items": self.configmaps}))
            if args[0] == "delete":
                self.deleted.extend(args[2:-1])
                self.events.append("delete_session_configmaps")
            return SimpleNamespace(stdout="")

        def note(name):
            return lambda *a, **k: self.events.append(name)

        monkeypatch.setattr(warm_environment, "kubectl", kubectl)
        monkeypatch.setattr(warm_environment, "reset_control_state", note("reset_control_state"))
        monkeypatch.setattr(warm_environment, "stop_runtime", note("stop_runtime"))
        monkeypatch.setattr(warm_environment, "release_leader_lease", note("release_leader_lease"))
        monkeypatch.setattr(warm_environment, "restart_observability", note("restart_observability"))
        monkeypatch.setattr(warm_environment, "await_observability", note("await_observability"))
        monkeypatch.setattr(warm_environment, "wait_standby", note("wait_standby"))

        def install(native):
            self.events.append("install_existing")
            self.installs.append(native)

        monkeypatch.setattr(warm_environment, "install_existing", install)
        monkeypatch.setattr(warm_environment, "native_install_config", lambda value: value)
        monkeypatch.setattr(warm_environment.time, "sleep", lambda _s: None)

        class Reset:
            def __init__(inner, kubeconfig, control):
                inner.control = control

            def reset_controller(inner, head, *, preserve_repository):
                self.events.append(f"reset_controller({head}, preserve_repository={preserve_repository})")

        monkeypatch.setattr(warm_environment, "KubectlResetCluster", Reset)
        import benchmarks.sregym.fastloop.evidence_queue as queue
        import benchmarks.sregym.fastloop.evidence_sink as sink

        self.evidence_failures: list[str] = []
        monkeypatch.setattr(queue, "settle_lane_exports", lambda environment: self.events.append("settle_exports"))
        monkeypatch.setattr(sink, "settle_lane_evidence", lambda environment: self.evidence_failures)
        self.ops = SimpleNamespace(controller_pod=lambda control: self.pod if self.pod_ready else None)
        self.lane = object.__new__(WarmEnvironment)
        self.lane.environment, self.lane.ops, self.lane.control = self.environment, self.ops, "app-sdo"
        self.lane.config = replace(config(evidence_run_id="row-2"), reuse_existing=True)
        self.lane.directory = tmp_path / "warm"
        self.lane.planned_commit = PLANNED
        self.lane._head = lambda: HEAD  # type: ignore[method-assign]
        self.full_resets = 0
        self.lane._full_reset = lambda: setattr(self, "full_resets", self.full_resets + 1)


@pytest.fixture
def cluster(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Cluster:
    return Cluster(tmp_path, monkeypatch)


def test_reseed_stops_the_controller_at_the_head_it_holds_then_retires_state_then_installs_the_rows_config(cluster):
    cluster.lane.reseed(lambda: cluster.events.append("application_restored"))
    names = cluster.events
    assert names.index("settle_exports") < names.index(f"reset_controller({HEAD}, preserve_repository=False)")
    # The leader Lease is freed once no controller process is left, so the new controller does not wait it out.
    assert names.index("stop_runtime") < names.index("release_leader_lease") < names.index("reset_control_state")
    assert names.index("reset_control_state") < names.index("delete_session_configmaps")
    assert names.index("delete_session_configmaps") < names.index("restart_observability")
    # The old controller's stop and state retirement overlap the restore; the new one starts on the restored app.
    assert names.index("restart_observability") < names.index("application_restored")
    assert names.index("application_restored") < names.index("install_existing") < names.index("wait_standby")
    assert names.index("wait_standby") < names.index("await_observability")
    assert cluster.deleted == ["sdo-app-1-abc-request", "sdo-reflection-1"]
    # The Job is replaced from this row's own configuration: the Go install must not take the existing one.
    (installed,) = cluster.installs
    assert installed.reuse_existing is False
    assert installed.evidence_run_id == "row-2"
    assert json.loads((cluster.lane.directory / "reseed-phases.json").read_text())["install_controller"] >= 0


def test_reseed_records_the_new_controller_as_the_lanes_with_this_rows_configuration(cluster):
    cluster.lane.reseed()
    record = PersistentState.load(cluster.environment.state_path).controllers["app"]
    assert (record.controller_pod_uid, record.controller_pod_name) == ("pod-new", "controller-new")
    assert record.runtime_config["evidence_run_id"] == "row-2"
    assert record.pending is None


def test_evidence_that_is_not_copied_out_stops_the_reseed_before_anything_is_deleted(cluster):
    cluster.evidence_failures = ["row-1: copy failed"]
    with pytest.raises(ReseedError) as raised:
        cluster.lane.reseed()
    assert raised.value.reason == "settle_evidence:ValueError"
    assert not any(row.startswith(("reset_controller", "stop_runtime")) for row in cluster.events)


def test_a_controller_that_does_not_start_is_a_failed_reseed(cluster, monkeypatch):
    cluster.pod_ready = False
    ticks = iter(range(10_000))
    monkeypatch.setattr(warm_environment.time, "monotonic", lambda: float(next(ticks)) * 100.0)
    with pytest.raises(ReseedError) as raised:
        cluster.lane.reseed()
    assert raised.value.reason == "wait_started:TimeoutError"


def test_after_a_failed_reseed_the_fallback_does_not_call_the_stopped_lane_busy(cluster, monkeypatch):
    def not_idle():
        raise WarmLaneBusy("warm lane has unclosed or unknown controller work")

    cluster.lane._assert_idle = not_idle  # type: ignore[method-assign]
    cluster.evidence_failures = []
    cluster.pod_ready = False
    ticks = iter(range(10_000))
    monkeypatch.setattr(warm_environment.time, "monotonic", lambda: float(next(ticks)) * 100.0)
    with pytest.raises(ReseedError):
        cluster.lane.reseed()
    cluster.lane.full_reset()
    assert cluster.full_resets == 1


def test_a_failed_reseed_with_no_planned_memory_refuses_to_rewind_accepted_memory(cluster):
    cluster.lane._reseeding = True
    cluster.lane.planned_commit = None
    with pytest.raises(WarmLaneBusy):
        cluster.lane.full_reset()
    assert cluster.full_resets == 0


@pytest.mark.parametrize("planned", [PLANNED, None])
def test_pause_syncs_then_proves_the_setup_and_only_then_moves_the_host_copy_to_the_planned_memory(
    cluster, monkeypatch, planned
):
    """The setup fingerprint refuses a host copy that differs from the memory on the controller's volume. Moving the
    host copy to the planned commit before it made every second row fall back at pause with a bare ValueError."""
    import benchmarks.sregym.fastloop.setup_proof as setup_proof
    import benchmarks.sregym.fastloop.warm_scope as warm_scope

    memory = {"host": "learned-by-the-earlier-row", "volume": "learned-by-the-earlier-row"}
    calls: list[str] = []

    def sync(native) -> None:
        calls.append("sync_existing")
        memory["host"] = memory["volume"]

    def fingerprint(environment):
        calls.append("fingerprint")
        if memory["host"] != memory["volume"]:
            raise ValueError("eval: installed accepted-memory commit differs from accepted workspace")

    def move(workspace, commit) -> None:
        calls.append(f"reseed_workspace({commit})")
        memory["host"] = commit

    monkeypatch.setattr(warm_environment, "pause_for_recovery", lambda *a, **k: calls.append("pause"))
    monkeypatch.setattr(warm_environment, "sync_existing", sync)
    monkeypatch.setattr(warm_environment, "reseed_workspace", move)
    monkeypatch.setattr(setup_proof, "collect_fingerprint", fingerprint)
    monkeypatch.setattr(warm_scope, "check_cluster_scope", lambda *args: calls.append("scope"))
    monkeypatch.setattr(warm_scope, "check_application_scope", lambda *args: calls.append("scope"))
    monkeypatch.setattr(setup_proof, "collect_application_fingerprint", lambda environment: None)
    cluster.lane._assert_idle = lambda: None  # type: ignore[method-assign]
    cluster.lane.planned_commit = planned
    cluster.lane.pause()
    expected = ["pause", "sync_existing", "fingerprint", "scope"]
    assert calls == [*expected, f"reseed_workspace({planned})"] if planned else expected


def test_a_fresh_installs_onboarding_becomes_the_reference_a_reseeded_controller_is_held_to(tmp_path, monkeypatch):
    """The writer (a fresh install's admission) and the reader (the resident-start comparison) agree on one file."""
    from datetime import datetime, timezone

    from benchmarks.sregym.fastloop.admission import LaneAdmission

    base, learned = "c" * 40, PLANNED
    fresh_record = {
        "namespace": "app",
        "acceptedCommit": base,
        "establishedAt": "2026-01-01T00:00:00Z",
        "objects": [{"apiVersion": "apps/v1", "kind": "Deployment", "namespace": "app", "name": "web"}],
        "environmentObjects": [{"digest": "sha256:" + "1" * 64}],
    }
    admission = object.__new__(LaneAdmission)
    admission.environment = SimpleNamespace(run_dir=tmp_path)
    admission._record_fresh_onboarding(fresh_record)
    reference = json.loads((tmp_path / "fresh-onboarding" / f"{base}.json").read_text())
    assert reference == {"accepted_commit": base, "objects": ["apps/v1/Deployment/app/web"]}
    assert json.loads((tmp_path / "fresh-onboarding" / "latest.json").read_text()) == reference

    # A reseeded controller onboards on the learned memory: same objects, its own commit, install-bound digests.
    state = {
        "setup_evaluated_revision": learned,
        "startup": {"began_at": "2026-01-01T00:11:00Z", "established_at": "2026-01-01T00:13:00Z"},
        "history": [{"detector_id": "d", "evaluated_at": "2026-01-01T00:11:30Z"}],
        "namespace_onboarding": {
            "accepted_commit": learned,
            "objects": [{"api_version": "apps/v1", "kind": "Deployment", "namespace": "app", "name": "web"}],
            "environment_objects": [{"digest": "sha256:" + "2" * 64}],
        },
    }
    monkeypatch.setattr(warm_environment, "kubectl", lambda *a, **k: SimpleNamespace(stdout="configmap/sdo-config\n"))
    lane = object.__new__(WarmEnvironment)
    lane.environment = SimpleNamespace(
        kubeconfig=tmp_path / "kube", namespace="app", state_path=tmp_path, run_dir=tmp_path
    )
    lane.control, lane.config, lane.planned_commit = "app-sdo", config(), learned
    lane._installed_digest = warm_environment_digest(config())
    lane._require_fresh_equivalent(state, datetime(2026, 1, 1, 0, 10, tzinfo=timezone.utc), learned)


def warm_environment_digest(value: RuntimeConfig) -> str:
    from benchmarks.sregym.fastloop.setup_proof import digest

    return digest(identity_settings(runtime_config_payload(value)))


def test_inspect_names_why_the_previous_attempt_is_not_clean(cluster, tmp_path):
    from benchmarks.sregym.fastloop.warm_lane import WarmLaneRefusal
    from benchmarks.sregym.fastloop.wipe import UNRECOVERED_PREFIX, AttemptMarker, LaneInjectionState

    LaneInjectionState(cluster.environment.kubeconfig, "app").finish(["some_problem"], {"some_problem": []})
    cluster.lane._assert_idle = lambda: None  # type: ignore[method-assign]
    cluster.lane.incoming_data_faults = ()
    marker = AttemptMarker(cluster.environment.run_dir)
    marker.path.write_text(f"123\n{UNRECOVERED_PREFIX}some_problem\n")
    with pytest.raises(WarmLaneRefusal, match="not verified recovered: some_problem"):
        cluster.lane.inspect()


CONTROL = "app-sdo"
ROLE = "clusterrole.rbac.authorization.k8s.io"
BINDING = "clusterrolebinding.rbac.authorization.k8s.io"
INSTALL_OBJECTS = [
    "namespace/kube-system",
    "namespace/app",
    f"namespace/{CONTROL}",
    f"{ROLE}/sdo-admin-{CONTROL}",
    f"{ROLE}/sdo-prometheus-{CONTROL}",
    f"{BINDING}/sdo-admin-{CONTROL}",
    f"{BINDING}/sdo-prometheus-{CONTROL}",
]


def scope_cluster(cluster, monkeypatch, objects, baseline=("namespace/kube-system", "namespace/app")):
    from benchmarks.sregym.fastloop import wipe

    def kubectl(args, **kwargs):
        if args[:2] == ["get", "clusterrolebindings"]:
            subject = [{"kind": "ServiceAccount", "namespace": CONTROL, "name": "sdo"}]
            items = [
                {
                    "metadata": {"name": f"sdo-admin-{CONTROL}"},
                    "subjects": subject,
                    "roleRef": {"kind": "ClusterRole", "name": f"sdo-admin-{CONTROL}"},
                },
                {
                    "metadata": {"name": f"sdo-prometheus-{CONTROL}"},
                    "subjects": subject,
                    "roleRef": {"kind": "ClusterRole", "name": f"sdo-prometheus-{CONTROL}"},
                },
                {
                    "metadata": {"name": "fault-binding"},
                    "subjects": [{"kind": "User", "name": "x"}],
                    "roleRef": {"kind": "ClusterRole", "name": "fault-role"},
                },
            ]
            return SimpleNamespace(stdout=json.dumps({"items": items}))
        return SimpleNamespace(stdout="")

    monkeypatch.setattr(warm_environment, "kubectl", kubectl)
    monkeypatch.setattr(warm_environment, "load_inventory", lambda path: frozenset(baseline))
    monkeypatch.setattr(wipe.KubectlLaneCluster, "list_objects", lambda self: frozenset(objects))
    monkeypatch.setattr(warm_environment, "assert_clean_baseline", lambda *a, **k: None)
    cluster.environment.host_owned_kinds = ()


def test_the_resident_installs_own_namespace_and_rbac_are_not_foreign_to_the_warm_scope_check(cluster, monkeypatch):
    scope_cluster(cluster, monkeypatch, INSTALL_OBJECTS)
    cluster.lane._clean_scope()


@pytest.mark.parametrize(
    "foreign",
    [f"{ROLE}/fault-role", f"{BINDING}/fault-binding", "namespace/leftover", f"{ROLE}/sdo-admin-other-sdo"],
)
def test_anything_beyond_the_installs_own_objects_still_fails_the_warm_scope_check(cluster, monkeypatch, foreign):
    from benchmarks.sregym.fastloop.warm_lane import WarmLaneRefusal

    scope_cluster(cluster, monkeypatch, [*INSTALL_OBJECTS, foreign])
    with pytest.raises(WarmLaneRefusal, match="outside its installed inventory"):
        cluster.lane._clean_scope()


STARTUP_BOUND = "2026-11-23T03:00:00Z"
START_HOLD = {
    "busy_until": STARTUP_BOUND,
    "startup": {"began_at": "2026-11-23T02:30:00Z", "bound_at": STARTUP_BOUND},
}


def with_state(cluster, state):
    cluster.ops.runtime_state = lambda control: state


def test_a_controller_in_its_startup_hold_is_not_a_busy_lane(cluster):
    """A fresh controller publishes its start-up bound as ``busy_until`` although no incident exists or can open. A row
    that failed before injection leaves exactly that, and it must not refuse every later warm start for the hold."""
    with_state(cluster, START_HOLD)
    cluster.lane._assert_idle()
    cluster.lane._assert_idle(destructive=True)


@pytest.mark.parametrize(
    "state",
    [
        {**START_HOLD, "incident_open": True},
        {**START_HOLD, "pending_closure": {"incident_id": "app-1"}},
        {**START_HOLD, "queued_reflection_incident_ids": ["app-1"]},
        {"busy_until": "2026-11-23T02:40:00Z", "incident_open": True},
    ],
)
def test_real_incident_work_still_refuses_even_a_destructive_start(cluster, state):
    with_state(cluster, state)
    for destructive in (False, True):
        with pytest.raises(WarmLaneBusy):
            cluster.lane._assert_idle(destructive=destructive)


def test_a_controller_that_published_no_state_is_a_refusal_for_the_warm_start_and_free_to_replace(cluster):
    from benchmarks.sregym.fastloop.warm_lane import WarmLaneRefusal

    with_state(cluster, {})
    with pytest.raises(WarmLaneRefusal, match="no state"):
        cluster.lane._assert_idle()
    cluster.lane._assert_idle(destructive=True)  # nothing was published: no incident can be open


def test_a_state_that_cannot_be_read_is_still_busy(cluster, monkeypatch):
    def unreadable(args, **kwargs):
        raise RuntimeError("apiserver unreachable")

    with_state(cluster, {})
    monkeypatch.setattr(warm_environment, "kubectl", unreadable)
    for destructive in (False, True):
        with pytest.raises(WarmLaneBusy):
            cluster.lane._assert_idle(destructive=destructive)


def test_the_warm_restore_replaces_the_apps_trace_stores_before_restoring_and_before_the_reseed_install(
    cluster, monkeypatch
):
    import benchmarks.sregym.fastloop.app_observability as app_observability
    import benchmarks.sregym.fastloop.baseline_recovery as baseline_recovery

    events: list[str] = []
    cluster.environment.baseline_path = cluster.environment.run_dir / "baseline.json"
    monkeypatch.setattr(
        app_observability, "restart_in_memory_trace_stores", lambda kube, namespaces, **kw: events.append("restart")
    )
    monkeypatch.setattr(baseline_recovery, "KubectlApplicationCluster", lambda kube: None)
    monkeypatch.setattr(
        warm_environment,
        "RestoreChecks",
        lambda environment: SimpleNamespace(validate=lambda: events.append("validate")),
    )

    def restore(*_args, restored=None, **_kwargs):
        events.append("restore")
        restored()

    monkeypatch.setattr(warm_environment, "restore_application", restore)
    cluster.lane.worker = None
    cluster.lane.reset_changed(lambda: events.append("restored"))
    assert events == ["validate", "restart", "restore", "restored"]


def _application_fingerprint(**changes):
    from benchmarks.sregym.fastloop.setup_proof import ApplicationFingerprint

    base = {
        "lane": "synthetic",
        "code_revision": "a" * 40,
        "images": {"controller": "synthetic"},
        "app_manifests": "synthetic",
        "live_objects": {"<cluster>/ClusterRole/base": "baseline", "app/Service/app": "service-before"},
        "tools": {"fixture": "synthetic"},
    }
    return ApplicationFingerprint(**{**base, **changes})


@pytest.mark.parametrize("cluster_changed", [False, True])
def test_pause_proves_the_cluster_scope_from_the_application_setup_a_lane_seals_at_admission(
    cluster, monkeypatch, cluster_changed
):
    """SREGym lanes never run the no-fault setup rehearsal that writes ``setup-proof/``; every second row fell back
    at pause ("warm lane requires unchanged cluster configuration and a sealed passing setup") because the check read
    only that receipt. The application setup receipt each passing admission seals carries the same cluster scope."""
    import benchmarks.sregym.fastloop.setup_proof as setup_proof

    setup_proof.seal_application_receipt(
        cluster.environment.run_dir / setup_proof.APPLICATION_RECEIPTS, _application_fingerprint(), {"passed": True}
    )
    objects = {"<cluster>/ClusterRole/base": "changed" if cluster_changed else "baseline", "app/Service/app": "after"}
    monkeypatch.setattr(warm_environment, "pause_for_recovery", lambda *a, **k: None)
    monkeypatch.setattr(warm_environment, "sync_existing", lambda native: None)
    monkeypatch.setattr(warm_environment, "reseed_workspace", lambda workspace, commit: None)
    monkeypatch.setattr(setup_proof, "collect_fingerprint", lambda environment: None)
    monkeypatch.setattr(
        setup_proof,
        "collect_application_fingerprint",
        lambda environment: _application_fingerprint(live_objects=objects),
    )
    cluster.lane._assert_idle = lambda: None  # type: ignore[method-assign]
    cluster.lane.planned_commit = PLANNED
    if cluster_changed:
        with pytest.raises(ValueError, match="ClusterRole/base"):
            cluster.lane.pause()
    else:
        cluster.lane.pause()


def test_inspect_accepts_a_lane_whose_last_row_left_only_its_baseline_proof_to_this_start(cluster, monkeypatch):
    """The warm reset restores the app and proves its baseline before anything is injected: that is the proof the
    previous row left to this start, so its marker is no reason to fall back (an unrecovered row still is)."""
    from benchmarks.sregym.fastloop.wipe import AttemptMarker, LaneInjectionState

    class Reached(Exception):
        pass

    def planned_application_unchanged(*_args, **_kwargs):
        raise Reached

    LaneInjectionState(cluster.environment.kubeconfig, "app").finish(["some_problem"], {"some_problem": []})
    cluster.lane._assert_idle = lambda: None  # type: ignore[method-assign]
    cluster.lane.incoming_data_faults = ()
    monkeypatch.setattr(warm_environment, "planned_application_unchanged", planned_application_unchanged)
    marker = AttemptMarker(cluster.environment.run_dir)
    marker.begin()
    marker.end((), baseline_proof_owed=True)
    with pytest.raises(Reached):
        cluster.lane.inspect()


def _commit(repo: Path, message: str) -> str:
    import subprocess

    (repo / "f.txt").write_text(message)
    subprocess.run(["git", "-C", str(repo), "add", "f.txt"], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@invalid", "commit", "-qm", message],
        check=True,
    )
    return subprocess.run(["git", "-C", str(repo), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()


def test_the_installs_own_tree_preparation_on_top_of_the_planned_memory_is_the_planned_memory(tmp_path):
    """Go install-existing commits the deployment plan record and the generic detectors on top of the planned memory
    (trailer ``SDO-Origin: install-existing``), as a fresh install does; every reseeded row was refused with
    'accepted head ... is not the planned memory'. Any other commit above the planned memory still differs."""
    import subprocess

    from benchmarks.sregym.fastloop.warm_environment import install_prepared_head

    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    planned = _commit(tmp_path, "memory: planned")
    prepared = _commit(tmp_path, "sdo: record the deployment plan\n\nSDO-Actor: human\nSDO-Origin: install-existing")
    prepared = _commit(tmp_path, "sdo: prepare the generic detectors\n\nSDO-Origin: install-existing")
    assert install_prepared_head(tmp_path, planned, planned)
    assert install_prepared_head(tmp_path, planned, prepared)
    learned = _commit(tmp_path, "playbook: learned by an earlier row\n\nSDO-Origin: closing-turn")
    assert not install_prepared_head(tmp_path, planned, learned)
    assert not install_prepared_head(tmp_path, prepared, planned)  # not a descendant


def test_an_incidents_unlabelled_state_store_is_session_state_the_reseed_retires():
    """An incident Job's ``<job>-tools-state`` ConfigMap has no managed-by label; it survived the retire step and the
    resident-start comparison then refused the row ('session or reflection state remains: ...-tools-state')."""
    store = {
        "metadata": {
            "name": "sdo-app-1795415566658769303-05f348601e-tools-state",
            "annotations": {"sdo.dev/state-revision": "3"},
        },
        "data": {"runtime-state.json": "{}"},
    }
    assert warm_environment._session_state(store)
    install = {
        "metadata": {"name": "sdo-controller-state", "annotations": {"sdo.dev/state-revision": "9"}},
        "data": {"runtime-state.json": "{}"},
    }
    assert not warm_environment._session_state(install)
    assert not warm_environment._session_state(
        {"metadata": {"name": "user-config"}, "data": {"runtime-state.json": ""}}
    )


def test_each_transaction_step_is_timed_and_a_failed_step_still_names_where_it_failed(cluster):
    """The warm receipt times whole phases; the steps of each phase are timed beside it, written after every step, so a
    slow or failed phase names the step that took the time."""
    proof = {"passed": True}
    cluster.ops.set_maintenance = lambda control, **_kwargs: None
    cluster.ops.runtime_state = lambda control: {"setup_evaluated_revision": HEAD}
    cluster.lane.generation = "warm-synthetic"
    cluster.lane.admission = SimpleNamespace(prove=lambda control: proof)
    cluster.lane._require_fresh_equivalent = lambda state, not_before, revision: None  # type: ignore[method-assign]
    assert cluster.lane.prove(__import__("datetime").datetime.now(), HEAD) is proof
    cluster.lane.reseed()
    steps = json.loads((cluster.lane.directory / warm_environment.STEP_SECONDS_FILE).read_text())
    assert set(steps["prove"]) == {"maintenance_resume", "admission", "runtime_state", "fresh_equivalence"}
    assert set(steps["reseed.wait_started"]) == {"controller_running", "standby_ready"}

    def unreadable(control):
        raise RuntimeError("synthetic API failure")

    cluster.ops.runtime_state = unreadable
    with pytest.raises(RuntimeError):
        cluster.lane.prove(__import__("datetime").datetime.now(), HEAD)
    steps = json.loads((cluster.lane.directory / warm_environment.STEP_SECONDS_FILE).read_text())
    assert "runtime_state" in steps["prove"]
