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
        monkeypatch.setattr(warm_environment, "restart_observability", note("restart_observability"))
        monkeypatch.setattr(warm_environment, "await_observability", note("await_observability"))
        monkeypatch.setattr(warm_environment, "wait_standby", note("wait_standby"))
        monkeypatch.setattr(warm_environment, "install_existing", lambda native: self.installs.append(native))
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
    cluster.lane.reseed()
    names = cluster.events
    assert names.index("settle_exports") < names.index(f"reset_controller({HEAD}, preserve_repository=False)")
    assert names.index("stop_runtime") < names.index("reset_control_state") < names.index("delete_session_configmaps")
    assert names.index("delete_session_configmaps") < names.index("restart_observability")
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
def test_pause_puts_the_host_copy_at_the_planned_memory_or_syncs_what_the_lane_holds(cluster, monkeypatch, planned):
    import benchmarks.sregym.fastloop.setup_proof as setup_proof
    import benchmarks.sregym.fastloop.warm_scope as warm_scope

    calls: list[str] = []
    monkeypatch.setattr(warm_environment, "pause_for_recovery", lambda *a, **k: calls.append("pause"))
    monkeypatch.setattr(warm_environment, "sync_existing", lambda native: calls.append("sync_existing"))
    monkeypatch.setattr(
        warm_environment, "reseed_workspace", lambda workspace, commit: calls.append(f"reseed_workspace({commit})")
    )
    monkeypatch.setattr(setup_proof, "collect_fingerprint", lambda environment: None)
    monkeypatch.setattr(warm_scope, "check_cluster_scope", lambda *args: calls.append("scope"))
    cluster.lane._assert_idle = lambda: None  # type: ignore[method-assign]
    cluster.lane.planned_commit = planned
    cluster.lane.pause()
    # The application is restored from the host copy next, so it must already be the row's planned memory; a lane with
    # no planned memory keeps (and syncs) what it holds. Never both: the sync would overwrite the planned copy.
    assert calls == ["pause", f"reseed_workspace({planned})" if planned else "sync_existing", "scope"]


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
